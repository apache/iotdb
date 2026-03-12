/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.procedure.impl.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.enums.DataPartitionTableGeneratorState;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.DataPartitionTableIntegrityCheckProcedureState;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.mpp.rpc.thrift.TGenerateDataPartitionTableHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TGenerateDataPartitionTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TGenerateDataPartitionTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TGetEarliestTimeslotsResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Procedure for checking and restoring data partition table integrity. This procedure scans all
 * DataNodes to detect missing data partitions and restores the DataPartitionTable on the ConfigNode
 * Leader.
 */
public class DataPartitionTableIntegrityCheckProcedure
    extends StateMachineProcedure<
        ConfigNodeProcedureEnv, DataPartitionTableIntegrityCheckProcedureState> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataPartitionTableIntegrityCheckProcedure.class);

  private static final int MAX_RETRY_COUNT = 3;
  private static final long HEART_BEAT_REQUEST_RATE = 60000;

  NodeManager dataNodeManager;
  private List<TDataNodeConfiguration> allDataNodes = new ArrayList<>();

  // ============Need serialize BEGIN=============/
  /** Collected earliest timeslots from DataNodes: database -> earliest timeslot */
  private Map<String, Long> earliestTimeslots = new ConcurrentHashMap<>();

  /** DataPartitionTables collected from DataNodes: dataNodeId -> DataPartitionTable */
  private Map<Integer, DataPartitionTable> dataPartitionTables = new ConcurrentHashMap<>();

  private Set<String> lostDataPartitionsOfDatabases = new HashSet<>();

  /** Final merged DataPartitionTable */
  private DataPartitionTable finalDataPartitionTable;

  private static Set<TDataNodeConfiguration> skipDataNodes = new HashSet<>();
  private static Set<TDataNodeConfiguration> failedDataNodes = new HashSet<>();

  private static ScheduledExecutorService heartBeatExecutor;

  // ============Need serialize END=============/

  public DataPartitionTableIntegrityCheckProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DataPartitionTableIntegrityCheckProcedureState state)
      throws InterruptedException {
    try {
      // Ensure to get the real-time DataNodes in the current cluster at every step
      dataNodeManager = env.getConfigManager().getNodeManager();
      allDataNodes = dataNodeManager.getRegisteredDataNodes();

      switch (state) {
        case COLLECT_EARLIEST_TIMESLOTS:
          failedDataNodes = new HashSet<>();
          return collectEarliestTimeslots();
        case ANALYZE_MISSING_PARTITIONS:
          lostDataPartitionsOfDatabases = new HashSet<>();
          return analyzeMissingPartitions(env);
        case REQUEST_PARTITION_TABLES:
          heartBeatExecutor = Executors.newScheduledThreadPool(1);
          return requestPartitionTables(env);
        case MERGE_PARTITION_TABLES:
          return mergePartitionTables(env);
        case WRITE_PARTITION_TABLE_TO_RAFT:
          return writePartitionTableToRaft(env);
        default:
          throw new ProcedureException("Unknown state: " + state);
      }
    } catch (Exception e) {
      LOG.error("Error executing state {}: {}", state, e.getMessage(), e);
      setFailure("DataPartitionTableIntegrityCheckProcedure", e);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final DataPartitionTableIntegrityCheckProcedureState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case COLLECT_EARLIEST_TIMESLOTS:
      case ANALYZE_MISSING_PARTITIONS:
      case REQUEST_PARTITION_TABLES:
      case MERGE_PARTITION_TABLES:
      case WRITE_PARTITION_TABLE_TO_RAFT:
        // Cleanup resources
        earliestTimeslots.clear();
        dataPartitionTables.clear();
        allDataNodes.clear();
        finalDataPartitionTable = null;
        break;
      default:
        throw new ProcedureException("Unknown state for rollback: " + state);
    }
  }

  @Override
  protected DataPartitionTableIntegrityCheckProcedureState getState(final int stateId) {
    return DataPartitionTableIntegrityCheckProcedureState.values()[stateId];
  }

  @Override
  protected int getStateId(final DataPartitionTableIntegrityCheckProcedureState state) {
    return state.ordinal();
  }

  @Override
  protected DataPartitionTableIntegrityCheckProcedureState getInitialState() {
    skipDataNodes = new HashSet<>();
    failedDataNodes = new HashSet<>();
    return DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS;
  }

  /**
   * Collect earliest timeslot information from all DataNodes. Each DataNode returns a Map<String,
   * Long> where key is database name and value is the earliest timeslot id.
   */
  private Flow collectEarliestTimeslots() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Collecting earliest timeslots from all DataNodes...");
    }

    if (allDataNodes.isEmpty()) {
      LOG.error(
          "No DataNodes registered, no way to collect earliest timeslots, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    // Collect earliest timeslots from all DataNodes
    allDataNodes.removeAll(skipDataNodes);
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      try {
        TGetEarliestTimeslotsResp resp =
            (TGetEarliestTimeslotsResp)
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithGivenRetry(
                        dataNode.getLocation().getInternalEndPoint(),
                        null,
                        CnToDnSyncRequestType.COLLECT_EARLIEST_TIMESLOTS,
                        MAX_RETRY_COUNT);
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failedDataNodes.add(dataNode);
          LOG.error(
              "Failed to collected earliest timeslots from the DataNode[id={}], response status is {}",
              dataNode.getLocation().getDataNodeId(),
              resp.getStatus());
          continue;
        }

        Map<String, Long> nodeTimeslots = resp.getDatabaseToEarliestTimeslot();

        // Merge with existing timeslots (take minimum)
        for (Map.Entry<String, Long> entry : nodeTimeslots.entrySet()) {
          earliestTimeslots.merge(entry.getKey(), entry.getValue(), Math::min);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Collected earliest timeslots from the DataNode[id={}]: {}",
              dataNode.getLocation().getDataNodeId(),
              nodeTimeslots);
        }
      } catch (Exception e) {
        LOG.error(
            "Failed to collect earliest timeslots from the DataNode[id={}]: {}",
            dataNode.getLocation().getDataNodeId(),
            e.getMessage(),
            e);
        failedDataNodes.add(dataNode);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.info(
          "Collected earliest timeslots from {} DataNodes: {}, the number of successful DataNodes is {}",
          allDataNodes.size(),
          earliestTimeslots,
          allDataNodes.size() - failedDataNodes.size());
    }

    if (failedDataNodes.size() == allDataNodes.size()
        && new HashSet<>(allDataNodes).containsAll(failedDataNodes)) {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
    } else {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.ANALYZE_MISSING_PARTITIONS);
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Analyze which data partitions are missing based on earliest timeslots. Identify data partitions
   * of databases need to be repaired.
   */
  private Flow analyzeMissingPartitions(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Analyzing missing data partitions...");
    }

    if (earliestTimeslots.isEmpty()) {
      LOG.error(
          "No missing data partitions detected, nothing needs to be repaired, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    // Find all databases that have lost data partition tables
    for (Map.Entry<String, Long> entry : earliestTimeslots.entrySet()) {
      String database = entry.getKey();
      long earliestTimeslot = entry.getValue();

      // Get current DataPartitionTable from ConfigManager
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          localDataPartitionTable = getLocalDataPartitionTable(env, database);

      // Check if ConfigNode has a data partition that is associated with the earliestTimeslot
      if (localDataPartitionTable == null
          || localDataPartitionTable.isEmpty()
          || localDataPartitionTable.get(database) == null
          || localDataPartitionTable.get(database).isEmpty()) {
        lostDataPartitionsOfDatabases.add(database);
        LOG.warn(
            "No data partition table related to database {} was found from the ConfigNode, and this issue needs to be repaired",
            database);
        continue;
      }

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionMap = localDataPartitionTable.get(database);
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionEntry : seriesPartitionMap.entrySet()) {
        Map<TTimePartitionSlot, List<TConsensusGroupId>> tTimePartitionSlotListMap =
            seriesPartitionEntry.getValue();
        tTimePartitionSlotListMap
            .keySet()
            .forEach(
                slot -> {
                  if (!TimePartitionUtils.satisfyPartitionId(
                      slot.getStartTime(), earliestTimeslot)) {
                    lostDataPartitionsOfDatabases.add(database);
                    LOG.warn(
                        "Database {} has lost timeslot {} in its data table partition, and this issue needs to be repaired",
                        database,
                        earliestTimeslot);
                  }
                });
      }
    }

    if (lostDataPartitionsOfDatabases.isEmpty()) {
      LOG.info("No databases have lost data partitions, terminating procedure");
      return Flow.NO_MORE_STATE;
    }

    LOG.info(
        "Identified {} databases have lost data partitions, will request DataPartitionTable generation from {} DataNodes",
        lostDataPartitionsOfDatabases.size(),
        allDataNodes.size() - failedDataNodes.size());
    setNextState(DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES);
    return Flow.HAS_MORE_STATE;
  }

  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
      getLocalDataPartitionTable(final ConfigNodeProcedureEnv env, final String database) {
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        env.getConfigManager()
            .getSchemaPartition(Collections.singletonMap(database, Collections.emptyList()))
            .getSchemaPartitionTable();

    // Construct request for getting data partition
    final Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    schemaPartitionTable.forEach(
        (key, value) -> {
          Map<TSeriesPartitionSlot, TTimeSlotList> slotListMap = new HashMap<>();
          value
              .keySet()
              .forEach(
                  slot ->
                      slotListMap.put(
                          slot, new TTimeSlotList(Collections.emptyList(), true, true)));
          partitionSlotsMap.put(key, slotListMap);
        });
    final GetDataPartitionPlan getDataPartitionPlan = new GetDataPartitionPlan(partitionSlotsMap);
    return env.getConfigManager().getDataPartition(getDataPartitionPlan).getDataPartitionTable();
  }

  /**
   * Request DataPartitionTable generation from target DataNodes. Each DataNode scans its tsfile
   * resources and generates a DataPartitionTable.
   */
  private Flow requestPartitionTables(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Requesting DataPartitionTable generation from {} DataNodes...", allDataNodes.size());
    }

    if (allDataNodes.isEmpty()) {
      LOG.error(
          "No DataNodes registered, no way to requested DataPartitionTable generation, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    ScheduledExecutorUtil.safelyScheduleAtFixedRate(heartBeatExecutor, this::checkPartitionTableGenerationStatus,
            0,
            HEART_BEAT_REQUEST_RATE,
            TimeUnit.MILLISECONDS);

    allDataNodes.removeAll(skipDataNodes);
    allDataNodes.removeAll(failedDataNodes);
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();
      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableReq req = new TGenerateDataPartitionTableReq();
          req.setDatabases(lostDataPartitionsOfDatabases);
          TGenerateDataPartitionTableResp resp =
              (TGenerateDataPartitionTableResp)
                  SyncDataNodeClientPool.getInstance()
                      .sendSyncRequestToDataNodeWithGivenRetry(
                          dataNode.getLocation().getInternalEndPoint(),
                          req,
                          CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE,
                          MAX_RETRY_COUNT);
          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedDataNodes.add(dataNode);
            LOG.error(
                "Failed to request DataPartitionTable generation from the DataNode[id={}], response status is {}",
                dataNode.getLocation().getDataNodeId(),
                resp.getStatus());
            continue;
          }

          byte[] bytes = resp.getDataPartitionTable();
          DataPartitionTable dataPartitionTable = new DataPartitionTable();
          dataPartitionTable.deserialize(ByteBuffer.wrap(bytes));
          dataPartitionTables.put(dataNodeId, dataPartitionTable);
        } catch (Exception e) {
          failedDataNodes.add(dataNode);
          LOG.error(
              "Failed to request DataPartitionTable generation from DataNode[id={}]: {}",
              dataNodeId,
              e.getMessage(),
              e);
        }
      }
    }

    if (failedDataNodes.size() == allDataNodes.size()
        && new HashSet<>(allDataNodes).containsAll(failedDataNodes)) {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    setNextState(DataPartitionTableIntegrityCheckProcedureState.MERGE_PARTITION_TABLES);
    return Flow.HAS_MORE_STATE;
  }

  /** Check completion status of DataPartitionTable generation tasks. */
  private void checkPartitionTableGenerationStatus() {
    if (LOG.isDebugEnabled()) {
      LOG.info("Checking DataPartitionTable generation completion status...");
    }

    int completeCount = 0;
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();

      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableHeartbeatResp resp =
              (TGenerateDataPartitionTableHeartbeatResp)
                  SyncDataNodeClientPool.getInstance()
                      .sendSyncRequestToDataNodeWithGivenRetry(
                          dataNode.getLocation().getInternalEndPoint(),
                          null,
                          CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE_HEART_BEAT,
                          MAX_RETRY_COUNT);
          DataPartitionTableGeneratorState state =
              DataPartitionTableGeneratorState.getStateByCode(resp.getErrorCode());

          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOG.error(
                "Failed to request DataPartitionTable generation heart beat from the DataNode[id={}], state is {}, response status is {}",
                dataNode.getLocation().getDataNodeId(),
                state,
                resp.getStatus());
            continue;
          }
          switch (state) {
            case SUCCESS:
              LOG.info(
                  "DataNode {} completed DataPartitionTable generation, terminating heart beat",
                  dataNodeId);
              completeCount++;
              break;
            case IN_PROGRESS:
              LOG.info("DataNode {} still generating DataPartitionTable", dataNodeId);
              break;
            case FAILED:
              LOG.error(
                  "DataNode {} failed to generate DataPartitionTable, terminating heart beat",
                  dataNodeId);
              completeCount++;
              break;
            default:
              LOG.error(
                  "DataNode {} returned unknown error code: {}", dataNodeId, resp.getErrorCode());
              break;
          }
        } catch (Exception e) {
          LOG.error(
              "Error checking DataPartitionTable status from DataNode {}: {}, terminating heart beat",
              dataNodeId,
              e.getMessage(),
              e);
          completeCount++;
        }
      } else {
        completeCount++;
      }
    }

    if (completeCount >= allDataNodes.size()) {
      heartBeatExecutor.shutdown();
    }
  }

  /** Merge DataPartitionTables from all DataNodes into a final table. */
  private Flow mergePartitionTables(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Merging DataPartitionTables from {} DataNodes...", dataPartitionTables.size());
    }

    if (dataPartitionTables.isEmpty()) {
      LOG.error("No DataPartitionTables to merge, dataPartitionTables is empty");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

     Map<TSeriesPartitionSlot, SeriesPartitionTable> finalDataPartitionMap = new HashMap<>();

        for (String database : lostDataPartitionsOfDatabases) {
          // Get current DataPartitionTable from ConfigManager
          Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
              localDataPartitionTableMap = getLocalDataPartitionTable(env, database);

          // Check if ConfigNode has a data partition that is associated with the earliestTimeslot
          if (localDataPartitionTableMap == null
              || localDataPartitionTableMap.isEmpty()
              || localDataPartitionTableMap.get(database) == null
              || localDataPartitionTableMap.get(database).isEmpty()) {
            LOG.warn(
                "No data partition table related to database {} was found from the ConfigNode, use data partition table of DataNode directly",
                database);
            continue;
          }

          localDataPartitionTableMap
              .values()
              .forEach(
                  map ->
                      map.forEach(
                          (tSeriesPartitionSlot, seriesPartitionTableMap) -> {
                            if (tSeriesPartitionSlot == null
                                || seriesPartitionTableMap == null
                                || seriesPartitionTableMap.isEmpty()) {
                              return;
                            }
                            finalDataPartitionMap.computeIfAbsent(
                                tSeriesPartitionSlot,
                                k -> new SeriesPartitionTable(seriesPartitionTableMap));
                          }));
        }

        if (finalDataPartitionMap.isEmpty()) {
          dataPartitionTables
                  .values()
                  .forEach(
                          dataPartitionTable -> {
                            if (dataPartitionTable == null
                                    || dataPartitionTable.getDataPartitionMap() == null
                                    || dataPartitionTable.getDataPartitionMap().isEmpty()) {
                              return;
                            }
                            dataPartitionTable
                                    .getDataPartitionMap().forEach(
                                            (dnSeriesPartitionSlot, dnSeriesPartitionTable) -> {
                                              if (dnSeriesPartitionSlot == null
                                                      || dnSeriesPartitionTable == null) {
                                                return;
                                              }
                                              finalDataPartitionMap.computeIfAbsent(
                                                      dnSeriesPartitionSlot,
                                                      k -> dnSeriesPartitionTable);
                                            });
                          });
        } else {
          finalDataPartitionMap.forEach(
                  (tSeriesPartitionSlot, seriesPartitionTable) -> {
                    dataPartitionTables
                            .values()
                            .forEach(
                                    dataPartitionTable -> {
                                      if (dataPartitionTable == null
                                              || dataPartitionTable.getDataPartitionMap() == null
                                              || dataPartitionTable.getDataPartitionMap().isEmpty()) {
                                        return;
                                      }
                                      dataPartitionTable
                                              .getDataPartitionMap()
                                              .forEach(
                                                      (dnSeriesPartitionSlot, dnSeriesPartitionTable) -> {
                                                        if (!tSeriesPartitionSlot.equals(dnSeriesPartitionSlot)) {
                                                          return;
                                                        }

                                                        if (seriesPartitionTable == null
                                                                || seriesPartitionTable.getSeriesPartitionMap() == null
                                                                || seriesPartitionTable.getSeriesPartitionMap().isEmpty()) {
                                                          finalDataPartitionMap.put(
                                                                  tSeriesPartitionSlot, dnSeriesPartitionTable);
                                                        }

                                                        // dnDataPartitionTable merged to seriesPartitionTable
                                                        dnSeriesPartitionTable
                                                                .getSeriesPartitionMap()
                                                                .forEach(
                                                                        (k, v) ->
                                                                                v.forEach(
                                                                                        tConsensusGroupId -> {
                                                                                          if (seriesPartitionTable == null) {
                                                                                            return;
                                                                                          }
                                                                                          seriesPartitionTable.putDataPartition(
                                                                                                  k, tConsensusGroupId);
                                                                                        }));
                                                      });
                                    });
                  });
        }

        finalDataPartitionTable = new DataPartitionTable(finalDataPartitionMap);

    LOG.info("DataPartitionTable merge completed successfully");
    setNextState(DataPartitionTableIntegrityCheckProcedureState.WRITE_PARTITION_TABLE_TO_RAFT);
    return Flow.HAS_MORE_STATE;
  }

  /** Write the final DataPartitionTable to raft log. */
  private Flow writePartitionTableToRaft(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Writing DataPartitionTable to raft log...");
    }

    if (lostDataPartitionsOfDatabases.isEmpty()) {
      LOG.error("No database lost data partition table");
      setFailure(
          "DataPartitionTableIntegrityCheckProcedure",
          new ProcedureException("No database lost data partition table for raft write"));
      return getFlow();
    }

    if (finalDataPartitionTable == null) {
      LOG.error("No DataPartitionTable to write to raft");
      setFailure(
          "DataPartitionTableIntegrityCheckProcedure",
          new ProcedureException("No DataPartitionTable available for raft write"));
      return getFlow();
    }

    int failedCnt = 0;
    while (failedCnt < MAX_RETRY_COUNT) {
      try {
        CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
        Map<String, DataPartitionTable> assignedDataPartition = new HashMap<>();
        assignedDataPartition.put(
            lostDataPartitionsOfDatabases.stream().findFirst().get(), finalDataPartitionTable);
        createPlan.setAssignedDataPartition(assignedDataPartition);
        TSStatus tsStatus = env.getConfigManager().getConsensusManager().write(createPlan);

        if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOG.info("DataPartitionTable successfully written to raft log");
          break;
        } else {
          LOG.error("Failed to write DataPartitionTable to raft log");
          setFailure(
              "DataPartitionTableIntegrityCheckProcedure",
              new ProcedureException("Failed to write DataPartitionTable to raft log"));
        }
      } catch (Exception e) {
        LOG.error("Error writing DataPartitionTable to raft log", e);
        setFailure("DataPartitionTableIntegrityCheckProcedure", e);
      }
      failedCnt++;
    }

    return getFlow();
  }

  private Flow getFlow() {
    if (!failedDataNodes.isEmpty()) {
      allDataNodes.removeAll(failedDataNodes);
      skipDataNodes = new HashSet<>(allDataNodes);
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    } else {
      skipDataNodes.clear();
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);

    // Serialize earliestTimeslots
    stream.writeInt(earliestTimeslots.size());
    for (Map.Entry<String, Long> entry : earliestTimeslots.entrySet()) {
      stream.writeUTF(entry.getKey());
      stream.writeLong(entry.getValue());
    }

    // Serialize dataPartitionTables count
    stream.writeInt(dataPartitionTables.size());
    for (Map.Entry<Integer, DataPartitionTable> entry : dataPartitionTables.entrySet()) {
      stream.writeInt(entry.getKey());
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        TTransport transport = new TIOStreamTransport(oos);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        entry.getValue().serialize(oos, protocol);

        // Write the size and data for byte array after serialize
        byte[] data = baos.toByteArray();
        stream.writeInt(data.length);
        stream.write(data);
      } catch (IOException | TException e) {
        LOG.error("{} serialize failed", this.getClass().getSimpleName(), e);
        throw new IOException("Failed to serialize dataPartitionTables", e);
      }
    }

    stream.writeInt(lostDataPartitionsOfDatabases.size());
    for (String database : lostDataPartitionsOfDatabases) {
      stream.writeUTF(database);
    }

    if (finalDataPartitionTable != null) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        TTransport transport = new TIOStreamTransport(oos);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        finalDataPartitionTable.serialize(oos, protocol);

        // Write the size and data for byte array after serialize
        byte[] data = baos.toByteArray();
        stream.writeInt(data.length);
        stream.write(data);
      } catch (IOException | TException e) {
        LOG.error("{} serialize failed", this.getClass().getSimpleName(), e);
        throw new IOException("Failed to serialize finalDataPartitionTable", e);
      }
    } else {
      stream.writeInt(0);
    }

    stream.writeInt(skipDataNodes.size());
    for (TDataNodeConfiguration skipDataNode : skipDataNodes) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        TTransport transport = new TIOStreamTransport(baos);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        skipDataNode.write(protocol);

        byte[] data = baos.toByteArray();
        stream.writeInt(data.length);
        stream.write(data);
      } catch (TException e) {
        LOG.error("Failed to serialize skipDataNode", e);
        throw new IOException("Failed to serialize skipDataNode", e);
      }
    }

    stream.writeInt(failedDataNodes.size());
    for (TDataNodeConfiguration failedDataNode : failedDataNodes) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        TTransport transport = new TIOStreamTransport(baos);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        failedDataNode.write(protocol);

        byte[] data = baos.toByteArray();
        stream.writeInt(data.length);
        stream.write(data);
      } catch (TException e) {
        LOG.error("Failed to serialize failedDataNode", e);
        throw new IOException("Failed to serialize failedDataNode", e);
      }
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    // Deserialize earliestTimeslots
    int earliestTimeslotsSize = byteBuffer.getInt();
    earliestTimeslots = new ConcurrentHashMap<>();
    for (int i = 0; i < earliestTimeslotsSize; i++) {
      String database = String.valueOf(byteBuffer.getChar());
      long timeslot = byteBuffer.getLong();
      earliestTimeslots.put(database, timeslot);
    }

    // Deserialize dataPartitionTables count
    int dataPartitionTablesSize = byteBuffer.getInt();
    dataPartitionTables = new HashMap<>();
    for (int i = 0; i < dataPartitionTablesSize; i++) {
      int key = byteBuffer.getInt();
      int size = byteBuffer.getInt();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = new ObjectInputStream(bais)) {
        TTransport transport = new TIOStreamTransport(ois);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        // Deserialize by input stream and protocol
        DataPartitionTable value = new DataPartitionTable();
        value.deserialize(ois, protocol);
        dataPartitionTables.put(key, value);
      } catch (IOException | TException e) {
        LOG.error("{} deserialize failed", this.getClass().getSimpleName(), e);
        throw new RuntimeException(e);
      }
    }

    int lostDataPartitionsOfDatabasesSize = byteBuffer.getInt();
    for (int i = 0; i < lostDataPartitionsOfDatabasesSize; i++) {
      String database = ReadWriteIOUtils.readString(byteBuffer);
      lostDataPartitionsOfDatabases.add(database);
    }

    // Deserialize finalDataPartitionTable size
    int finalDataPartitionTableSize = byteBuffer.getInt();
    if (finalDataPartitionTableSize > 0) {
      byte[] finalDataPartitionTableBytes = new byte[finalDataPartitionTableSize];
      byteBuffer.get(finalDataPartitionTableBytes);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(finalDataPartitionTableBytes);
          ObjectInputStream ois = new ObjectInputStream(bais)) {
        TTransport transport = new TIOStreamTransport(ois);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        // Deserialize by input stream and protocol
        finalDataPartitionTable = new DataPartitionTable();
        finalDataPartitionTable.deserialize(ois, protocol);
      } catch (IOException | TException e) {
        LOG.error("{} deserialize failed", this.getClass().getSimpleName(), e);
        throw new RuntimeException(e);
      }
    } else {
      finalDataPartitionTable = null;
    }

    skipDataNodes = new HashSet<>();
    int skipDataNodesSize = byteBuffer.getInt();
    for (int i = 0; i < skipDataNodesSize; i++) {
      int size = byteBuffer.getInt();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
        TTransport transport = new TIOStreamTransport(bais);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        TDataNodeConfiguration dataNode = new TDataNodeConfiguration();
        dataNode.read(protocol);
        skipDataNodes.add(dataNode);
      } catch (TException | IOException e) {
        LOG.error("Failed to deserialize skipDataNode", e);
        throw new RuntimeException(e);
      }
    }

    failedDataNodes = new HashSet<>();
    int failedDataNodesSize = byteBuffer.getInt();
    for (int i = 0; i < failedDataNodesSize; i++) {
      int size = byteBuffer.getInt();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
        TTransport transport = new TIOStreamTransport(bais);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        TDataNodeConfiguration dataNode = new TDataNodeConfiguration();
        dataNode.read(protocol);
        failedDataNodes.add(dataNode);
      } catch (TException | IOException e) {
        LOG.error("Failed to deserialize failedDataNode", e);
        throw new RuntimeException(e);
      }
    }
  }
}
