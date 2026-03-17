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
import org.apache.iotdb.commons.enums.DataPartitionTableGeneratorState;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.DatabaseScopedDataPartitionTable;
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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
  private static final long HEART_BEAT_REQUEST_RATE = 10000;

  NodeManager dataNodeManager;
  private List<TDataNodeConfiguration> allDataNodes = new ArrayList<>();

  // ============Need serialize BEGIN=============/
  /** Collected earliest timeslots from DataNodes: database -> earliest timeslot */
  private Map<String, Long> earliestTimeslots = new ConcurrentHashMap<>();

  /** DataPartitionTables collected from DataNodes: dataNodeId -> DataPartitionTable */
  private Map<Integer, List<DatabaseScopedDataPartitionTable>> dataPartitionTables = new ConcurrentHashMap<>();

  private Set<String> lostDataPartitionsOfDatabases = new HashSet<>();

  /** Final merged DataPartitionTable */
  private Map<String, DataPartitionTable> finalDataPartitionTables;

  private static Set<TDataNodeConfiguration> skipDataNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private static Set<TDataNodeConfiguration> failedDataNodes =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
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
          return requestPartitionTables();
        case REQUEST_PARTITION_TABLES_HEART_BEAT:
          return requestPartitionTablesHeartBeat();
        case MERGE_PARTITION_TABLES:
          finalDataPartitionTables = new HashMap<>();
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
    // Cleanup resources
    switch (state) {
      case COLLECT_EARLIEST_TIMESLOTS:
        earliestTimeslots.clear();
        break;
      case ANALYZE_MISSING_PARTITIONS:
        lostDataPartitionsOfDatabases.clear();
        break;
      case REQUEST_PARTITION_TABLES:
      case REQUEST_PARTITION_TABLES_HEART_BEAT:
        dataPartitionTables.clear();
        break;
      case MERGE_PARTITION_TABLES:
        finalDataPartitionTables.clear();
        break;
      default:
        allDataNodes.clear();
        earliestTimeslots.clear();
        dataPartitionTables.clear();
        finalDataPartitionTables.clear();
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
          "No DataNodes registered, no way to collect earliest timeslots, waiting for them to go up");
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

    if (failedDataNodes.size() == allDataNodes.size()) {
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
      LOG.warn(
          "No missing data partitions detected, nothing needs to be repaired, terminating procedure");
      return Flow.NO_MORE_STATE;
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

        if (tTimePartitionSlotListMap.isEmpty()) {
          continue;
        }

        TTimePartitionSlot localEarliestSlot = tTimePartitionSlotListMap.keySet()
                .stream()
                .min(Comparator.comparingLong(TTimePartitionSlot::getStartTime))
                .orElse(null);

        if (localEarliestSlot.getStartTime() > TimePartitionUtils.getTimeByPartitionId(earliestTimeslot)) {
          lostDataPartitionsOfDatabases.add(database);
          LOG.warn(
                  "Database {} has lost timeslot {} in its data table partition, and this issue needs to be repaired",
                  database,
                  earliestTimeslot);
        }
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
  private Flow requestPartitionTables() {
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
          }
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

    setNextState(DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES_HEART_BEAT);
    return Flow.HAS_MORE_STATE;
  }

  private Flow requestPartitionTablesHeartBeat() {
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
              List<ByteBuffer> byteBufferList = resp.getDatabaseScopedDataPartitionTables();
              List<DatabaseScopedDataPartitionTable> databaseScopedDataPartitionTableList = deserializeDatabaseScopedTableList(byteBufferList);
              dataPartitionTables.put(dataNodeId, databaseScopedDataPartitionTableList);
              LOG.info(
                      "DataNode {} completed DataPartitionTable generation, terminating heart beat",
                      dataNodeId);
              completeCount++;
              break;
            case IN_PROGRESS:
              LOG.info("DataNode {} still generating DataPartitionTable", dataNodeId);
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
      setNextState(DataPartitionTableIntegrityCheckProcedureState.MERGE_PARTITION_TABLES);
      return Flow.HAS_MORE_STATE;
    }

    try {
      Thread.sleep(HEART_BEAT_REQUEST_RATE);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Error checking DataPartitionTable status due to thread interruption.");
    }
    setNextState(DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES_HEART_BEAT);
    return Flow.HAS_MORE_STATE;
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

      dataPartitionTables.forEach((k, v) -> v.forEach(databaseScopedDataPartitionTable -> {
        if (!databaseScopedDataPartitionTable.getDatabase().equals(database)) {
          return;
        }
        finalDataPartitionTables.put(database, new DataPartitionTable(finalDataPartitionMap).merge(databaseScopedDataPartitionTable.getDataPartitionTable()));
      }));
    }

    LOG.info("DataPartitionTables merge completed successfully");
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

    if (finalDataPartitionTables.isEmpty()) {
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
        for (String database : lostDataPartitionsOfDatabases) {
          assignedDataPartition.put(database, finalDataPartitionTables.get(database));
        }
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

  /** Determine whether there are still DataNode nodes with failed execution of a certain step in this round. If such nodes exist, calculate the skipDataNodes and exclude these nodes when requesting the list of DataNode nodes in the cluster for the next round; if no such nodes exist, it means the procedure has been completed */
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
    for (Map.Entry<Integer, List<DatabaseScopedDataPartitionTable>> entry : dataPartitionTables.entrySet()) {
      stream.writeInt(entry.getKey());

      List<DatabaseScopedDataPartitionTable> tableList = entry.getValue();
      stream.writeInt(tableList.size());

      for (DatabaseScopedDataPartitionTable table : tableList) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

          TTransport transport = new TIOStreamTransport(dos);
          TBinaryProtocol protocol = new TBinaryProtocol(transport);

          table.serialize(dos, protocol);

          byte[] data = baos.toByteArray();
          // Length of data written for a single object
          stream.writeInt(data.length);
          // data written for a single object
          stream.write(data);
        } catch (IOException | TException e) {
          LOG.error("{} serialize failed for dataNodeId: {}", this.getClass().getSimpleName(), entry.getKey(), e);
          throw new IOException("Failed to serialize dataPartitionTables", e);
        }
      }
    }

    stream.writeInt(lostDataPartitionsOfDatabases.size());
    for (String database : lostDataPartitionsOfDatabases) {
      stream.writeUTF(database);
    }

      if (finalDataPartitionTables != null && !finalDataPartitionTables.isEmpty()) {
        stream.writeInt(finalDataPartitionTables.size());

        for (Map.Entry<String, DataPartitionTable> entry : finalDataPartitionTables.entrySet()) {
          stream.writeUTF(entry.getKey());

          try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
               DataOutputStream dos = new DataOutputStream(baos)) {

            TTransport transport = new TIOStreamTransport(dos);
            TBinaryProtocol protocol = new TBinaryProtocol(transport);

            entry.getValue().serialize(dos, protocol);

            byte[] data = baos.toByteArray();
            stream.writeInt(data.length);
            stream.write(data);
          } catch (IOException | TException e) {
            LOG.error("{} serialize finalDataPartitionTables failed", this.getClass().getSimpleName(), e);
            throw new IOException("Failed to serialize finalDataPartitionTables", e);
          }
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
      String database = ReadWriteIOUtils.readString(byteBuffer);
      long timeslot = byteBuffer.getLong();
      earliestTimeslots.put(database, timeslot);
    }

    // Deserialize dataPartitionTables count
    int dataPartitionTablesSize = byteBuffer.getInt();
    dataPartitionTables = new ConcurrentHashMap<>();
    for (int i = 0; i < dataPartitionTablesSize; i++) {
      int dataNodeId = byteBuffer.getInt();
      int listSize = byteBuffer.getInt();

      List<DatabaseScopedDataPartitionTable> tableList = new ArrayList<>(listSize);

      for (int j = 0; j < listSize; j++) {
        int dataSize = byteBuffer.getInt();
        byte[] bytes = new byte[dataSize];
        byteBuffer.get(bytes);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {

          TTransport transport = new TIOStreamTransport(dis);
          TBinaryProtocol protocol = new TBinaryProtocol(transport);

          DatabaseScopedDataPartitionTable table =
                  DatabaseScopedDataPartitionTable.deserialize(dis, protocol);
          tableList.add(table);

        } catch (IOException | TException e) {
          LOG.error("{} deserialize failed for dataNodeId: {}", this.getClass().getSimpleName(), dataNodeId, e);
          throw new RuntimeException("Failed to deserialize dataPartitionTables", e);
        }
      }

      dataPartitionTables.put(dataNodeId, tableList);
    }

    int lostDataPartitionsOfDatabasesSize = byteBuffer.getInt();
    for (int i = 0; i < lostDataPartitionsOfDatabasesSize; i++) {
      String database = ReadWriteIOUtils.readString(byteBuffer);
      lostDataPartitionsOfDatabases.add(database);
    }

    // Deserialize finalDataPartitionTable size
    int finalDataPartitionTablesSize = byteBuffer.getInt();
    finalDataPartitionTables = new ConcurrentHashMap<>();

    for (int i = 0; i < finalDataPartitionTablesSize; i++) {
      String database = ReadWriteIOUtils.readString(byteBuffer);

      int dataSize = byteBuffer.getInt();
      byte[] bytes = new byte[dataSize];
      byteBuffer.get(bytes);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
           DataInputStream dis = new DataInputStream(bais)) {

        TTransport transport = new TIOStreamTransport(dis);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        DataPartitionTable dataPartitionTable = new DataPartitionTable();
        dataPartitionTable.deserialize(dis, protocol);

        finalDataPartitionTables.put(database, dataPartitionTable);

      } catch (IOException | TException e) {
        LOG.error("{} deserialize finalDataPartitionTables failed", this.getClass().getSimpleName(), e);
        throw new RuntimeException("Failed to deserialize finalDataPartitionTables", e);
      }
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

  private List<DatabaseScopedDataPartitionTable> deserializeDatabaseScopedTableList(List<ByteBuffer> dataList) {
    if (dataList == null || dataList.isEmpty()) {
      return Collections.emptyList();
    }

    List<DatabaseScopedDataPartitionTable> result = new ArrayList<>(dataList.size());

    for (ByteBuffer data : dataList) {
      if (data == null || data.remaining() == 0) {
        LOG.warn("Skipping empty ByteBuffer during deserialization");
        continue;
      }

      try {
        ByteBuffer dataBuffer = data.duplicate();

        DatabaseScopedDataPartitionTable table =
                DatabaseScopedDataPartitionTable.deserialize(dataBuffer);

        result.add(table);

      } catch (Exception e) {
        LOG.error("Failed to deserialize DatabaseScopedDataPartitionTable", e);
      }
    }

    return result;
  }
}
