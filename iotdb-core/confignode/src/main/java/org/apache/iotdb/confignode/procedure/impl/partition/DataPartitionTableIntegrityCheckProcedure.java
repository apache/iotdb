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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.enums.DataPartitionTableGeneratorState;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.DatabaseScopedDataPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.DataPartitionTableIntegrityCheckProcedureState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
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
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
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

  // how many times will retry after rpc request failed
  private static final int MAX_RETRY_COUNT = 3;

  // how long to start a heartbeat request, the unit is ms
  private static final long HEART_BEAT_REQUEST_INTERVAL = 10000;

  // how long to check all datanode are alive, the unit is ms
  private static final long CHECK_ALL_DATANODE_IS_ALIVE_INTERVAL = 10000;

  // how long to roll back the next state, the unit is ms
  private static final long ROLL_BACK_NEXT_STATE_INTERVAL = 60000;

  NodeManager dataNodeManager;
  LoadManager loadManager;
  private List<TDataNodeConfiguration> allDataNodes = new ArrayList<>();

  // ============Need serialize BEGIN=============/
  /** Collected earliest timeslots from DataNodes: database -> earliest timeslot */
  private Map<String, Long> earliestTimeslots = new ConcurrentHashMap<>();

  /** DataPartitionTables collected from DataNodes: dataNodeId -> <database, DataPartitionTable> */
  private Map<Integer, List<DatabaseScopedDataPartitionTable>> dataPartitionTables =
      new ConcurrentHashMap<>();

  /**
   * Collect all database names that those database lost data partition, the string in the Set
   * collection is database name
   */
  private Set<String> databasesWithLostDataPartition = new HashSet<>();

  /**
   * Final merged DataPartitionTable for every database Map<String, DataPartitionTable> key(String):
   * database name
   */
  private Map<String, DataPartitionTable> finalDataPartitionTables;

  private Set<TDataNodeConfiguration> skipDataNodes =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private Set<TDataNodeConfiguration> failedDataNodes =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  // ============Need serialize END=============/

  public DataPartitionTableIntegrityCheckProcedure() {
    super();
  }

  @Override
  protected void updateMetricsOnFinish(
      final ConfigNodeProcedureEnv env, final long runtime, final boolean success) {
    super.updateMetricsOnFinish(env, runtime, success);
    env.getConfigManager()
        .getPartitionManager()
        .markDataPartitionTableIntegrityCheckProcedureFinished();
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DataPartitionTableIntegrityCheckProcedureState state)
      throws InterruptedException {
    try {
      // Ensure to get the real-time DataNodes in the current cluster at every step
      dataNodeManager = env.getConfigManager().getNodeManager();
      loadManager = env.getConfigManager().getLoadManager();
      allDataNodes = dataNodeManager.getRegisteredDataNodes();

      switch (state) {
        case COLLECT_EARLIEST_TIMESLOTS:
          failedDataNodes = new HashSet<>();
          return collectEarliestTimeslots();
        case ANALYZE_MISSING_PARTITIONS:
          databasesWithLostDataPartition = new HashSet<>();
          return analyzeMissingPartitions(env);
        case REQUEST_PARTITION_TABLES:
          return requestPartitionTables();
        case REQUEST_PARTITION_TABLES_HEART_BEAT:
          return requestPartitionTablesHeartBeat();
        case MERGE_PARTITION_TABLES:
          finalDataPartitionTables = new HashMap<>();
          return mergePartitionTables(env);
        case WRITE_PARTITION_TABLE_TO_CONSENSUS:
          return writePartitionTableToConsensus(env);
        default:
          throw new ProcedureException("Unknown state: " + state);
      }
    } catch (Exception e) {
      LOG.error("[DataPartitionIntegrity] Error executing state {}: {}", state, e.getMessage(), e);
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
        databasesWithLostDataPartition.clear();
        break;
      case REQUEST_PARTITION_TABLES:
      case REQUEST_PARTITION_TABLES_HEART_BEAT:
        dataPartitionTables.clear();
        break;
      case MERGE_PARTITION_TABLES:
        finalDataPartitionTables.clear();
        break;
      case WRITE_PARTITION_TABLE_TO_CONSENSUS:
        allDataNodes.clear();
        earliestTimeslots.clear();
        dataPartitionTables.clear();
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
          "[DataPartitionIntegrity] No DataNodes registered, no way to collect earliest timeslots, waiting for them to go up");
      sleep(
          CHECK_ALL_DATANODE_IS_ALIVE_INTERVAL,
          "[DataPartitionIntegrity] Error waiting for DataNode startup due to thread interruption.");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    // Collect earliest timeslots from all DataNodes
    allDataNodes.removeAll(skipDataNodes);
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      // Check if DataNode is alive before sending request
      NodeStatus nodeStatus = loadManager.getNodeStatus(dataNode.getLocation().getDataNodeId());
      if (!NodeStatus.Running.equals(nodeStatus)) {
        failedDataNodes.add(dataNode);
        continue;
      }

      try {
        Object response =
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithGivenRetry(
                    dataNode.getLocation().getInternalEndPoint(),
                    null,
                    CnToDnSyncRequestType.COLLECT_EARLIEST_TIMESLOTS,
                    MAX_RETRY_COUNT);

        if (response instanceof TSStatus) {
          failedDataNodes.add(dataNode);
          LOG.error(
              "[DataPartitionIntegrity] Failed to collected earliest timeslots from the DataNode[id={}], already out of max retry time",
              dataNode.getLocation().getDataNodeId());
          continue;
        }

        TGetEarliestTimeslotsResp resp = (TGetEarliestTimeslotsResp) response;
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failedDataNodes.add(dataNode);
          LOG.error(
              "[DataPartitionIntegrity] Failed to collected earliest timeslots from the DataNode[id={}], response status is {}",
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
            "[DataPartitionIntegrity] Failed to collect earliest timeslots from the DataNode[id={}]: {}",
            dataNode.getLocation().getDataNodeId(),
            e.getMessage(),
            e);
        failedDataNodes.add(dataNode);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Collected earliest timeslots from {} DataNodes: {}, the number of successful DataNodes is {}",
          allDataNodes.size(),
          earliestTimeslots,
          allDataNodes.size() - failedDataNodes.size());
    }

    if (failedDataNodes.size() == allDataNodes.size()) {
      delayRollbackNextState(
          DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
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
          "[DataPartitionIntegrity] No missing data partitions detected, nothing needs to be repaired, terminating procedure");
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
        databasesWithLostDataPartition.add(database);
        LOG.warn(
            "[DataPartitionIntegrity] No data partition table related to database {} was found from the ConfigNode, and this issue needs to be repaired",
            database);
        continue;
      }

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionMap = localDataPartitionTable.get(database);
      long localEarliestSlotStartTime = Long.MAX_VALUE;
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionEntry : seriesPartitionMap.entrySet()) {
        Map<TTimePartitionSlot, List<TConsensusGroupId>> tTimePartitionSlotListMap =
            seriesPartitionEntry.getValue();

        if (tTimePartitionSlotListMap.isEmpty()) {
          continue;
        }

        TTimePartitionSlot localEarliestSlot =
            tTimePartitionSlotListMap.keySet().stream()
                .min(Comparator.comparingLong(TTimePartitionSlot::getStartTime))
                .orElse(null);

        localEarliestSlotStartTime =
            Math.min(localEarliestSlotStartTime, localEarliestSlot.getStartTime());
      }

      if (localEarliestSlotStartTime
          > TimePartitionUtils.getStartTimeByPartitionId(earliestTimeslot)) {
        databasesWithLostDataPartition.add(database);
        LOG.warn(
            "[DataPartitionIntegrity] Database {} has lost timeslot {} in its data table partition, and this issue needs to be repaired",
            database,
            earliestTimeslot);
      }
    }

    if (databasesWithLostDataPartition.isEmpty()) {
      LOG.info(
          "[DataPartitionIntegrity] No databases have lost data partitions, terminating procedure");
      return Flow.NO_MORE_STATE;
    }

    LOG.info(
        "[DataPartitionIntegrity] Identified {} databases have lost data partitions, will request DataPartitionTable generation from {} DataNodes",
        databasesWithLostDataPartition.size(),
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
          "[DataPartitionIntegrity] No DataNodes registered, no way to requested DataPartitionTable generation, terminating procedure");
      sleep(
          CHECK_ALL_DATANODE_IS_ALIVE_INTERVAL,
          "[DataPartitionIntegrity] Error waiting for DataNode startup due to thread interruption.");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    allDataNodes.removeAll(skipDataNodes);
    allDataNodes.removeAll(failedDataNodes);
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();
      // Check if DataNode is alive before sending request
      NodeStatus nodeStatus = loadManager.getNodeStatus(dataNodeId);
      if (!NodeStatus.Running.equals(nodeStatus)) {
        failedDataNodes.add(dataNode);
        continue;
      }

      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableReq req = new TGenerateDataPartitionTableReq();
          req.setDatabases(databasesWithLostDataPartition);
          Object response =
              SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithGivenRetry(
                      dataNode.getLocation().getInternalEndPoint(),
                      req,
                      CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE,
                      MAX_RETRY_COUNT);

          if (response instanceof TSStatus) {
            failedDataNodes.add(dataNode);
            LOG.error(
                "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from the DataNode[id={}], already out of max retry time",
                dataNode.getLocation().getDataNodeId());
            continue;
          }

          TGenerateDataPartitionTableResp resp = (TGenerateDataPartitionTableResp) response;
          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedDataNodes.add(dataNode);
            LOG.error(
                "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from the DataNode[id={}], response status is {}",
                dataNode.getLocation().getDataNodeId(),
                resp.getStatus());
          }
        } catch (Exception e) {
          failedDataNodes.add(dataNode);
          LOG.error(
              "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from DataNode[id={}]: {}",
              dataNodeId,
              e.getMessage(),
              e);
        }
      }
    }

    if (failedDataNodes.size() == allDataNodes.size()) {
      delayRollbackNextState(
          DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    setNextState(
        DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES_HEART_BEAT);
    return Flow.HAS_MORE_STATE;
  }

  private Flow requestPartitionTablesHeartBeat() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking DataPartitionTable generation completion status...");
    }

    int completeCount = 0;
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();
      // Check if DataNode is alive before sending request
      NodeStatus nodeStatus = loadManager.getNodeStatus(dataNodeId);
      if (!NodeStatus.Running.equals(nodeStatus)) {
        failedDataNodes.add(dataNode);
        continue;
      }

      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableReq req = new TGenerateDataPartitionTableReq();
          req.setDatabases(databasesWithLostDataPartition);
          Object response =
              SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithGivenRetry(
                      dataNode.getLocation().getInternalEndPoint(),
                      req,
                      CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE_HEART_BEAT,
                      MAX_RETRY_COUNT);

          if (response instanceof TSStatus) {
            failedDataNodes.add(dataNode);
            LOG.error(
                "[DataPartitionIntegrity] Failed to request DataPartitionTable generation heart beat from the DataNode[id={}], already out of max retry time",
                dataNode.getLocation().getDataNodeId());
            continue;
          }

          TGenerateDataPartitionTableHeartbeatResp resp =
              (TGenerateDataPartitionTableHeartbeatResp) response;
          DataPartitionTableGeneratorState state =
              DataPartitionTableGeneratorState.getStateByCode(resp.getErrorCode());

          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOG.error(
                "[DataPartitionIntegrity] Failed to request DataPartitionTable generation heart beat from the DataNode[id={}], state is {}, response status is {}",
                dataNode.getLocation().getDataNodeId(),
                state,
                resp.getStatus());
            continue;
          }

          switch (state) {
            case SUCCESS:
              List<ByteBuffer> byteBufferList = resp.getDatabaseScopedDataPartitionTables();
              List<DatabaseScopedDataPartitionTable> databaseScopedDataPartitionTableList =
                  deserializeDatabaseScopedTableList(byteBufferList);
              dataPartitionTables.put(dataNodeId, databaseScopedDataPartitionTableList);
              LOG.info(
                  "[DataPartitionIntegrity] DataNode {} completed DataPartitionTable generation, terminating heart beat",
                  dataNodeId);
              completeCount++;
              break;
            case IN_PROGRESS:
              LOG.info(
                  "[DataPartitionIntegrity] DataNode {} still generating DataPartitionTable",
                  dataNodeId);
              break;
            default:
              failedDataNodes.add(dataNode);
              LOG.error(
                  "[DataPartitionIntegrity] DataNode {} returned unknown error code: {}",
                  dataNodeId,
                  resp.getErrorCode());
              break;
          }
        } catch (Exception e) {
          LOG.error(
              "[DataPartitionIntegrity] Error checking DataPartitionTable status from DataNode {}: {}, terminating heart beat",
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

    // Don't find any one data partition table generation task on all registered DataNodes, go back
    // to the REQUEST_PARTITION_TABLES step and re-execute
    if (failedDataNodes.size() == allDataNodes.size()) {
      delayRollbackNextState(
          DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES);
      return Flow.HAS_MORE_STATE;
    }

    sleep(
        HEART_BEAT_REQUEST_INTERVAL,
        "[DataPartitionIntegrity] Error checking DataPartitionTable status due to thread interruption.");
    setNextState(
        DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES_HEART_BEAT);
    return Flow.HAS_MORE_STATE;
  }

  private static void sleep(long intervalTime, String logMessage) {
    try {
      Thread.sleep(intervalTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error(logMessage);
    }
  }

  /** Merge DataPartitionTables from all DataNodes into a final table. */
  private Flow mergePartitionTables(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Merging DataPartitionTables from {} DataNodes...", dataPartitionTables.size());
    }

    if (dataPartitionTables.isEmpty()) {
      LOG.error(
          "[DataPartitionIntegrity] No DataPartitionTables to merge, dataPartitionTables is empty");
      delayRollbackNextState(
          DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    for (String database : databasesWithLostDataPartition) {
      Map<TSeriesPartitionSlot, SeriesPartitionTable> finalDataPartitionMap = new HashMap<>();

      // Get current DataPartitionTable from ConfigManager
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          localDataPartitionTableMap = getLocalDataPartitionTable(env, database);

      // Check if ConfigNode has a data partition that is associated with the earliestTimeslot
      if (localDataPartitionTableMap == null
          || localDataPartitionTableMap.isEmpty()
          || localDataPartitionTableMap.get(database) == null
          || localDataPartitionTableMap.get(database).isEmpty()) {
        LOG.warn(
            "[DataPartitionIntegrity] No data partition table related to database {} was found from the ConfigNode, use data partition table of DataNode directly",
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

      dataPartitionTables.forEach(
          (k, v) ->
              v.forEach(
                  databaseScopedDataPartitionTable -> {
                    if (!databaseScopedDataPartitionTable.getDatabase().equals(database)) {
                      return;
                    }
                    finalDataPartitionTables.put(
                        database,
                        new DataPartitionTable(finalDataPartitionMap)
                            .merge(databaseScopedDataPartitionTable.getDataPartitionTable()));
                  }));
    }

    LOG.info("[DataPartitionIntegrity] DataPartitionTables merge completed successfully");
    setNextState(DataPartitionTableIntegrityCheckProcedureState.WRITE_PARTITION_TABLE_TO_CONSENSUS);
    return Flow.HAS_MORE_STATE;
  }

  /** Write the final DataPartitionTable to consensus log. */
  private Flow writePartitionTableToConsensus(final ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing DataPartitionTable to consensus log...");
    }

    if (databasesWithLostDataPartition.isEmpty()) {
      LOG.error("[DataPartitionIntegrity] No database lost data partition table");
      setFailure(
          "DataPartitionTableIntegrityCheckProcedure",
          new ProcedureException("No database lost data partition table for consensus write"));
      return getFlow();
    }

    if (finalDataPartitionTables.isEmpty()) {
      LOG.error("[DataPartitionIntegrity] DataPartitionTable to write to consensus");
      setFailure(
          "DataPartitionTableIntegrityCheckProcedure",
          new ProcedureException("No DataPartitionTable available for consensus write"));
      return getFlow();
    }

    int failedCnt = 0;
    final int maxRetryCountForConsensus = 3;
    while (failedCnt < maxRetryCountForConsensus) {
      try {
        CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
        Map<String, DataPartitionTable> assignedDataPartition = new HashMap<>();
        for (String database : databasesWithLostDataPartition) {
          assignedDataPartition.put(database, finalDataPartitionTables.get(database));
        }
        createPlan.setAssignedDataPartition(assignedDataPartition);
        TSStatus tsStatus = env.getConfigManager().getConsensusManager().write(createPlan);

        if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOG.info(
              "[DataPartitionIntegrity] DataPartitionTable successfully written to consensus log");
          break;
        } else {
          LOG.error("[DataPartitionIntegrity] Failed to write DataPartitionTable to consensus log");
          setFailure(
              "DataPartitionTableIntegrityCheckProcedure",
              new ProcedureException("Failed to write DataPartitionTable to consensus log"));
        }
      } catch (Exception e) {
        LOG.error("[DataPartitionIntegrity] Error writing DataPartitionTable to consensus log", e);
        setFailure("DataPartitionTableIntegrityCheckProcedure", e);
      }
      failedCnt++;
    }

    return getFlow();
  }

  /**
   * Determine whether there are still DataNode nodes with failed execution of a certain step in
   * this round. If such nodes exist, calculate the skipDataNodes and exclude these nodes when
   * requesting the list of DataNode nodes in the cluster for the next round; if no such nodes
   * exist, it means the procedure has been completed
   */
  private Flow getFlow() {
    if (!failedDataNodes.isEmpty()) {
      allDataNodes.removeAll(failedDataNodes);
      skipDataNodes = new HashSet<>(allDataNodes);
      delayRollbackNextState(
          DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    } else {
      skipDataNodes.clear();
      return Flow.NO_MORE_STATE;
    }
  }

  /** Delay to jump to next state, avoid write raft logs frequently when exception occur */
  private void delayRollbackNextState(DataPartitionTableIntegrityCheckProcedureState state) {
    sleep(
        ROLL_BACK_NEXT_STATE_INTERVAL,
        String.format(
            "[DataPartitionIntegrity] Error waiting for roll back the %s state due to thread interruption.",
            state));
    setNextState(state);
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DATA_PARTITION_TABLE_INTEGRITY_CHECK_PROCEDURE.getTypeCode());
    super.serialize(stream);

    // Serialize earliestTimeslots
    stream.writeInt(earliestTimeslots.size());
    for (Map.Entry<String, Long> entry : earliestTimeslots.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      stream.writeLong(entry.getValue());
    }

    // Serialize dataPartitionTables count
    stream.writeInt(dataPartitionTables.size());
    for (Map.Entry<Integer, List<DatabaseScopedDataPartitionTable>> entry :
        dataPartitionTables.entrySet()) {
      stream.writeInt(entry.getKey());

      List<DatabaseScopedDataPartitionTable> tableList = entry.getValue();
      stream.writeInt(tableList.size());

      for (DatabaseScopedDataPartitionTable table : tableList) {
        try (final PublicBAOS publicBAOS = new PublicBAOS();
            final DataOutputStream tmpStream = new DataOutputStream(publicBAOS)) {

          TTransport transport = new TIOStreamTransport(tmpStream);
          TBinaryProtocol protocol = new TBinaryProtocol(transport);

          table.serialize(tmpStream, protocol);

          byte[] buf = publicBAOS.getBuf();
          int size = publicBAOS.size();
          ReadWriteIOUtils.write(size, stream);
          stream.write(buf, 0, size);
        } catch (IOException | TException e) {
          LOG.error(
              "[DataPartitionIntegrity] {} serialize failed for dataNodeId: {}",
              this.getClass().getSimpleName(),
              entry.getKey(),
              e);
          throw new IOException("Failed to serialize dataPartitionTables", e);
        }
      }
    }

    stream.writeInt(databasesWithLostDataPartition.size());
    for (String database : databasesWithLostDataPartition) {
      ReadWriteIOUtils.write(database, stream);
    }

    if (finalDataPartitionTables != null && !finalDataPartitionTables.isEmpty()) {
      stream.writeInt(finalDataPartitionTables.size());

      for (Map.Entry<String, DataPartitionTable> entry : finalDataPartitionTables.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);

        try (final PublicBAOS publicBAOS = new PublicBAOS();
            final DataOutputStream tmpStream = new DataOutputStream(publicBAOS)) {
          TTransport transport = new TIOStreamTransport(tmpStream);
          TBinaryProtocol protocol = new TBinaryProtocol(transport);

          entry.getValue().serialize(tmpStream, protocol);

          byte[] buf = publicBAOS.getBuf();
          int size = publicBAOS.size();
          ReadWriteIOUtils.write(size, stream);
          stream.write(buf, 0, size);
        } catch (IOException | TException e) {
          LOG.error(
              "[DataPartitionIntegrity] {} serialize finalDataPartitionTables failed",
              this.getClass().getSimpleName(),
              e);
          throw new IOException("Failed to serialize finalDataPartitionTables", e);
        }
      }
    } else {
      stream.writeInt(0);
    }

    stream.writeInt(skipDataNodes.size());
    for (TDataNodeConfiguration skipDataNode : skipDataNodes) {
      try (final PublicBAOS publicBAOS = new PublicBAOS();
          final DataOutputStream tmpStream = new DataOutputStream(publicBAOS)) {
        TTransport transport = new TIOStreamTransport(tmpStream);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        skipDataNode.write(protocol);

        byte[] buf = publicBAOS.getBuf();
        int size = publicBAOS.size();
        ReadWriteIOUtils.write(size, stream);
        stream.write(buf, 0, size);
      } catch (TException e) {
        LOG.error("[DataPartitionIntegrity] Failed to serialize skipDataNode", e);
        throw new IOException("Failed to serialize skipDataNode", e);
      }
    }

    stream.writeInt(failedDataNodes.size());
    for (TDataNodeConfiguration failedDataNode : failedDataNodes) {
      try (final PublicBAOS publicBAOS = new PublicBAOS();
          final DataOutputStream tmpStream = new DataOutputStream(publicBAOS)) {
        TTransport transport = new TIOStreamTransport(tmpStream);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        failedDataNode.write(protocol);

        byte[] buf = publicBAOS.getBuf();
        int size = publicBAOS.size();
        ReadWriteIOUtils.write(size, stream);
        stream.write(buf, 0, size);
      } catch (TException e) {
        LOG.error("[DataPartitionIntegrity] Failed to serialize failedDataNode", e);
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
          LOG.error(
              "[DataPartitionIntegrity] {} deserialize failed for dataNodeId: {}",
              this.getClass().getSimpleName(),
              dataNodeId,
              e);
          throw new RuntimeException("Failed to deserialize dataPartitionTables", e);
        }
      }

      dataPartitionTables.put(dataNodeId, tableList);
    }

    int databasesWithLostDataPartitionSize = byteBuffer.getInt();
    for (int i = 0; i < databasesWithLostDataPartitionSize; i++) {
      String database = ReadWriteIOUtils.readString(byteBuffer);
      databasesWithLostDataPartition.add(database);
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
        LOG.error(
            "[DataPartitionIntegrity] {} deserialize finalDataPartitionTables failed",
            this.getClass().getSimpleName(),
            e);
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
        LOG.error("[DataPartitionIntegrity] Failed to deserialize skipDataNode", e);
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
        LOG.error("[DataPartitionIntegrity] Failed to deserialize failedDataNode", e);
        throw new RuntimeException(e);
      }
    }
  }

  private List<DatabaseScopedDataPartitionTable> deserializeDatabaseScopedTableList(
      List<ByteBuffer> dataList) {
    if (dataList == null || dataList.isEmpty()) {
      return Collections.emptyList();
    }

    List<DatabaseScopedDataPartitionTable> result = new ArrayList<>(dataList.size());

    for (ByteBuffer data : dataList) {
      if (data == null || data.remaining() == 0) {
        LOG.warn("[DataPartitionIntegrity] Skipping empty ByteBuffer during deserialization");
        continue;
      }

      try {
        DatabaseScopedDataPartitionTable table = DatabaseScopedDataPartitionTable.deserialize(data);
        result.add(table);
      } catch (Exception e) {
        LOG.error(
            "[DataPartitionIntegrity] Failed to deserialize DatabaseScopedDataPartitionTable", e);
      }
    }

    return result;
  }

  public Map<String, Long> getEarliestTimeslots() {
    return earliestTimeslots;
  }

  public Map<Integer, List<DatabaseScopedDataPartitionTable>> getDataPartitionTables() {
    return dataPartitionTables;
  }

  public Set<String> getDatabasesWithLostDataPartition() {
    return databasesWithLostDataPartition;
  }

  public Map<String, DataPartitionTable> getFinalDataPartitionTables() {
    return finalDataPartitionTables;
  }

  public Set<TDataNodeConfiguration> getSkipDataNodes() {
    return skipDataNodes;
  }

  public Set<TDataNodeConfiguration> getFailedDataNodes() {
    return failedDataNodes;
  }

  public void setEarliestTimeslots(Map<String, Long> earliestTimeslots) {
    this.earliestTimeslots = earliestTimeslots;
  }

  public void setDataPartitionTables(
      Map<Integer, List<DatabaseScopedDataPartitionTable>> dataPartitionTables) {
    this.dataPartitionTables = dataPartitionTables;
  }

  public void setDatabasesWithLostDataPartition(Set<String> databasesWithLostDataPartition) {
    this.databasesWithLostDataPartition = databasesWithLostDataPartition;
  }

  public void setFinalDataPartitionTables(
      Map<String, DataPartitionTable> finalDataPartitionTables) {
    this.finalDataPartitionTables = finalDataPartitionTables;
  }

  public void setSkipDataNodes(Set<TDataNodeConfiguration> skipDataNodes) {
    this.skipDataNodes = skipDataNodes;
  }

  public void setFailedDataNodes(Set<TDataNodeConfiguration> failedDataNodes) {
    this.failedDataNodes = failedDataNodes;
  }
}
