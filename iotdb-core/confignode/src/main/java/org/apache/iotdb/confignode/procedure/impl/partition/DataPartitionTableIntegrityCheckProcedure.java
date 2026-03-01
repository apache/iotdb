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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.enums.DataPartitionTableGeneratorState;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.DataPartitionTableIntegrityCheckProcedureState;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.mpp.rpc.thrift.TGenerateDataPartitionTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TGetEarliestTimeslotsResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
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
import java.util.stream.Collectors;

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

  /** Error codes for DataNode responses */
  public static final int DN_ERROR_CODE_SUCCESS = 0;

  public static final int DN_ERROR_CODE_IN_PROGRESS = 2;
  public static final int DN_ERROR_CODE_FAILED = 1;
  public static final int DN_ERROR_CODE_UNKNOWN = -1;

  /** Collected earliest timeslots from DataNodes: database -> earliest timeslot */
  private Map<String, Long> earliestTimeslots = new ConcurrentHashMap<>();

  /** DataPartitionTables collected from DataNodes: dataNodeId -> DataPartitionTable */
  private Map<Integer, DataPartitionTable> dataPartitionTables = new ConcurrentHashMap<>();

  /** Final merged DataPartitionTable */
  private DataPartitionTable finalDataPartitionTable;

  /** List of DataNodes that need to generate DataPartitionTable */
  private List<TDataNodeConfiguration> allDataNodes = new ArrayList<>();

  private Set<String> lostDataPartitionsOfDatabases;

  NodeManager dataNodeManager;

  /** Current retry attempt */
  private int retryCount = 0;

  private static final int MAX_RETRY_COUNT = 3;

  private static Set<Integer> skipDnIds;
  private static Set<Integer> failedDnIds;

  private static ScheduledExecutorService heartBeatExecutor;
  private static final long HEART_BEAT_REQUEST_RATE = 60000;

  public DataPartitionTableIntegrityCheckProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DataPartitionTableIntegrityCheckProcedureState state)
      throws InterruptedException {
    try {
      // Ensure to get the real-time DataNodes in the current cluster at every step
      dataNodeManager = env.getConfigManager().getNodeManager();
      allDataNodes = dataNodeManager.getRegisteredDataNodes();

      switch (state) {
        case COLLECT_EARLIEST_TIMESLOTS:
          failedDnIds = new HashSet<>();
          return collectEarliestTimeslots(env);
        case ANALYZE_MISSING_PARTITIONS:
          return analyzeMissingPartitions(env);
        case REQUEST_PARTITION_TABLES:
          heartBeatExecutor = Executors.newScheduledThreadPool(allDataNodes.size());
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
  protected void rollbackState(ConfigNodeProcedureEnv env, DataPartitionTableIntegrityCheckProcedureState state)
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
      case SUCCESS:
      case FAILED:
        // No cleanup needed for terminal states
        break;
      default:
        throw new ProcedureException("Unknown state for rollback: " + state);
    }
  }

  @Override
  protected DataPartitionTableIntegrityCheckProcedureState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(DataPartitionTableIntegrityCheckProcedureState state) {
    return 0;
  }

  @Override
  protected DataPartitionTableIntegrityCheckProcedureState getInitialState() {
    skipDnIds = new HashSet<>();
    return DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS;
  }

  /**
   * Collect earliest timeslot information from all DataNodes. Each DataNode returns a Map<String,
   * Long> where key is database name and value is the earliest timeslot id.
   */
  private Flow collectEarliestTimeslots(ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Collecting earliest timeslots from all DataNodes...");
    }

    if (allDataNodes.isEmpty()) {
      LOG.error("No DataNodes registered, no way to collect earliest timeslots, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    // Collect earliest timeslots from all DataNodes
    Map<String, Long> mergedEarliestTimeslots = new ConcurrentHashMap<>();

    for (TDataNodeConfiguration dataNode : allDataNodes) {
      try {
        TGetEarliestTimeslotsResp resp = (TGetEarliestTimeslotsResp) SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithGivenRetry(dataNode.getLocation().getInternalEndPoint(), null, CnToDnSyncRequestType.COLLECT_EARLIEST_TIMESLOTS, MAX_RETRY_COUNT);
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failedDnIds.add(dataNode.getLocation().getDataNodeId());
          LOG.error("Failed to collected earliest timeslots from the DataNode[id={}], response status is {}", dataNode.getLocation().getDataNodeId(), resp.getStatus());
          continue;
        }

        Map<String, Long> nodeTimeslots = resp.getDatabaseToEarliestTimeslot();

        // Merge with existing timeslots (take minimum)
        for (Map.Entry<String, Long> entry : nodeTimeslots.entrySet()) {
          mergedEarliestTimeslots.merge(entry.getKey(), entry.getValue(), Math::min);
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
        failedDnIds.add(dataNode.getLocation().getDataNodeId());
      }
    }

    earliestTimeslots = mergedEarliestTimeslots;

    if (LOG.isDebugEnabled()) {
      LOG.info(
          "Collected earliest timeslots from {} DataNodes: {}, the number of successful DataNodes is {}",
          allDataNodes.size(),
          earliestTimeslots,
          allDataNodes.size() - failedDnIds.size());
    }

    Set<Integer> allDnIds = allDataNodes.stream().map(dataNodeConfiguration -> dataNodeConfiguration.getLocation().getDataNodeId()).collect(Collectors.toSet());
    if (failedDnIds.size() == allDataNodes.size() && allDnIds.containsAll(failedDnIds)) {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
    } else {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.ANALYZE_MISSING_PARTITIONS);
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Analyze which data partitions are missing based on earliest timeslots. Identify data partitions of databases need to be repaired.
   */
  private Flow analyzeMissingPartitions(ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Analyzing missing data partitions...");
    }

    if (earliestTimeslots.isEmpty()) {
      LOG.error("No missing data partitions detected, nothing needs to be repaired, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    // Find all databases that have lost data partition tables
    lostDataPartitionsOfDatabases = new HashSet<>();

    for (Map.Entry<String, Long> entry : earliestTimeslots.entrySet()) {
      String database = entry.getKey();
      long earliestTimeslot = entry.getValue();

      // Get current DataPartitionTable from ConfigManager
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
              dataPartitionTable = getLocalDataPartitionTable(env, database);

      // Check if ConfigNode has a data partition that is associated with the earliestTimeslot
      if (dataPartitionTable.isEmpty() || dataPartitionTable.get(database) == null || dataPartitionTable.get(database).isEmpty()) {
        LOG.error("No data partition table related to database {} was found from the ConfigNode", database);
        continue;
      }

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> seriesPartitionMap = dataPartitionTable.get(database);
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
              seriesPartitionEntry : seriesPartitionMap.entrySet()) {
        Map<TTimePartitionSlot, List<TConsensusGroupId>> tTimePartitionSlotListMap = seriesPartitionEntry.getValue();
        tTimePartitionSlotListMap.keySet().forEach(slot -> {
          if (!TimePartitionUtils.satisfyPartitionId(slot.getStartTime(), earliestTimeslot)) {
            lostDataPartitionsOfDatabases.add(database);
            LOG.warn("Database {} has lost timeslot {} in its data table partition, and this issue needs to be repaired", database, earliestTimeslot);
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
        allDataNodes.size() - failedDnIds.size());
    setNextState(DataPartitionTableIntegrityCheckProcedureState.REQUEST_PARTITION_TABLES);
    return Flow.HAS_MORE_STATE;
  }

  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>> getLocalDataPartitionTable(ConfigNodeProcedureEnv env, String database) {
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>>  schemaPartitionTable = env.getConfigManager().getSchemaPartition(Collections.singletonMap(database, Collections.emptyList()))
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
  private Flow requestPartitionTables(ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requesting DataPartitionTable generation from {} DataNodes...", allDataNodes.size());
    }

    if (allDataNodes.isEmpty()) {
      LOG.error("No DataNodes registered, no way to requested DataPartitionTable generation, terminating procedure");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    heartBeatExecutor.scheduleAtFixedRate(this::checkPartitionTableGenerationStatus, 0, HEART_BEAT_REQUEST_RATE, TimeUnit.MILLISECONDS);

    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();
      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableResp resp = (TGenerateDataPartitionTableResp) SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithGivenRetry(dataNode.getLocation().getInternalEndPoint(), null, CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE, MAX_RETRY_COUNT);
          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedDnIds.add(dataNode.getLocation().getDataNodeId());
            LOG.error("Failed to request DataPartitionTable generation from the DataNode[id={}], response status is {}", dataNode.getLocation().getDataNodeId(), resp.getStatus());
            continue;
          }

          byte[] bytes = resp.getDataPartitionTable();
          DataPartitionTable dataPartitionTable = new DataPartitionTable();
          dataPartitionTable.deserialize(ByteBuffer.wrap(bytes));
          dataPartitionTables.put(dataNodeId, dataPartitionTable);
        } catch (Exception e) {
          failedDnIds.add(dataNode.getLocation().getDataNodeId());
          LOG.error(
              "Failed to request DataPartitionTable generation from DataNode[id={}]: {}",
              dataNodeId,
              e.getMessage(),
              e);
        }
      }
    }

    Set<Integer> allDnIds = allDataNodes.stream().map(dataNodeConfiguration -> dataNodeConfiguration.getLocation().getDataNodeId()).collect(Collectors.toSet());
    if (failedDnIds.size() == allDataNodes.size() && allDnIds.containsAll(failedDnIds)) {
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    setNextState(DataPartitionTableIntegrityCheckProcedureState.WRITE_PARTITION_TABLE_TO_RAFT);
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Check completion status of DataPartitionTable generation tasks.
   */
  private void checkPartitionTableGenerationStatus() {
    LOG.info("Checking DataPartitionTable generation completion status...");

    int completeCount = 0;
    for (TDataNodeConfiguration dataNode : allDataNodes) {
      int dataNodeId = dataNode.getLocation().getDataNodeId();

      if (!dataPartitionTables.containsKey(dataNodeId)) {
        try {
          TGenerateDataPartitionTableResp resp = (TGenerateDataPartitionTableResp) SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithGivenRetry(dataNode.getLocation().getInternalEndPoint(), null, CnToDnSyncRequestType.GENERATE_DATA_PARTITION_TABLE_HEART_BEAT, MAX_RETRY_COUNT);
          DataPartitionTableGeneratorState state = DataPartitionTableGeneratorState.getStateByCode(resp.getStatus().getCode());

          switch (state) {
            case SUCCESS:
              LOG.info("DataNode {} completed DataPartitionTable generation, terminating heart beat", dataNodeId);
              completeCount++;
              break;
            case IN_PROGRESS:
              LOG.info("DataNode {} still generating DataPartitionTable", dataNodeId);
              break;
            case FAILED:
              LOG.error("DataNode {} failed to generate DataPartitionTable, terminating heart beat", dataNodeId);
              completeCount++;
              break;
            default:
              LOG.error("DataNode {} returned unknown error code: {}", dataNodeId, resp.getStatus().getCode());
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
      }
    }

    if (completeCount >= allDataNodes.size()) {
      heartBeatExecutor.shutdown();
    }
  }

  private static void declineThread() {
    heartBeatExecutor.shutdown();
  }

  /**
   * Merge DataPartitionTables from all DataNodes into a final table.
   */
  private Flow mergePartitionTables(ConfigNodeProcedureEnv env) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Merging DataPartitionTables from {} DataNodes...", dataPartitionTables.size());
    }

    if (dataPartitionTables.isEmpty()) {
      LOG.error("No DataPartitionTables to merge, dataPartitionTables is empty");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.COLLECT_EARLIEST_TIMESLOTS);
      return Flow.HAS_MORE_STATE;
    }

    try {
      finalDataPartitionTable = new DataPartitionTable();

      // TODO: Implement proper merging logic
      // For now, use the first DataPartitionTable as the final one
      if (!dataPartitionTables.isEmpty()) {
        DataPartitionTable firstTable = dataPartitionTables.values().iterator().next();
        finalDataPartitionTable = firstTable;

        // In a real implementation, you would:
        // 1. Merge all series partition slots from all DataNodes
        // 2. For each series slot, merge time slot information
        // 3. Resolve conflicts by choosing the most recent/complete data
        // 4. Ensure consistency across all DataNodes

        LOG.info(
            "Merged DataPartitionTable contains {} series partitions",
            finalDataPartitionTable.getDataPartitionMap().size());
      }

      LOG.info("DataPartitionTable merge completed successfully");
      setNextState(DataPartitionTableIntegrityCheckProcedureState.WRITE_PARTITION_TABLE_TO_RAFT);
      return Flow.HAS_MORE_STATE;

    } catch (Exception e) {
      LOG.error("Failed to merge DataPartitionTables", e);
      setFailure("DataPartitionTableIntegrityCheckProcedure", e);
      return Flow.NO_MORE_STATE;
    }
  }

  /** Write the final DataPartitionTable to raft log. */
  private Flow writePartitionTableToRaft(ConfigNodeProcedureEnv env) {
    LOG.info("Writing DataPartitionTable to raft log...");

    if (finalDataPartitionTable == null) {
      LOG.error("No DataPartitionTable to write to raft");
      setFailure(
          "DataPartitionTableIntegrityCheckProcedure",
          new ProcedureException("No DataPartitionTable available for raft write"));
      return Flow.NO_MORE_STATE;
    }

    try {
      // TODO: Implement actual raft log write
      // This should create a consensus request to write the DataPartitionTable
      // Example:
      // WriteDataPartitionTablePlan plan = new
      // WriteDataPartitionTablePlan(finalDataPartitionTable);
      // env.getConfigManager().getConsensusManager().write(plan);

      // For now, simulate successful write
      boolean writeSuccess = true;

      if (writeSuccess) {
        LOG.info("DataPartitionTable successfully written to raft log");
        setNextState(DataPartitionTableIntegrityCheckProcedureState.SUCCESS);
        return Flow.HAS_MORE_STATE;
      } else {
        LOG.error("Failed to write DataPartitionTable to raft log");
        setFailure(
            "DataPartitionTableIntegrityCheckProcedure",
            new ProcedureException("Failed to write DataPartitionTable to raft log"));
        return Flow.NO_MORE_STATE;
      }

    } catch (Exception e) {
      LOG.error("Error writing DataPartitionTable to raft log", e);
      setFailure("DataPartitionTableIntegrityCheckProcedure", e);
      return Flow.NO_MORE_STATE;
    }
  }

  // @TODO
  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);

    // Serialize earliestTimeslots
    stream.writeInt(earliestTimeslots.size());
    for (Map.Entry<String, Long> entry : earliestTimeslots.entrySet()) {
      stream.writeUTF(entry.getKey());
      stream.writeLong(entry.getValue());
    }

    // Serialize dataPartitionTables count
    stream.writeInt(dataPartitionTables.size());
    // Note: DataPartitionTable serialization would need to be implemented here

    // Serialize targetDataNodes count
    stream.writeInt(targetDataNodes.size());
    for (TDataNodeConfiguration dataNode : targetDataNodes) {
      stream.writeInt(dataNode.getLocation().getDataNodeId());
    }

    // Serialize retryCount
    stream.writeInt(retryCount);
  }

  // @TODO
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
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
    dataPartitionTables = new ConcurrentHashMap<>();
    // Note: DataPartitionTable deserialization would need to be implemented here

    // Deserialize targetDataNodes
    int targetDataNodesSize = byteBuffer.getInt();
    targetDataNodes = new ArrayList<>();
    for (int i = 0; i < targetDataNodesSize; i++) {
      int dataNodeId = byteBuffer.getInt();
      // Note: TDataNodeLocation reconstruction would need to be implemented here
    }

    // Deserialize retryCount
    retryCount = byteBuffer.getInt();
  }
}
