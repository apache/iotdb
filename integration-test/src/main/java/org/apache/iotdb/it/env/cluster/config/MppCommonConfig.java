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

package org.apache.iotdb.it.env.cluster.config;

import org.apache.iotdb.itbase.env.CommonConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HYPHEN;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.EnvUtils.fromConsensusFullNameToAbbr;

public class MppCommonConfig extends MppBaseConfig implements CommonConfig {

  public MppCommonConfig() {
    super();
    // Set the default disk_space_warning_threshold in ClusterIT environment to 1%
    setProperty("disk_space_warning_threshold", String.valueOf(0.01));
  }

  // This constructor is no longer in use
  protected MppCommonConfig(String filePath) throws IOException {
    super(filePath);
  }

  @Override
  public void updateProperties(MppBaseConfig persistentConfig) {
    if (persistentConfig instanceof MppCommonConfig) {
      super.updateProperties(persistentConfig);
    } else {
      throw new UnsupportedOperationException(
          "MppCommonConfig can't be override by an instance of "
              + persistentConfig.getClass().getCanonicalName());
    }
  }

  @Override
  public MppBaseConfig emptyClone() {
    return new MppCommonConfig();
  }

  @Override
  public CommonConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    setProperty("max_number_of_points_in_page", String.valueOf(maxNumberOfPointsInPage));
    return this;
  }

  @Override
  public CommonConfig setPageSizeInByte(int pageSizeInByte) {
    setProperty("page_size_in_byte", String.valueOf(pageSizeInByte));
    return this;
  }

  @Override
  public CommonConfig setGroupSizeInByte(int groupSizeInByte) {
    setProperty("group_size_in_byte", String.valueOf(groupSizeInByte));
    return this;
  }

  @Override
  public CommonConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    setProperty("memtable_size_threshold", String.valueOf(memtableSizeThreshold));
    return this;
  }

  @Override
  public CommonConfig setPartitionInterval(long partitionInterval) {
    setProperty("time_partition_interval", String.valueOf(partitionInterval));
    return this;
  }

  @Override
  public CommonConfig setCompressor(String compressor) {
    setProperty("compressor", compressor);
    return this;
  }

  @Override
  public CommonConfig setUdfMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    // udf_memory_budget_in_mb
    // udf_reader_transformer_collector_memory_proportion
    setProperty("udf_memory_budget_in_mb", String.valueOf(udfCollectorMemoryBudgetInMB * 3));
    return this;
  }

  @Override
  public CommonConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    setProperty("enable_seq_space_compaction", String.valueOf(enableSeqSpaceCompaction));
    return this;
  }

  @Override
  public CommonConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    setProperty("enable_unseq_space_compaction", String.valueOf(enableUnseqSpaceCompaction));
    return this;
  }

  @Override
  public CommonConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    setProperty("enable_cross_space_compaction", String.valueOf(enableCrossSpaceCompaction));
    return this;
  }

  @Override
  public CommonConfig setMaxInnerCompactionCandidateFileNum(
      int maxInnerCompactionCandidateFileNum) {
    setProperty(
        "max_inner_compaction_candidate_file_num",
        String.valueOf(maxInnerCompactionCandidateFileNum));
    return this;
  }

  @Override
  public CommonConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    setProperty("enable_auto_create_schema", String.valueOf(enableAutoCreateSchema));
    return this;
  }

  @Override
  public CommonConfig setEnableLastCache(boolean lastCacheEnable) {
    setProperty("enable_last_cache", String.valueOf(lastCacheEnable));
    return this;
  }

  @Override
  public CommonConfig setPrimitiveArraySize(int primitiveArraySize) {
    setProperty("primitive_array_size", String.valueOf(primitiveArraySize));
    return this;
  }

  @Override
  public CommonConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    setProperty("avg_series_point_number_threshold", String.valueOf(avgSeriesPointNumberThreshold));
    return this;
  }

  @Override
  public CommonConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    setProperty("max_tsblock_line_number", String.valueOf(maxTsBlockLineNumber));
    return this;
  }

  @Override
  public CommonConfig setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(int maxMs) {
    setProperty("config_node_ratis_rpc_leader_election_timeout_max_ms", String.valueOf(maxMs));
    return this;
  }

  @Override
  public CommonConfig setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass) {
    setProperty("config_node_consensus_protocol_class", configNodeConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionConsensusProtocolClass(
      String schemaRegionConsensusProtocolClass) {
    setProperty("schema_region_consensus_protocol_class", schemaRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    setProperty("data_region_consensus_protocol_class", dataRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionGroupExtensionPolicy(String schemaRegionGroupExtensionPolicy) {
    setProperty("schema_region_group_extension_policy", schemaRegionGroupExtensionPolicy);
    return this;
  }

  @Override
  public CommonConfig setDefaultSchemaRegionGroupNumPerDatabase(int schemaRegionGroupPerDatabase) {
    setProperty(
        "default_schema_region_group_num_per_database",
        String.valueOf(schemaRegionGroupPerDatabase));
    return this;
  }

  @Override
  public CommonConfig setDataRegionGroupExtensionPolicy(String dataRegionGroupExtensionPolicy) {
    setProperty("data_region_group_extension_policy", dataRegionGroupExtensionPolicy);
    return this;
  }

  @Override
  public CommonConfig setDefaultDataRegionGroupNumPerDatabase(int dataRegionGroupPerDatabase) {
    setProperty(
        "default_data_region_group_num_per_database", String.valueOf(dataRegionGroupPerDatabase));
    return this;
  }

  @Override
  public CommonConfig setSchemaReplicationFactor(int schemaReplicationFactor) {
    setProperty("schema_replication_factor", String.valueOf(schemaReplicationFactor));
    return this;
  }

  @Override
  public CommonConfig setDataReplicationFactor(int dataReplicationFactor) {
    setProperty("data_replication_factor", String.valueOf(dataReplicationFactor));
    return this;
  }

  @Override
  public CommonConfig setTimePartitionInterval(long timePartitionInterval) {
    setProperty("time_partition_interval", String.valueOf(timePartitionInterval));
    return this;
  }

  @Override
  public CommonConfig setTimePartitionOrigin(long timePartitionOrigin) {
    setProperty("time_partition_origin", String.valueOf(timePartitionOrigin));
    return this;
  }

  @Override
  public CommonConfig setTimestampPrecision(String timestampPrecision) {
    setProperty("timestamp_precision", timestampPrecision);
    return this;
  }

  @Override
  public TimeUnit getTimestampPrecision() {
    String precision = properties.getProperty("timestamp_precision", "ms");
    switch (precision) {
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "us":
        return TimeUnit.MICROSECONDS;
      case "ns":
        return TimeUnit.NANOSECONDS;
      default:
        throw new UnsupportedOperationException(precision);
    }
  }

  @Override
  public CommonConfig setTimestampPrecisionCheckEnabled(boolean timestampPrecisionCheckEnabled) {
    setProperty(
        "timestamp_precision_check_enabled", String.valueOf(timestampPrecisionCheckEnabled));
    return this;
  }

  @Override
  public CommonConfig setEnableMemControl(boolean enableMemControl) {
    setProperty("enable_mem_control", String.valueOf(enableMemControl));
    return this;
  }

  @Override
  public CommonConfig setConfigNodeRatisSnapshotTriggerThreshold(
      int ratisSnapshotTriggerThreshold) {
    setProperty(
        "config_node_ratis_snapshot_trigger_threshold",
        String.valueOf(ratisSnapshotTriggerThreshold));
    return this;
  }

  @Override
  public CommonConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    setProperty("max_degree_of_index_node", String.valueOf(maxDegreeOfIndexNode));
    return this;
  }

  @Override
  public CommonConfig setEnableMQTTService(boolean enableMQTTService) {
    setProperty("enable_mqtt_service", String.valueOf(enableMQTTService));
    return this;
  }

  @Override
  public CommonConfig setSchemaEngineMode(String schemaEngineMode) {
    setProperty("schema_engine_mode", schemaEngineMode);
    return this;
  }

  @Override
  public CommonConfig setSelectIntoInsertTabletPlanRowLimit(
      int selectIntoInsertTabletPlanRowLimit) {
    setProperty(
        "select_into_insert_tablet_plan_row_limit",
        String.valueOf(selectIntoInsertTabletPlanRowLimit));
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus) {
    setProperty(
        "enable_auto_leader_balance_for_ratis_consensus",
        String.valueOf(enableAutoLeaderBalanceForRatisConsensus));
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus) {
    setProperty(
        "enable_auto_leader_balance_for_iot_consensus",
        String.valueOf(enableAutoLeaderBalanceForIoTConsensus));
    return this;
  }

  @Override
  public CommonConfig setQueryThreadCount(int queryThreadCount) {
    if (queryThreadCount <= 0) {
      queryThreadCount = Runtime.getRuntime().availableProcessors();
    }
    setProperty("query_thread_count", String.valueOf(queryThreadCount));
    return this;
  }

  @Override
  public CommonConfig setWalBufferSize(int walBufferSize) {
    setProperty("wal_buffer_size_in_byte", String.valueOf(walBufferSize));
    return this;
  }

  @Override
  public CommonConfig setDegreeOfParallelism(int degreeOfParallelism) {
    setProperty("degree_of_query_parallelism", String.valueOf(degreeOfParallelism));
    return this;
  }

  @Override
  public CommonConfig setDataRatisTriggerSnapshotThreshold(long threshold) {
    setProperty("data_region_ratis_snapshot_trigger_threshold", String.valueOf(threshold));
    return this;
  }

  @Override
  public CommonConfig setSeriesSlotNum(int seriesSlotNum) {
    setProperty("series_slot_num", String.valueOf(seriesSlotNum));
    return this;
  }

  @Override
  public CommonConfig setSchemaMemoryAllocate(String schemaMemoryAllocate) {
    setProperty("schema_memory_proportion", String.valueOf(schemaMemoryAllocate));
    return this;
  }

  @Override
  public CommonConfig setWriteMemoryProportion(String writeMemoryProportion) {
    setProperty("write_memory_proportion", writeMemoryProportion);
    return this;
  }

  @Override
  public CommonConfig setQuotaEnable(boolean quotaEnable) {
    setProperty("quota_enable", String.valueOf(quotaEnable));
    return this;
  }

  @Override
  public CommonConfig setSortBufferSize(long sortBufferSize) {
    setProperty("sort_buffer_size_in_bytes", String.valueOf(sortBufferSize));
    return this;
  }

  @Override
  public CommonConfig setMaxTsBlockSizeInByte(long maxTsBlockSizeInByte) {
    setProperty("max_tsblock_size_in_bytes", String.valueOf(maxTsBlockSizeInByte));
    return this;
  }

  @Override
  public CommonConfig setClusterTimeseriesLimitThreshold(long clusterSchemaLimitThreshold) {
    setProperty("cluster_timeseries_limit_threshold", String.valueOf(clusterSchemaLimitThreshold));
    return this;
  }

  @Override
  public CommonConfig setClusterDeviceLimitThreshold(long clusterDeviceLimitThreshold) {
    setProperty("cluster_device_limit_threshold", String.valueOf(clusterDeviceLimitThreshold));
    return this;
  }

  @Override
  public CommonConfig setDatabaseLimitThreshold(long databaseLimitThreshold) {
    setProperty("database_limit_threshold", String.valueOf(databaseLimitThreshold));
    return this;
  }

  @Override
  public CommonConfig setDataRegionPerDataNode(double dataRegionPerDataNode) {
    setProperty("data_region_per_data_node", String.valueOf(dataRegionPerDataNode));
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionPerDataNode(double schemaRegionPerDataNode) {
    setProperty("schema_region_per_data_node", String.valueOf(schemaRegionPerDataNode));
    return this;
  }

  @Override
  public CommonConfig setPipeAirGapReceiverEnabled(boolean isPipeAirGapReceiverEnabled) {
    setProperty("pipe_air_gap_receiver_enabled", String.valueOf(isPipeAirGapReceiverEnabled));
    return this;
  }

  @Override
  public CommonConfig setDriverTaskExecutionTimeSliceInMs(long driverTaskExecutionTimeSliceInMs) {
    setProperty(
        "driver_task_execution_time_slice_in_ms", String.valueOf(driverTaskExecutionTimeSliceInMs));
    return this;
  }

  @Override
  public CommonConfig setWalMode(String walMode) {
    setProperty("wal_mode", walMode);
    return this;
  }

  @Override
  public CommonConfig setTagAttributeTotalSize(int tagAttributeTotalSize) {
    setProperty("tag_attribute_total_size", String.valueOf(tagAttributeTotalSize));
    return this;
  }

  @Override
  public CommonConfig setCnConnectionTimeoutMs(int connectionTimeoutMs) {
    setProperty("cn_connection_timeout_ms", String.valueOf(connectionTimeoutMs));
    return this;
  }

  @Override
  public CommonConfig setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
      int pipeHeartbeatIntervalSecondsForCollectingPipeMeta) {
    setProperty(
        "pipe_heartbeat_interval_seconds_for_collecting_pipe_meta",
        String.valueOf(pipeHeartbeatIntervalSecondsForCollectingPipeMeta));
    return this;
  }

  @Override
  public CommonConfig setPipeMetaSyncerInitialSyncDelayMinutes(
      long pipeMetaSyncerInitialSyncDelayMinutes) {
    setProperty(
        "pipe_meta_syncer_initial_sync_delay_minutes",
        String.valueOf(pipeMetaSyncerInitialSyncDelayMinutes));
    return this;
  }

  @Override
  public CommonConfig setPipeMetaSyncerSyncIntervalMinutes(long pipeMetaSyncerSyncIntervalMinutes) {
    setProperty(
        "pipe_meta_syncer_sync_interval_minutes",
        String.valueOf(pipeMetaSyncerSyncIntervalMinutes));
    return this;
  }

  // For part of the log directory
  public String getClusterConfigStr() {
    return fromConsensusFullNameToAbbr(properties.getProperty(CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS))
        + HYPHEN
        + fromConsensusFullNameToAbbr(
            properties.getProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS))
        + HYPHEN
        + fromConsensusFullNameToAbbr(properties.getProperty(DATA_REGION_CONSENSUS_PROTOCOL_CLASS))
        + HYPHEN
        + properties.getProperty(SCHEMA_REPLICATION_FACTOR)
        + HYPHEN
        + properties.getProperty(DATA_REPLICATION_FACTOR);
  }
}
