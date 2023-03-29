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
package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.itbase.env.CommonConfig;

import java.io.IOException;

public class MppCommonConfig extends MppBaseConfig implements CommonConfig {

  public MppCommonConfig() {
    super();
  }

  public MppCommonConfig(String filePath) throws IOException {
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
  public CommonConfig setEnableDataPartitionInheritPolicy(
      boolean enableDataPartitionInheritPolicy) {
    setProperty(
        "enable_data_partition_inherit_policy", String.valueOf(enableDataPartitionInheritPolicy));
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
  public CommonConfig setEnableWatermark(boolean enableWatermark) {
    setProperty("watermark_module_opened", String.valueOf(enableWatermark));
    return this;
  }

  @Override
  public CommonConfig setWatermarkSecretKey(String watermarkSecretKey) {
    setProperty("watermark_secret_key", watermarkSecretKey);
    return this;
  }

  @Override
  public CommonConfig setWatermarkBitString(String watermarkBitString) {
    setProperty("watermark_bit_string", watermarkBitString);
    return this;
  }

  @Override
  public CommonConfig setWatermarkMethod(String watermarkMethod) {
    setProperty("watermark_method", watermarkMethod);
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
    setProperty("schema_memory_allocate_proportion", String.valueOf(schemaMemoryAllocate));
    return this;
  }

  @Override
  public CommonConfig setWriteMemoryProportion(String writeMemoryProportion) {
    setProperty("write_memory_proportion", writeMemoryProportion);
    return this;
  }
}
