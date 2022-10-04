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
package org.apache.iotdb.it.env;

import org.apache.iotdb.itbase.env.BaseConfig;

import java.util.Properties;

public class MppConfig implements BaseConfig {
  private final Properties engineProperties;
  private final Properties confignodeProperties;

  public MppConfig() {
    engineProperties = new Properties();
    confignodeProperties = new Properties();
  }

  @Override
  public void clearAllProperties() {
    engineProperties.clear();
    confignodeProperties.clear();
  }

  @Override
  public Properties getEngineProperties() {
    return this.engineProperties;
  }

  @Override
  public Properties getConfignodeProperties() {
    return this.confignodeProperties;
  }

  @Override
  public BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    engineProperties.setProperty(
        "max_number_of_points_in_page", String.valueOf(maxNumberOfPointsInPage));
    return this;
  }

  @Override
  public BaseConfig setPageSizeInByte(int pageSizeInByte) {
    engineProperties.setProperty("page_size_in_byte", String.valueOf(pageSizeInByte));
    return this;
  }

  @Override
  public BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    engineProperties.setProperty("group_size_in_byte", String.valueOf(groupSizeInByte));
    return this;
  }

  @Override
  public BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    engineProperties.setProperty("memtable_size_threshold", String.valueOf(memtableSizeThreshold));
    return this;
  }

  @Override
  public BaseConfig setDataRegionNum(int dataRegionNum) {
    engineProperties.setProperty("data_region_num", String.valueOf(dataRegionNum));
    return this;
  }

  @Override
  public BaseConfig setPartitionInterval(long partitionInterval) {
    engineProperties.setProperty("partition_interval", String.valueOf(partitionInterval));
    return this;
  }

  @Override
  public BaseConfig setTimePartitionIntervalForStorage(long partitionInterval) {
    engineProperties.setProperty(
        "time_partition_interval_for_storage", String.valueOf(partitionInterval));
    return this;
  }

  @Override
  public BaseConfig setCompressor(String compressor) {
    engineProperties.setProperty("compressor", compressor);
    return this;
  }

  @Override
  public BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    engineProperties.setProperty(
        "max_deduplicated_path_num", String.valueOf(maxQueryDeduplicatedPathNum));
    return this;
  }

  @Override
  public BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    engineProperties.setProperty(
        "rpc_thrift_compression_enable", String.valueOf(rpcThriftCompressionEnable));
    return this;
  }

  @Override
  public BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    engineProperties.setProperty(
        "rpc_advanced_compression_enable", String.valueOf(rpcAdvancedCompressionEnable));
    return this;
  }

  @Override
  public BaseConfig setEnablePartition(boolean enablePartition) {
    engineProperties.setProperty("enable_partition", String.valueOf(enablePartition));
    return this;
  }

  @Override
  public BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    // udf_memory_budget_in_mb
    // udf_reader_transformer_collector_memory_proportion
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfCollectorMemoryBudgetInMB * 3));
    return this;
  }

  @Override
  public BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfTransformerMemoryBudgetInMB * 3));
    return this;
  }

  @Override
  public BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfReaderMemoryBudgetInMB * 3));
    return this;
  }

  @Override
  public BaseConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    engineProperties.setProperty(
        "enable_seq_space_compaction", String.valueOf(enableSeqSpaceCompaction));
    return this;
  }

  @Override
  public BaseConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    engineProperties.setProperty(
        "enable_unseq_space_compaction", String.valueOf(enableUnseqSpaceCompaction));
    return this;
  }

  @Override
  public BaseConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    engineProperties.setProperty(
        "enable_cross_space_compaction", String.valueOf(enableCrossSpaceCompaction));
    return this;
  }

  @Override
  public BaseConfig setEnableIDTable(boolean isEnableIDTable) {
    engineProperties.setProperty("enable_id_table", String.valueOf(isEnableIDTable));
    return this;
  }

  @Override
  public BaseConfig setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    engineProperties.setProperty("device_id_transformation_method", deviceIDTransformationMethod);
    return this;
  }

  @Override
  public BaseConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    engineProperties.setProperty(
        "enable_auto_create_schema", String.valueOf(enableAutoCreateSchema));
    return this;
  }

  @Override
  public BaseConfig setEnableLastCache(boolean lastCacheEnable) {
    engineProperties.setProperty("enable_last_cache", String.valueOf(lastCacheEnable));
    return this;
  }

  @Override
  public BaseConfig setPrimitiveArraySize(int primitiveArraySize) {
    engineProperties.setProperty("primitive_array_size", String.valueOf(primitiveArraySize));
    return this;
  }

  @Override
  public BaseConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    engineProperties.setProperty(
        "avg_series_point_number_threshold", String.valueOf(avgSeriesPointNumberThreshold));
    return this;
  }

  @Override
  public BaseConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    engineProperties.setProperty("max_tsblock_line_number", String.valueOf(maxTsBlockLineNumber));
    return this;
  }

  @Override
  public BaseConfig setConfigNodeConsesusProtocolClass(String configNodeConsesusProtocolClass) {
    confignodeProperties.setProperty(
        "config_node_consensus_protocol_class", configNodeConsesusProtocolClass);
    return this;
  }

  @Override
  public BaseConfig setSchemaRegionConsensusProtocolClass(
      String schemaRegionConsensusProtocolClass) {
    confignodeProperties.setProperty(
        "schema_region_consensus_protocol_class", schemaRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public BaseConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    confignodeProperties.setProperty(
        "data_region_consensus_protocol_class", dataRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public BaseConfig setSchemaReplicationFactor(int schemaReplicationFactor) {
    confignodeProperties.setProperty(
        "schema_replication_factor", String.valueOf(schemaReplicationFactor));
    return this;
  }

  @Override
  public BaseConfig setDataReplicationFactor(int dataReplicationFactor) {
    confignodeProperties.setProperty(
        "data_replication_factor", String.valueOf(dataReplicationFactor));
    return this;
  }

  @Override
  public BaseConfig setTimePartitionIntervalForRouting(long timePartitionInterval) {
    confignodeProperties.setProperty(
        "time_partition_interval_for_routing", String.valueOf(timePartitionInterval));
    return this;
  }

  @Override
  public BaseConfig setEnableMemControl(boolean enableMemControl) {
    confignodeProperties.setProperty("enable_mem_control", String.valueOf(enableMemControl));
    return this;
  }

  @Override
  public BaseConfig setRatisSnapshotTriggerThreshold(int ratisSnapshotTriggerThreshold) {
    confignodeProperties.setProperty(
        "ratis_snapshot_trigger_threshold", String.valueOf(ratisSnapshotTriggerThreshold));
    return this;
  }

  @Override
  public BaseConfig setConcurrentCompactionThread(int concurrentCompactionThread) {
    confignodeProperties.setProperty(
        "concurrent_compaction_thread", String.valueOf(concurrentCompactionThread));
    return this;
  }

  @Override
  public BaseConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    engineProperties.setProperty("max_degree_of_index_node", String.valueOf(maxDegreeOfIndexNode));
    return this;
  }
}
