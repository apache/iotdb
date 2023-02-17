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
package org.apache.iotdb.integration.env;

import org.apache.iotdb.itbase.env.BaseConfig;

import java.util.Properties;

public class ClusterEnvConfig implements BaseConfig {
  private final Properties engineProperties;
  private final Properties clusterProperties;

  public ClusterEnvConfig() {
    engineProperties = new Properties();
    clusterProperties = new Properties();
  }

  public void clearAllProperties() {
    engineProperties.clear();
    clusterProperties.clear();
  }

  public Properties getEngineProperties() {
    return this.engineProperties;
  }

  public Properties getClusterProperties() {
    return this.clusterProperties;
  }

  public BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    engineProperties.setProperty(
        "max_number_of_points_in_page", String.valueOf(maxNumberOfPointsInPage));
    return this;
  }

  public BaseConfig setPageSizeInByte(int pageSizeInByte) {
    engineProperties.setProperty("page_size_in_byte", String.valueOf(pageSizeInByte));
    return this;
  }

  public BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    engineProperties.setProperty("group_size_in_byte", String.valueOf(groupSizeInByte));
    return this;
  }

  public BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    engineProperties.setProperty("memtable_size_threshold", String.valueOf(memtableSizeThreshold));
    return this;
  }

  public BaseConfig setDataRegionNum(int dataRegionNum) {
    engineProperties.setProperty("data_region_num", String.valueOf(dataRegionNum));
    return this;
  }

  public BaseConfig setPartitionInterval(long partitionInterval) {
    engineProperties.setProperty("time_partition_interval", String.valueOf(partitionInterval));
    return this;
  }

  public BaseConfig setCompressor(String compressor) {
    engineProperties.setProperty("compressor", compressor);
    return this;
  }

  public BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    engineProperties.setProperty(
        "max_deduplicated_path_num", String.valueOf(maxQueryDeduplicatedPathNum));
    return this;
  }

  public BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    engineProperties.setProperty(
        "rpc_thrift_compression_enable", String.valueOf(rpcThriftCompressionEnable));
    return this;
  }

  public BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    engineProperties.setProperty(
        "rpc_advanced_compression_enable", String.valueOf(rpcAdvancedCompressionEnable));
    return this;
  }

  public BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    // udf_memory_budget_in_mb
    // udf_reader_transformer_collector_memory_proportion
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfCollectorMemoryBudgetInMB * 3));
    return this;
  }

  public BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfTransformerMemoryBudgetInMB * 3));
    return this;
  }

  public BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    engineProperties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfReaderMemoryBudgetInMB * 3));
    return this;
  }

  public BaseConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    engineProperties.setProperty(
        "enable_seq_space_compaction", String.valueOf(enableSeqSpaceCompaction));
    return this;
  }

  public BaseConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    engineProperties.setProperty(
        "enable_unseq_space_compaction", String.valueOf(enableUnseqSpaceCompaction));
    return this;
  }

  public BaseConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    engineProperties.setProperty(
        "enable_cross_space_compaction", String.valueOf(enableCrossSpaceCompaction));
    return this;
  }

  public BaseConfig setEnableIDTable(boolean isEnableIDTable) {
    engineProperties.setProperty("enable_id_table", String.valueOf(isEnableIDTable));
    return this;
  }

  public BaseConfig setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    engineProperties.setProperty("device_id_transformation_method", deviceIDTransformationMethod);
    return this;
  }

  public BaseConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    clusterProperties.setProperty(
        "enable_auto_create_schema", String.valueOf(enableAutoCreateSchema));
    return this;
  }
}
