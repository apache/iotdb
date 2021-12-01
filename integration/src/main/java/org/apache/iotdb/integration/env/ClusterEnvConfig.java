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
  private final Properties properties;

  public ClusterEnvConfig() {
    properties = new Properties();
  }

  public void clearProperties() {
    properties.clear();
  }

  public Properties getProperties() {
    return this.properties;
  }

  public BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    properties.setProperty("max_number_of_points_in_page", String.valueOf(maxNumberOfPointsInPage));
    return this;
  }

  public BaseConfig setPageSizeInByte(int pageSizeInByte) {
    properties.setProperty("page_size_in_byte", String.valueOf(pageSizeInByte));
    return this;
  }

  public BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    properties.setProperty("group_size_in_byte", String.valueOf(groupSizeInByte));
    return this;
  }

  public BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    properties.setProperty("memtable_size_threshold", String.valueOf(memtableSizeThreshold));
    return this;
  }

  public BaseConfig setVirtualStorageGroupNum(int virtualStorageGroupNum) {
    properties.setProperty("virtual_storage_group_num", String.valueOf(virtualStorageGroupNum));
    return this;
  }

  public BaseConfig setPartitionInterval(long partitionInterval) {
    properties.setProperty("partition_interval", String.valueOf(partitionInterval));
    return this;
  }

  public BaseConfig setCompressor(String compressor) {
    properties.setProperty("compressor", compressor);
    return this;
  }

  public BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    properties.setProperty(
        "max_deduplicated_path_num", String.valueOf(maxQueryDeduplicatedPathNum));
    return this;
  }

  public BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    properties.setProperty(
        "rpc_thrift_compression_enable", String.valueOf(rpcThriftCompressionEnable));
    return this;
  }

  public BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    properties.setProperty(
        "rpc_advanced_compression_enable", String.valueOf(rpcAdvancedCompressionEnable));
    return this;
  }

  public BaseConfig setEnablePartition(boolean enablePartition) {
    properties.setProperty("enable_partition", String.valueOf(enablePartition));
    return this;
  }

  public BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    // udf_memory_budget_in_mb
    // udf_reader_transformer_collector_memory_proportion
    properties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfCollectorMemoryBudgetInMB * 3));
    return this;
  }

  public BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    properties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfTransformerMemoryBudgetInMB * 3));
    return this;
  }

  public BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    properties.setProperty(
        "udf_memory_budget_in_mb", String.valueOf(udfReaderMemoryBudgetInMB * 3));
    return this;
  }
}
