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

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.consensus.ConsensusProtocolClass;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.loadbalance.LeaderDistributionPolicy;
import org.apache.iotdb.commons.loadbalance.RegionGroupExtensionPolicy;
import org.apache.iotdb.commons.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class CommonDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonDescriptor.class);

  private final CommonConfig CONF = new CommonConfig();

  private CommonDescriptor() {
    // Empty constructor
  }

  public static CommonDescriptor getInstance() {
    return CommonDescriptorHolder.INSTANCE;
  }

  private static class CommonDescriptorHolder {

    private static final CommonDescriptor INSTANCE = new CommonDescriptor();

    private CommonDescriptorHolder() {
      // Empty constructor
    }
  }

  public CommonConfig getConfig() {
    return CONF;
  }

  public void initCommonConfigDir(String systemDir) {
    CONF.setUserFolder(systemDir + File.separator + "users");
    CONF.setRoleFolder(systemDir + File.separator + "roles");
    CONF.setProcedureWalFolder(systemDir + File.separator + "procedure");
  }

  private void loadCommonProps(Properties properties) {

    /* Cluster Configuration */
    CONF.setClusterName(
        properties.getProperty(IoTDBConstant.CLUSTER_NAME, CONF.getClusterName()).trim());

    /* Replication configuration */
    try {
      CONF.setConfigNodeConsensusProtocolClass(
          ConsensusProtocolClass.parse(
              properties
                  .getProperty(
                      "config_node_consensus_protocol_class",
                      CONF.getConfigNodeConsensusProtocolClass().getProtocol())
                  .trim()));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown config_node_consensus_protocol_class in iotdb-common.properties file, use default config",
          e);
    }

    CONF.setSchemaReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_replication_factor", String.valueOf(CONF.getSchemaReplicationFactor()))
                .trim()));

    try {
      CONF.setSchemaRegionConsensusProtocolClass(
          ConsensusProtocolClass.parse(
              properties
                  .getProperty(
                      "schema_region_consensus_protocol_class",
                      CONF.getSchemaRegionConsensusProtocolClass().getProtocol())
                  .trim()));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown schema_region_consensus_protocol_class in iotdb-common.properties file, use default config",
          e);
    }

    CONF.setDataReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_replication_factor", String.valueOf(CONF.getDataReplicationFactor()))
                .trim()));

    try {
      CONF.setDataRegionConsensusProtocolClass(
          ConsensusProtocolClass.parse(
              properties
                  .getProperty(
                      "data_region_consensus_protocol_class",
                      CONF.getDataRegionConsensusProtocolClass().getProtocol())
                  .trim()));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown data_region_consensus_protocol_class in iotdb-common.properties file, use default config",
          e);
    }

    /* Load balancing configuration */
    CONF.setSeriesSlotNum(
        Integer.parseInt(
            properties
                .getProperty("series_slot_num", String.valueOf(CONF.getSeriesSlotNum()))
                .trim()));

    CONF.setSeriesPartitionExecutorClass(
        properties
            .getProperty("series_partition_executor_class", CONF.getSeriesPartitionExecutorClass())
            .trim());

    CONF.setSchemaRegionPerDataNode(
        Double.parseDouble(
            properties
                .getProperty(
                    "schema_region_per_data_node",
                    String.valueOf(CONF.getSchemaReplicationFactor()))
                .trim()));

    CONF.setDataRegionPerProcessor(
        Double.parseDouble(
            properties
                .getProperty(
                    "data_region_per_processor", String.valueOf(CONF.getDataRegionPerProcessor()))
                .trim()));

    try {
      CONF.setSchemaRegionGroupExtensionPolicy(
          RegionGroupExtensionPolicy.parse(
              properties.getProperty(
                  "schema_region_group_extension_policy",
                  CONF.getSchemaRegionGroupExtensionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown schema_region_group_extension_policy in iotdb-common.properties file, use default config",
          e);
    }

    CONF.setSchemaRegionGroupPerDatabase(
        Integer.parseInt(
            properties.getProperty(
                "schema_region_group_per_database",
                String.valueOf(CONF.getSchemaRegionGroupPerDatabase()).trim())));

    try {
      CONF.setDataRegionGroupExtensionPolicy(
          RegionGroupExtensionPolicy.parse(
              properties.getProperty(
                  "data_region_group_extension_policy",
                  CONF.getDataRegionGroupExtensionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown data_region_group_extension_policy in iotdb-common.properties file, use default config",
          e);
    }

    CONF.setDataRegionGroupPerDatabase(
        Integer.parseInt(
            properties.getProperty(
                "data_region_group_per_database",
                String.valueOf(CONF.getDataRegionGroupPerDatabase()).trim())));

    CONF.setLeastDataRegionGroupNum(
        Integer.parseInt(
            properties.getProperty(
                "least_data_region_group_num", String.valueOf(CONF.getLeastDataRegionGroupNum()))));

    CONF.setEnableDataPartitionInheritPolicy(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_data_partition_inherit_policy",
                String.valueOf(CONF.isEnableDataPartitionInheritPolicy()))));

    try {
      CONF.setLeaderDistributionPolicy(
          LeaderDistributionPolicy.parse(
              properties.getProperty(
                  "leader_distribution_policy",
                  CONF.getLeaderDistributionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn(
          "Unknown leader_distribution_policy in iotdb-common.properties file, use default config",
          e);
    }

    CONF.setEnableAutoLeaderBalanceForRatisConsensus(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_auto_leader_balance_for_ratis_consensus",
                    String.valueOf(CONF.isEnableAutoLeaderBalanceForRatisConsensus()))
                .trim()));

    CONF.setEnableAutoLeaderBalanceForIoTConsensus(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_auto_leader_balance_for_iot_consensus",
                    String.valueOf(CONF.isEnableAutoLeaderBalanceForIoTConsensus()))
                .trim()));

    /* Cluster management */
    CONF.setTimePartitionInterval(
        Long.parseLong(
            properties
                .getProperty(
                    "time_partition_interval", String.valueOf(CONF.getTimePartitionInterval()))
                .trim()));

    CONF.setHeartbeatIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "heartbeat_interval_in_ms", String.valueOf(CONF.getHeartbeatIntervalInMs()))
                .trim()));

    CONF.setDiskSpaceWarningThreshold(
        Double.parseDouble(
            properties
                .getProperty(
                    "disk_space_warning_threshold",
                    String.valueOf(CONF.getDiskSpaceWarningThreshold()))
                .trim()));

    /* Schema Engine Configuration */
    CONF.setCoordinatorReadExecutorSize(
        Integer.parseInt(
            properties.getProperty(
                "coordinator_read_executor_size",
                Integer.toString(CONF.getCoordinatorReadExecutorSize()))));

    CONF.setCoordinatorWriteExecutorSize(
        Integer.parseInt(
            properties.getProperty(
                "coordinator_write_executor_size",
                Integer.toString(CONF.getCoordinatorWriteExecutorSize()))));

    CONF.setPartitionCacheSize(
        Integer.parseInt(
            properties.getProperty(
                "partition_cache_size", Integer.toString(CONF.getPartitionCacheSize()))));

    int mlogBufferSize =
        Integer.parseInt(
            properties.getProperty("mlog_buffer_size", Integer.toString(CONF.getMlogBufferSize())));
    if (mlogBufferSize > 0) {
      CONF.setMlogBufferSize(mlogBufferSize);
    }

    long forceMlogPeriodInMs =
        Long.parseLong(
            properties.getProperty(
                "sync_mlog_period_in_ms", Long.toString(CONF.getSyncMlogPeriodInMs())));
    if (forceMlogPeriodInMs > 0) {
      CONF.setSyncMlogPeriodInMs(forceMlogPeriodInMs);
    }

    CONF.setTagAttributeFlushInterval(
        Integer.parseInt(
            properties.getProperty(
                "tag_attribute_flush_interval",
                String.valueOf(CONF.getTagAttributeFlushInterval()))));

    CONF.setTagAttributeTotalSize(
        Integer.parseInt(
            properties.getProperty(
                "tag_attribute_total_size", String.valueOf(CONF.getTagAttributeTotalSize()))));

    CONF.setMaxMeasurementNumOfInternalRequest(
        Integer.parseInt(
            properties.getProperty(
                "max_measurement_num_of_internal_request",
                String.valueOf(CONF.getMaxMeasurementNumOfInternalRequest()))));

    /* Configurations for creating schema automatically */
    loadAutoCreateSchemaProps(properties);

    /* Query Configurations */

    /* Storage Engine Configuration */
    CONF.setTimestampPrecision(
        properties.getProperty("timestamp_precision", CONF.getTimestampPrecision()).trim());

    CONF.setDefaultTtlInMs(
        Long.parseLong(
            properties
                .getProperty("default_ttl_in_ms", String.valueOf(CONF.getDefaultTtlInMs()))
                .trim()));

    CONF.setMaxWaitingTimeWhenInsertBlockedInMs(
        Integer.parseInt(
            properties.getProperty(
                "max_waiting_time_when_insert_blocked",
                Integer.toString(CONF.getMaxWaitingTimeWhenInsertBlockedInMs()))));

    CONF.setEnableDiscardOutOfOrderData(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_discard_out_of_order_data",
                Boolean.toString(CONF.isEnableDiscardOutOfOrderData()))));

    CONF.setHandleSystemErrorStrategy(
        HandleSystemErrorStrategy.valueOf(
            properties
                .getProperty(
                    "handle_system_error", String.valueOf(CONF.getHandleSystemErrorStrategy()))
                .trim()));

    long memTableSizeThreshold =
        Long.parseLong(
            properties
                .getProperty(
                    "memtable_size_threshold", Long.toString(CONF.getMemtableSizeThreshold()))
                .trim());
    if (memTableSizeThreshold > 0) {
      CONF.setMemtableSizeThreshold(memTableSizeThreshold);
    }

    loadTimedService(properties);

    CONF.setTvListSortAlgorithm(
        TVListSortAlgorithm.valueOf(
            properties.getProperty(
                "tvlist_sort_algorithm", CONF.getTvListSortAlgorithm().toString())));

    CONF.setAvgSeriesPointNumberThreshold(
        Integer.parseInt(
            properties.getProperty(
                "avg_series_point_number_threshold",
                Integer.toString(CONF.getAvgSeriesPointNumberThreshold()))));

    CONF.setFlushThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "flush_thread_count", Integer.toString(CONF.getFlushThreadCount()))));
    if (CONF.getFlushThreadCount() <= 0) {
      CONF.setFlushThreadCount(Runtime.getRuntime().availableProcessors());
    }

    CONF.setEnablePartialInsert(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_partial_insert", String.valueOf(CONF.isEnablePartialInsert()))));

    CONF.setRecoveryLogIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "recovery_log_interval_in_ms", String.valueOf(CONF.getRecoveryLogIntervalInMs()))));

    CONF.setUpgradeThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "upgrade_thread_count", Integer.toString(CONF.getUpgradeThreadCount()))));

    String readConsistencyLevel =
        properties.getProperty("read_consistency_level", CONF.getReadConsistencyLevel()).trim();
    if (readConsistencyLevel.equals("strong") || readConsistencyLevel.equals("weak")) {
      CONF.setReadConsistencyLevel(readConsistencyLevel);
    } else {
      LOGGER.warn(
          String.format(
              "Unknown read_consistency_level: %s, please set to \"strong\" or \"weak\"",
              readConsistencyLevel));
    }

    CONF.setAuthorizerProvider(
        properties.getProperty("authorizer_provider_class", CONF.getAuthorizerProvider()).trim());

    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    CONF.setOpenIdProviderUrl(
        properties.getProperty("openID_url", CONF.getOpenIdProviderUrl()).trim());

    CONF.setAdminName(properties.getProperty("admin_name", CONF.getAdminName()).trim());

    CONF.setAdminPassword(properties.getProperty("admin_password", CONF.getAdminPassword()).trim());

    CONF.setEncryptDecryptProvider(
        properties
            .getProperty("iotdb_server_encrypt_decrypt_provider", CONF.getEncryptDecryptProvider())
            .trim());

    CONF.setEncryptDecryptProviderParameter(
        properties.getProperty(
            "iotdb_server_encrypt_decrypt_provider_parameter",
            CONF.getEncryptDecryptProviderParameter()));

    CONF.setUdfDir(properties.getProperty("udf_lib_dir", CONF.getUdfDir()).trim());

    CONF.setTriggerDir(properties.getProperty("trigger_lib_dir", CONF.getTriggerDir()).trim());

    CONF.setSyncDir(properties.getProperty("dn_sync_dir", CONF.getSyncDir()).trim());

    CONF.setWalDirs(
        properties
            .getProperty("dn_wal_dirs", String.join(",", CONF.getWalDirs()))
            .trim()
            .split(","));
  }

  private void loadAutoCreateSchemaProps(Properties properties) {

    CONF.setEnableAutoCreateSchema(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_auto_create_schema",
                Boolean.toString(CONF.isEnableAutoCreateSchema()).trim())));

    CONF.setDefaultStorageGroupLevel(
        Integer.parseInt(
            properties.getProperty(
                "default_storage_group_level",
                Integer.toString(CONF.getDefaultStorageGroupLevel()))));

    CONF.setBooleanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "boolean_string_infer_type", CONF.getBooleanStringInferType().toString())));
    CONF.setIntegerStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "integer_string_infer_type", CONF.getIntegerStringInferType().toString())));
    CONF.setLongStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "long_string_infer_type", CONF.getLongStringInferType().toString())));
    CONF.setFloatingStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "floating_string_infer_type", CONF.getFloatingStringInferType().toString())));
    CONF.setNanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "nan_string_infer_type", CONF.getNanStringInferType().toString())));

    CONF.setDefaultBooleanEncoding(
        properties.getProperty(
            "default_boolean_encoding", CONF.getDefaultBooleanEncoding().toString()));
    CONF.setDefaultInt32Encoding(
        properties.getProperty(
            "default_int32_encoding", CONF.getDefaultInt32Encoding().toString()));
    CONF.setDefaultInt64Encoding(
        properties.getProperty(
            "default_int64_encoding", CONF.getDefaultInt64Encoding().toString()));
    CONF.setDefaultFloatEncoding(
        properties.getProperty(
            "default_float_encoding", CONF.getDefaultFloatEncoding().toString()));
    CONF.setDefaultDoubleEncoding(
        properties.getProperty(
            "default_double_encoding", CONF.getDefaultDoubleEncoding().toString()));
    CONF.setDefaultTextEncoding(
        properties.getProperty("default_text_encoding", CONF.getDefaultTextEncoding().toString()));
  }

  // Timed flush memtable
  private void loadTimedService(Properties properties) {
    CONF.setEnableTimedFlushSeqMemtable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_flush_seq_memtable",
                Boolean.toString(CONF.isEnableTimedFlushSeqMemtable()))));

    long seqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_interval_in_ms",
                    Long.toString(CONF.getSeqMemtableFlushInterval()))
                .trim());
    if (seqMemTableFlushInterval > 0) {
      CONF.setSeqMemtableFlushInterval(seqMemTableFlushInterval);
    }

    long seqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_check_interval_in_ms",
                    Long.toString(CONF.getSeqMemtableFlushCheckInterval()))
                .trim());
    if (seqMemTableFlushCheckInterval > 0) {
      CONF.setSeqMemtableFlushCheckInterval(seqMemTableFlushCheckInterval);
    }

    CONF.setEnableTimedFlushUnseqMemtable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_flush_unseq_memtable",
                Boolean.toString(CONF.isEnableTimedFlushUnseqMemtable()))));

    long unseqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_interval_in_ms",
                    Long.toString(CONF.getUnseqMemtableFlushInterval()))
                .trim());
    if (unseqMemTableFlushInterval > 0) {
      CONF.setUnseqMemtableFlushInterval(unseqMemTableFlushInterval);
    }

    long unseqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_check_interval_in_ms",
                    Long.toString(CONF.getUnseqMemtableFlushCheckInterval()))
                .trim());
    if (unseqMemTableFlushCheckInterval > 0) {
      CONF.setUnseqMemtableFlushCheckInterval(unseqMemTableFlushCheckInterval);
    }
  }

  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    CONF.setDiskSpaceWarningThreshold(globalConfig.getDiskSpaceWarningThreshold());
  }
}
