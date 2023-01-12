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
import org.apache.iotdb.commons.wal.WALMode;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

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

    /* Memory Control Configuration */
    CONF.setEnableMemControl(
        (Boolean.parseBoolean(
            properties.getProperty(
                "enable_mem_control", Boolean.toString(CONF.isEnableMemControl())))));
    LOGGER.info("IoTDB enable memory control: {}", CONF.isEnableMemControl());
    initMemoryAllocate(properties);

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

    CONF.setMetaDataCacheEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "meta_data_cache_enable", Boolean.toString(CONF.isMetaDataCacheEnable()))
                .trim()));

    String queryMemoryAllocateProportion =
        properties.getProperty("chunk_timeseriesmeta_free_memory_proportion");
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = CONF.getAllocateMemoryForRead();
      if (proportionSum != 0) {
        try {
          CONF.setAllocateMemoryForBloomFilterCache(
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
          CONF.setAllocateMemoryForChunkCache(
              maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
          CONF.setAllocateMemoryForTimeSeriesMetaDataCache(
              maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
          CONF.setAllocateMemoryForCoordinator(
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
          CONF.setAllocateMemoryForOperators(
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum);
          CONF.setAllocateMemoryForDataExchange(
              maxMemoryAvailable * Integer.parseInt(proportions[5].trim()) / proportionSum);
          CONF.setAllocateMemoryForTimeIndex(
              maxMemoryAvailable * Integer.parseInt(proportions[6].trim()) / proportionSum);
        } catch (Exception e) {
          throw new RuntimeException(
              "Each subsection of configuration item chunkmeta_chunk_timeseriesmeta_free_memory_proportion"
                  + " should be an integer, which is "
                  + queryMemoryAllocateProportion);
        }
      }
    }

    // metadata cache is disabled, we need to move all their allocated memory to other parts
    if (!CONF.isMetaDataCacheEnable()) {
      long sum =
          CONF.getAllocateMemoryForBloomFilterCache()
              + CONF.getAllocateMemoryForChunkCache()
              + CONF.getAllocateMemoryForTimeSeriesMetaDataCache();
      CONF.setAllocateMemoryForBloomFilterCache(0);
      CONF.setAllocateMemoryForChunkCache(0);
      CONF.setAllocateMemoryForTimeSeriesMetaDataCache(0);
      long partForDataExchange = sum / 2;
      long partForOperators = sum - partForDataExchange;
      CONF.setAllocateMemoryForDataExchange(
          CONF.getAllocateMemoryForDataExchange() + partForDataExchange);
      CONF.setAllocateMemoryForOperators(CONF.getAllocateMemoryForOperators() + partForOperators);
    }

    CONF.setEnableLastCache(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_last_cache", Boolean.toString(CONF.isEnableLastCache()))));

    CONF.setMaxDeduplicatedPathNum(
        Integer.parseInt(
            properties.getProperty(
                "max_deduplicated_path_num", Integer.toString(CONF.getMaxDeduplicatedPathNum()))));

    loadShuffleProps(properties);

    CONF.setMaxTsBlockSizeInBytes(
        Integer.parseInt(
            properties.getProperty(
                "max_ts_block_size_in_bytes", Integer.toString(CONF.getMaxTsBlockLineNumber()))));

    CONF.setMaxTsBlockLineNumber(
        Integer.parseInt(
            properties.getProperty(
                "max_ts_block_line_number", Integer.toString(CONF.getMaxTsBlockLineNumber()))));

    CONF.setSlowQueryThreshold(
        Long.parseLong(
            properties.getProperty(
                "slow_query_threshold", String.valueOf(CONF.getSlowQueryThreshold()))));

    CONF.setQueryTimeoutThreshold(
        Long.parseLong(
            properties.getProperty(
                "query_timeout_threshold", Long.toString(CONF.getQueryTimeoutThreshold()))));

    CONF.setMaxAllowedConcurrentQueries(
        Integer.parseInt(
            properties.getProperty(
                "max_allowed_concurrent_queries",
                Integer.toString(CONF.getMaxAllowedConcurrentQueries()))));
    if (CONF.getMaxAllowedConcurrentQueries() <= 0) {
      CONF.setMaxAllowedConcurrentQueries(1000);
    }

    CONF.setQueryThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "query_thread_count", Integer.toString(CONF.getQueryThreadCount()))));
    if (CONF.getQueryThreadCount() <= 0) {
      CONF.setQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    CONF.setBatchSize(
        Integer.parseInt(
            properties.getProperty("batch_size", Integer.toString(CONF.getBatchSize()))));

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

    /* Compaction Configurations */
    // TODO: Move from IoTDBConfig

    /* Write Ahead Log Configuration */
    loadWALProps(properties);

    /* TsFile Configurations */
    loadTsFileProps(properties);

    /* Watermark Configuration */
    CONF.setEnableWatermark(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_watermark", Boolean.toString(CONF.isEnableWatermark()).trim())));
    CONF.setWatermarkSecretKey(
        properties.getProperty("watermark_secret_key", CONF.getWatermarkSecretKey()));
    CONF.setWatermarkBitString(
        properties.getProperty("watermark_bit_string", CONF.getWatermarkBitString()));
    CONF.setWatermarkMethod(properties.getProperty("watermark_method", CONF.getWatermarkMethod()));

    /* Authorization Configuration */
    CONF.setAuthorizerProvider(
        properties.getProperty("authorizer_provider_class", CONF.getAuthorizerProvider()).trim());

    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    CONF.setOpenIdProviderUrl(
        properties.getProperty("openID_url", CONF.getOpenIdProviderUrl()).trim());

    CONF.setAdminName(properties.getProperty("admin_name", CONF.getAdminName()).trim());

    CONF.setAdminPassword(properties.getProperty("admin_password", CONF.getAdminPassword()).trim());

    CONF.setIotdbServerEncryptDecryptProvider(
        properties
            .getProperty(
                "iotdb_server_encrypt_decrypt_provider",
                CONF.getIotdbServerEncryptDecryptProvider())
            .trim());

    CONF.setIotdbServerEncryptDecryptProviderParameter(
        properties.getProperty(
            "iotdb_server_encrypt_decrypt_provider_parameter",
            CONF.getIotdbServerEncryptDecryptProviderParameter()));

    CONF.setAuthorCacheSize(
      Integer.parseInt(
        properties.getProperty(
          "author_cache_size", String.valueOf(CONF.getAuthorCacheSize()))));

    CONF.setAuthorCacheExpireTime(
      Integer.parseInt(
        properties.getProperty(
          "author_cache_expire_time", String.valueOf(CONF.getAuthorCacheExpireTime()))));

    /* UDF Configuration */
    loadUDFProps(properties);

    /* Trigger Configuration */
    loadTriggerProps(properties);

    /* Select-Into Configuration */
    CONF.setSelectIntoInsertTabletPlanRowLimit(
      Integer.parseInt(
        properties.getProperty(
          "select_into_insert_tablet_plan_row_limit",
          String.valueOf(CONF.getSelectIntoInsertTabletPlanRowLimit()))));
    CONF.setIntoOperationExecutionThreadCount(
      Integer.parseInt(
        properties.getProperty(
          "into_operation_execution_thread_count",
          String.valueOf(CONF.getIntoOperationExecutionThreadCount()))));
    if (CONF.getIntoOperationExecutionThreadCount() <= 0) {
      CONF.setIntoOperationExecutionThreadCount(2);
    }

    /* Continuous Query Configuration */
    loadCQConfig(properties);

    /* PIPE Configuration */
    CONF.setIpWhiteList(properties.getProperty("ip_white_list", CONF.getIpWhiteList()));
    CONF.setMaxNumberOfSyncFileRetry(
      Integer.parseInt(
        properties
          .getProperty(
            "max_number_of_sync_file_retry",
            Integer.toString(CONF.getMaxNumberOfSyncFileRetry()))
          .trim()));

    /* RatisConsensus Configuration */
    loadRatisConsensusConfig(properties);

    /* Procedure Configuration */
    CONF.setProcedureCoreWorkerThreadsCount(
      Integer.parseInt(
        properties
          .getProperty(
            "procedure_core_worker_thread_count",
            String.valueOf(CONF.getProcedureCoreWorkerThreadsCount()))
          .trim()));

    CONF.setProcedureCompletedCleanInterval(
      Integer.parseInt(
        properties
          .getProperty(
            "procedure_completed_clean_interval",
            String.valueOf(CONF.getProcedureCompletedCleanInterval()))
          .trim()));

    CONF.setProcedureCompletedEvictTTL(
      Integer.parseInt(
        properties
          .getProperty(
            "procedure_completed_evict_ttl",
            String.valueOf(CONF.getProcedureCompletedEvictTTL()))
          .trim()));

    /* MQTT Broker Configuration */
    loadMqttProps(properties);

    /* REST Service Configuration */
    // TODO: Rest service

    /* InfluxDB RPC Service Configuration */
    CONF.setEnableInfluxDBRpcService(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "enable_influxdb_rpc_service",
            Boolean.toString(CONF.isEnableInfluxDBRpcService()))
          .trim()));

    CONF.setInfluxDBRpcPort(
      Integer.parseInt(
        properties
          .getProperty("influxdb_rpc_port", Integer.toString(CONF.getInfluxDBRpcPort()))
          .trim()));


    CONF.setSyncDir(properties.getProperty("dn_sync_dir", CONF.getSyncDir()).trim());

    CONF.setWalDirs(
        properties
            .getProperty("dn_wal_dirs", String.join(",", CONF.getWalDirs()))
            .trim()
            .split(","));
  }

  private void initMemoryAllocate(Properties properties) {
    String memoryAllocateProportion =
        properties.getProperty("storage_query_schema_consensus_free_memory_proportion");
    if (memoryAllocateProportion != null) {
      String[] proportions = memoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = Runtime.getRuntime().maxMemory();
      if (proportionSum != 0) {
        CONF.setAllocateMemoryForStorageEngine(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        CONF.setAllocateMemoryForRead(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        CONF.setAllocateMemoryForSchema(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
        CONF.setAllocateMemoryForConsensus(
            maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
      }
    }

    LOGGER.info("initial allocateMemoryForRead = {}", CONF.getAllocateMemoryForRead());
    LOGGER.info("initial allocateMemoryForWrite = {}", CONF.getAllocateMemoryForStorageEngine());
    LOGGER.info("initial allocateMemoryForSchema = {}", CONF.getAllocateMemoryForSchema());
    LOGGER.info("initial allocateMemoryForConsensus = {}", CONF.getAllocateMemoryForConsensus());

    initSchemaMemoryAllocate(properties);
    initStorageEngineAllocate(properties);

    CONF.setConcurrentWritingTimePartition(
        Integer.parseInt(
            properties.getProperty(
                "concurrent_writing_time_partition",
                String.valueOf(CONF.getConcurrentWritingTimePartition()))));

    CONF.setPrimitiveArraySize(
        (Integer.parseInt(
            properties.getProperty(
                "primitive_array_size", String.valueOf(CONF.getPrimitiveArraySize())))));

    CONF.setChunkMetadataSizeProportion(
        Double.parseDouble(
            properties.getProperty(
                "chunk_metadata_size_proportion",
                Double.toString(CONF.getChunkMetadataSizeProportion()))));

    CONF.setFlushProportion(
        Double.parseDouble(
            properties
                .getProperty("flush_proportion", Double.toString(CONF.getFlushProportion()))
                .trim()));

    CONF.setBufferedArraysMemoryProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "buffered_arrays_memory_proportion",
                    Double.toString(CONF.getBufferedArraysMemoryProportion()))
                .trim()));

    CONF.setRejectProportion(
        Double.parseDouble(
            properties
                .getProperty("reject_proportion", Double.toString(CONF.getRejectProportion()))
                .trim()));

    CONF.setWriteMemoryVariationReportProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "write_memory_variation_report_proportion",
                    Double.toString(CONF.getWriteMemoryVariationReportProportion()))
                .trim()));

    CONF.setCheckPeriodWhenInsertBlocked(
        Integer.parseInt(
            properties.getProperty(
                "check_period_when_insert_blocked",
                Integer.toString(CONF.getCheckPeriodWhenInsertBlocked()))));

    CONF.setIoTaskQueueSizeForFlushing(
        Integer.parseInt(
            properties.getProperty(
                "io_task_queue_size_for_flushing",
                Integer.toString(CONF.getIoTaskQueueSizeForFlushing()))));

    CONF.setEnableQueryMemoryEstimation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_query_memory_estimation",
                Boolean.toString(CONF.isEnableQueryMemoryEstimation()))));
  }

  private void initSchemaMemoryAllocate(Properties properties) {
    long schemaMemoryTotal = CONF.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 8;
    int schemaCacheProportion = 1;
    int partitionCacheProportion = 0;
    int lastCacheProportion = 1;

    String schemaMemoryAllocatePortion =
        properties.getProperty("schema_memory_allocate_proportion");
    if (schemaMemoryAllocatePortion != null) {
      CONF.setDefaultSchemaMemoryConfig(false);
      String[] proportions = schemaMemoryAllocatePortion.split(":");
      int loadedProportionSum = 0;
      for (String proportion : proportions) {
        loadedProportionSum += Integer.parseInt(proportion.trim());
      }

      if (loadedProportionSum != 0) {
        proportionSum = loadedProportionSum;
        schemaRegionProportion = Integer.parseInt(proportions[0].trim());
        schemaCacheProportion = Integer.parseInt(proportions[1].trim());
        partitionCacheProportion = Integer.parseInt(proportions[2].trim());
        lastCacheProportion = Integer.parseInt(proportions[3].trim());
      }
    } else {
      CONF.setDefaultSchemaMemoryConfig(true);
    }

    CONF.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * schemaRegionProportion / proportionSum);
    LOGGER.info("allocateMemoryForSchemaRegion = {}", CONF.getAllocateMemoryForSchemaRegion());

    CONF.setAllocateMemoryForSchemaCache(schemaMemoryTotal * schemaCacheProportion / proportionSum);
    LOGGER.info("allocateMemoryForSchemaCache = {}", CONF.getAllocateMemoryForSchemaCache());

    CONF.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * partitionCacheProportion / proportionSum);
    LOGGER.info("allocateMemoryForPartitionCache = {}", CONF.getAllocateMemoryForPartitionCache());

    CONF.setAllocateMemoryForLastCache(schemaMemoryTotal * lastCacheProportion / proportionSum);
    LOGGER.info("allocateMemoryForLastCache = {}", CONF.getAllocateMemoryForLastCache());
  }

  private void initStorageEngineAllocate(Properties properties) {
    String allocationRatio = properties.getProperty("storage_engine_memory_proportion", "8:2");
    String[] proportions = allocationRatio.split(":");
    int proportionForWrite = Integer.parseInt(proportions[0].trim());
    int proportionForCompaction = Integer.parseInt(proportions[1].trim());

    double writeProportion =
        ((double) (proportionForWrite) / (double) (proportionForCompaction + proportionForWrite));

    String allocationRatioForWrite = properties.getProperty("write_memory_proportion", "19:1");
    proportions = allocationRatioForWrite.split(":");
    int proportionForMemTable = Integer.parseInt(proportions[0].trim());
    int proportionForTimePartitionInfo = Integer.parseInt(proportions[1].trim());

    double memtableProportionForWrite =
        ((double) (proportionForMemTable)
            / (double) (proportionForMemTable + proportionForTimePartitionInfo));
    double timePartitionInfoForWrite =
        ((double) (proportionForTimePartitionInfo)
            / (double) (proportionForMemTable + proportionForTimePartitionInfo));
    CONF.setWriteProportionForMemtable(writeProportion * memtableProportionForWrite);

    CONF.setAllocateMemoryForTimePartitionInfo(
        (long)
            ((writeProportion * timePartitionInfoForWrite)
                * CONF.getAllocateMemoryForStorageEngine()));

    CONF.setCompactionProportion(
        ((double) (proportionForCompaction)
            / (double) (proportionForCompaction + proportionForWrite)));
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
        TSEncoding.parse(
            properties.getProperty(
                "default_boolean_encoding", CONF.getDefaultBooleanEncoding().toString())));
    CONF.setDefaultInt32Encoding(
        TSEncoding.parse(
            properties.getProperty(
                "default_int32_encoding", CONF.getDefaultInt32Encoding().toString())));
    CONF.setDefaultInt64Encoding(
        TSEncoding.parse(
            properties.getProperty(
                "default_int64_encoding", CONF.getDefaultInt64Encoding().toString())));
    CONF.setDefaultFloatEncoding(
        TSEncoding.parse(
            properties.getProperty(
                "default_float_encoding", CONF.getDefaultFloatEncoding().toString())));
    CONF.setDefaultDoubleEncoding(
        TSEncoding.parse(
            properties.getProperty(
                "default_double_encoding", CONF.getDefaultDoubleEncoding().toString())));
    CONF.setDefaultTextEncoding(
        TSEncoding.parse(
            properties.getProperty(
                "default_text_encoding", CONF.getDefaultTextEncoding().toString())));
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

  public void loadShuffleProps(Properties properties) {
    CONF.setMppDataExchangeCorePoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_core_pool_size",
                Integer.toString(CONF.getMppDataExchangeCorePoolSize()))));
    CONF.setMppDataExchangeMaxPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_max_pool_size",
                Integer.toString(CONF.getMppDataExchangeMaxPoolSize()))));
    CONF.setMppDataExchangeKeepAliveTimeInMs(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_keep_alive_time_in_ms",
                Integer.toString(CONF.getMppDataExchangeKeepAliveTimeInMs()))));

    CONF.setDriverTaskExecutionTimeSliceInMs(
        Integer.parseInt(
            properties.getProperty(
                "driver_task_execution_time_slice_in_ms",
                Integer.toString(CONF.getDriverTaskExecutionTimeSliceInMs()))));
  }

  private void loadWALProps(Properties properties) {
    CONF.setWalMode(
      WALMode.valueOf((properties.getProperty("wal_mode", CONF.getWalMode().toString()))));

    int maxWalNodesNum =
      Integer.parseInt(
        properties.getProperty(
          "max_wal_nodes_num", Integer.toString(CONF.getMaxWalNodesNum())));
    if (maxWalNodesNum > 0) {
      CONF.setMaxWalNodesNum(maxWalNodesNum);
    }

    long fsyncWalDelayInMs =
      Long.parseLong(
        properties.getProperty(
          "fsync_wal_delay_in_ms", Long.toString(CONF.getFsyncWalDelayInMs())));
    if (fsyncWalDelayInMs > 0) {
      CONF.setFsyncWalDelayInMs(fsyncWalDelayInMs);
    }

    int walBufferSize =
      Integer.parseInt(
        properties.getProperty(
          "wal_buffer_size_in_byte", Integer.toString(CONF.getWalBufferSizeInByte())));
    if (walBufferSize > 0) {
      CONF.setWalBufferSizeInByte(walBufferSize);
    }

    int walBufferQueueCapacity =
      Integer.parseInt(
        properties.getProperty(
          "wal_buffer_queue_capacity", Integer.toString(CONF.getWalBufferQueueCapacity())));
    if (walBufferQueueCapacity > 0) {
      CONF.setWalBufferQueueCapacity(walBufferQueueCapacity);
    }

    long walFileSizeThreshold =
      Long.parseLong(
        properties.getProperty(
          "wal_file_size_threshold_in_byte",
          Long.toString(CONF.getWalFileSizeThresholdInByte())));
    if (walFileSizeThreshold > 0) {
      CONF.setWalFileSizeThresholdInByte(walFileSizeThreshold);
    }

    double walMinEffectiveInfoRatio =
      Double.parseDouble(
        properties.getProperty(
          "wal_min_effective_info_ratio",
          Double.toString(CONF.getWalMinEffectiveInfoRatio())));
    if (walMinEffectiveInfoRatio > 0) {
      CONF.setWalMinEffectiveInfoRatio(walMinEffectiveInfoRatio);
    }

    long walMemTableSnapshotThreshold =
      Long.parseLong(
        properties.getProperty(
          "wal_memtable_snapshot_threshold_in_byte",
          Long.toString(CONF.getWalMemTableSnapshotThreshold())));
    if (walMemTableSnapshotThreshold > 0) {
      CONF.setWalMemTableSnapshotThreshold(walMemTableSnapshotThreshold);
    }

    int maxWalMemTableSnapshotNum =
      Integer.parseInt(
        properties.getProperty(
          "max_wal_memtable_snapshot_num",
          Integer.toString(CONF.getMaxWalMemTableSnapshotNum())));
    if (maxWalMemTableSnapshotNum > 0) {
      CONF.setMaxWalMemTableSnapshotNum(maxWalMemTableSnapshotNum);
    }

    long deleteWalFilesPeriod =
      Long.parseLong(
        properties.getProperty(
          "delete_wal_files_period_in_ms",
          Long.toString(CONF.getDeleteWalFilesPeriodInMs())));
    if (deleteWalFilesPeriod > 0) {
      CONF.setDeleteWalFilesPeriodInMs(deleteWalFilesPeriod);
    }

    long throttleDownThresholdInByte =
      Long.parseLong(
        properties.getProperty(
          "iot_consensus_throttle_threshold_in_byte",
          Long.toString(CONF.getIotConsensusThrottleThresholdInByte())));
    if (throttleDownThresholdInByte > 0) {
      CONF.setIotConsensusThrottleThresholdInByte(throttleDownThresholdInByte);
    }

    long cacheWindowInMs =
      Long.parseLong(
        properties.getProperty(
          "iot_consensus_cache_window_time_in_ms",
          Long.toString(CONF.getIotConsensusCacheWindowTimeInMs())));
    if (cacheWindowInMs > 0) {
      CONF.setIotConsensusCacheWindowTimeInMs(cacheWindowInMs);
    }
  }

  private void loadTsFileProps(Properties properties) {
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

    tsFileConfig.setGroupSizeInByte(
        Integer.parseInt(
            properties.getProperty(
                "group_size_in_byte", Integer.toString(tsFileConfig.getGroupSizeInByte()))));

    tsFileConfig.setPageSizeInByte(
        Integer.parseInt(
            properties.getProperty(
                "page_size_in_byte", Integer.toString(tsFileConfig.getPageSizeInByte()))));
    if (tsFileConfig.getPageSizeInByte() > tsFileConfig.getGroupSizeInByte()) {
      LOGGER.warn("page_size is greater than group size, will set it as the same with group size");
      tsFileConfig.setPageSizeInByte(tsFileConfig.getGroupSizeInByte());
    }

    tsFileConfig.setMaxNumberOfPointsInPage(
        Integer.parseInt(
            properties.getProperty(
                "max_number_of_points_in_page",
                Integer.toString(tsFileConfig.getMaxNumberOfPointsInPage()))));

    tsFileConfig.setPatternMatchingThreshold(
        Integer.parseInt(
            properties.getProperty(
                "pattern_matching_threshold",
                String.valueOf(tsFileConfig.getPatternMatchingThreshold()))));

    tsFileConfig.setMaxStringLength(
        Integer.parseInt(
            properties.getProperty(
                "max_string_length", Integer.toString(tsFileConfig.getMaxStringLength()))));

    tsFileConfig.setFloatPrecision(
        Integer.parseInt(
            properties.getProperty(
                "float_precision", Integer.toString(tsFileConfig.getFloatPrecision()))));

    tsFileConfig.setTimeEncoder(
        properties.getProperty("time_encoder", tsFileConfig.getTimeEncoder()));

    tsFileConfig.setValueEncoder(
        properties.getProperty("value_encoder", tsFileConfig.getValueEncoder()));

    tsFileConfig.setCompressor(properties.getProperty("compressor", tsFileConfig.toString()));

    tsFileConfig.setMaxDegreeOfIndexNode(
        Integer.parseInt(
            properties.getProperty(
                "max_degree_of_index_node",
                Integer.toString(tsFileConfig.getMaxDegreeOfIndexNode()))));

    tsFileConfig.setFrequencyIntervalInMinute(
        Integer.parseInt(
            properties.getProperty(
                "frequency_interval_in_minute",
                Integer.toString(tsFileConfig.getFrequencyIntervalInMinute()))));

    TSFileDescriptor.getInstance().overwriteConfigByCustomSettings(properties);
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl =
      properties.getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      CONF.setUdfInitialByteArrayLengthForMemoryControl(
        Integer.parseInt(initialByteArrayLengthForMemoryControl));
    }

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      CONF.setUdfMemoryBudgetInMB(
        (float)
          Math.min(Float.parseFloat(memoryBudgetInMb), 0.2 * CONF.getAllocateMemoryForRead()));
    }

    String readerTransformerCollectorMemoryProportion =
      properties.getProperty("udf_reader_transformer_collector_memory_proportion");
    if (readerTransformerCollectorMemoryProportion != null) {
      String[] proportions = readerTransformerCollectorMemoryProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      float maxMemoryAvailable = CONF.getUdfMemoryBudgetInMB();
      try {
        CONF.setUdfReaderMemoryBudgetInMB(
          maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        CONF.setUdfTransformerMemoryBudgetInMB(
          maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        CONF.setUdfCollectorMemoryBudgetInMB(
          maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
      } catch (Exception e) {
        throw new RuntimeException(
          "Each subsection of configuration item udf_reader_transformer_collector_memory_proportion"
            + " should be an integer, which is "
            + readerTransformerCollectorMemoryProportion);
      }
    }

    CONF.setUdfDir(properties.getProperty("udf_lib_dir", CONF.getUdfDir()));
  }

  private void loadTriggerProps(Properties properties) {
    CONF.setTriggerDir(properties.getProperty("trigger_lib_dir", CONF.getTriggerDir()));

    CONF.setStatefulTriggerRetryNumWhenNotFound(
      Integer.parseInt(
        properties.getProperty(
          "stateful_trigger_retry_num_when_not_found",
          Integer.toString(CONF.getStatefulTriggerRetryNumWhenNotFound()))));

    int tlogBufferSize =
      Integer.parseInt(
        properties.getProperty("tlog_buffer_size", Integer.toString(CONF.getTlogBufferSize())));
    if (tlogBufferSize > 0) {
      CONF.setTlogBufferSize(tlogBufferSize);
    }

    CONF.setTriggerForwardMaxQueueNumber(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_max_queue_number",
          Integer.toString(CONF.getTriggerForwardMaxQueueNumber()))));
    CONF.setTriggerForwardMaxSizePerQueue(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_max_size_per_queue",
          Integer.toString(CONF.getTriggerForwardMaxSizePerQueue()))));
    CONF.setTriggerForwardBatchSize(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_batch_size",
          Integer.toString(CONF.getTriggerForwardBatchSize()))));
    CONF.setTriggerForwardHTTPPoolSize(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_http_pool_size",
          Integer.toString(CONF.getTriggerForwardHTTPPoolSize()))));
    CONF.setTriggerForwardHTTPPOOLMaxPerRoute(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_http_pool_max_per_route",
          Integer.toString(CONF.getTriggerForwardHTTPPOOLMaxPerRoute()))));
    CONF.setTriggerForwardMQTTPoolSize(
      Integer.parseInt(
        properties.getProperty(
          "trigger_forward_mqtt_pool_size",
          Integer.toString(CONF.getTriggerForwardMQTTPoolSize()))));
  }

  private void loadCQConfig(Properties properties) {
    int cqSubmitThread =
      Integer.parseInt(
        properties
          .getProperty(
            "continuous_query_submit_thread_count",
            String.valueOf(CONF.getContinuousQueryThreadCount()))
          .trim());
    if (cqSubmitThread <= 0) {
      LOGGER.warn(
        "continuous_query_submit_thread_count should be greater than 0, but current value is {}, ignore that and use the default value {}",
        cqSubmitThread,
        CONF.getContinuousQueryThreadCount());
      cqSubmitThread = CONF.getContinuousQueryThreadCount();
    }
    CONF.setContinuousQueryThreadCount(cqSubmitThread);

    long cqMinEveryIntervalInMs =
      Long.parseLong(
        properties
          .getProperty(
            "continuous_query_min_every_interval_in_ms",
            String.valueOf(CONF.getContinuousQueryMinEveryIntervalInMs()))
          .trim());
    if (cqMinEveryIntervalInMs <= 0) {
      LOGGER.warn(
        "continuous_query_min_every_interval_in_ms should be greater than 0, but current value is {}, ignore that and use the default value {}",
        cqMinEveryIntervalInMs,
        CONF.getContinuousQueryMinEveryIntervalInMs());
      cqMinEveryIntervalInMs = CONF.getContinuousQueryMinEveryIntervalInMs();
    }
    CONF.setContinuousQueryMinEveryIntervalInMs(cqMinEveryIntervalInMs);
  }

  private void loadRatisConsensusConfig(Properties properties) {
    CONF.setDataRegionRatisConsensusLogAppenderBufferSize(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_log_appender_buffer_size_max",
            String.valueOf(CONF.getDataRegionRatisConsensusLogAppenderBufferSize()))
          .trim()));

    CONF.setConfigNodeRatisConsensusLogAppenderBufferSize(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_log_appender_buffer_size_max",
            String.valueOf(CONF.getConfigNodeRatisConsensusLogAppenderBufferSize()))
          .trim()));

    CONF.setSchemaRegionRatisConsensusLogAppenderBufferSize(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_log_appender_buffer_size_max",
            String.valueOf(CONF.getSchemaRegionRatisConsensusLogAppenderBufferSize()))
          .trim()));

    CONF.setDataRegionRatisSnapshotTriggerThreshold(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_snapshot_trigger_threshold",
            String.valueOf(CONF.getDataRegionRatisSnapshotTriggerThreshold()))
          .trim()));

    CONF.setConfigNodeRatisSnapshotTriggerThreshold(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_snapshot_trigger_threshold",
            String.valueOf(CONF.getConfigNodeRatisSnapshotTriggerThreshold()))
          .trim()));

    CONF.setSchemaRegionRatisSnapshotTriggerThreshold(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_snapshot_trigger_threshold",
            String.valueOf(CONF.getSchemaRegionRatisSnapshotTriggerThreshold()))
          .trim()));

    CONF.setDataRegionRatisLogUnsafeFlushEnable(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "data_region_ratis_log_unsafe_flush_enable",
            String.valueOf(CONF.isDataRegionRatisLogUnsafeFlushEnable()))
          .trim()));

    CONF.setConfigNodeRatisLogUnsafeFlushEnable(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "config_node_ratis_log_unsafe_flush_enable",
            String.valueOf(CONF.isConfigNodeRatisLogUnsafeFlushEnable()))
          .trim()));

    CONF.setSchemaRegionRatisLogUnsafeFlushEnable(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "schema_region_ratis_log_unsafe_flush_enable",
            String.valueOf(CONF.isSchemaRegionRatisLogUnsafeFlushEnable()))
          .trim()));

    CONF.setDataRegionRatisLogSegmentSizeMax(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_log_segment_size_max_in_byte",
            String.valueOf(CONF.getDataRegionRatisLogSegmentSizeMax()))
          .trim()));

    CONF.setConfigNodeRatisLogSegmentSizeMax(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_log_segment_size_max_in_byte",
            String.valueOf(CONF.getConfigNodeRatisLogSegmentSizeMax()))
          .trim()));

    CONF.setSchemaRegionRatisLogSegmentSizeMax(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_log_segment_size_max_in_byte",
            String.valueOf(CONF.getSchemaRegionRatisLogSegmentSizeMax()))
          .trim()));

    CONF.setConfigNodeSimpleConsensusLogSegmentSizeMax(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_simple_consensus_log_segment_size_max_in_byte",
            String.valueOf(CONF.getConfigNodeSimpleConsensusLogSegmentSizeMax()))
          .trim()));

    CONF.setDataRegionRatisGrpcFlowControlWindow(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_grpc_flow_control_window",
            String.valueOf(CONF.getDataRegionRatisGrpcFlowControlWindow()))
          .trim()));

    CONF.setConfigNodeRatisGrpcFlowControlWindow(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_grpc_flow_control_window",
            String.valueOf(CONF.getConfigNodeRatisGrpcFlowControlWindow()))
          .trim()));

    CONF.setSchemaRegionRatisGrpcFlowControlWindow(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_grpc_flow_control_window",
            String.valueOf(CONF.getSchemaRegionRatisGrpcFlowControlWindow()))
          .trim()));

    CONF.setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_rpc_leader_election_timeout_min_ms",
            String.valueOf(CONF.getDataRegionRatisRpcLeaderElectionTimeoutMinMs()))
          .trim()));

    CONF.setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_rpc_leader_election_timeout_min_ms",
            String.valueOf(CONF.getConfigNodeRatisRpcLeaderElectionTimeoutMinMs()))
          .trim()));

    CONF.setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_rpc_leader_election_timeout_min_ms",
            String.valueOf(CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs()))
          .trim()));

    CONF.setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_rpc_leader_election_timeout_max_ms",
            String.valueOf(CONF.getDataRegionRatisRpcLeaderElectionTimeoutMaxMs()))
          .trim()));

    CONF.setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_rpc_leader_election_timeout_max_ms",
            String.valueOf(CONF.getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs()))
          .trim()));

    CONF.setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_rpc_leader_election_timeout_max_ms",
            String.valueOf(CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()))
          .trim()));

    CONF.setConfigNodeRatisRequestTimeoutMs(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_request_timeout_ms",
            String.valueOf(CONF.getConfigNodeRatisRequestTimeoutMs()))
          .trim()));
    CONF.setSchemaRegionRatisRequestTimeoutMs(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_request_timeout_ms",
            String.valueOf(CONF.getSchemaRegionRatisRequestTimeoutMs()))
          .trim()));
    CONF.setDataRegionRatisRequestTimeoutMs(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_request_timeout_ms",
            String.valueOf(CONF.getDataRegionRatisRequestTimeoutMs()))
          .trim()));

    CONF.setConfigNodeRatisMaxRetryAttempts(
      Integer.parseInt(
        properties
          .getProperty(
            "config_node_ratis_max_retry_attempts",
            String.valueOf(CONF.getConfigNodeRatisMaxRetryAttempts()))
          .trim()));
    CONF.setConfigNodeRatisInitialSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_initial_sleep_time_ms",
            String.valueOf(CONF.getConfigNodeRatisInitialSleepTimeMs()))
          .trim()));
    CONF.setConfigNodeRatisMaxSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_max_sleep_time_ms",
            String.valueOf(CONF.getConfigNodeRatisMaxSleepTimeMs()))
          .trim()));

    CONF.setDataRegionRatisMaxRetryAttempts(
      Integer.parseInt(
        properties
          .getProperty(
            "data_region_ratis_max_retry_attempts",
            String.valueOf(CONF.getDataRegionRatisMaxRetryAttempts()))
          .trim()));
    CONF.setDataRegionRatisInitialSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_initial_sleep_time_ms",
            String.valueOf(CONF.getDataRegionRatisInitialSleepTimeMs()))
          .trim()));
    CONF.setDataRegionRatisMaxSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_max_sleep_time_ms",
            String.valueOf(CONF.getDataRegionRatisMaxSleepTimeMs()))
          .trim()));

    CONF.setSchemaRegionRatisMaxRetryAttempts(
      Integer.parseInt(
        properties
          .getProperty(
            "schema_region_ratis_max_retry_attempts",
            String.valueOf(CONF.getSchemaRegionRatisMaxRetryAttempts()))
          .trim()));
    CONF.setSchemaRegionRatisInitialSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_initial_sleep_time_ms",
            String.valueOf(CONF.getSchemaRegionRatisInitialSleepTimeMs()))
          .trim()));
    CONF.setSchemaRegionRatisMaxSleepTimeMs(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_max_sleep_time_ms",
            String.valueOf(CONF.getSchemaRegionRatisMaxSleepTimeMs()))
          .trim()));

    CONF.setConfigNodeRatisPreserveLogsWhenPurge(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_preserve_logs_num_when_purge",
            String.valueOf(CONF.getConfigNodeRatisPreserveLogsWhenPurge()))
          .trim()));

    CONF.setSchemaRegionRatisPreserveLogsWhenPurge(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_preserve_logs_num_when_purge",
            String.valueOf(CONF.getSchemaRegionRatisPreserveLogsWhenPurge()))
          .trim()));

    CONF.setDataRegionRatisPreserveLogsWhenPurge(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_preserve_logs_num_when_purge",
            String.valueOf(CONF.getDataRegionRatisPreserveLogsWhenPurge()))
          .trim()));

    CONF.setRatisFirstElectionTimeoutMinMs(
      Long.parseLong(
        properties
          .getProperty(
            "ratis_first_election_timeout_min_ms",
            String.valueOf(CONF.getRatisFirstElectionTimeoutMinMs()))
          .trim()));

    CONF.setRatisFirstElectionTimeoutMaxMs(
      Long.parseLong(
        properties
          .getProperty(
            "ratis_first_election_timeout_max_ms",
            String.valueOf(CONF.getRatisFirstElectionTimeoutMaxMs()))
          .trim()));

    CONF.setConfigNodeRatisLogMax(
      Long.parseLong(
        properties
          .getProperty(
            "config_node_ratis_log_max_size",
            String.valueOf(CONF.getConfigNodeRatisLogMax()))
          .trim()));

    CONF.setSchemaRegionRatisLogMax(
      Long.parseLong(
        properties
          .getProperty(
            "schema_region_ratis_log_max_size",
            String.valueOf(CONF.getSchemaRegionRatisLogMax()))
          .trim()));

    CONF.setDataRegionRatisLogMax(
      Long.parseLong(
        properties
          .getProperty(
            "data_region_ratis_log_max_size",
            String.valueOf(CONF.getDataRegionRatisLogMax()))
          .trim()));
  }

  // Mqtt related
  private void loadMqttProps(Properties properties) {
    CONF.setMqttDir(properties.getProperty("mqtt_root_dir", CONF.getMqttDir()));

    if (properties.getProperty(IoTDBConstant.ENABLE_MQTT) != null) {
      CONF.setEnableMqttService(
        Boolean.parseBoolean(properties.getProperty(IoTDBConstant.ENABLE_MQTT)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      CONF.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PORT_NAME) != null) {
      CONF.setMqttPort(Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_PORT_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME) != null) {
      CONF.setMqttHandlerPoolSize(
        Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME) != null) {
      CONF.setMqttPayloadFormatter(
        properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE) != null) {
      CONF.setMqttMaxMessageSize(
        Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE)));
    }
  }

  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    CONF.setDiskSpaceWarningThreshold(globalConfig.getDiskSpaceWarningThreshold());
  }
}
