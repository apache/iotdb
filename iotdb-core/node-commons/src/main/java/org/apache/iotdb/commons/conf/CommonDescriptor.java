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

import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.pipe.config.PipeDescriptor;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuditConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class CommonDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonDescriptor.class);
  private final CommonConfig config = new CommonConfig();

  private CommonDescriptor() {}

  public static CommonDescriptor getInstance() {
    return CommonDescriptorHolder.INSTANCE;
  }

  private static class CommonDescriptorHolder {

    private static final CommonDescriptor INSTANCE = new CommonDescriptor();

    private CommonDescriptorHolder() {
      // empty constructor
    }
  }

  public CommonConfig getConfig() {
    return config;
  }

  public String getConfDir() {
    // Check if a config-directory was specified first.
    String confString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (confString == null) {
      confString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (confString != null) {
        confString = confString + File.separatorChar + "conf";
      }
    }
    return confString;
  }

  public void initCommonConfigDir(String systemDir) {
    config.setUserFolder(systemDir + File.separator + "users");
    config.setRoleFolder(systemDir + File.separator + "roles");
    config.setProcedureWalFolder(systemDir + File.separator + "procedure");
  }

  public void loadCommonProps(TrimProperties properties) throws IOException {
    config.setAuthorizerProvider(
        properties.getProperty("authorizer_provider_class", config.getAuthorizerProvider()).trim());
    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    config.setOpenIdProviderUrl(
        properties.getProperty("openID_url", config.getOpenIdProviderUrl()).trim());
    config.setEncryptDecryptProvider(
        properties
            .getProperty(
                "iotdb_server_encrypt_decrypt_provider", config.getEncryptDecryptProvider())
            .trim());

    config.setEncryptDecryptProviderParameter(
        properties.getProperty(
            "iotdb_server_encrypt_decrypt_provider_parameter",
            config.getEncryptDecryptProviderParameter()));

    config.setUserEncryptTokenHint(System.getenv("user_encrypt_token_hint"));

    config.setEnableGrantOption(
        Boolean.parseBoolean(
            properties.getProperty("enable_grant_option", String.valueOf("true"))));

    String[] tierTTLStr = new String[config.getTierTTLInMs().length];
    for (int i = 0; i < tierTTLStr.length; ++i) {
      tierTTLStr[i] = String.valueOf(config.getTierTTLInMs()[i]);
    }
    tierTTLStr =
        properties
            .getProperty("tier_ttl_in_ms", String.join(IoTDBConstant.TIER_SEPARATOR, tierTTLStr))
            .split(IoTDBConstant.TIER_SEPARATOR);
    long[] tierTTL = new long[tierTTLStr.length];
    for (int i = 0; i < tierTTL.length; ++i) {
      tierTTL[i] = Long.parseLong(tierTTLStr[i]);
      if (tierTTL[i] < 0) {
        tierTTL[i] = Long.MAX_VALUE;
      }
    }
    config.setTierTTLInMs(tierTTL);

    config.setTTLCheckInterval(
        Long.parseLong(
            properties.getProperty(
                "ttl_check_interval", Long.toString(config.getTTLCheckInterval()))));

    config.setSyncDir(properties.getProperty("dn_sync_dir", config.getSyncDir()).trim());

    config.setWalDirs(
        properties
            .getProperty("dn_wal_dirs", String.join(",", config.getWalDirs()))
            .trim()
            .split(","));

    config.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_thrift_compression_enable",
                    String.valueOf(config.isRpcThriftCompressionEnabled()))
                .trim()));

    config.setCnConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_connection_timeout_ms", String.valueOf(config.getCnConnectionTimeoutInMS()))
                .trim()));

    config.setSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_selector_thread_nums_of_client_manager",
                    String.valueOf(config.getSelectorNumOfClientManager()))
                .trim()));

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxClientNumForEachNode()))
                .trim()));

    config.setDnConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_connection_timeout_ms", String.valueOf(config.getDnConnectionTimeoutInMS()))
                .trim()));

    config.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_thrift_compression_enable",
                    String.valueOf(config.isRpcThriftCompressionEnabled()))
                .trim()));

    config.setSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_selector_thread_nums_of_client_manager",
                    String.valueOf(config.getSelectorNumOfClientManager()))
                .trim()));

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxClientNumForEachNode()))
                .trim()));

    config.setHandleSystemErrorStrategy(
        HandleSystemErrorStrategy.valueOf(
            properties
                .getProperty(
                    "handle_system_error", String.valueOf(config.getHandleSystemErrorStrategy()))
                .trim()));

    config.setDiskSpaceWarningThreshold(
        Double.parseDouble(
            properties
                .getProperty(
                    "disk_space_warning_threshold",
                    String.valueOf(config.getDiskSpaceWarningThreshold()))
                .trim()));

    config.setTimestampPrecision(
        properties.getProperty("timestamp_precision", config.getTimestampPrecision()).trim());

    config.setTimestampPrecisionCheckEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "timestamp_precision_check_enabled",
                String.valueOf(config.isTimestampPrecisionCheckEnabled()))));
    config.setDatanodeTokenTimeoutMS(
        Integer.parseInt(
            properties.getProperty("datanode_token_timeout", String.valueOf(3 * 60 * 1000))));

    config.setEnableAuditLog(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_audit_log", String.valueOf(config.isEnableAuditLog()))
                .trim()));
    config.setAuditableOperationType(
        properties.getProperty("auditable_operation_type", "DDL,DML,QUERY,CONTROL").trim());
    config.setAuditableOperationLevel(
        properties
            .getProperty(
                "auditable_operation_level", config.getAuditableOperationLevel().toString())
            .trim()
            .toUpperCase());
    config.setAuditableOperationResult(
        properties
            .getProperty("auditable_operation_result", config.getAuditableOperationResult())
            .trim()
            .toUpperCase());

    PipeDescriptor.loadPipeProps(config, properties, false);
    loadSubscriptionProps(properties);

    config.setSchemaEngineMode(
        properties.getProperty("schema_engine_mode", String.valueOf(config.getSchemaEngineMode())));

    config.setLastCacheEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_last_cache", Boolean.toString(config.isLastCacheEnable()))));

    config.setTagAttributeTotalSize(
        Integer.parseInt(
            properties.getProperty(
                "tag_attribute_total_size", String.valueOf(config.getTagAttributeTotalSize()))));

    config.setTimePartitionOrigin(
        Long.parseLong(
            properties.getProperty(
                "time_partition_origin", String.valueOf(config.getTimePartitionOrigin()))));

    config.setTimePartitionInterval(
        Long.parseLong(
            properties.getProperty(
                "time_partition_interval", String.valueOf(config.getTimePartitionInterval()))));

    config.setDatabaseLimitThreshold(
        Integer.parseInt(
            properties.getProperty(
                "database_limit_threshold", String.valueOf(config.getDatabaseLimitThreshold()))));
    config.setSeriesLimitThreshold(
        Long.parseLong(
            properties.getProperty(
                "cluster_timeseries_limit_threshold",
                String.valueOf(config.getSeriesLimitThreshold()))));
    config.setDeviceLimitThreshold(
        Long.parseLong(
            properties.getProperty(
                "cluster_device_limit_threshold",
                String.valueOf(config.getDeviceLimitThreshold()))));

    config.setPathLogMaxSize(
        Integer.parseInt(
            properties.getProperty(
                "path_log_max_size", String.valueOf(config.getPathLogMaxSize()))));

    loadRetryProperties(properties);
    loadBinaryAllocatorProps(properties);
  }

  private void loadSubscriptionProps(TrimProperties properties) {
    config.setSubscriptionEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "subscription_enabled", String.valueOf(config.getSubscriptionEnabled()))));

    config.setSubscriptionCacheMemoryUsagePercentage(
        Float.parseFloat(
            properties.getProperty(
                "subscription_cache_memory_usage_percentage",
                String.valueOf(config.getSubscriptionCacheMemoryUsagePercentage()))));
    config.setSubscriptionSubtaskExecutorMaxThreadNum(
        Integer.parseInt(
            properties.getProperty(
                "subscription_subtask_executor_max_thread_num",
                Integer.toString(config.getSubscriptionSubtaskExecutorMaxThreadNum()))));

    config.setSubscriptionPrefetchTabletBatchMaxDelayInMs(
        Integer.parseInt(
            properties.getProperty(
                "subscription_prefetch_tablet_batch_max_delay_in_ms",
                String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxDelayInMs()))));
    config.setSubscriptionPrefetchTabletBatchMaxSizeInBytes(
        Long.parseLong(
            properties.getProperty(
                "subscription_prefetch_tablet_batch_max_size_in_bytes",
                String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxSizeInBytes()))));
    config.setSubscriptionPrefetchTsFileBatchMaxDelayInMs(
        Integer.parseInt(
            properties.getProperty(
                "subscription_prefetch_ts_file_batch_max_delay_in_ms",
                String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxDelayInMs()))));
    config.setSubscriptionPrefetchTsFileBatchMaxSizeInBytes(
        Long.parseLong(
            properties.getProperty(
                "subscription_prefetch_ts_file_batch_max_size_in_bytes",
                String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxSizeInBytes()))));
    config.setSubscriptionPollMaxBlockingTimeMs(
        Integer.parseInt(
            properties.getProperty(
                "subscription_poll_max_blocking_time_ms",
                String.valueOf(config.getSubscriptionPollMaxBlockingTimeMs()))));
    config.setSubscriptionDefaultTimeoutInMs(
        Integer.parseInt(
            properties.getProperty(
                "subscription_default_timeout_in_ms",
                String.valueOf(config.getSubscriptionDefaultTimeoutInMs()))));
    config.setSubscriptionLaunchRetryIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "subscription_launch_retry_interval_ms",
                String.valueOf(config.getSubscriptionLaunchRetryIntervalMs()))));
    config.setSubscriptionRecycleUncommittedEventIntervalMs(
        Integer.parseInt(
            properties.getProperty(
                "subscription_recycle_uncommitted_event_interval_ms",
                String.valueOf(config.getSubscriptionRecycleUncommittedEventIntervalMs()))));
    config.setSubscriptionReadFileBufferSize(
        Long.parseLong(
            properties.getProperty(
                "subscription_read_file_buffer_size",
                String.valueOf(config.getSubscriptionReadFileBufferSize()))));
    config.setSubscriptionReadTabletBufferSize(
        Long.parseLong(
            properties.getProperty(
                "subscription_read_tablet_buffer_size",
                String.valueOf(config.getSubscriptionReadTabletBufferSize()))));
    config.setSubscriptionTsFileDeduplicationWindowSeconds(
        Long.parseLong(
            properties.getProperty(
                "subscription_ts_file_deduplication_window_seconds",
                String.valueOf(config.getSubscriptionTsFileDeduplicationWindowSeconds()))));
    config.setSubscriptionCheckMemoryEnoughIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "subscription_check_memory_enough_interval_ms",
                String.valueOf(config.getSubscriptionCheckMemoryEnoughIntervalMs()))));
    config.setSubscriptionEstimatedInsertNodeTabletInsertionEventSize(
        Long.parseLong(
            properties.getProperty(
                "subscription_estimated_insert_node_tablet_insertion_event_size",
                String.valueOf(
                    config.getSubscriptionEstimatedInsertNodeTabletInsertionEventSize()))));
    config.setSubscriptionEstimatedRawTabletInsertionEventSize(
        Long.parseLong(
            properties.getProperty(
                "subscription_estimated_raw_tablet_insertion_event_size",
                String.valueOf(config.getSubscriptionEstimatedRawTabletInsertionEventSize()))));
    config.setSubscriptionMaxAllowedEventCountInTabletBatch(
        Long.parseLong(
            properties.getProperty(
                "subscription_max_allowed_event_count_in_tablet_batch",
                String.valueOf(config.getSubscriptionMaxAllowedEventCountInTabletBatch()))));
    config.setSubscriptionLogManagerWindowSeconds(
        Long.parseLong(
            properties.getProperty(
                "subscription_log_manager_window_seconds",
                String.valueOf(config.getSubscriptionLogManagerWindowSeconds()))));
    config.setSubscriptionLogManagerBaseIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "subscription_log_manager_base_interval_ms",
                String.valueOf(config.getSubscriptionLogManagerBaseIntervalMs()))));

    config.setSubscriptionPrefetchEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "subscription_prefetch_enabled",
                String.valueOf(config.getSubscriptionPrefetchEnabled()))));
    config.setSubscriptionPrefetchMemoryThreshold(
        Float.parseFloat(
            properties.getProperty(
                "subscription_prefetch_memory_threshold",
                String.valueOf(config.getSubscriptionPrefetchMemoryThreshold()))));
    config.setSubscriptionPrefetchMissingRateThreshold(
        Float.parseFloat(
            properties.getProperty(
                "subscription_prefetch_missing_rate_threshold",
                String.valueOf(config.getSubscriptionPrefetchMissingRateThreshold()))));
    config.setSubscriptionPrefetchEventLocalCountThreshold(
        Integer.parseInt(
            properties.getProperty(
                "subscription_prefetch_event_local_count_threshold",
                String.valueOf(config.getSubscriptionPrefetchEventLocalCountThreshold()))));
    config.setSubscriptionPrefetchEventGlobalCountThreshold(
        Integer.parseInt(
            properties.getProperty(
                "subscription_prefetch_event_global_count_threshold",
                String.valueOf(config.getSubscriptionPrefetchEventGlobalCountThreshold()))));

    config.setSubscriptionMetaSyncerInitialSyncDelayMinutes(
        Long.parseLong(
            properties.getProperty(
                "subscription_meta_syncer_initial_sync_delay_minutes",
                String.valueOf(config.getSubscriptionMetaSyncerInitialSyncDelayMinutes()))));
    config.setSubscriptionMetaSyncerSyncIntervalMinutes(
        Long.parseLong(
            properties.getProperty(
                "subscription_meta_syncer_sync_interval_minutes",
                String.valueOf(config.getSubscriptionMetaSyncerSyncIntervalMinutes()))));
  }

  public void loadRetryProperties(TrimProperties properties) throws IOException {
    config.setRemoteWriteMaxRetryDurationInMs(
        Long.parseLong(
            properties.getProperty(
                "write_request_remote_dispatch_max_retry_duration_in_ms",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "write_request_remote_dispatch_max_retry_duration_in_ms"))));

    config.setRetryForUnknownErrors(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_retry_for_unknown_error",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_retry_for_unknown_error"))));
  }

  public void loadBinaryAllocatorProps(TrimProperties properties) {
    config.setEnableBinaryAllocator(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_binary_allocator", Boolean.toString(config.isEnableBinaryAllocator()))));
    config.setMinAllocateSize(
        Integer.parseInt(
            properties.getProperty(
                "small_blob_object", String.valueOf(config.getMinAllocateSize()))));
    config.setMaxAllocateSize(
        Integer.parseInt(
            properties.getProperty(
                "huge_blob_object", String.valueOf(config.getMaxAllocateSize()))));
    int arenaNum =
        Integer.parseInt(properties.getProperty("arena_num", String.valueOf(config.getArenaNum())));
    if (arenaNum > 0) {
      config.setArenaNum(arenaNum);
    }
    config.setLog2SizeClassGroup(
        Integer.parseInt(
            properties.getProperty(
                "log2_size_class_group", String.valueOf(config.getLog2SizeClassGroup()))));
  }

  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    config.setTimestampPrecision(globalConfig.timestampPrecision);
    config.setTimePartitionOrigin(
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            globalConfig.timePartitionOrigin, config.getTimestampPrecision()));
    config.setTimePartitionInterval(
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            globalConfig.timePartitionInterval, config.getTimestampPrecision()));
    config.setSchemaEngineMode(globalConfig.schemaEngineMode);
    config.setTagAttributeTotalSize(globalConfig.tagAttributeTotalSize);
    config.setDiskSpaceWarningThreshold(globalConfig.getDiskSpaceWarningThreshold());
    config.setEnableGrantOption(globalConfig.isEnableGrantOption());
    config.setRestrictObjectLimit(globalConfig.isRestrictObjectLimit());
  }

  public void loadAuditConfig(TAuditConfig auditConfig) {
    config.setEnableAuditLog(auditConfig.isEnableAuditLog());
    if (auditConfig.isEnableAuditLog()) {
      config.setAuditableOperationType(auditConfig.getAuditableOperationType());
      config.setAuditableOperationLevel(auditConfig.getAuditableOperationLevel());
      config.setAuditableOperationResult(auditConfig.getAuditableOperationResult());
    }
  }

  public void initThriftSSL(TrimProperties properties) {
    config.setEnableThriftClientSSL(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_thrift_ssl", Boolean.toString(config.isEnableThriftClientSSL()))));
    config.setKeyStorePath(properties.getProperty("key_store_path", config.getKeyStorePath()));
    config.setKeyStorePwd(properties.getProperty("key_store_pwd", config.getKeyStorePwd()));
    config.setTrustStorePath(
        properties.getProperty("trust_store_path", config.getTrustStorePath()));
    config.setTrustStorePwd(properties.getProperty("trust_store_pwd", config.getTrustStorePwd()));
  }
}
