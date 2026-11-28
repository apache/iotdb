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
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.pipe.config.PipeDescriptor;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuditConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.rpc.RpcSslUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.commons.conf.ConfigValidation.BOOLEAN;
import static org.apache.iotdb.commons.conf.ConfigValidation.FLOAT;
import static org.apache.iotdb.commons.conf.ConfigValidation.INT;
import static org.apache.iotdb.commons.conf.ConfigValidation.LONG;
import static org.apache.iotdb.commons.conf.ConfigValidation.STRING;
import static org.apache.iotdb.commons.conf.ConfigValidation.database;
import static org.apache.iotdb.commons.conf.ConfigValidation.databaseText;
import static org.apache.iotdb.commons.conf.ConfigValidation.directories;
import static org.apache.iotdb.commons.conf.ConfigValidation.nonNegative;
import static org.apache.iotdb.commons.conf.ConfigValidation.oneOf;
import static org.apache.iotdb.commons.conf.ConfigValidation.positive;
import static org.apache.iotdb.commons.conf.ConfigValidation.range;
import static org.apache.iotdb.commons.conf.ConfigValidation.unrestricted;
import static org.apache.iotdb.commons.conf.ConfigValidation.withUnlimited;

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
        database(
                properties,
                "tier_ttl_in_ms",
                String.join(IoTDBConstant.TIER_SEPARATOR, tierTTLStr),
                STRING,
                ConfigValidation.elements(IoTDBConstant.TIER_SEPARATOR, LONG, unrestricted()))
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
        database(properties, "ttl_check_interval", config.getTTLCheckInterval(), LONG, positive()));

    config.setSyncDir(properties.getProperty("dn_sync_dir", config.getSyncDir()).trim());

    config.setWalDirs(
        database(
                properties,
                "dn_wal_dirs",
                String.join(",", config.getWalDirs()),
                STRING,
                directories(",", false))
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

    int cnSelectorNumOfClientManager =
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_selector_thread_nums_of_client_manager",
                    String.valueOf(config.getSelectorNumOfClientManager()))
                .trim());
    if (cnSelectorNumOfClientManager > 0) {
      config.setSelectorNumOfClientManager(cnSelectorNumOfClientManager);
    }

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxClientNumForEachNode()))
                .trim()));

    int cnMaxIdleClientNumForEachNode =
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_max_idle_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxIdleClientNumForEachNode()))
                .trim());
    if (cnMaxIdleClientNumForEachNode >= 0) {
      config.setMaxIdleClientNumForEachNode(cnMaxIdleClientNumForEachNode);
    }

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

    int dnSelectorNumOfClientManager =
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_selector_thread_nums_of_client_manager",
                    String.valueOf(config.getSelectorNumOfClientManager()))
                .trim());
    if (dnSelectorNumOfClientManager > 0) {
      config.setSelectorNumOfClientManager(dnSelectorNumOfClientManager);
    }

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxClientNumForEachNode()))
                .trim()));

    int dnMaxIdleClientNumForEachNode =
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_idle_client_count_for_each_node_in_client_manager",
                    String.valueOf(config.getMaxIdleClientNumForEachNode()))
                .trim());
    if (dnMaxIdleClientNumForEachNode >= 0) {
      config.setMaxIdleClientNumForEachNode(dnMaxIdleClientNumForEachNode);
    }

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

    long minFolderOccupiedSpaceCacheRefreshIntervalMs =
        Long.parseLong(
            properties
                .getProperty(
                    "min_folder_occupied_space_cache_refresh_interval_ms",
                    String.valueOf(config.getMinFolderOccupiedSpaceCacheRefreshIntervalMs()))
                .trim());
    if (minFolderOccupiedSpaceCacheRefreshIntervalMs > 0) {
      config.setMinFolderOccupiedSpaceCacheRefreshIntervalMs(
          minFolderOccupiedSpaceCacheRefreshIntervalMs);
    }

    int minFolderOccupiedSpaceCacheRefreshSelectionThreshold =
        Integer.parseInt(
            properties
                .getProperty(
                    "min_folder_occupied_space_cache_refresh_selection_threshold",
                    String.valueOf(
                        config.getMinFolderOccupiedSpaceCacheRefreshSelectionThreshold()))
                .trim());
    if (minFolderOccupiedSpaceCacheRefreshSelectionThreshold > 0) {
      config.setMinFolderOccupiedSpaceCacheRefreshSelectionThreshold(
          minFolderOccupiedSpaceCacheRefreshSelectionThreshold);
    }

    config.setTimestampPrecision(
        database(
            properties,
            "timestamp_precision",
            config.getTimestampPrecision(),
            STRING,
            oneOf(false, "ms", "us", "ns")));

    config.setTimestampPrecisionCheckEnabled(
        database(
            properties,
            "timestamp_precision_check_enabled",
            config.isTimestampPrecisionCheckEnabled(),
            BOOLEAN,
            unrestricted()));
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
        database(
            properties,
            "schema_engine_mode",
            config.getSchemaEngineMode(),
            STRING,
            oneOf(false, "Memory", "PBTree")));

    config.setLastCacheEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_last_cache", Boolean.toString(config.isLastCacheEnable()))));

    config.setTagAttributeTotalSize(
        database(
            properties,
            "tag_attribute_total_size",
            config.getTagAttributeTotalSize(),
            INT,
            nonNegative()));

    int singleMeasurementCheckCacheSize =
        database(
            properties,
            "single_measurement_check_cache_size",
            config.getSingleMeasurementCheckCacheSize(),
            INT,
            nonNegative());
    if (singleMeasurementCheckCacheSize >= 0) {
      config.setSingleMeasurementCheckCacheSize(singleMeasurementCheckCacheSize);
    }

    config.setTimePartitionOrigin(
        database(
            properties,
            "time_partition_origin",
            config.getTimePartitionOrigin(),
            LONG,
            unrestricted()));

    config.setTimePartitionInterval(
        database(
            properties,
            "time_partition_interval",
            config.getTimePartitionInterval(),
            LONG,
            positive()));

    config.setDatabaseLimitThreshold(
        database(
            properties,
            "database_limit_threshold",
            config.getDatabaseLimitThreshold(),
            INT,
            withUnlimited(nonNegative())));
    config.setSeriesLimitThreshold(
        database(
            properties,
            "cluster_timeseries_limit_threshold",
            config.getSeriesLimitThreshold(),
            LONG,
            withUnlimited(nonNegative())));
    config.setDeviceLimitThreshold(
        database(
            properties,
            "cluster_device_limit_threshold",
            config.getDeviceLimitThreshold(),
            LONG,
            withUnlimited(nonNegative())));
    config.setPathLogMaxSize(
        Integer.parseInt(
            properties.getProperty(
                "path_log_max_size", String.valueOf(config.getPathLogMaxSize()))));

    loadRetryProperties(properties);
  }

  void loadSubscriptionProps(TrimProperties properties) {
    config.setSubscriptionCacheMemoryUsagePercentage(
        database(
            properties,
            "subscription_cache_memory_usage_percentage",
            config.getSubscriptionCacheMemoryUsagePercentage(),
            FLOAT,
            range(0.0f, true, 1.0f, true)));
    config.setSubscriptionSubtaskExecutorMaxThreadNum(
        database(
            properties,
            "subscription_subtask_executor_max_thread_num",
            config.getSubscriptionSubtaskExecutorMaxThreadNum(),
            INT,
            positive()));
    config.setSubscriptionConsensusPrefetchExecutorMaxThreadNum(
        database(
            properties,
            "subscription_consensus_prefetch_executor_max_thread_num",
            config.getSubscriptionConsensusPrefetchExecutorMaxThreadNum(),
            INT,
            positive()));

    config.setSubscriptionPrefetchTabletBatchMaxDelayInMs(
        database(
            properties,
            "subscription_prefetch_tablet_batch_max_delay_in_ms",
            config.getSubscriptionPrefetchTabletBatchMaxDelayInMs(),
            INT,
            nonNegative()));
    config.setSubscriptionPrefetchTabletBatchMaxSizeInBytes(
        database(
            properties,
            "subscription_prefetch_tablet_batch_max_size_in_bytes",
            config.getSubscriptionPrefetchTabletBatchMaxSizeInBytes(),
            LONG,
            nonNegative()));
    config.setSubscriptionPrefetchTsFileBatchMaxDelayInMs(
        database(
            properties,
            "subscription_prefetch_ts_file_batch_max_delay_in_ms",
            config.getSubscriptionPrefetchTsFileBatchMaxDelayInMs(),
            INT,
            nonNegative()));
    config.setSubscriptionPrefetchTsFileBatchMaxSizeInBytes(
        database(
            properties,
            "subscription_prefetch_ts_file_batch_max_size_in_bytes",
            config.getSubscriptionPrefetchTsFileBatchMaxSizeInBytes(),
            LONG,
            nonNegative()));
    config.setSubscriptionPollMaxBlockingTimeMs(
        database(
            properties,
            "subscription_poll_max_blocking_time_ms",
            config.getSubscriptionPollMaxBlockingTimeMs(),
            INT,
            nonNegative()));
    config.setSubscriptionDefaultTimeoutInMs(
        database(
            properties,
            "subscription_default_timeout_in_ms",
            config.getSubscriptionDefaultTimeoutInMs(),
            INT,
            nonNegative()));
    config.setSubscriptionLaunchRetryIntervalMs(
        database(
            properties,
            "subscription_launch_retry_interval_ms",
            config.getSubscriptionLaunchRetryIntervalMs(),
            LONG,
            nonNegative()));
    config.setSubscriptionRecycleUncommittedEventIntervalMs(
        database(
            properties,
            "subscription_recycle_uncommitted_event_interval_ms",
            config.getSubscriptionRecycleUncommittedEventIntervalMs(),
            INT,
            unrestricted()));
    config.setSubscriptionReadFileBufferSize(
        database(
            properties,
            "subscription_read_file_buffer_size",
            config.getSubscriptionReadFileBufferSize(),
            LONG,
            positive()));
    config.setSubscriptionReadTabletBufferSize(
        database(
            properties,
            "subscription_read_tablet_buffer_size",
            config.getSubscriptionReadTabletBufferSize(),
            LONG,
            positive()));
    config.setSubscriptionTsFileDeduplicationWindowSeconds(
        database(
            properties,
            "subscription_ts_file_deduplication_window_seconds",
            config.getSubscriptionTsFileDeduplicationWindowSeconds(),
            LONG,
            nonNegative()));
    config.setSubscriptionCheckMemoryEnoughIntervalMs(
        database(
            properties,
            "subscription_check_memory_enough_interval_ms",
            config.getSubscriptionCheckMemoryEnoughIntervalMs(),
            LONG,
            nonNegative()));
    config.setSubscriptionEstimatedInsertNodeTabletInsertionEventSize(
        database(
            properties,
            "subscription_estimated_insert_node_tablet_insertion_event_size",
            config.getSubscriptionEstimatedInsertNodeTabletInsertionEventSize(),
            LONG,
            nonNegative()));
    config.setSubscriptionEstimatedRawTabletInsertionEventSize(
        database(
            properties,
            "subscription_estimated_raw_tablet_insertion_event_size",
            config.getSubscriptionEstimatedRawTabletInsertionEventSize(),
            LONG,
            nonNegative()));
    config.setSubscriptionMaxAllowedEventCountInTabletBatch(
        database(
            properties,
            "subscription_max_allowed_event_count_in_tablet_batch",
            config.getSubscriptionMaxAllowedEventCountInTabletBatch(),
            LONG,
            nonNegative()));
    config.setSubscriptionLogManagerWindowSeconds(
        database(
            properties,
            "subscription_log_manager_window_seconds",
            config.getSubscriptionLogManagerWindowSeconds(),
            LONG,
            nonNegative()));
    config.setSubscriptionLogManagerBaseIntervalMs(
        database(
            properties,
            "subscription_log_manager_base_interval_ms",
            config.getSubscriptionLogManagerBaseIntervalMs(),
            LONG,
            nonNegative()));

    config.setSubscriptionPrefetchEnabled(
        database(
            properties,
            "subscription_prefetch_enabled",
            config.getSubscriptionPrefetchEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setSubscriptionPrefetchMemoryThreshold(
        database(
            properties,
            "subscription_prefetch_memory_threshold",
            config.getSubscriptionPrefetchMemoryThreshold(),
            FLOAT,
            range(0.0f, true, 1.0f, true)));
    config.setSubscriptionPrefetchMissingRateThreshold(
        database(
            properties,
            "subscription_prefetch_missing_rate_threshold",
            config.getSubscriptionPrefetchMissingRateThreshold(),
            FLOAT,
            range(0.0f, true, 1.0f, true)));
    config.setSubscriptionPrefetchEventLocalCountThreshold(
        database(
            properties,
            "subscription_prefetch_event_local_count_threshold",
            config.getSubscriptionPrefetchEventLocalCountThreshold(),
            INT,
            nonNegative()));
    config.setSubscriptionPrefetchEventGlobalCountThreshold(
        database(
            properties,
            "subscription_prefetch_event_global_count_threshold",
            config.getSubscriptionPrefetchEventGlobalCountThreshold(),
            INT,
            nonNegative()));

    config.setSubscriptionMetaSyncerInitialSyncDelayMinutes(
        database(
            properties,
            "subscription_meta_syncer_initial_sync_delay_minutes",
            config.getSubscriptionMetaSyncerInitialSyncDelayMinutes(),
            LONG,
            nonNegative()));
    config.setSubscriptionMetaSyncerSyncIntervalMinutes(
        database(
            properties,
            "subscription_meta_syncer_sync_interval_minutes",
            config.getSubscriptionMetaSyncerSyncIntervalMinutes(),
            LONG,
            nonNegative()));

    config.setSubscriptionOwnerLeaseDurationMsMin(
        database(
            properties,
            "subscription_owner_lease_duration_ms_min",
            config.getSubscriptionOwnerLeaseDurationMsMin(),
            LONG,
            nonNegative()));

    config.setSubscriptionConsensusBatchMaxDelayInMs(
        database(
            properties,
            "subscription_consensus_batch_max_delay_in_ms",
            config.getSubscriptionConsensusBatchMaxDelayInMs(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusBatchMaxSizeInBytes(
        database(
            properties,
            "subscription_consensus_batch_max_size_in_bytes",
            config.getSubscriptionConsensusBatchMaxSizeInBytes(),
            LONG,
            nonNegative()));
    config.setSubscriptionConsensusBatchMaxTabletCount(
        database(
            properties,
            "subscription_consensus_batch_max_tablet_count",
            config.getSubscriptionConsensusBatchMaxTabletCount(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusWalRetentionSizeInBytes(
        database(
            properties,
            "subscription_consensus_wal_retention_size_in_bytes",
            config.getSubscriptionConsensusWalRetentionSizeInBytes(),
            LONG,
            withUnlimited(nonNegative())));
    config.setSubscriptionConsensusWalRetentionTimeMs(
        database(
            properties,
            "subscription_consensus_wal_retention_time_ms",
            config.getSubscriptionConsensusWalRetentionTimeMs(),
            LONG,
            withUnlimited(nonNegative())));
    config.setSubscriptionConsensusBatchMaxWalEntries(
        database(
            properties,
            "subscription_consensus_batch_max_wal_entries",
            config.getSubscriptionConsensusBatchMaxWalEntries(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusCommitPersistInterval(
        database(
            properties,
            "subscription_consensus_commit_persist_interval",
            config.getSubscriptionConsensusCommitPersistInterval(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusCommitFsyncEnabled(
        database(
            properties,
            "subscription_consensus_commit_fsync_enabled",
            config.isSubscriptionConsensusCommitFsyncEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setSubscriptionConsensusConsumerEvictionTimeoutMs(
        database(
            properties,
            "subscription_consensus_consumer_eviction_timeout_ms",
            config.getSubscriptionConsensusConsumerEvictionTimeoutMs(),
            LONG,
            nonNegative()));
    config.setSubscriptionConsensusLagBasedPriority(
        database(
            properties,
            "subscription_consensus_lag_based_priority",
            config.isSubscriptionConsensusLagBasedPriority(),
            BOOLEAN,
            unrestricted()));
    config.setSubscriptionConsensusFallbackPollingEnabled(
        database(
            properties,
            "subscription_consensus_fallback_polling_enabled",
            config.isSubscriptionConsensusFallbackPollingEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setSubscriptionConsensusPrefetchingQueueCapacity(
        database(
            properties,
            "subscription_consensus_prefetching_queue_capacity",
            config.getSubscriptionConsensusPrefetchingQueueCapacity(),
            INT,
            positive()));
    config.setSubscriptionConsensusWatermarkEnabled(
        database(
            properties,
            "subscription_consensus_watermark_enabled",
            config.isSubscriptionConsensusWatermarkEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setSubscriptionConsensusWatermarkIntervalMs(
        database(
            properties,
            "subscription_consensus_watermark_interval_ms",
            config.getSubscriptionConsensusWatermarkIntervalMs(),
            LONG,
            nonNegative()));
    config.setSubscriptionConsensusIdleSafeTimeBarrierIntervalMs(
        database(
            properties,
            "subscription_consensus_idle_safe_time_barrier_interval_ms",
            config.getSubscriptionConsensusIdleSafeTimeBarrierIntervalMs(),
            LONG,
            nonNegative()));
  }

  public void loadRetryProperties(TrimProperties properties) throws IOException {
    config.setRemoteWriteMaxRetryDurationInMs(
        databaseText(
            properties,
            "write_request_remote_dispatch_max_retry_duration_in_ms",
            ConfigurationFileUtils.getConfigurationDefaultValue(
                "write_request_remote_dispatch_max_retry_duration_in_ms"),
            LONG,
            unrestricted()));

    config.setRetryForUnknownErrors(
        databaseText(
            properties,
            "enable_retry_for_unknown_error",
            ConfigurationFileUtils.getConfigurationDefaultValue("enable_retry_for_unknown_error"),
            BOOLEAN,
            unrestricted()));
  }

  /**
   * Parse, validate and apply {@code disk_space_warning_threshold} for a runtime hot reload, then
   * return the applied value. Shared by the ConfigNode and DataNode hot-reload paths so both use
   * the same parsing / bounds rules. Callers on the DataNode must additionally refresh the {@code
   * JVMCommonUtils} static copy that the ReadOnly disk guard actually consumes.
   */
  public double loadHotModifiedDiskSpaceWarningThreshold(final TrimProperties properties)
      throws IOException {
    double diskSpaceWarningThreshold =
        Double.parseDouble(
            properties.getProperty(
                "disk_space_warning_threshold",
                String.valueOf(config.getDiskSpaceWarningThreshold())));
    if (!Double.isFinite(diskSpaceWarningThreshold)
        || diskSpaceWarningThreshold < 0
        || diskSpaceWarningThreshold >= 1) {
      throw new IOException(
          CommonMessages.EXCEPTION_DISK_SPACE_WARNING_THRESHOLD_MUST_BE_IN_0_1_BUT_WAS_7B345766
              + diskSpaceWarningThreshold
              + ".");
    }
    config.setDiskSpaceWarningThreshold(diskSpaceWarningThreshold);
    return diskSpaceWarningThreshold;
  }

  /**
   * Reload only the subscription consensus properties that are intended to take effect on hot
   * configuration reload.
   *
   * <p>Batching related properties are read dynamically by running consensus subscription queues
   * and therefore take effect immediately after this method updates {@link CommonConfig}. Retention
   * defaults are only used when new consensus subscription queues are created, so hot reload
   * affects future topics / bindings and does not retroactively mutate existing queue policies.
   */
  public void loadHotModifiedSubscriptionConsensusProps(final TrimProperties properties) {
    config.setSubscriptionConsensusBatchMaxDelayInMs(
        database(
            properties,
            "subscription_consensus_batch_max_delay_in_ms",
            config.getSubscriptionConsensusBatchMaxDelayInMs(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusBatchMaxSizeInBytes(
        database(
            properties,
            "subscription_consensus_batch_max_size_in_bytes",
            config.getSubscriptionConsensusBatchMaxSizeInBytes(),
            LONG,
            nonNegative()));
    config.setSubscriptionConsensusBatchMaxTabletCount(
        database(
            properties,
            "subscription_consensus_batch_max_tablet_count",
            config.getSubscriptionConsensusBatchMaxTabletCount(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusBatchMaxWalEntries(
        database(
            properties,
            "subscription_consensus_batch_max_wal_entries",
            config.getSubscriptionConsensusBatchMaxWalEntries(),
            INT,
            nonNegative()));
    config.setSubscriptionConsensusWalRetentionSizeInBytes(
        database(
            properties,
            "subscription_consensus_wal_retention_size_in_bytes",
            config.getSubscriptionConsensusWalRetentionSizeInBytes(),
            LONG,
            withUnlimited(nonNegative())));
    config.setSubscriptionConsensusWalRetentionTimeMs(
        database(
            properties,
            "subscription_consensus_wal_retention_time_ms",
            config.getSubscriptionConsensusWalRetentionTimeMs(),
            LONG,
            withUnlimited(nonNegative())));
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
    config.setThriftSSLClientAuth(
        Boolean.parseBoolean(
            properties.getProperty(
                "thrift_ssl_client_auth", Boolean.toString(config.isThriftSSLClientAuth()))));
    config.setKeyStorePath(properties.getProperty("key_store_path", config.getKeyStorePath()));
    config.setKeyStorePwd(properties.getProperty("key_store_pwd", config.getKeyStorePwd()));
    config.setTrustStorePath(
        properties.getProperty("trust_store_path", config.getTrustStorePath()));
    config.setTrustStorePwd(properties.getProperty("trust_store_pwd", config.getTrustStorePwd()));
    config.setSslProtocol(
        RpcSslUtils.normalizeProtocol(
            properties.getProperty("ssl_protocol", config.getSslProtocol())));
    configureRpcSsl();
  }

  public void configureRpcSsl() {
    RpcSslUtils.configure(config.getSslProtocol());
  }
}
