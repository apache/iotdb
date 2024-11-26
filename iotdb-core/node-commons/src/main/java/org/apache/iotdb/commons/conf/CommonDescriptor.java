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
import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class CommonDescriptor {

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

  public void loadCommonProps(Properties properties) throws BadNodeUrlException, IOException {
    config.setAuthorizerProvider(
        Optional.ofNullable(
                properties.getProperty("authorizer_provider_class", config.getAuthorizerProvider()))
            .map(String::trim)
            .orElse(config.getAuthorizerProvider()));
    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    config.setOpenIdProviderUrl(
        Optional.ofNullable(properties.getProperty("openID_url", config.getOpenIdProviderUrl()))
            .map(String::trim)
            .orElse(config.getOpenIdProviderUrl()));
    config.setEncryptDecryptProvider(
        Optional.ofNullable(
                properties.getProperty(
                    "iotdb_server_encrypt_decrypt_provider", config.getEncryptDecryptProvider()))
            .map(String::trim)
            .orElse(config.getEncryptDecryptProvider()));

    config.setEncryptDecryptProviderParameter(
        Optional.ofNullable(
                properties.getProperty(
                    "iotdb_server_encrypt_decrypt_provider_parameter",
                    config.getEncryptDecryptProviderParameter()))
            .map(String::trim)
            .orElse(config.getEncryptDecryptProviderParameter()));

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

    config.setSyncDir(
        Optional.ofNullable(properties.getProperty("dn_sync_dir", config.getSyncDir()))
            .map(String::trim)
            .orElse(config.getSyncDir()));

    config.setWalDirs(
        Optional.ofNullable(
                properties.getProperty("dn_wal_dirs", String.join(",", config.getWalDirs())))
            .map(String::trim)
            .map(value -> value.split(","))
            .orElse(new String[0]));
    ;

    config.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "cn_rpc_thrift_compression_enable",
                        String.valueOf(config.isRpcThriftCompressionEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.isRpcThriftCompressionEnabled()))));

    config.setConnectionTimeoutInMS(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "cn_connection_timeout_ms",
                        String.valueOf(config.getConnectionTimeoutInMS())))
                .map(String::trim)
                .orElse(String.valueOf(config.getConnectionTimeoutInMS()))));

    config.setSelectorNumOfClientManager(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "cn_selector_thread_nums_of_client_manager",
                        String.valueOf(config.getSelectorNumOfClientManager())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSelectorNumOfClientManager()))));

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "cn_max_client_count_for_each_node_in_client_manager",
                        String.valueOf(config.getMaxClientNumForEachNode())))
                .map(String::trim)
                .orElse(String.valueOf(config.getMaxClientNumForEachNode()))));

    config.setConnectionTimeoutInMS(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "dn_connection_timeout_ms",
                        String.valueOf(config.getConnectionTimeoutInMS())))
                .map(String::trim)
                .orElse(String.valueOf(config.getConnectionTimeoutInMS()))));

    config.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "dn_rpc_thrift_compression_enable",
                        String.valueOf(config.isRpcThriftCompressionEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.isRpcThriftCompressionEnabled()))));

    config.setSelectorNumOfClientManager(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "dn_selector_thread_nums_of_client_manager",
                        String.valueOf(config.getSelectorNumOfClientManager())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSelectorNumOfClientManager()))));

    config.setMaxClientNumForEachNode(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "dn_max_client_count_for_each_node_in_client_manager",
                        String.valueOf(config.getMaxClientNumForEachNode())))
                .map(String::trim)
                .orElse(String.valueOf(config.getMaxClientNumForEachNode()))));

    config.setHandleSystemErrorStrategy(
        HandleSystemErrorStrategy.valueOf(
            Optional.ofNullable(
                    properties.getProperty(
                        "handle_system_error",
                        String.valueOf(config.getHandleSystemErrorStrategy())))
                .map(String::trim)
                .orElse(String.valueOf(config.getHandleSystemErrorStrategy()))));

    config.setDiskSpaceWarningThreshold(
        Double.parseDouble(
            Optional.ofNullable(
                    properties.getProperty(
                        "disk_space_warning_threshold",
                        String.valueOf(config.getDiskSpaceWarningThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getDiskSpaceWarningThreshold()))));

    config.setTimestampPrecision(
        Optional.ofNullable(
                properties.getProperty("timestamp_precision", config.getTimestampPrecision()))
            .map(String::trim)
            .orElse(config.getTimestampPrecision()));

    config.setTimestampPrecisionCheckEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "timestamp_precision_check_enabled",
                        String.valueOf(config.isTimestampPrecisionCheckEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.isTimestampPrecisionCheckEnabled()))));

    config.setDatanodeTokenTimeoutMS(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("datanode_token_timeout", String.valueOf(3 * 60 * 1000)))
                .map(String::trim)
                .orElse(String.valueOf(3 * 60 * 1000))));

    String endPointUrl =
        Optional.ofNullable(
                properties.getProperty(
                    "target_ml_node_endpoint",
                    NodeUrlUtils.convertTEndPointUrl(config.getTargetAINodeEndPoint())))
            .map(String::trim)
            .orElse(NodeUrlUtils.convertTEndPointUrl(config.getTargetAINodeEndPoint()));

    loadPipeProps(properties);
    loadSubscriptionProps(properties);

    config.setSchemaEngineMode(
        Optional.ofNullable(
                properties.getProperty(
                    "schema_engine_mode", String.valueOf(config.getSchemaEngineMode())))
            .map(String::trim)
            .orElse(String.valueOf(config.getSchemaEngineMode())));

    config.setLastCacheEnable(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "enable_last_cache", Boolean.toString(config.isLastCacheEnable())))
                .map(String::trim)
                .orElse(Boolean.toString(config.isLastCacheEnable()))));

    config.setTagAttributeTotalSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "tag_attribute_total_size",
                        String.valueOf(config.getTagAttributeTotalSize())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTagAttributeTotalSize()))));

    config.setTimePartitionOrigin(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "time_partition_origin", String.valueOf(config.getTimePartitionOrigin())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTimePartitionOrigin()))));

    config.setTimePartitionInterval(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "time_partition_interval",
                        String.valueOf(config.getTimePartitionInterval())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTimePartitionInterval()))));

    config.setDatabaseLimitThreshold(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "database_limit_threshold",
                        String.valueOf(config.getDatabaseLimitThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getDatabaseLimitThreshold()))));
    config.setModelInferenceExecutionThreadCount(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "model_inference_execution_thread_count",
                        String.valueOf(config.getModelInferenceExecutionThreadCount())))
                .map(String::trim)
                .orElse(String.valueOf(config.getModelInferenceExecutionThreadCount()))));
    config.setSeriesLimitThreshold(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "cluster_timeseries_limit_threshold",
                        String.valueOf(config.getSeriesLimitThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSeriesLimitThreshold()))));
    config.setDeviceLimitThreshold(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "cluster_device_limit_threshold",
                        String.valueOf(config.getDeviceLimitThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getDeviceLimitThreshold()))));

    loadRetryProperties(properties);
  }

  private void loadPipeProps(Properties properties) {
    config.setPipeNonForwardingEventsProgressReportInterval(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_non_forwarding_events_progress_report_interval",
                        Integer.toString(
                            config.getPipeNonForwardingEventsProgressReportInterval())))
                .map(String::trim)
                .orElse(
                    Integer.toString(config.getPipeNonForwardingEventsProgressReportInterval()))));

    config.setPipeHardlinkBaseDirName(
        Optional.ofNullable(
                properties.getProperty(
                    "pipe_hardlink_base_dir_name", config.getPipeHardlinkBaseDirName()))
            .map(String::trim)
            .orElse(config.getPipeHardlinkBaseDirName()));
    config.setPipeHardlinkTsFileDirName(
        Optional.ofNullable(
                properties.getProperty(
                    "pipe_hardlink_tsfile_dir_name", config.getPipeHardlinkTsFileDirName()))
            .map(String::trim)
            .orElse(config.getPipeHardlinkTsFileDirName()));
    config.setPipeHardlinkWALDirName(
        Optional.ofNullable(
                properties.getProperty(
                    "pipe_hardlink_wal_dir_name", config.getPipeHardlinkWALDirName()))
            .map(String::trim)
            .orElse(config.getPipeHardlinkWALDirName()));
    config.setPipeHardLinkWALEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_hardlink_wal_enabled",
                        Boolean.toString(config.getPipeHardLinkWALEnabled())))
                .map(String::trim)
                .orElse(Boolean.toString(config.getPipeHardLinkWALEnabled()))));
    config.setPipeFileReceiverFsyncEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_file_receiver_fsync_enabled",
                        Boolean.toString(config.getPipeFileReceiverFsyncEnabled())))
                .map(String::trim)
                .orElse(Boolean.toString(config.getPipeFileReceiverFsyncEnabled()))));

    config.setPipeDataStructureTabletRowSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_data_structure_tablet_row_size",
                        String.valueOf(config.getPipeDataStructureTabletRowSize())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeDataStructureTabletRowSize()))));
    config.setPipeDataStructureTabletSizeInBytes(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_data_structure_tablet_size_in_bytes",
                        String.valueOf(config.getPipeDataStructureTabletSizeInBytes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeDataStructureTabletSizeInBytes()))));
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
        Double.parseDouble(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_data_structure_tablet_memory_block_allocation_reject_threshold",
                        String.valueOf(
                            config
                                .getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()))));
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
        Double.parseDouble(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_data_structure_ts_file_memory_block_allocation_reject_threshold",
                        String.valueOf(
                            config
                                .getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()))));

    config.setPipeRealTimeQueuePollHistoryThreshold(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_realtime_queue_poll_history_threshold",
                        Integer.toString(config.getPipeRealTimeQueuePollHistoryThreshold())))
                .map(String::trim)
                .orElse(Integer.toString(config.getPipeRealTimeQueuePollHistoryThreshold()))));

    config.setPipeSubtaskExecutorMaxThreadNum(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_max_thread_num",
                        Integer.toString(config.getPipeSubtaskExecutorMaxThreadNum())))
                .map(String::trim)
                .orElse(Integer.toString(config.getPipeSubtaskExecutorMaxThreadNum()))));
    if (config.getPipeSubtaskExecutorMaxThreadNum() <= 0) {
      config.setPipeSubtaskExecutorMaxThreadNum(5);
    }
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_basic_check_point_interval_by_consumed_event_count",
                        String.valueOf(
                            config
                                .getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config
                            .getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount()))));
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_basic_check_point_interval_by_time_duration",
                        String.valueOf(
                            config.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration()))));
    config.setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_pending_queue_max_blocking_time_ms",
                        String.valueOf(
                            config.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs()))));
    config.setPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_cron_heartbeat_event_interval_seconds",
                        String.valueOf(
                            config.getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds()))));
    config.setPipeSubtaskExecutorForcedRestartIntervalMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_subtask_executor_forced_restart_interval_ms",
                        String.valueOf(config.getPipeSubtaskExecutorForcedRestartIntervalMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeSubtaskExecutorForcedRestartIntervalMs()))));

    config.setPipeExtractorAssignerDisruptorRingBufferSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_source_assigner_disruptor_ring_buffer_size"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_assigner_disruptor_ring_buffer_size",
                        String.valueOf(
                            config.getPipeExtractorAssignerDisruptorRingBufferSize())))));
    config.setPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes( // 1MB
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_source_assigner_disruptor_ring_buffer_entry_size_in_bytes"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_assigner_disruptor_ring_buffer_entry_size_in_bytes",
                        String.valueOf(
                            config
                                .getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes())))));
    config.setPipeExtractorMatcherCacheSize(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_source_matcher_cache_size"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_matcher_cache_size",
                        String.valueOf(config.getPipeExtractorMatcherCacheSize())))));

    config.setPipeConnectorHandshakeTimeoutMs(
        Long.parseLong(
            Optional.ofNullable(properties.getProperty("pipe_sink_handshake_timeout_ms"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_connector_handshake_timeout_ms",
                        String.valueOf(config.getPipeConnectorHandshakeTimeoutMs())))));
    config.setPipeConnectorTransferTimeoutMs(
        Long.parseLong(
            Optional.ofNullable(properties.getProperty("pipe_sink_timeout_ms"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_connector_timeout_ms",
                        String.valueOf(config.getPipeConnectorTransferTimeoutMs())))));
    config.setPipeConnectorReadFileBufferSize(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_sink_read_file_buffer_size"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_connector_read_file_buffer_size",
                        String.valueOf(config.getPipeConnectorReadFileBufferSize())))));
    config.setPipeConnectorRetryIntervalMs(
        Long.parseLong(
            Optional.ofNullable(properties.getProperty("pipe_sink_retry_interval_ms"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_connector_retry_interval_ms",
                        String.valueOf(config.getPipeConnectorRetryIntervalMs())))));
    config.setPipeConnectorRPCThriftCompressionEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(properties.getProperty("pipe_sink_rpc_thrift_compression_enabled"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_connector_rpc_thrift_compression_enabled",
                        String.valueOf(config.isPipeConnectorRPCThriftCompressionEnabled())))));

    config.setPipeAsyncConnectorSelectorNumber(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_sink_selector_number"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_selector_number",
                        String.valueOf(config.getPipeAsyncConnectorSelectorNumber())))));
    config.setPipeAsyncConnectorMaxClientNumber(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_sink_max_client_number"))
                .map(String::trim)
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_max_client_number",
                        String.valueOf(config.getPipeAsyncConnectorMaxClientNumber())))));

    config.setPipeAllSinksRateLimitBytesPerSecond(
        Double.parseDouble(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_all_sinks_rate_limit_bytes_per_second",
                        String.valueOf(config.getPipeAllSinksRateLimitBytesPerSecond())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeAllSinksRateLimitBytesPerSecond()))));
    config.setRateLimiterHotReloadCheckIntervalMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "rate_limiter_hot_reload_check_interval_ms",
                        String.valueOf(config.getRateLimiterHotReloadCheckIntervalMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getRateLimiterHotReloadCheckIntervalMs()))));

    config.setPipeConnectorRequestSliceThresholdBytes(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_connector_request_slice_threshold_bytes",
                        String.valueOf(config.getPipeConnectorRequestSliceThresholdBytes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeConnectorRequestSliceThresholdBytes()))));

    config.setSeperatedPipeHeartbeatEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_heartbeat_seperated_mode_enabled",
                        String.valueOf(config.isSeperatedPipeHeartbeatEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.isSeperatedPipeHeartbeatEnabled()))));
    config.setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_heartbeat_interval_seconds_for_collecting_pipe_meta",
                        String.valueOf(
                            config.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta()))));
    config.setPipeMetaSyncerInitialSyncDelayMinutes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_meta_syncer_initial_sync_delay_minutes",
                        String.valueOf(config.getPipeMetaSyncerInitialSyncDelayMinutes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMetaSyncerInitialSyncDelayMinutes()))));
    config.setPipeMetaSyncerSyncIntervalMinutes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_meta_syncer_sync_interval_minutes",
                        String.valueOf(config.getPipeMetaSyncerSyncIntervalMinutes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMetaSyncerSyncIntervalMinutes()))));

    config.setPipeMetaSyncerAutoRestartPipeCheckIntervalRound(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_meta_syncer_auto_restart_pipe_check_interval_round",
                        String.valueOf(
                            config.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound()))));
    config.setPipeAutoRestartEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_auto_restart_enabled",
                        String.valueOf(config.getPipeAutoRestartEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeAutoRestartEnabled()))));

    config.setPipeAirGapReceiverEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_air_gap_receiver_enabled",
                        Boolean.toString(config.getPipeAirGapReceiverEnabled())))
                .map(String::trim)
                .orElse(Boolean.toString(config.getPipeAirGapReceiverEnabled()))));
    config.setPipeAirGapReceiverPort(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_air_gap_receiver_port",
                        Integer.toString(config.getPipeAirGapReceiverPort())))
                .map(String::trim)
                .orElse(Integer.toString(config.getPipeAirGapReceiverPort()))));

    config.setPipeMaxAllowedHistoricalTsFilePerDataRegion(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_max_allowed_historical_tsfile_per_data_region",
                        String.valueOf(config.getPipeMaxAllowedHistoricalTsFilePerDataRegion())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMaxAllowedHistoricalTsFilePerDataRegion()))));
    config.setPipeMaxAllowedPendingTsFileEpochPerDataRegion(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_max_allowed_pending_tsfile_epoch_per_data_region",
                        String.valueOf(config.getPipeMaxAllowedPendingTsFileEpochPerDataRegion())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getPipeMaxAllowedPendingTsFileEpochPerDataRegion()))));
    config.setPipeMaxAllowedPinnedMemTableCount(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_max_allowed_pinned_memtable_count",
                        String.valueOf(config.getPipeMaxAllowedPinnedMemTableCount())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMaxAllowedPinnedMemTableCount()))));
    config.setPipeMaxAllowedLinkedTsFileCount(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_max_allowed_linked_tsfile_count",
                        String.valueOf(config.getPipeMaxAllowedLinkedTsFileCount())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMaxAllowedLinkedTsFileCount()))));
    config.setPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage(
        Float.parseFloat(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_max_allowed_linked_deleted_tsfile_disk_usage_percentage",
                        String.valueOf(
                            config.getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage()))));
    config.setPipeStuckRestartIntervalSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_stuck_restart_interval_seconds",
                        String.valueOf(config.getPipeStuckRestartIntervalSeconds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeStuckRestartIntervalSeconds()))));

    config.setPipeMetaReportMaxLogNumPerRound(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_meta_report_max_log_num_per_round",
                        String.valueOf(config.getPipeMetaReportMaxLogNumPerRound())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMetaReportMaxLogNumPerRound()))));
    config.setPipeMetaReportMaxLogIntervalRounds(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_meta_report_max_log_interval_rounds",
                        String.valueOf(config.getPipeMetaReportMaxLogIntervalRounds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMetaReportMaxLogIntervalRounds()))));
    config.setPipeTsFilePinMaxLogNumPerRound(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_tsfile_pin_max_log_num_per_round",
                        String.valueOf(config.getPipeTsFilePinMaxLogNumPerRound())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeTsFilePinMaxLogNumPerRound()))));
    config.setPipeTsFilePinMaxLogIntervalRounds(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_tsfile_pin_max_log_interval_rounds",
                        String.valueOf(config.getPipeTsFilePinMaxLogIntervalRounds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeTsFilePinMaxLogIntervalRounds()))));
    config.setPipeWalPinMaxLogNumPerRound(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_wal_pin_max_log_num_per_round",
                        String.valueOf(config.getPipeWalPinMaxLogNumPerRound())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeWalPinMaxLogNumPerRound()))));
    config.setPipeWalPinMaxLogIntervalRounds(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_wal_pin_max_log_interval_rounds",
                        String.valueOf(config.getPipeWalPinMaxLogIntervalRounds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeWalPinMaxLogIntervalRounds()))));

    config.setPipeMemoryManagementEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_management_enabled",
                        String.valueOf(config.getPipeMemoryManagementEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMemoryManagementEnabled()))));
    config.setPipeMemoryAllocateMaxRetries(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_allocate_max_retries",
                        String.valueOf(config.getPipeMemoryAllocateMaxRetries())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMemoryAllocateMaxRetries()))));
    config.setPipeMemoryAllocateRetryIntervalInMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_allocate_retry_interval_in_ms",
                        String.valueOf(config.getPipeMemoryAllocateRetryIntervalInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMemoryAllocateRetryIntervalInMs()))));
    config.setPipeMemoryAllocateMinSizeInBytes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_allocate_min_size_in_bytes",
                        String.valueOf(config.getPipeMemoryAllocateMinSizeInBytes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMemoryAllocateMinSizeInBytes()))));
    config.setPipeMemoryAllocateForTsFileSequenceReaderInBytes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_allocate_for_tsfile_sequence_reader_in_bytes",
                        String.valueOf(
                            config.getPipeMemoryAllocateForTsFileSequenceReaderInBytes())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getPipeMemoryAllocateForTsFileSequenceReaderInBytes()))));
    config.setPipeMemoryExpanderIntervalSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_memory_expander_interval_seconds",
                        String.valueOf(config.getPipeMemoryExpanderIntervalSeconds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeMemoryExpanderIntervalSeconds()))));
    config.setPipeTsFileParserCheckMemoryEnoughIntervalMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_tsfile_parser_check_memory_enough_interval_ms",
                        String.valueOf(config.getPipeTsFileParserCheckMemoryEnoughIntervalMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeTsFileParserCheckMemoryEnoughIntervalMs()))));
    config.setPipeLeaderCacheMemoryUsagePercentage(
        Float.parseFloat(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_leader_cache_memory_usage_percentage",
                        String.valueOf(config.getPipeLeaderCacheMemoryUsagePercentage())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeLeaderCacheMemoryUsagePercentage()))));
    config.setPipeListeningQueueTransferSnapshotThreshold(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_listening_queue_transfer_snapshot_threshold",
                        String.valueOf(config.getPipeListeningQueueTransferSnapshotThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeListeningQueueTransferSnapshotThreshold()))));

    config.setPipeSnapshotExecutionMaxBatchSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_snapshot_execution_max_batch_size",
                        String.valueOf(config.getPipeSnapshotExecutionMaxBatchSize())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeSnapshotExecutionMaxBatchSize()))));
    config.setPipeRemainingTimeCommitRateAutoSwitchSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_remaining_time_commit_rate_auto_switch_seconds",
                        String.valueOf(config.getPipeRemainingTimeCommitRateAutoSwitchSeconds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeRemainingTimeCommitRateAutoSwitchSeconds()))));
    config.setPipeRemainingTimeCommitRateAverageTime(
        PipeRemainingTimeRateAverageTime.valueOf(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_remaining_time_commit_rate_average_time",
                        String.valueOf(config.getPipeRemainingTimeCommitRateAverageTime())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeRemainingTimeCommitRateAverageTime()))));
    config.setPipeTsFileScanParsingThreshold(
        Double.parseDouble(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_tsfile_scan_parsing_threshold",
                        String.valueOf(config.getPipeTsFileScanParsingThreshold())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeTsFileScanParsingThreshold()))));

    config.setTwoStageAggregateMaxCombinerLiveTimeInMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "two_stage_aggregate_max_combiner_live_time_in_ms",
                        String.valueOf(config.getTwoStageAggregateMaxCombinerLiveTimeInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTwoStageAggregateMaxCombinerLiveTimeInMs()))));
    config.setTwoStageAggregateDataRegionInfoCacheTimeInMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "two_stage_aggregate_data_region_info_cache_time_in_ms",
                        String.valueOf(config.getTwoStageAggregateDataRegionInfoCacheTimeInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTwoStageAggregateDataRegionInfoCacheTimeInMs()))));
    config.setTwoStageAggregateSenderEndPointsCacheInMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "two_stage_aggregate_sender_end_points_cache_in_ms",
                        String.valueOf(config.getTwoStageAggregateSenderEndPointsCacheInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getTwoStageAggregateSenderEndPointsCacheInMs()))));

    config.setPipeEventReferenceTrackingEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_event_reference_tracking_enabled",
                        String.valueOf(config.getPipeEventReferenceTrackingEnabled())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeEventReferenceTrackingEnabled()))));
    config.setPipeEventReferenceEliminateIntervalSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_event_reference_eliminate_interval_seconds",
                        String.valueOf(config.getPipeEventReferenceEliminateIntervalSeconds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getPipeEventReferenceEliminateIntervalSeconds()))));
  }

  private void loadSubscriptionProps(Properties properties) {
    config.setSubscriptionCacheMemoryUsagePercentage(
        Float.parseFloat(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_cache_memory_usage_percentage",
                        String.valueOf(config.getSubscriptionCacheMemoryUsagePercentage())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionCacheMemoryUsagePercentage()))));

    config.setSubscriptionSubtaskExecutorMaxThreadNum(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_subtask_executor_max_thread_num",
                        Integer.toString(config.getSubscriptionSubtaskExecutorMaxThreadNum())))
                .map(String::trim)
                .orElse(Integer.toString(config.getSubscriptionSubtaskExecutorMaxThreadNum()))));
    if (config.getSubscriptionSubtaskExecutorMaxThreadNum() <= 0) {
      config.setSubscriptionSubtaskExecutorMaxThreadNum(5);
    }
    config.setSubscriptionPrefetchTabletBatchMaxDelayInMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_prefetch_tablet_batch_max_delay_in_ms",
                        String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxDelayInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxDelayInMs()))));
    config.setSubscriptionPrefetchTabletBatchMaxSizeInBytes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_prefetch_tablet_batch_max_size_in_bytes",
                        String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxSizeInBytes())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getSubscriptionPrefetchTabletBatchMaxSizeInBytes()))));
    config.setSubscriptionPrefetchTsFileBatchMaxDelayInMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_prefetch_ts_file_batch_max_delay_in_ms",
                        String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxDelayInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxDelayInMs()))));
    config.setSubscriptionPrefetchTsFileBatchMaxSizeInBytes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_prefetch_ts_file_batch_max_size_in_bytes",
                        String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxSizeInBytes())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getSubscriptionPrefetchTsFileBatchMaxSizeInBytes()))));
    config.setSubscriptionPollMaxBlockingTimeMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_poll_max_blocking_time_ms",
                        String.valueOf(config.getSubscriptionPollMaxBlockingTimeMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionPollMaxBlockingTimeMs()))));
    config.setSubscriptionDefaultTimeoutInMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_default_timeout_in_ms",
                        String.valueOf(config.getSubscriptionDefaultTimeoutInMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionDefaultTimeoutInMs()))));
    config.setSubscriptionLaunchRetryIntervalMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_launch_retry_interval_ms",
                        String.valueOf(config.getSubscriptionLaunchRetryIntervalMs())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionLaunchRetryIntervalMs()))));
    config.setSubscriptionRecycleUncommittedEventIntervalMs(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_recycle_uncommitted_event_interval_ms",
                        String.valueOf(config.getSubscriptionRecycleUncommittedEventIntervalMs())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getSubscriptionRecycleUncommittedEventIntervalMs()))));
    config.setSubscriptionReadFileBufferSize(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_read_file_buffer_size",
                        String.valueOf(config.getSubscriptionReadFileBufferSize())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionReadFileBufferSize()))));
    config.setSubscriptionReadTabletBufferSize(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_read_tablet_buffer_size",
                        String.valueOf(config.getSubscriptionReadTabletBufferSize())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionReadTabletBufferSize()))));
    config.setSubscriptionTsFileDeduplicationWindowSeconds(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_ts_file_deduplication_window_seconds",
                        String.valueOf(config.getSubscriptionTsFileDeduplicationWindowSeconds())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionTsFileDeduplicationWindowSeconds()))));
    config.setSubscriptionTsFileSlicerCheckMemoryEnoughIntervalMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_ts_file_slicer_check_memory_enough_interval_ms",
                        String.valueOf(
                            config.getSubscriptionTsFileSlicerCheckMemoryEnoughIntervalMs())))
                .map(String::trim)
                .orElse(
                    String.valueOf(
                        config.getSubscriptionTsFileSlicerCheckMemoryEnoughIntervalMs()))));

    config.setSubscriptionMetaSyncerInitialSyncDelayMinutes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_meta_syncer_initial_sync_delay_minutes",
                        String.valueOf(config.getSubscriptionMetaSyncerInitialSyncDelayMinutes())))
                .map(String::trim)
                .orElse(
                    String.valueOf(config.getSubscriptionMetaSyncerInitialSyncDelayMinutes()))));
    config.setSubscriptionMetaSyncerSyncIntervalMinutes(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "subscription_meta_syncer_sync_interval_minutes",
                        String.valueOf(config.getSubscriptionMetaSyncerSyncIntervalMinutes())))
                .map(String::trim)
                .orElse(String.valueOf(config.getSubscriptionMetaSyncerSyncIntervalMinutes()))));
  }

  public void loadRetryProperties(Properties properties) throws IOException {
    config.setRemoteWriteMaxRetryDurationInMs(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty(
                        "write_request_remote_dispatch_max_retry_duration_in_ms",
                        ConfigurationFileUtils.getConfigurationDefaultValue(
                            "write_request_remote_dispatch_max_retry_duration_in_ms")))
                .map(String::trim)
                .orElse(
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "write_request_remote_dispatch_max_retry_duration_in_ms"))));

    config.setRetryForUnknownErrors(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    properties.getProperty(
                        "enable_retry_for_unknown_error",
                        ConfigurationFileUtils.getConfigurationDefaultValue(
                            "enable_retry_for_unknown_error")))
                .map(String::trim)
                .orElse(
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "enable_retry_for_unknown_error"))));
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
  }
}
