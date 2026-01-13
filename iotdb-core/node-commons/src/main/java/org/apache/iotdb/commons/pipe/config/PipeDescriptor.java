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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.enums.PipeRateAverage;

import java.io.IOException;
import java.util.Optional;

public class PipeDescriptor {

  public static void loadPipeProps(
      CommonConfig config, TrimProperties properties, boolean isHotModify) throws IOException {
    if (!isHotModify) {
      loadPipeStaticConfig(config, properties);
    }

    loadPipeInternalConfig(config, properties);

    loadPipeExternalConfig(config, properties, isHotModify);
  }

  public static void loadPipeStaticConfig(CommonConfig config, TrimProperties properties) {
    config.setPipeHardlinkBaseDirName(
        properties.getProperty("pipe_hardlink_base_dir_name", config.getPipeHardlinkBaseDirName()));
    config.setPipeHardlinkTsFileDirName(
        properties.getProperty(
            "pipe_hardlink_tsfile_dir_name", config.getPipeHardlinkTsFileDirName()));
    int pipeSubtaskExecutorMaxThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "pipe_subtask_executor_max_thread_num",
                Integer.toString(config.getPipeSubtaskExecutorMaxThreadNum())));
    if (pipeSubtaskExecutorMaxThreadNum > 0) {
      config.setPipeSubtaskExecutorMaxThreadNum(pipeSubtaskExecutorMaxThreadNum);
    }

    config.setPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds(
        Long.parseLong(
            properties.getProperty(
                "pipe_subtask_executor_cron_heartbeat_event_interval_seconds",
                String.valueOf(config.getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds()))));
    config.setSeperatedPipeHeartbeatEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_heartbeat_seperated_mode_enabled",
                String.valueOf(config.isSeperatedPipeHeartbeatEnabled()))));
    config.setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
        Integer.parseInt(
            properties.getProperty(
                "pipe_heartbeat_interval_seconds_for_collecting_pipe_meta",
                String.valueOf(config.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta()))));

    config.setPipeMetaSyncerInitialSyncDelayMinutes(
        Long.parseLong(
            properties.getProperty(
                "pipe_meta_syncer_initial_sync_delay_minutes",
                String.valueOf(config.getPipeMetaSyncerInitialSyncDelayMinutes()))));
    config.setPipeMetaSyncerSyncIntervalMinutes(
        Long.parseLong(
            properties.getProperty(
                "pipe_meta_syncer_sync_interval_minutes",
                String.valueOf(config.getPipeMetaSyncerSyncIntervalMinutes()))));
    config.setPipeMetaSyncerAutoRestartPipeCheckIntervalRound(
        Long.parseLong(
            properties.getProperty(
                "pipe_meta_syncer_auto_restart_pipe_check_interval_round",
                String.valueOf(config.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound()))));
    config.setPipeAutoRestartEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_auto_restart_enabled", String.valueOf(config.getPipeAutoRestartEnabled()))));

    config.setPipeAirGapReceiverEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_air_gap_receiver_enabled",
                Boolean.toString(config.getPipeAirGapReceiverEnabled()))));
    config.setPipeAirGapReceiverPort(
        Integer.parseInt(
            properties.getProperty(
                "pipe_air_gap_receiver_port",
                Integer.toString(config.getPipeAirGapReceiverPort()))));

    config.setPipeMetaReportMaxLogNumPerRound(
        Double.parseDouble(
            properties.getProperty(
                "pipe_meta_report_max_log_num_per_round",
                String.valueOf(config.getPipeMetaReportMaxLogNumPerRound()))));
    config.setPipeMetaReportMaxLogIntervalRounds(
        Integer.parseInt(
            properties.getProperty(
                "pipe_meta_report_max_log_interval_rounds",
                String.valueOf(config.getPipeMetaReportMaxLogIntervalRounds()))));
    config.setPipeTsFilePinMaxLogNumPerRound(
        Integer.parseInt(
            properties.getProperty(
                "pipe_tsfile_pin_max_log_num_per_round",
                String.valueOf(config.getPipeTsFilePinMaxLogNumPerRound()))));
    config.setPipeTsFilePinMaxLogIntervalRounds(
        Integer.parseInt(
            properties.getProperty(
                "pipe_tsfile_pin_max_log_interval_rounds",
                String.valueOf(config.getPipeTsFilePinMaxLogIntervalRounds()))));
    config.setPipeMemoryManagementEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_memory_management_enabled",
                String.valueOf(config.getPipeMemoryManagementEnabled()))));

    config.setTwoStageAggregateMaxCombinerLiveTimeInMs(
        Long.parseLong(
            properties.getProperty(
                "two_stage_aggregate_max_combiner_live_time_in_ms",
                String.valueOf(config.getTwoStageAggregateMaxCombinerLiveTimeInMs()))));
    config.setTwoStageAggregateDataRegionInfoCacheTimeInMs(
        Long.parseLong(
            properties.getProperty(
                "two_stage_aggregate_data_region_info_cache_time_in_ms",
                String.valueOf(config.getTwoStageAggregateDataRegionInfoCacheTimeInMs()))));
    config.setTwoStageAggregateSenderEndPointsCacheInMs(
        Long.parseLong(
            properties.getProperty(
                "two_stage_aggregate_sender_end_points_cache_in_ms",
                String.valueOf(config.getTwoStageAggregateSenderEndPointsCacheInMs()))));

    config.setPipeEventReferenceTrackingEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_event_reference_tracking_enabled",
                String.valueOf(config.getPipeEventReferenceTrackingEnabled()))));
    config.setPipeEventReferenceEliminateIntervalSeconds(
        Long.parseLong(
            properties.getProperty(
                "pipe_event_reference_eliminate_interval_seconds",
                String.valueOf(config.getPipeEventReferenceEliminateIntervalSeconds()))));
    config.setPipeListeningQueueTransferSnapshotThreshold(
        Long.parseLong(
            properties.getProperty(
                "pipe_listening_queue_transfer_snapshot_threshold",
                String.valueOf(config.getPipeListeningQueueTransferSnapshotThreshold()))));

    config.setPipeSnapshotExecutionMaxBatchSize(
        Integer.parseInt(
            properties.getProperty(
                "pipe_snapshot_execution_max_batch_size",
                String.valueOf(config.getPipeSnapshotExecutionMaxBatchSize()))));
    config.setPipeRemainingTimeCommitRateAverageTime(
        PipeRateAverage.valueOf(
            properties
                .getProperty(
                    "pipe_remaining_time_commit_rate_average_time",
                    String.valueOf(config.getPipeRemainingTimeCommitRateAverageTime()))
                .trim()));
    config.setPipeRemainingInsertNodeCountEMAAlpha(
        Double.parseDouble(
            properties
                .getProperty(
                    "pipe_remaining_insert_node_count_ema_alpha",
                    String.valueOf(config.getPipeRemainingInsertNodeCountEMAAlpha()))
                .trim()));
  }

  public static void loadPipeInternalConfig(CommonConfig config, TrimProperties properties) {
    config.setPipeFileReceiverFsyncEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_file_receiver_fsync_enabled",
                Boolean.toString(config.getPipeFileReceiverFsyncEnabled()))));

    config.setPipeDataStructureTabletRowSize(
        Integer.parseInt(
            properties.getProperty(
                "pipe_data_structure_tablet_row_size",
                String.valueOf(config.getPipeDataStructureTabletRowSize()))));
    config.setPipeDataStructureTabletSizeInBytes(
        Integer.parseInt(
            properties.getProperty(
                "pipe_data_structure_tablet_size_in_bytes",
                String.valueOf(config.getPipeDataStructureTabletSizeInBytes()))));
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_data_structure_tablet_memory_block_allocation_reject_threshold",
                String.valueOf(
                    config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()))));
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_data_structure_ts_file_memory_block_allocation_reject_threshold",
                String.valueOf(
                    config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()))));
    config.setPipeTotalFloatingMemoryProportion(
        Double.parseDouble(
            properties.getProperty(
                "pipe_total_floating_memory_proportion",
                String.valueOf(config.getPipeTotalFloatingMemoryProportion()))));

    config.setIsPipeEnableMemoryChecked(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_enable_memory_checked", String.valueOf(config.isPipeEnableMemoryChecked()))));
    config.setPipeInsertNodeQueueMemory(
        Long.parseLong(
            properties.getProperty(
                "pipe_insert_node_queue_memory",
                String.valueOf(config.getPipeInsertNodeQueueMemory()))));
    config.setPipeTsFileParserMemory(
        Long.parseLong(
            properties.getProperty(
                "pipe_tsfile_parser_memory", String.valueOf(config.getPipeTsFileParserMemory()))));
    config.setPipeSinkBatchMemoryInsertNode(
        Long.parseLong(
            properties.getProperty(
                "pipe_sink_batch_memory_insert_node",
                String.valueOf(config.getPipeSinkBatchMemoryInsertNode()))));
    config.setPipeSinkBatchMemoryTsFile(
        Long.parseLong(
            properties.getProperty(
                "pipe_sink_batch_memory_ts_file",
                String.valueOf(config.getPipeSinkBatchMemoryTsFile()))));
    config.setPipeSendTsFileReadBuffer(
        Long.parseLong(
            properties.getProperty(
                "pipe_send_tsfile_read_buffer",
                String.valueOf(config.getPipeSendTsFileReadBuffer()))));
    config.setPipeReservedMemoryPercentage(
        Double.parseDouble(
            properties.getProperty(
                "pipe_reserved_memory_percentage",
                String.valueOf(config.getPipeReservedMemoryPercentage()))));
    config.setPipeMinimumReceiverMemory(
        Long.parseLong(
            properties.getProperty(
                "pipe_minimum_receiver_memory",
                String.valueOf(config.getPipeMinimumReceiverMemory()))));

    config.setPipeRealTimeQueuePollTsFileThreshold(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_realtime_queue_poll_history_threshold"))
                .orElse(
                    properties.getProperty(
                        "pipe_realtime_queue_poll_tsfile_threshold",
                        String.valueOf(config.getPipeRealTimeQueuePollTsFileThreshold())))));
    config.setPipeRealTimeQueuePollHistoricalTsFileThreshold(
        Integer.parseInt(
            properties.getProperty(
                "pipe_realtime_queue_poll_historical_tsfile_threshold",
                String.valueOf(config.getPipeRealTimeQueuePollHistoricalTsFileThreshold()))));
    config.setPipeRealTimeQueueMaxWaitingTsFileSize(
        Integer.parseInt(
            properties.getProperty(
                "pipe_realTime_queue_max_waiting_tsFile_size",
                String.valueOf(config.getPipeRealTimeQueueMaxWaitingTsFileSize()))));
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount(
        Integer.parseInt(
            properties.getProperty(
                "pipe_subtask_executor_basic_check_point_interval_by_consumed_event_count",
                String.valueOf(
                    config.getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount()))));
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration(
        Long.parseLong(
            properties.getProperty(
                "pipe_subtask_executor_basic_check_point_interval_by_time_duration",
                String.valueOf(
                    config.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration()))));
    config.setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_subtask_executor_pending_queue_max_blocking_time_ms",
                String.valueOf(config.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs()))));
    config.setPipeRetryLocallyForParallelOrUserConflict(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_retry_locally_for_user_conflict",
                String.valueOf(config.isPipeRetryLocallyForParallelOrUserConflict()))));

    config.setPipeSinkSubtaskSleepIntervalInitMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_sink_subtask_sleep_interval_init_ms",
                String.valueOf(config.getPipeSinkSubtaskSleepIntervalInitMs()))));
    config.setPipeSinkSubtaskSleepIntervalMaxMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_sink_subtask_sleep_interval_max_ms",
                String.valueOf(config.getPipeSinkSubtaskSleepIntervalMaxMs()))));

    config.setPipeSourceAssignerDisruptorRingBufferSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_source_assigner_disruptor_ring_buffer_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_assigner_disruptor_ring_buffer_size",
                        String.valueOf(config.getPipeSourceAssignerDisruptorRingBufferSize())))));
    config.setPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes( // 1MB
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty(
                        "pipe_source_assigner_disruptor_ring_buffer_entry_size_in_bytes"))
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_assigner_disruptor_ring_buffer_entry_size_in_bytes",
                        String.valueOf(
                            config.getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes())))));

    config.setPipeSourceMatcherCacheSize(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_source_matcher_cache_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_extractor_matcher_cache_size",
                        String.valueOf(config.getPipeSourceMatcherCacheSize())))));

    config.setPipeConnectorHandshakeTimeoutMs(
        Long.parseLong(
            Optional.ofNullable(properties.getProperty("pipe_sink_handshake_timeout_ms"))
                .orElse(
                    properties.getProperty(
                        "pipe_connector_handshake_timeout_ms",
                        String.valueOf(config.getPipeConnectorHandshakeTimeoutMs())))));
    config.setPipeConnectorReadFileBufferSize(
        Integer.parseInt(
            Optional.ofNullable(properties.getProperty("pipe_sink_read_file_buffer_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_connector_read_file_buffer_size",
                        String.valueOf(config.getPipeConnectorReadFileBufferSize())))));
    config.setIsPipeConnectorReadFileBufferMemoryControlEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(properties.getProperty("pipe_sink_read_file_buffer_memory_control"))
                .orElse(
                    properties.getProperty(
                        "pipe_connector_read_file_buffer_memory_control",
                        String.valueOf(
                            config.isPipeConnectorReadFileBufferMemoryControlEnabled())))));
    config.setPipeConnectorRetryIntervalMs(
        Long.parseLong(
            Optional.ofNullable(properties.getProperty("pipe_sink_retry_interval_ms"))
                .orElse(
                    properties.getProperty(
                        "pipe_connector_retry_interval_ms",
                        String.valueOf(config.getPipeConnectorRetryIntervalMs())))));
    config.setPipeConnectorRPCThriftCompressionEnabled(
        Boolean.parseBoolean(
            Optional.ofNullable(properties.getProperty("pipe_sink_rpc_thrift_compression_enabled"))
                .orElse(
                    properties.getProperty(
                        "pipe_connector_rpc_thrift_compression_enabled",
                        String.valueOf(config.isPipeConnectorRPCThriftCompressionEnabled())))));
    config.setPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall(
        Long.parseLong(
            Optional.ofNullable(
                    properties.getProperty("pipe_async_sink_max_retry_execution_time_ms_per_call"))
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_max_retry_execution_time_ms_per_call",
                        String.valueOf(
                            config.getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall())))));
    config.setPipeAsyncSinkForcedRetryTsFileEventQueueSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_async_sink_forced_retry_tsfile_event_queue_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_forced_retry_tsfile_event_queue_size",
                        String.valueOf(
                            config.getPipeAsyncSinkForcedRetryTsFileEventQueueSize())))));
    config.setPipeAsyncSinkForcedRetryTabletEventQueueSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_async_sink_forced_retry_tablet_event_queue_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_forced_retry_tablet_event_queue_size",
                        String.valueOf(
                            config.getPipeAsyncSinkForcedRetryTabletEventQueueSize())))));
    config.setPipeAsyncSinkForcedRetryTotalEventQueueSize(
        Integer.parseInt(
            Optional.ofNullable(
                    properties.getProperty("pipe_async_sink_forced_retry_total_event_queue_size"))
                .orElse(
                    properties.getProperty(
                        "pipe_async_connector_forced_retry_total_event_queue_size",
                        String.valueOf(config.getPipeAsyncSinkForcedRetryTotalEventQueueSize())))));
    config.setRateLimiterHotReloadCheckIntervalMs(
        Integer.parseInt(
            properties.getProperty(
                "rate_limiter_hot_reload_check_interval_ms",
                String.valueOf(config.getRateLimiterHotReloadCheckIntervalMs()))));

    config.setPipeConnectorRequestSliceThresholdBytes(
        Integer.parseInt(
            properties.getProperty(
                "pipe_connector_request_slice_threshold_bytes",
                String.valueOf(config.getPipeConnectorRequestSliceThresholdBytes()))));

    config.setPipeReceiverLoginPeriodicVerificationIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_receiver_login_periodic_verification_interval_ms",
                Long.toString(config.getPipeReceiverLoginPeriodicVerificationIntervalMs()))));
    config.setPipeReceiverActualToEstimatedMemoryRatio(
        Double.parseDouble(
            properties.getProperty(
                "pipe_receiver_actual_to_estimated_memory_ratio",
                Double.toString(config.getPipeReceiverActualToEstimatedMemoryRatio()))));
    config.setPipeReceiverReqDecompressedMaxLengthInBytes(
        Integer.parseInt(
            properties.getProperty(
                "pipe_receiver_req_decompressed_max_length_in_bytes",
                String.valueOf(config.getPipeReceiverReqDecompressedMaxLengthInBytes()))));
    config.setPipeReceiverLoadConversionEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_receiver_load_conversion_enabled",
                String.valueOf(config.isPipeReceiverLoadConversionEnabled()))));
    config.setPipePeriodicalLogMinIntervalSeconds(
        Long.parseLong(
            properties.getProperty(
                "pipe_periodical_log_min_interval_seconds",
                String.valueOf(config.getPipePeriodicalLogMinIntervalSeconds()))));
    config.setPipeLoggerCacheMaxSizeInBytes(
        Long.parseLong(
            properties.getProperty(
                "pipe_logger_cache_max_size_in_bytes",
                String.valueOf(config.getPipeLoggerCacheMaxSizeInBytes()))));

    config.setPipeMemoryAllocateMaxRetries(
        Integer.parseInt(
            properties.getProperty(
                "pipe_memory_allocate_max_retries",
                String.valueOf(config.getPipeMemoryAllocateMaxRetries()))));
    config.setPipeMemoryAllocateRetryIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_memory_allocate_retry_interval_in_ms",
                String.valueOf(config.getPipeMemoryAllocateRetryIntervalInMs()))));
    config.setPipeMemoryAllocateMinSizeInBytes(
        Long.parseLong(
            properties.getProperty(
                "pipe_memory_allocate_min_size_in_bytes",
                String.valueOf(config.getPipeMemoryAllocateMinSizeInBytes()))));
    config.setPipeMemoryAllocateForTsFileSequenceReaderInBytes(
        Long.parseLong(
            properties.getProperty(
                "pipe_memory_allocate_for_tsfile_sequence_reader_in_bytes",
                String.valueOf(config.getPipeMemoryAllocateForTsFileSequenceReaderInBytes()))));
    config.setPipeMemoryExpanderIntervalSeconds(
        Long.parseLong(
            properties.getProperty(
                "pipe_memory_expander_interval_seconds",
                String.valueOf(config.getPipeMemoryExpanderIntervalSeconds()))));
    config.setPipeCheckMemoryEnoughIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_check_memory_enough_interval_ms",
                String.valueOf(config.getPipeCheckMemoryEnoughIntervalMs()))));
    config.setPipeLeaderCacheMemoryUsagePercentage(
        Float.parseFloat(
            properties.getProperty(
                "pipe_leader_cache_memory_usage_percentage",
                String.valueOf(config.getPipeLeaderCacheMemoryUsagePercentage()))));
    config.setPipeMaxReaderChunkSize(
        Long.parseLong(
            properties.getProperty(
                "pipe_max_reader_chunk_size", String.valueOf(config.getPipeMaxReaderChunkSize()))));

    config.setPipeTransferTsFileSync(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_transfer_tsfile_sync", String.valueOf(config.getPipeTransferTsFileSync()))));
    config.setPipeCheckAllSyncClientLiveTimeIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "pipe_check_all_sync_client_live_time_interval_ms",
                String.valueOf(config.getPipeCheckAllSyncClientLiveTimeIntervalMs()))));
    config.setPipeTsFileResourceSegmentLockNum(
        Integer.parseInt(
            properties.getProperty(
                "pipe_tsfile_resource_segment_lock_num",
                String.valueOf(config.getPipeTsFileResourceSegmentLockNum()))));

    config.setPipeRemainingTimeCommitRateAutoSwitchSeconds(
        Long.parseLong(
            properties.getProperty(
                "pipe_remaining_time_commit_rate_auto_switch_seconds",
                String.valueOf(config.getPipeRemainingTimeCommitRateAutoSwitchSeconds()))));

    config.setPipeTsFileScanParsingThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_tsfile_scan_parsing_threshold",
                String.valueOf(config.getPipeTsFileScanParsingThreshold()))));

    config.setPipeDynamicMemoryHistoryWeight(
        Double.parseDouble(
            properties.getProperty(
                "pipe_dynamic_memory_history_weight",
                String.valueOf(config.getPipeDynamicMemoryHistoryWeight()))));

    config.setPipeDynamicMemoryAdjustmentThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_dynamic_memory_adjustment_threshold",
                String.valueOf(config.getPipeDynamicMemoryAdjustmentThreshold()))));

    config.setPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio(
        Double.parseDouble(
            properties.getProperty(
                "pipe_threshold_allocation_strategy_maximum_memory_increment_ratio",
                String.valueOf(
                    config.getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio()))));

    config.setPipeThresholdAllocationStrategyLowUsageThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_threshold_allocation_strategy_low_usage_threshold",
                String.valueOf(config.getPipeThresholdAllocationStrategyLowUsageThreshold()))));

    config.setPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold(
        Double.parseDouble(
            properties.getProperty(
                "pipe_threshold_allocation_strategy_high_usage_threshold",
                String.valueOf(
                    config.getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold()))));

    config.setPipeMaxWaitFinishTime(
        Long.parseLong(
            properties.getProperty(
                "pipe_max_wait_finish_time", String.valueOf(config.getPipeMaxWaitFinishTime()))));

    config.setPipeAutoSplitFullEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "pipe_auto_split_full_enabled",
                String.valueOf(config.getPipeAutoSplitFullEnabled()))));
  }

  public static void loadPipeExternalConfig(
      CommonConfig config, TrimProperties properties, boolean isHotModify) throws IOException {
    String value =
        parserPipeConfig(
            properties, "pipe_sink_timeout_ms", "pipe_connector_timeout_ms", isHotModify);
    if (value != null) {
      config.setPipeConnectorTransferTimeoutMs(Long.parseLong(value));
    }

    value =
        parserPipeConfig(
            properties,
            "pipe_sink_selector_number",
            "pipe_async_connector_selector_number",
            isHotModify);
    if (value != null) {
      config.setPipeAsyncConnectorSelectorNumber(Integer.parseInt(value));
    }

    value =
        parserPipeConfig(
            properties,
            "pipe_sink_max_client_number",
            "pipe_async_connector_max_client_number",
            isHotModify);
    if (value != null) {
      config.setPipeAsyncConnectorMaxClientNumber(Integer.parseInt(value));
    }

    value =
        parserPipeConfig(
            properties,
            "pipe_sink_max_tsfile_client_number",
            "pipe_async_connector_max_tsfile_client_number",
            isHotModify);
    if (value != null) {
      config.setPipeAsyncConnectorMaxTsFileClientNumber(Integer.parseInt(value));
    }

    value =
        parserPipeConfig(properties, "pipe_send_tsfile_rate_limit_bytes_per_second", isHotModify);
    if (value != null) {
      config.setPipeSendTsFileRateLimitBytesPerSecond(Double.parseDouble(value));
    }

    value = parserPipeConfig(properties, "pipe_all_sinks_rate_limit_bytes_per_second", isHotModify);
    if (value != null) {
      config.setPipeAllSinksRateLimitBytesPerSecond(Double.parseDouble(value));
    }

    value = parserPipeConfig(properties, "print_log_when_encounter_exception", isHotModify);
    if (value != null) {
      config.setPrintLogWhenEncounterException(Boolean.parseBoolean(value));
    }
  }

  private static String parserPipeConfig(
      final TrimProperties properties, final String key, final String otherKey, boolean isHotModify)
      throws IOException {

    String value =
        Optional.ofNullable(properties.getProperty(key)).orElse(properties.getProperty(otherKey));

    if (value == null && isHotModify) {
      value =
          Optional.ofNullable(ConfigurationFileUtils.getConfigurationDefaultValue(key))
              .orElse(ConfigurationFileUtils.getConfigurationDefaultValue(otherKey));
    }

    return value;
  }

  private static String parserPipeConfig(
      final TrimProperties properties, final String key, boolean isHotModify) throws IOException {
    return Optional.ofNullable(properties.getProperty(key))
        .orElse(isHotModify ? ConfigurationFileUtils.getConfigurationDefaultValue(key) : null);
  }
}
