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
import org.apache.iotdb.commons.conf.ConfigValidation;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.enums.PipeRateAverage;

import java.io.IOException;
import java.util.function.Consumer;

import static org.apache.iotdb.commons.conf.ConfigValidation.BOOLEAN;
import static org.apache.iotdb.commons.conf.ConfigValidation.DOUBLE;
import static org.apache.iotdb.commons.conf.ConfigValidation.FLOAT;
import static org.apache.iotdb.commons.conf.ConfigValidation.INT;
import static org.apache.iotdb.commons.conf.ConfigValidation.LONG;
import static org.apache.iotdb.commons.conf.ConfigValidation.STRING;
import static org.apache.iotdb.commons.conf.ConfigValidation.database;
import static org.apache.iotdb.commons.conf.ConfigValidation.databaseAlias;
import static org.apache.iotdb.commons.conf.ConfigValidation.databaseText;
import static org.apache.iotdb.commons.conf.ConfigValidation.directory;
import static org.apache.iotdb.commons.conf.ConfigValidation.enumType;
import static org.apache.iotdb.commons.conf.ConfigValidation.nonNegative;
import static org.apache.iotdb.commons.conf.ConfigValidation.positive;
import static org.apache.iotdb.commons.conf.ConfigValidation.range;
import static org.apache.iotdb.commons.conf.ConfigValidation.unrestricted;
import static org.apache.iotdb.commons.conf.ConfigValidation.withUnlimited;

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
        database(
            properties,
            "pipe_hardlink_base_dir_name",
            config.getPipeHardlinkBaseDirName(),
            STRING,
            directory()));
    config.setPipeHardlinkTsFileDirName(
        database(
            properties,
            "pipe_hardlink_tsfile_dir_name",
            config.getPipeHardlinkTsFileDirName(),
            STRING,
            directory()));
    int pipeSubtaskExecutorMaxThreadNum =
        database(
            properties,
            "pipe_subtask_executor_max_thread_num",
            config.getPipeSubtaskExecutorMaxThreadNum(),
            INT,
            unrestricted());
    if (pipeSubtaskExecutorMaxThreadNum > 0) {
      config.setPipeSubtaskExecutorMaxThreadNum(pipeSubtaskExecutorMaxThreadNum);
    }

    config.setPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds(
        databaseText(
            properties,
            "pipe_subtask_executor_cron_heartbeat_event_interval_seconds",
            String.valueOf(config.getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds()),
            LONG,
            nonNegative()));
    config.setSeperatedPipeHeartbeatEnabled(
        database(
            properties,
            "pipe_heartbeat_seperated_mode_enabled",
            config.isSeperatedPipeHeartbeatEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
        database(
            properties,
            "pipe_heartbeat_interval_seconds_for_collecting_pipe_meta",
            config.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta(),
            INT,
            nonNegative()));

    config.setPipeMetaSyncerInitialSyncDelayMinutes(
        database(
            properties,
            "pipe_meta_syncer_initial_sync_delay_minutes",
            config.getPipeMetaSyncerInitialSyncDelayMinutes(),
            LONG,
            nonNegative()));
    config.setPipeMetaSyncerSyncIntervalMinutes(
        database(
            properties,
            "pipe_meta_syncer_sync_interval_minutes",
            config.getPipeMetaSyncerSyncIntervalMinutes(),
            LONG,
            nonNegative()));
    config.setPipeMetaSyncerAutoRestartPipeCheckIntervalRound(
        database(
            properties,
            "pipe_meta_syncer_auto_restart_pipe_check_interval_round",
            config.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound(),
            LONG,
            nonNegative()));
    config.setPipeAutoRestartEnabled(
        database(
            properties,
            "pipe_auto_restart_enabled",
            config.getPipeAutoRestartEnabled(),
            BOOLEAN,
            unrestricted()));

    config.setPipeAirGapReceiverEnabled(
        database(
            properties,
            "pipe_air_gap_receiver_enabled",
            config.getPipeAirGapReceiverEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeAirGapReceiverPort(
        database(
            properties,
            "pipe_air_gap_receiver_port",
            config.getPipeAirGapReceiverPort(),
            INT,
            range(1, true, 65535, true)));
    config.setPipeAirGapRetryLocalIntervalMs(
        database(
            properties,
            "pipe_air_gap_retry_local_interval_ms",
            config.getPipeAirGapRetryLocalIntervalMs(),
            LONG,
            nonNegative()));
    config.setPipeAirGapRetryMaxMs(
        database(
            properties,
            "pipe_air_gap_retry_max_ms",
            config.getPipeAirGapRetryMaxMs(),
            LONG,
            withUnlimited(nonNegative())));

    config.setPipeMetaReportMaxLogNumPerRound(
        database(
            properties,
            "pipe_meta_report_max_log_num_per_round",
            config.getPipeMetaReportMaxLogNumPerRound(),
            DOUBLE,
            nonNegative()));
    config.setPipeMetaReportMaxLogIntervalRounds(
        database(
            properties,
            "pipe_meta_report_max_log_interval_rounds",
            config.getPipeMetaReportMaxLogIntervalRounds(),
            INT,
            nonNegative()));
    config.setPipeTsFilePinMaxLogNumPerRound(
        database(
            properties,
            "pipe_tsfile_pin_max_log_num_per_round",
            config.getPipeTsFilePinMaxLogNumPerRound(),
            INT,
            nonNegative()));
    config.setPipeTsFilePinMaxLogIntervalRounds(
        database(
            properties,
            "pipe_tsfile_pin_max_log_interval_rounds",
            config.getPipeTsFilePinMaxLogIntervalRounds(),
            INT,
            nonNegative()));
    config.setPipeMemoryManagementEnabled(
        database(
            properties,
            "pipe_memory_management_enabled",
            config.getPipeMemoryManagementEnabled(),
            BOOLEAN,
            unrestricted()));

    config.setTwoStageAggregateMaxCombinerLiveTimeInMs(
        database(
            properties,
            "two_stage_aggregate_max_combiner_live_time_in_ms",
            config.getTwoStageAggregateMaxCombinerLiveTimeInMs(),
            LONG,
            nonNegative()));
    config.setTwoStageAggregateDataRegionInfoCacheTimeInMs(
        database(
            properties,
            "two_stage_aggregate_data_region_info_cache_time_in_ms",
            config.getTwoStageAggregateDataRegionInfoCacheTimeInMs(),
            LONG,
            nonNegative()));
    config.setTwoStageAggregateSenderEndPointsCacheInMs(
        database(
            properties,
            "two_stage_aggregate_sender_end_points_cache_in_ms",
            config.getTwoStageAggregateSenderEndPointsCacheInMs(),
            LONG,
            nonNegative()));

    config.setPipeEventReferenceTrackingEnabled(
        database(
            properties,
            "pipe_event_reference_tracking_enabled",
            config.getPipeEventReferenceTrackingEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeEventReferenceEliminateIntervalSeconds(
        database(
            properties,
            "pipe_event_reference_eliminate_interval_seconds",
            config.getPipeEventReferenceEliminateIntervalSeconds(),
            LONG,
            nonNegative()));
    config.setPipeListeningQueueTransferSnapshotThreshold(
        database(
            properties,
            "pipe_listening_queue_transfer_snapshot_threshold",
            config.getPipeListeningQueueTransferSnapshotThreshold(),
            LONG,
            nonNegative()));

    config.setPipeSnapshotExecutionMaxBatchSize(
        database(
            properties,
            "pipe_snapshot_execution_max_batch_size",
            config.getPipeSnapshotExecutionMaxBatchSize(),
            INT,
            nonNegative()));
    config.setPipeRemainingTimeCommitRateAverageTime(
        databaseText(
            properties,
            "pipe_remaining_time_commit_rate_average_time",
            String.valueOf(config.getPipeRemainingTimeCommitRateAverageTime()),
            enumType(PipeRateAverage.class),
            unrestricted()));
    config.setPipeRemainingInsertNodeCountEMAAlpha(
        database(
            properties,
            "pipe_remaining_insert_node_count_ema_alpha",
            config.getPipeRemainingInsertNodeCountEMAAlpha(),
            DOUBLE,
            range(0.0, true, 1.0, true)));
  }

  public static void loadPipeInternalConfig(CommonConfig config, TrimProperties properties) {
    config.setPipeFileReceiverFsyncEnabled(
        database(
            properties,
            "pipe_file_receiver_fsync_enabled",
            config.getPipeFileReceiverFsyncEnabled(),
            BOOLEAN,
            unrestricted()));

    config.setPipeDataStructureTabletRowSize(
        database(
            properties,
            "pipe_data_structure_tablet_row_size",
            config.getPipeDataStructureTabletRowSize(),
            INT,
            nonNegative()));
    config.setPipeDataStructureTabletSizeInBytes(
        database(
            properties,
            "pipe_data_structure_tablet_size_in_bytes",
            config.getPipeDataStructureTabletSizeInBytes(),
            INT,
            nonNegative()));
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
        database(
            properties,
            "pipe_data_structure_tablet_memory_block_allocation_reject_threshold",
            config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(),
            DOUBLE,
            range(0.0, true, 1.0, true)));
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
        database(
            properties,
            "pipe_data_structure_ts_file_memory_block_allocation_reject_threshold",
            config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(),
            DOUBLE,
            nonNegative()));
    config.setPipeTotalFloatingMemoryProportion(
        database(
            properties,
            "pipe_total_floating_memory_proportion",
            config.getPipeTotalFloatingMemoryProportion(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setIsPipeEnableMemoryChecked(
        database(
            properties,
            "pipe_enable_memory_checked",
            config.isPipeEnableMemoryChecked(),
            BOOLEAN,
            unrestricted()));
    config.setPipeInsertNodeQueueMemory(
        database(
            properties,
            "pipe_insert_node_queue_memory",
            config.getPipeInsertNodeQueueMemory(),
            LONG,
            nonNegative()));
    config.setPipeTsFileParserMemory(
        database(
            properties,
            "pipe_tsfile_parser_memory",
            config.getPipeTsFileParserMemory(),
            LONG,
            nonNegative()));
    config.setPipeTsFileParserInFlightMaxNum(
        database(
            properties,
            "pipe_tsfile_parser_in_flight_max_num",
            config.getPipeTsFileParserInFlightMaxNum(),
            INT,
            nonNegative()));
    config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(
        database(
            properties,
            "pipe_tsfile_parser_in_flight_max_num_per_pipe_region",
            config.getPipeTsFileParserInFlightMaxNumPerPipeRegion(),
            INT,
            nonNegative()));
    config.setPipeSinkBatchMemoryInsertNode(
        database(
            properties,
            "pipe_sink_batch_memory_insert_node",
            config.getPipeSinkBatchMemoryInsertNode(),
            LONG,
            nonNegative()));
    config.setPipeSinkBatchMemoryTsFile(
        database(
            properties,
            "pipe_sink_batch_memory_ts_file",
            config.getPipeSinkBatchMemoryTsFile(),
            LONG,
            nonNegative()));
    config.setPipeSendTsFileReadBuffer(
        database(
            properties,
            "pipe_send_tsfile_read_buffer",
            config.getPipeSendTsFileReadBuffer(),
            LONG,
            nonNegative()));
    config.setPipeReservedMemoryPercentage(
        database(
            properties,
            "pipe_reserved_memory_percentage",
            config.getPipeReservedMemoryPercentage(),
            DOUBLE,
            range(0.0, true, 1.0, true)));
    config.setPipeMinimumReceiverMemory(
        database(
            properties,
            "pipe_minimum_receiver_memory",
            config.getPipeMinimumReceiverMemory(),
            LONG,
            nonNegative()));

    config.setPipeRealTimeQueuePollTsFileThreshold(
        databaseAlias(
            properties,
            "pipe_realtime_queue_poll_history_threshold",
            "pipe_realtime_queue_poll_tsfile_threshold",
            config.getPipeRealTimeQueuePollTsFileThreshold(),
            INT,
            nonNegative()));
    config.setPipeRealTimeQueuePollHistoricalTsFileThreshold(
        database(
            properties,
            "pipe_realtime_queue_poll_historical_tsfile_threshold",
            config.getPipeRealTimeQueuePollHistoricalTsFileThreshold(),
            INT,
            nonNegative()));
    config.setPipeRealTimeQueueMaxWaitingTsFileSize(
        database(
            properties,
            "pipe_realTime_queue_max_waiting_tsFile_size",
            config.getPipeRealTimeQueueMaxWaitingTsFileSize(),
            INT,
            nonNegative()));
    config.setPipeRealtimeForceDowngradingEnabled(
        database(
            properties,
            "pipe_realtime_force_downgrading_enabled",
            config.getPipeRealtimeForceDowngradingEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeRealtimeForceDowngradingProportion(
        database(
            properties,
            "pipe_realtime_force_downgrading_proportion",
            config.getPipeRealtimeForceDowngradingProportion(),
            DOUBLE,
            range(0.0, true, 1.0, true)));
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount(
        database(
            properties,
            "pipe_subtask_executor_basic_check_point_interval_by_consumed_event_count",
            config.getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount(),
            INT,
            nonNegative()));
    config.setPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration(
        database(
            properties,
            "pipe_subtask_executor_basic_check_point_interval_by_time_duration",
            config.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration(),
            LONG,
            nonNegative()));
    config.setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(
        database(
            properties,
            "pipe_subtask_executor_pending_queue_max_blocking_time_ms",
            config.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(),
            LONG,
            nonNegative()));
    config.setPipeRetryLocallyForParallelOrUserConflict(
        database(
            properties,
            "pipe_retry_locally_for_user_conflict",
            config.isPipeRetryLocallyForParallelOrUserConflict(),
            BOOLEAN,
            unrestricted()));

    config.setPipeSinkSubtaskSleepIntervalInitMs(
        database(
            properties,
            "pipe_sink_subtask_sleep_interval_init_ms",
            config.getPipeSinkSubtaskSleepIntervalInitMs(),
            LONG,
            nonNegative()));
    config.setPipeSinkSubtaskSleepIntervalMaxMs(
        database(
            properties,
            "pipe_sink_subtask_sleep_interval_max_ms",
            config.getPipeSinkSubtaskSleepIntervalMaxMs(),
            LONG,
            nonNegative()));

    config.setPipeSourceAssignerDisruptorRingBufferSize(
        databaseAlias(
            properties,
            "pipe_source_assigner_disruptor_ring_buffer_size",
            "pipe_extractor_assigner_disruptor_ring_buffer_size",
            config.getPipeSourceAssignerDisruptorRingBufferSize(),
            INT,
            positive()));
    config.setPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes( // 1MB
        databaseAlias(
            properties,
            "pipe_source_assigner_disruptor_ring_buffer_entry_size_in_bytes",
            "pipe_extractor_assigner_disruptor_ring_buffer_entry_size_in_bytes",
            Math.toIntExact(config.getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes()),
            INT,
            nonNegative()));

    config.setPipeSinkHandshakeTimeoutMs(
        databaseAlias(
            properties,
            "pipe_sink_handshake_timeout_ms",
            "pipe_connector_handshake_timeout_ms",
            (long) config.getPipeSinkHandshakeTimeoutMs(),
            LONG,
            nonNegative()));
    config.setPipeAirGapSinkTabletTimeoutMs(
        databaseAlias(
            properties,
            "pipe_air_gap_sink_tablet_timeout_ms",
            "pipe_air_gap_connector_tablet_timeout_ms",
            (long) config.getPipeAirGapSinkTabletTimeoutMs(),
            LONG,
            nonNegative()));
    config.setPipeSinkReadFileBufferSize(
        databaseAlias(
            properties,
            "pipe_sink_read_file_buffer_size",
            "pipe_connector_read_file_buffer_size",
            config.getPipeSinkReadFileBufferSize(),
            INT,
            nonNegative()));
    config.setIsPipeSinkReadFileBufferMemoryControlEnabled(
        databaseAlias(
            properties,
            "pipe_sink_read_file_buffer_memory_control",
            "pipe_connector_read_file_buffer_memory_control",
            config.isPipeSinkReadFileBufferMemoryControlEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeSinkRetryIntervalMs(
        databaseAlias(
            properties,
            "pipe_sink_retry_interval_ms",
            "pipe_connector_retry_interval_ms",
            config.getPipeSinkRetryIntervalMs(),
            LONG,
            nonNegative()));
    config.setPipeSinkRetryLocallyForConnectionError(
        databaseAlias(
            properties,
            "pipe_sink_retry_locally_for_connection_error",
            "pipe_connector_retry_locally_for_connection_error",
            config.isPipeSinkRetryLocallyForConnectionError(),
            BOOLEAN,
            unrestricted()));
    config.setPipeSinkRPCThriftCompressionEnabled(
        databaseAlias(
            properties,
            "pipe_sink_rpc_thrift_compression_enabled",
            "pipe_connector_rpc_thrift_compression_enabled",
            config.isPipeSinkRPCThriftCompressionEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setPipeAsyncSinkMaxRetryExecutionTimeMsPerCall(
        databaseAlias(
            properties,
            "pipe_async_sink_max_retry_execution_time_ms_per_call",
            "pipe_async_connector_max_retry_execution_time_ms_per_call",
            config.getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall(),
            LONG,
            nonNegative()));
    config.setPipeAsyncSinkRetryMaxDurationMs(
        databaseAlias(
            properties,
            "pipe_async_sink_retry_max_duration_ms",
            "pipe_async_connector_retry_max_duration_ms",
            config.getPipeAsyncSinkRetryMaxDurationMs(),
            LONG,
            withUnlimited(nonNegative())));
    config.setPipeAsyncSinkRetryProbeIntervalMs(
        databaseAlias(
            properties,
            "pipe_async_sink_retry_probe_interval_ms",
            "pipe_async_connector_retry_probe_interval_ms",
            config.getPipeAsyncSinkRetryProbeIntervalMs(),
            LONG,
            nonNegative()));
    config.setPipeAsyncSinkForcedRetryTsFileEventQueueSize(
        databaseAlias(
            properties,
            "pipe_async_sink_forced_retry_tsfile_event_queue_size",
            "pipe_async_connector_forced_retry_tsfile_event_queue_size",
            config.getPipeAsyncSinkForcedRetryTsFileEventQueueSize(),
            INT,
            nonNegative()));
    config.setPipeAsyncSinkForcedRetryTabletEventQueueSize(
        databaseAlias(
            properties,
            "pipe_async_sink_forced_retry_tablet_event_queue_size",
            "pipe_async_connector_forced_retry_tablet_event_queue_size",
            config.getPipeAsyncSinkForcedRetryTabletEventQueueSize(),
            INT,
            nonNegative()));
    config.setPipeAsyncSinkForcedRetryTotalEventQueueSize(
        databaseAlias(
            properties,
            "pipe_async_sink_forced_retry_total_event_queue_size",
            "pipe_async_connector_forced_retry_total_event_queue_size",
            config.getPipeAsyncSinkForcedRetryTotalEventQueueSize(),
            INT,
            nonNegative()));
    config.setRateLimiterHotReloadCheckIntervalMs(
        database(
            properties,
            "rate_limiter_hot_reload_check_interval_ms",
            config.getRateLimiterHotReloadCheckIntervalMs(),
            INT,
            nonNegative()));

    config.setPipeSinkRequestSliceThresholdBytes(
        databaseAlias(
            properties,
            "pipe_sink_request_slice_threshold_bytes",
            "pipe_connector_request_slice_threshold_bytes",
            config.getPipeSinkRequestSliceThresholdBytes(),
            INT,
            nonNegative()));

    config.setPipeReceiverLoginPeriodicVerificationIntervalMs(
        database(
            properties,
            "pipe_receiver_login_periodic_verification_interval_ms",
            config.getPipeReceiverLoginPeriodicVerificationIntervalMs(),
            LONG,
            withUnlimited(nonNegative())));
    config.setPipeReceiverActualToEstimatedMemoryRatio(
        database(
            properties,
            "pipe_receiver_actual_to_estimated_memory_ratio",
            config.getPipeReceiverActualToEstimatedMemoryRatio(),
            DOUBLE,
            nonNegative()));
    config.setPipeReceiverReqDecompressedMaxLengthInBytes(
        database(
            properties,
            "pipe_receiver_req_decompressed_max_length_in_bytes",
            config.getPipeReceiverReqDecompressedMaxLengthInBytes(),
            INT,
            nonNegative()));
    config.setPipeAirGapReceiverMaxPayloadSizeInBytes(
        database(
            properties,
            "pipe_air_gap_receiver_max_payload_size_in_bytes",
            config.getPipeAirGapReceiverMaxPayloadSizeInBytes(),
            INT,
            nonNegative()));
    config.setPipeAirGapReceiverUdpSocketBufferSizeInBytes(
        database(
            properties,
            "pipe_air_gap_receiver_udp_socket_buffer_size_in_bytes",
            config.getPipeAirGapReceiverUdpSocketBufferSizeInBytes(),
            INT,
            nonNegative()));
    config.setPipeReceiverLoadConversionEnabled(
        database(
            properties,
            "pipe_receiver_load_conversion_enabled",
            config.isPipeReceiverLoadConversionEnabled(),
            BOOLEAN,
            unrestricted()));
    config.setLoggerPeriodicalLogMinIntervalSeconds(
        databaseAlias(
            properties,
            "logger_periodical_log_min_interval_seconds",
            "pipe_periodical_log_min_interval_seconds",
            config.getLoggerPeriodicalLogMinIntervalSeconds(),
            LONG,
            nonNegative()));
    config.setLoggerCacheMaxSizeInBytes(
        databaseAlias(
            properties,
            "logger_cache_max_size_in_bytes",
            "pipe_logger_cache_max_size_in_bytes",
            config.getLoggerCacheMaxSizeInBytes(),
            LONG,
            nonNegative()));

    config.setPipeMemoryAllocateMaxRetries(
        database(
            properties,
            "pipe_memory_allocate_max_retries",
            config.getPipeMemoryAllocateMaxRetries(),
            INT,
            nonNegative()));
    config.setPipeMemoryAllocateRetryIntervalInMs(
        database(
            properties,
            "pipe_memory_allocate_retry_interval_in_ms",
            config.getPipeMemoryAllocateRetryIntervalInMs(),
            LONG,
            nonNegative()));
    config.setPipeMemoryAllocateMinSizeInBytes(
        database(
            properties,
            "pipe_memory_allocate_min_size_in_bytes",
            config.getPipeMemoryAllocateMinSizeInBytes(),
            LONG,
            nonNegative()));
    config.setPipeMemoryAllocateForTsFileSequenceReaderInBytes(
        database(
            properties,
            "pipe_memory_allocate_for_tsfile_sequence_reader_in_bytes",
            config.getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
            LONG,
            nonNegative()));
    config.setPipeMemoryExpanderIntervalSeconds(
        database(
            properties,
            "pipe_memory_expander_interval_seconds",
            config.getPipeMemoryExpanderIntervalSeconds(),
            LONG,
            nonNegative()));
    config.setPipeCheckMemoryEnoughIntervalMs(
        database(
            properties,
            "pipe_check_memory_enough_interval_ms",
            config.getPipeCheckMemoryEnoughIntervalMs(),
            LONG,
            nonNegative()));
    config.setPipeLeaderCacheMemoryUsagePercentage(
        database(
            properties,
            "pipe_leader_cache_memory_usage_percentage",
            config.getPipeLeaderCacheMemoryUsagePercentage(),
            FLOAT,
            range(0.0f, true, 1.0f, true)));
    config.setPipeMaxReaderChunkSize(
        database(
            properties,
            "pipe_max_reader_chunk_size",
            config.getPipeMaxReaderChunkSize(),
            LONG,
            nonNegative()));

    config.setPipeTransferTsFileSync(
        database(
            properties,
            "pipe_transfer_tsfile_sync",
            config.getPipeTransferTsFileSync(),
            BOOLEAN,
            unrestricted()));
    config.setPipeCheckAllSyncClientLiveTimeIntervalMs(
        database(
            properties,
            "pipe_check_all_sync_client_live_time_interval_ms",
            config.getPipeCheckAllSyncClientLiveTimeIntervalMs(),
            LONG,
            nonNegative()));
    config.setPipeTsFileResourceSegmentLockNum(
        database(
            properties,
            "pipe_tsfile_resource_segment_lock_num",
            config.getPipeTsFileResourceSegmentLockNum(),
            INT,
            withUnlimited(positive())));

    config.setPipeRemainingTimeCommitRateAutoSwitchSeconds(
        database(
            properties,
            "pipe_remaining_time_commit_rate_auto_switch_seconds",
            config.getPipeRemainingTimeCommitRateAutoSwitchSeconds(),
            LONG,
            withUnlimited(nonNegative())));

    config.setPipeTsFileScanParsingThreshold(
        database(
            properties,
            "pipe_tsfile_scan_parsing_threshold",
            config.getPipeTsFileScanParsingThreshold(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setPipeDynamicMemoryHistoryWeight(
        database(
            properties,
            "pipe_dynamic_memory_history_weight",
            config.getPipeDynamicMemoryHistoryWeight(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setPipeDynamicMemoryAdjustmentThreshold(
        database(
            properties,
            "pipe_dynamic_memory_adjustment_threshold",
            config.getPipeDynamicMemoryAdjustmentThreshold(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio(
        database(
            properties,
            "pipe_threshold_allocation_strategy_maximum_memory_increment_ratio",
            config.getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setPipeThresholdAllocationStrategyLowUsageThreshold(
        database(
            properties,
            "pipe_threshold_allocation_strategy_low_usage_threshold",
            config.getPipeThresholdAllocationStrategyLowUsageThreshold(),
            DOUBLE,
            range(0.0, true, 1.0, true)));

    config.setPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold(
        database(
            properties,
            "pipe_threshold_allocation_strategy_high_usage_threshold",
            config.getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold(),
            DOUBLE,
            nonNegative()));

    config.setPipeMaxWaitFinishTime(
        database(
            properties,
            "pipe_max_wait_finish_time",
            config.getPipeMaxWaitFinishTime(),
            LONG,
            nonNegative()));

    config.setPipeAutoSplitFullEnabled(
        database(
            properties,
            "pipe_auto_split_full_enabled",
            config.getPipeAutoSplitFullEnabled(),
            BOOLEAN,
            unrestricted()));
  }

  public static void loadPipeExternalConfig(
      CommonConfig config, TrimProperties properties, boolean isHotModify) throws IOException {
    loadExternal(
        properties,
        isHotModify,
        "pipe_sink_timeout_ms",
        "pipe_connector_timeout_ms",
        new ConfigValidation.Definition<>(
            LONG, (long) config.getPipeSinkTransferTimeoutMs(), positive()),
        config::setPipeSinkTransferTimeoutMs);
    loadExternal(
        properties,
        isHotModify,
        "pipe_sink_selector_number",
        "pipe_async_connector_selector_number",
        new ConfigValidation.Definition<>(
            INT, config.getPipeAsyncSinkSelectorNumber(), unrestricted()),
        config::setPipeAsyncSinkSelectorNumber);
    loadExternal(
        properties,
        isHotModify,
        "pipe_sink_max_client_number",
        "pipe_async_connector_max_client_number",
        new ConfigValidation.Definition<>(
            INT, config.getPipeAsyncSinkMaxClientNumber(), unrestricted()),
        config::setPipeAsyncSinkMaxClientNumber);
    loadExternal(
        properties,
        isHotModify,
        "pipe_sink_max_tsfile_client_number",
        "pipe_async_connector_max_tsfile_client_number",
        new ConfigValidation.Definition<>(
            INT, config.getPipeAsyncSinkMaxTsFileClientNumber(), unrestricted()),
        config::setPipeAsyncSinkMaxTsFileClientNumber);
    loadExternal(
        properties,
        isHotModify,
        "pipe_send_tsfile_rate_limit_bytes_per_second",
        null,
        new ConfigValidation.Definition<>(
            DOUBLE, config.getPipeSendTsFileRateLimitBytesPerSecond(), unrestricted()),
        config::setPipeSendTsFileRateLimitBytesPerSecond);
    loadExternal(
        properties,
        isHotModify,
        "pipe_all_sinks_rate_limit_bytes_per_second",
        null,
        new ConfigValidation.Definition<>(
            DOUBLE, config.getPipeAllSinksRateLimitBytesPerSecond(), unrestricted()),
        config::setPipeAllSinksRateLimitBytesPerSecond);
    loadExternal(
        properties,
        isHotModify,
        "print_log_when_encounter_exception",
        null,
        new ConfigValidation.Definition<>(
            BOOLEAN, config.isPrintLogWhenEncounterException(), unrestricted()),
        config::setPrintLogWhenEncounterException);
  }

  private static <T> void loadExternal(
      final TrimProperties properties,
      final boolean hotReload,
      final String key,
      final String legacyKey,
      final ConfigValidation.Definition<T> definition,
      final Consumer<T> setter)
      throws IOException {
    final String value = properties.getProperty(key, null);
    final String legacyValue = legacyKey == null ? null : properties.getProperty(legacyKey, null);
    if (value == null && legacyValue == null && !hotReload) {
      return;
    }
    String defaultValue =
        hotReload ? ConfigurationFileUtils.getConfigurationDefaultValue(key) : null;
    if (defaultValue == null && hotReload && legacyKey != null) {
      defaultValue = ConfigurationFileUtils.getConfigurationDefaultValue(legacyKey);
    }
    if (value == null && legacyValue == null && defaultValue == null) {
      return;
    }
    final ConfigValidation.Definition<T> withDefault =
        new ConfigValidation.Definition<>(
            definition.type(),
            ConfigValidation.databaseValue(key, defaultValue, definition),
            definition.rule());
    setter.accept(
        value == null && legacyValue != null
            ? ConfigValidation.databaseValue(properties, legacyKey, legacyValue, withDefault)
            : ConfigValidation.databaseValue(properties, key, value, withDefault));
  }
}
