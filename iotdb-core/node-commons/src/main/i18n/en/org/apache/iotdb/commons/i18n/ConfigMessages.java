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

package org.apache.iotdb.commons.i18n;

public final class ConfigMessages {

  // ===================== Generic config-set-to pattern =====================
  public static final String CONFIG_SET_TO = "{} is set to {}.";

  // ===================== CommonConfig: system mode / status =====================
  public static final String FAIL_TO_GET_CANONICAL_PATH = "Fail to get canonical path of {}";
  public static final String SET_SYSTEM_MODE = "Set system mode from {} to {}.";
  public static final String STATUS_CHANGE_TO_READ_ONLY =
      "Change system status to ReadOnly! Only query statements are permitted!";
  public static final String STATUS_CHANGE_TO_REMOVING =
      "Change system status to Removing! The current Node is being removed from cluster!";

  // ===================== CommonConfig: timestamp precision =====================
  public static final String WRONG_TIMESTAMP_PRECISION =
      "Wrong timestamp precision, please set as: ms, us or ns ! Current is: {}";

  // ===================== CommonConfig: pipe timeout overflow =====================
  public static final String PIPE_CONNECTOR_HANDSHAKE_TIMEOUT_TOO_LARGE =
      "Given pipe connector handshake timeout is too large, set to {} ms.";
  public static final String PIPE_AIR_GAP_SINK_TABLET_TIMEOUT_TOO_LARGE =
      "Given pipe air gap sink tablet timeout is too large, set to {} ms.";
  public static final String PIPE_SINK_TRANSFER_TIMEOUT_TOO_LARGE =
      "Given pipe sink transfer timeout is too large, set to {} ms.";

  // ===================== CommonConfig: pipe validation =====================
  public static final String CONFIG_MUST_BE_POSITIVE =
      "{} should be greater than 0, configuring it not to change.";
  public static final String IGNORE_INVALID_CONFIG_MUST_BE_POSITIVE =
      "Ignore invalid {} {}, because it must be greater than 0.";

  // ===================== CommonConfig: audit log (SLF4J {} placeholders) =====================
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE =
      "Unsupported audit log operation type: {}";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL =
      "Unsupported audit log operation level: {}";

  // ===================== CommonConfig: audit log (String.format %s placeholders) ==============
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE_EX =
      "Unsupported audit log operation type: %s";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL_EX =
      "Unsupported audit log operation level: %s";

  // ===================== ConfigurationFileUtils =====================
  public static final String FAILED_TO_UPDATE_APPLIED_PROPERTIES =
      "Failed to update applied properties";
  public static final String FAILED_TO_READ_CONFIGURATION_TEMPLATE =
      "Failed to read configuration template";
  public static final String UPDATING_CONFIGURATION_FILE = "Updating configuration file {}";
  public static final String WAITING_TO_ACQUIRE_CONFIG_FILE_LOCK =
      "Waiting for {} seconds to acquire configuration file update lock."
          + " There may have been an unexpected interruption in the last"
          + " configuration file update. Ignore temporary file {}";

  private ConfigMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_PIPEDATASTRUCTURETABLETSIZEINBYTES_SET_ARG_243363B3 = "pipeDataStructureTabletSizeInBytes is set to {}.";
  public static final String LOG_PIPEDATASTRUCTURETABLETMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_EF34614A = "pipeDataStructureTabletMemoryBlockAllocationRejectThreshold is set to {}.";
  public static final String LOG_PIPEDATASTRUCTURETSFILEMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_309A7E12 = "pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold is set to {}.";
  public static final String LOG_PIPETOTALFLOATINGMEMORYPROPORTION_SET_ARG_FDCA8082 = "pipeTotalFloatingMemoryProportion is set to {}.";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERSIZE_SET_ARG_31C9A8D8 = "pipeSourceAssignerDisruptorRingBufferSize is set to {}.";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERENTRYSIZE_SET_ARG_95D31172 = "pipeSourceAssignerDisruptorRingBufferEntrySize is set to {}.";
  public static final String LOG_PIPECONNECTORHANDSHAKETIMEOUTMS_SET_ARG_64890ED2 = "pipeConnectorHandshakeTimeoutMs is set to {}.";
  public static final String LOG_PIPEAIRGAPSINKTABLETTIMEOUTMS_SET_ARG_3413AC05 = "pipeAirGapSinkTabletTimeoutMs is set to {}.";
  public static final String LOG_ISPIPESINKREADFILEBUFFERMEMORYCONTROLENABLED_SET_ARG_138BB142 = "isPipeSinkReadFileBufferMemoryControlEnabled is set to {}.";
  public static final String LOG_PIPESINKRPCTHRIFTCOMPRESSIONENABLED_SET_ARG_1F2B6AB4 = "pipeSinkRPCThriftCompressionEnabled is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTSFILEEVENTQUEUESIZE_SET_ARG_0BB1C280 = "pipeAsyncSinkForcedRetryTsFileEventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTABLETEVENTQUEUESIZE_SET_ARG_8FDA7023 = "pipeAsyncSinkForcedRetryTabletEventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTOTALEVENTQUEUESIZE_SET_ARG_92D6EACB = "pipeAsyncSinkForcedRetryTotalEventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKMAXRETRYEXECUTIONTIMEMSPERCALL_SET_ARG_77E7B216 = "pipeAsyncSinkMaxRetryExecutionTimeMsPerCall is set to {}.";
  public static final String LOG_PIPEASYNCSINKSELECTORNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_EEB9793C = "pipeAsyncSinkSelectorNumber should be greater than 0, configuring it not to change.";
  public static final String LOG_PIPEASYNCSINKMAXCLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_11EF47BF = " pipeAsyncSinkMaxClientNumber should be greater than 0, configuring it not to change.";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_AC812FE2 = "pipeAsyncSinkMaxTsFileClientNumber should be greater than 0, configuring it not to change.";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SET_ARG_7D83FCDE = "pipeAsyncSinkMaxTsFileClientNumber is set to {}.";
  public static final String LOG_PIPEHEARTBEATINTERVALSECONDSFORCOLLECTINGPIPEMETA_SET_ARG_E171AAAD = "pipeHeartbeatIntervalSecondsForCollectingPipeMeta is set to {}.";
  public static final String LOG_PIPEMETASYNCERINITIALSYNCDELAYMINUTES_SET_ARG_6E36A895 = "pipeMetaSyncerInitialSyncDelayMinutes is set to {}.";
  public static final String LOG_PIPEMETASYNCERSYNCINTERVALMINUTES_SET_ARG_CFBACD71 = "pipeMetaSyncerSyncIntervalMinutes is set to {}.";
  public static final String LOG_PIPEMETASYNCERAUTORESTARTPIPECHECKINTERVALROUND_SET_ARG_A80B4589 = "pipeMetaSyncerAutoRestartPipeCheckIntervalRound is set to {}.";
  public static final String LOG_PIPESINKRETRYLOCALLYFORCONNECTIONERROR_SET_ARG_5D886CE6 = "pipeSinkRetryLocallyForConnectionError is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYCONSUMEDEVENTCOUNT_SET_ARG_CFCECFCE = "pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYTIMEDURATION_SET_ARG_45B3F433 = "pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORMAXTHREADNUM_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_25E0CE6E = "pipeSubtaskExecutorMaxThreadNum should be greater than 0, configuring it not to change.";
  public static final String LOG_PIPERETRYLOCALLYFORPARALLELORUSERCONFLICT_SET_ARG_368926E5 = "pipeRetryLocallyForParallelOrUserConflict is set to {}.";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALINITMS_SET_ARG_B8DCF143 = "pipeSinkSubtaskSleepIntervalInitMs is set to {}.";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALMAXMS_SET_ARG_0010425D = "pipeSinkSubtaskSleepIntervalMaxMs is set to {}.";
  public static final String LOG_PIPESUBTASKEXECUTORPENDINGQUEUEMAXBLOCKINGTIMEMS_SET_ARG_2F1A6865 = "pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORCRONHEARTBEATEVENTINTERVALSECONDS_SET_ARG_B5C9E195 = "pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds is set to {}.";
  public static final String LOG_PIPEREALTIMEQUEUEPOLLHISTORICALTSFILETHRESHOLD_SET_ARG_FD88A384 = "pipeRealTimeQueuePollHistoricalTsFileThreshold is set to {}";
  public static final String LOG_PIPEREALTIMEQUEUEMAXWAITINGTSFILESIZE_SET_ARG_7E0698AB = "pipeRealTimeQueueMaxWaitingTsFileSize is set to {}.";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGTIME_SET_ARG_98A0F8AE = "pipeRealtimeForceDowngradingTime is set to {}.";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGPROPORTION_SET_ARG_92974D0B = "pipeRealtimeForceDowngradingProportion is set to {}.";
  public static final String LOG_PIPERECEIVERLOGINPERIODICVERIFICATIONINTERVALMS_SET_ARG_158C791C = "pipeReceiverLoginPeriodicVerificationIntervalMs is set to {}";
  public static final String LOG_PIPERECEIVERACTUALTOESTIMATEDMEMORYRATIO_SET_ARG_0D1F305D = "pipeReceiverActualToEstimatedMemoryRatio is set to {}";
  public static final String LOG_PIPERECEIVERREQDECOMPRESSEDMAXLENGTHINBYTES_SET_ARG_9356E410 = "pipeReceiverReqDecompressedMaxLengthInBytes is set to {}.";
  public static final String LOG_IGNORE_INVALID_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_ARG_BECAUSE_IT_MUST_GREATER_THAN_0_8ACA836C = "Ignore invalid pipeAirGapReceiverMaxPayloadSizeInBytes {}, because it must be greater than 0.";
  public static final String LOG_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_SET_ARG_9B21877F = "pipeAirGapReceiverMaxPayloadSizeInBytes is set to {}.";
  public static final String LOG_PIPEPERIODICALLOGMININTERVALSECONDS_SET_ARG_5535C79E = "pipePeriodicalLogMinIntervalSeconds is set to {}.";
  public static final String LOG_PIPEMETAREPORTMAXLOGINTERVALROUNDS_SET_ARG_0090AECB = "pipeMetaReportMaxLogIntervalRounds is set to {}";
  public static final String LOG_PIPETSFILEPINMAXLOGINTERVALROUNDS_SET_ARG_FAFE1040 = "pipeTsFilePinMaxLogIntervalRounds is set to {}";
  public static final String LOG_PIPEMEMORYALLOCATEFORTSFILESEQUENCEREADERINBYTES_SET_ARG_8A26960D = "pipeMemoryAllocateForTsFileSequenceReaderInBytes is set to {}";
  public static final String LOG_PIPEMEMORYEXPANDERINTERVALSECONDS_SET_ARG_73F96BBC = "pipeMemoryExpanderIntervalSeconds is set to {}";
  public static final String LOG_PIPEMEMORYALLOCATERETRYINTERVALMS_SET_ARG_39D52E47 = "pipeMemoryAllocateRetryIntervalMs is set to {}";
  public static final String LOG_PIPELEADERCACHEMEMORYUSAGEPERCENTAGE_SET_ARG_E32DE64B = "pipeLeaderCacheMemoryUsagePercentage is set to {}";
  public static final String LOG_PIPELISTENINGQUEUETRANSFERSNAPSHOTTHRESHOLD_SET_ARG_FD856477 = "pipeListeningQueueTransferSnapshotThreshold is set to {}";
  public static final String LOG_PIPESNAPSHOTEXECUTIONMAXBATCHSIZE_SET_ARG_F1C5C62C = "pipeSnapshotExecutionMaxBatchSize is set to {}";
  public static final String LOG_PIPEREMAININGTIMECOMMITRATEAUTOSWITCHSECONDS_SET_ARG_17E6C979 = "pipeRemainingTimeCommitRateAutoSwitchSeconds is set to {}";
  public static final String LOG_PIPEREMAININGTIMECOMMITRATEAVERAGETIME_SET_ARG_D010BE98 = "pipeRemainingTimeCommitRateAverageTime is set to {}";
  public static final String LOG_PIPEREMAININGINSERTEVENTCOUNTAVERAGE_SET_ARG_17C28F47 = "pipeRemainingInsertEventCountAverage is set to {}";
  public static final String LOG_PIPEDYNAMICMEMORYADJUSTMENTTHRESHOLD_SET_ARG_2F008DB1 = "pipeDynamicMemoryAdjustmentThreshold is set to {}";
  public static final String LOG_PIPETHRESHOLDALLOCATIONSTRATEGYMAXIMUMMEMORYINCREMENTRATIO_SET_ARG_BFAD04E0 = "pipeThresholdAllocationStrategyMaximumMemoryIncrementRatio is set to {}";
  public static final String LOG_PIPEMEMORYBLOCKLOWUSAGETHRESHOLD_SET_ARG_DDF99D69 = "pipeMemoryBlockLowUsageThreshold is set to {}";
  public static final String LOG_PIPETHRESHOLDALLOCATIONSTRATEGYFIXEDMEMORYHIGHUSAGETHRESHOLD_SET_ARG_82721CBE = "pipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold is set to {}";
  public static final String LOG_PIPECHECKSYNCALLCLIENTLIVETIMEINTERVALMS_SET_ARG_246CE0EB = "pipeCheckSyncAllClientLiveTimeIntervalMs is set to {}";
  public static final String LOG_PIPESENDTSFILERATELIMITBYTESPERSECOND_SET_ARG_653F2CC4 = "pipeSendTsFileRateLimitBytesPerSecond is set to {}";
  public static final String LOG_PIPEALLSINKSRATELIMITBYTESPERSECOND_SET_ARG_EE3FE2A0 = "pipeAllSinksRateLimitBytesPerSecond is set to {}";
  public static final String LOG_RATELIMITERHOTRELOADCHECKINTERVALMS_SET_ARG_E086A4F0 = "rateLimiterHotReloadCheckIntervalMs is set to {}";
  public static final String LOG_PIPECONNECTORREQUESTSLICETHRESHOLDBYTES_SET_ARG_7FAA56F2 = "pipeConnectorRequestSliceThresholdBytes is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATEMAXCOMBINERLIVETIMEINMS_SET_ARG_F10B7C02 = "twoStageAggregateMaxCombinerLiveTimeInMs is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATEDATAREGIONINFOCACHETIMEINMS_SET_ARG_C7895888 = "twoStageAggregateDataRegionInfoCacheTimeInMs is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATESENDERENDPOINTSCACHEINMS_SET_ARG_A3CF42B2 = "twoStageAggregateSenderEndPointsCacheInMs is set to {}";
  public static final String LOG_PIPEEVENTREFERENCETRACKINGENABLED_SET_ARG_98E9A640 = "pipeEventReferenceTrackingEnabled is set to {}";
  public static final String LOG_PIPEEVENTREFERENCEELIMINATEINTERVALSECONDS_SET_ARG_62542387 = "pipeEventReferenceEliminateIntervalSeconds is set to {}";

}
