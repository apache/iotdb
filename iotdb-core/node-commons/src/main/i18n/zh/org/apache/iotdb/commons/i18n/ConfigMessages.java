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
  public static final String CONFIG_SET_TO = "{} 已设置为 {}。";

  // ===================== CommonConfig: system mode / status =====================
  public static final String FAIL_TO_GET_CANONICAL_PATH = "无法获取 {} 的规范路径";
  public static final String SET_SYSTEM_MODE = "系统模式从 {} 切换为 {}。";
  public static final String STATUS_CHANGE_TO_READ_ONLY =
      "系统状态已切换为只读模式！仅允许执行查询语句！";
  public static final String STATUS_CHANGE_TO_REMOVING =
      "系统状态已切换为移除中！当前节点正在从集群中移除！";

  // ===================== CommonConfig: timestamp precision =====================
  public static final String WRONG_TIMESTAMP_PRECISION =
      "时间戳精度设置错误，请设置为 ms、us 或 ns！当前值为：{}";

  // ===================== CommonConfig: pipe timeout overflow =====================
  public static final String PIPE_CONNECTOR_HANDSHAKE_TIMEOUT_TOO_LARGE =
      "Pipe 连接器握手超时值过大，已设置为 {} 毫秒。";
  public static final String PIPE_AIR_GAP_SINK_TABLET_TIMEOUT_TOO_LARGE =
      "Pipe 气隙接收端 Tablet 超时值过大，已设置为 {} 毫秒。";
  public static final String PIPE_SINK_TRANSFER_TIMEOUT_TOO_LARGE =
      "Pipe 接收端传输超时值过大，已设置为 {} 毫秒。";

  // ===================== CommonConfig: pipe validation =====================
  public static final String CONFIG_MUST_BE_POSITIVE =
      "{} 必须大于 0，配置未变更。";
  public static final String IGNORE_INVALID_CONFIG_MUST_BE_POSITIVE =
      "忽略无效的 {} 值 {}，该配置项必须大于 0。";

  // ===================== CommonConfig: audit log (SLF4J {} placeholders) =====================
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE =
      "不支持的审计日志操作类型：{}";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL =
      "不支持的审计日志操作级别：{}";

  // ===================== CommonConfig: audit log (String.format %s placeholders) ==============
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE_EX =
      "不支持的审计日志操作类型：%s";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL_EX =
      "不支持的审计日志操作级别：%s";

  // ===================== ConfigurationFileUtils =====================
  public static final String FAILED_TO_UPDATE_APPLIED_PROPERTIES =
      "更新已应用的配置属性失败";
  public static final String FAILED_TO_READ_CONFIGURATION_TEMPLATE =
      "读取配置模板文件失败";
  public static final String UPDATING_CONFIGURATION_FILE = "正在更新配置文件 {}";
  public static final String WAITING_TO_ACQUIRE_CONFIG_FILE_LOCK =
      "已等待 {} 秒以获取配置文件更新锁。"
          + "上一次配置文件更新可能发生了意外中断。"
          + "忽略临时文件 {}";

  private ConfigMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_THERE_MAY_HAVE_BEEN_UNEXPECTED_INTERRUPTION_LAST_E784B008 = " There may 已an unexpected interruption in the last";
  public static final String LOG_CONFIGURATION_FILE_UPDATE_IGNORE_TEMPORARY_FILE_ARG_3DE7B218 = " configuration 文件 update. Ignore temporary 文件 {}";
  public static final String LOG_PIPEDATASTRUCTURETABLETSIZEINBYTES_SET_ARG_243363B3 = "pipeDataStructureTabletSizeInBytes is set to {}.";
  public static final String LOG_PIPEDATASTRUCTURETABLETMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_EF34614A = "pipeDataStructureTabletMemoryBlockAllocationRejectThreshold is set to {}.";
  public static final String LOG_PIPEDATASTRUCTURETSFILEMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_309A7E12 = "pipeDataStructureTs文件MemoryBlockAllocationRejectThreshold is set to {}.";
  public static final String LOG_PIPETOTALFLOATINGMEMORYPROPORTION_SET_ARG_FDCA8082 = "pipeTotalFloatingMemoryProportion is set to {}.";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERSIZE_SET_ARG_31C9A8D8 = "pipeSourceAssignerDisruptorRingBufferSize is set to {}.";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERENTRYSIZE_SET_ARG_95D31172 = "pipeSourceAssignerDisruptorRingBufferEntrySize is set to {}.";
  public static final String LOG_PIPECONNECTORHANDSHAKETIMEOUTMS_SET_ARG_64890ED2 = "pipeConnectorHandshake超时Ms is set to {}.";
  public static final String LOG_PIPEAIRGAPSINKTABLETTIMEOUTMS_SET_ARG_3413AC05 = "pipeAirGapSinkTablet超时Ms is set to {}.";
  public static final String LOG_ISPIPESINKREADFILEBUFFERMEMORYCONTROLENABLED_SET_ARG_138BB142 = "isPipeSink读取文件BufferMemoryControlEnabled is set to {}.";
  public static final String LOG_PIPESINKRPCTHRIFTCOMPRESSIONENABLED_SET_ARG_1F2B6AB4 = "pipeSinkRPCThriftCompressionEnabled is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTSFILEEVENTQUEUESIZE_SET_ARG_0BB1C280 = "pipeAsyncSinkForcedRetryTs文件EventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTABLETEVENTQUEUESIZE_SET_ARG_8FDA7023 = "pipeAsyncSinkForcedRetryTabletEventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTOTALEVENTQUEUESIZE_SET_ARG_92D6EACB = "pipeAsyncSinkForcedRetryTotalEventQueueSize is set to {}.";
  public static final String LOG_PIPEASYNCSINKMAXRETRYEXECUTIONTIMEMSPERCALL_SET_ARG_77E7B216 = "pipeAsyncSinkMaxRetryExecutionTimeMsPerCall is set to {}.";
  public static final String LOG_PIPEASYNCSINKSELECTORNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_EEB9793C = "pipeAsyncSinkSelectorNumber 应be greater than 0, configuring it 不to change.";
  public static final String LOG_PIPEASYNCSINKMAXCLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_11EF47BF = " pipeAsyncSinkMaxClientNumber 应be greater than 0, configuring it 不to change.";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_AC812FE2 = "pipeAsyncSinkMaxTs文件ClientNumber 应be greater than 0, configuring it 不to change.";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SET_ARG_7D83FCDE = "pipeAsyncSinkMaxTs文件ClientNumber is set to {}.";
  public static final String LOG_PIPEHEARTBEATINTERVALSECONDSFORCOLLECTINGPIPEMETA_SET_ARG_E171AAAD = "pipeHeartbeatIntervalSecondsForCollectingPipeMeta is set to {}.";
  public static final String LOG_PIPEMETASYNCERINITIALSYNCDELAYMINUTES_SET_ARG_6E36A895 = "pipeMetaSyncerInitialSyncDelayMinutes is set to {}.";
  public static final String LOG_PIPEMETASYNCERSYNCINTERVALMINUTES_SET_ARG_CFBACD71 = "pipeMetaSyncerSyncIntervalMinutes is set to {}.";
  public static final String LOG_PIPEMETASYNCERAUTORESTARTPIPECHECKINTERVALROUND_SET_ARG_A80B4589 = "pipeMetaSyncerAutoRe开始PipeCheckIntervalRound is set to {}.";
  public static final String LOG_PIPESINKRETRYLOCALLYFORCONNECTIONERROR_SET_ARG_5D886CE6 = "pipeSinkRetryLocallyFor连接错误 is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYCONSUMEDEVENTCOUNT_SET_ARG_CFCECFCE = "pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYTIMEDURATION_SET_ARG_45B3F433 = "pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORMAXTHREADNUM_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_25E0CE6E = "pipeSubtaskExecutorMaxTh读取Num 应be greater than 0, configuring it 不to change.";
  public static final String LOG_PIPERETRYLOCALLYFORPARALLELORUSERCONFLICT_SET_ARG_368926E5 = "pipeRetryLocallyForParallelOr用户Conflict is set to {}.";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALINITMS_SET_ARG_B8DCF143 = "pipeSinkSubtaskSleepIntervalInitMs is set to {}.";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALMAXMS_SET_ARG_0010425D = "pipeSinkSubtaskSleepIntervalMaxMs is set to {}.";
  public static final String LOG_PIPESUBTASKEXECUTORPENDINGQUEUEMAXBLOCKINGTIMEMS_SET_ARG_2F1A6865 = "pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs is set to {}";
  public static final String LOG_PIPESUBTASKEXECUTORCRONHEARTBEATEVENTINTERVALSECONDS_SET_ARG_B5C9E195 = "pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds is set to {}.";
  public static final String LOG_PIPEREALTIMEQUEUEPOLLHISTORICALTSFILETHRESHOLD_SET_ARG_FD88A384 = "pipeRealTimeQueuePollHistoricalTs文件Threshold is set to {}";
  public static final String LOG_PIPEREALTIMEQUEUEMAXWAITINGTSFILESIZE_SET_ARG_7E0698AB = "pipeRealTimeQueueMaxWaitingTs文件Size is set to {}.";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGTIME_SET_ARG_98A0F8AE = "pipeRealtimeForceDowngradingTime is set to {}.";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGPROPORTION_SET_ARG_92974D0B = "pipeRealtimeForceDowngradingProportion is set to {}.";
  public static final String LOG_PIPERECEIVERLOGINPERIODICVERIFICATIONINTERVALMS_SET_ARG_158C791C = "pipeReceiverLoginPeriodicVerificationIntervalMs is set to {}";
  public static final String LOG_PIPERECEIVERACTUALTOESTIMATEDMEMORYRATIO_SET_ARG_0D1F305D = "pipeReceiverActualToEstimatedMemoryRatio is set to {}";
  public static final String LOG_PIPERECEIVERREQDECOMPRESSEDMAXLENGTHINBYTES_SET_ARG_9356E410 = "pipeReceiverReqDecompressedMaxLengthInBytes is set to {}.";
  public static final String LOG_IGNORE_INVALID_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_ARG_BECAUSE_IT_MUST_GREATER_THAN_0_8ACA836C = "Ignore 无效的pipeAirGapReceiverMaxPay加载SizeInBytes {},，原因：it 必须be greater than 0.";
  public static final String LOG_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_SET_ARG_9B21877F = "pipeAirGapReceiverMaxPay加载SizeInBytes is set to {}.";
  public static final String LOG_PIPEPERIODICALLOGMININTERVALSECONDS_SET_ARG_5535C79E = "pipePeriodicalLogMinIntervalSeconds is set to {}.";
  public static final String LOG_PIPEMETAREPORTMAXLOGINTERVALROUNDS_SET_ARG_0090AECB = "pipeMetaReportMaxLogIntervalRounds is set to {}";
  public static final String LOG_PIPETSFILEPINMAXLOGINTERVALROUNDS_SET_ARG_FAFE1040 = "pipeTs文件PinMaxLogIntervalRounds is set to {}";
  public static final String LOG_PIPEMEMORYALLOCATEFORTSFILESEQUENCEREADERINBYTES_SET_ARG_8A26960D = "pipeMemoryAllocateForTs文件Sequence读取erInBytes is set to {}";
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
  public static final String LOG_PIPESENDTSFILERATELIMITBYTESPERSECOND_SET_ARG_653F2CC4 = "pipeSendTs文件RateLimitBytesPerSecond is set to {}";
  public static final String LOG_PIPEALLSINKSRATELIMITBYTESPERSECOND_SET_ARG_EE3FE2A0 = "pipeAllSinksRateLimitBytesPerSecond is set to {}";
  public static final String LOG_RATELIMITERHOTRELOADCHECKINTERVALMS_SET_ARG_E086A4F0 = "rateLimiterHotRe加载CheckIntervalMs is set to {}";
  public static final String LOG_PIPECONNECTORREQUESTSLICETHRESHOLDBYTES_SET_ARG_7FAA56F2 = "pipeConnectorRequestSliceThresholdBytes is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATEMAXCOMBINERLIVETIMEINMS_SET_ARG_F10B7C02 = "twoStageAggregateMaxCombinerLiveTimeInMs is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATEDATAREGIONINFOCACHETIMEINMS_SET_ARG_C7895888 = "twoStageAggregateDataRegionInfoCacheTimeInMs is set to {}";
  public static final String LOG_TWOSTAGEAGGREGATESENDERENDPOINTSCACHEINMS_SET_ARG_A3CF42B2 = "twoStageAggregateSenderEndPointsCacheInMs is set to {}";
  public static final String LOG_PIPEEVENTREFERENCETRACKINGENABLED_SET_ARG_98E9A640 = "pipeEventReferenceTrackingEnabled is set to {}";
  public static final String LOG_PIPEEVENTREFERENCEELIMINATEINTERVALSECONDS_SET_ARG_62542387 = "pipeEventReferenceEliminateIntervalSeconds is set to {}";

}
