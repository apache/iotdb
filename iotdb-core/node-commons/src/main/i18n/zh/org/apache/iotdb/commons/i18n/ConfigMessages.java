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

  // ---------------------------------------------------------------------------
  // protocol – ConfigNodeInfo
  // ---------------------------------------------------------------------------
  public static final String UPDATE_CONFIG_NODE_SUCCESSFULLY =
          "成功更新 ConfigNode：{}，耗时 {} 毫秒。";
  public static final String UPDATE_CONFIG_NODE_FAILED = "更新 ConfigNode 失败。";
  public static final String SYSTEM_PROPERTIES_NOT_EXIST =
          "系统属性文件不存在，无需存储 ConfigNode 列表";
  public static final String LOAD_CONFIG_NODE_SUCCESSFULLY =
          "成功加载 ConfigNode：{}，耗时 {} 毫秒。";
  public static final String CANNOT_PARSE_CONFIG_NODE_LIST =
          "无法解析 system.properties 中的 ConfigNode 列表";
  public static final String MISC_EXCEPTION_REMOVING_IS_ONLY_ALLOWED_IN_AN_ENVIRONMENT_WHEN_NODE_STARTED_2ACA2BD0 =
          "只有在 %s 已成功启动的环境中才允许移除。请检查它是否已在 ConfigNode 上移除，或是否误删了 system.properties 文件。";

  // --- ConfigNodeClient ---
  public static final String MSG_RECONNECTION_FAIL =
          "无法连接到任何 config node。请检查 ConfigNodes 的状态或已连接 %s 的日志";
  public static final String MSG_RECONNECTION_NODE_FAIL =
          "连接 ConfigNode %s 失败：从 %s %s 执行 %s 时，异常:";
  public static final String NODE_LEADER_MAY_DOWN_TRY_NEXT =
          "当前节点 leader 可能已宕机 {}，尝试下一个节点";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE =
          "等待尝试连接 ConfigNode 时发生意外中断";
  public static final String NODE_MAY_DOWN_TRY_NEXT =
          "当前节点可能已宕机 {}，尝试下一个节点";
  public static final String FAILED_CONNECT_CONFIG_NODE_NOT_LEADER =
          "连接 ConfigNode {} 失败：从 {} {} 发起，因为当前节点不是 leader 或尚未就绪，稍后将重试";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE_BREAK =
          "等待尝试连接 ConfigNode 时发生意外中断，可能因为当前节点已宕机。将中断当前执行流程以避免无意义的等待";
  public static final String MESSAGE_CONFIGNODE_LEADER_ARG_IS_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_REASON_ARG_3A2A4163 =
          "ConfigNode leader {} 正在预热，暂未对 DataNode {} 提供服务，将等待并重试。原因：{}";

  private ConfigMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_PIPEDATASTRUCTURETABLETSIZEINBYTES_SET_ARG_243363B3 = "pipeDataStructureTabletSizeInBytes 已设置为 {}。";
  public static final String LOG_PIPEDATASTRUCTURETABLETMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_EF34614A = "pipeDataStructureTabletMemoryBlockAllocationRejectThreshold 已设置为 {}。";
  public static final String LOG_PIPEDATASTRUCTURETSFILEMEMORYBLOCKALLOCATIONREJECTTHRESHOLD_SET_ARG_309A7E12 = "pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold 已设置为 {}。";
  public static final String LOG_PIPETOTALFLOATINGMEMORYPROPORTION_SET_ARG_FDCA8082 = "pipeTotalFloatingMemoryProportion 已设置为 {}。";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERSIZE_SET_ARG_31C9A8D8 = "pipeSourceAssignerDisruptorRingBufferSize 已设置为 {}。";
  public static final String LOG_PIPESOURCEASSIGNERDISRUPTORRINGBUFFERENTRYSIZE_SET_ARG_95D31172 = "pipeSourceAssignerDisruptorRingBufferEntrySize 已设置为 {}。";
  public static final String LOG_PIPECONNECTORHANDSHAKETIMEOUTMS_SET_ARG_64890ED2 = "pipeConnectorHandshakeTimeoutMs 已设置为 {}。";
  public static final String LOG_PIPEAIRGAPSINKTABLETTIMEOUTMS_SET_ARG_3413AC05 = "pipeAirGapSinkTabletTimeoutMs 已设置为 {}。";
  public static final String LOG_ISPIPESINKREADFILEBUFFERMEMORYCONTROLENABLED_SET_ARG_138BB142 = "isPipeSinkReadFileBufferMemoryControlEnabled 已设置为 {}。";
  public static final String LOG_PIPESINKRPCTHRIFTCOMPRESSIONENABLED_SET_ARG_1F2B6AB4 = "pipeSinkRPCThriftCompressionEnabled 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTSFILEEVENTQUEUESIZE_SET_ARG_0BB1C280 = "pipeAsyncSinkForcedRetryTsFileEventQueueSize 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTABLETEVENTQUEUESIZE_SET_ARG_8FDA7023 = "pipeAsyncSinkForcedRetryTabletEventQueueSize 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKFORCEDRETRYTOTALEVENTQUEUESIZE_SET_ARG_92D6EACB = "pipeAsyncSinkForcedRetryTotalEventQueueSize 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKMAXRETRYEXECUTIONTIMEMSPERCALL_SET_ARG_77E7B216 = "pipeAsyncSinkMaxRetryExecutionTimeMsPerCall 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKRETRYMAXDURATIONMS_IS_SET_TO_ARG_5058C99F =
      "pipeAsyncSinkRetryMaxDurationMs 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKRETRYPROBEINTERVALMS_IS_SET_TO_ARG_A1E9AF45 =
      "pipeAsyncSinkRetryProbeIntervalMs 已设置为 {}。";
  public static final String LOG_PIPEASYNCSINKSELECTORNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_EEB9793C = "pipeAsyncSinkSelectorNumber 应大于 0，配置保持不变。";
  public static final String LOG_PIPEASYNCSINKMAXCLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_11EF47BF = "pipeAsyncSinkMaxClientNumber 应大于 0，配置保持不变。";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_AC812FE2 = "pipeAsyncSinkMaxTsFileClientNumber 应大于 0，配置保持不变。";
  public static final String LOG_PIPEASYNCSINKMAXTSFILECLIENTNUMBER_SET_ARG_7D83FCDE = "pipeAsyncSinkMaxTsFileClientNumber 已设置为 {}。";
  public static final String LOG_PIPEHEARTBEATINTERVALSECONDSFORCOLLECTINGPIPEMETA_SET_ARG_E171AAAD = "pipeHeartbeatIntervalSecondsForCollectingPipeMeta 已设置为 {}。";
  public static final String LOG_PIPEMETASYNCERINITIALSYNCDELAYMINUTES_SET_ARG_6E36A895 = "pipeMetaSyncerInitialSyncDelayMinutes 已设置为 {}。";
  public static final String LOG_PIPEMETASYNCERSYNCINTERVALMINUTES_SET_ARG_CFBACD71 = "pipeMetaSyncerSyncIntervalMinutes 已设置为 {}。";
  public static final String LOG_PIPEMETASYNCERAUTORESTARTPIPECHECKINTERVALROUND_SET_ARG_A80B4589 = "pipeMetaSyncerAutoRestartPipeCheckIntervalRound 已设置为 {}。";
  public static final String LOG_PIPESINKRETRYLOCALLYFORCONNECTIONERROR_SET_ARG_5D886CE6 = "pipeSinkRetryLocallyForConnectionError 已设置为 {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYCONSUMEDEVENTCOUNT_SET_ARG_CFCECFCE = "pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount 已设置为 {}";
  public static final String LOG_PIPESUBTASKEXECUTORBASICCHECKPOINTINTERVALBYTIMEDURATION_SET_ARG_45B3F433 = "pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration 已设置为 {}";
  public static final String LOG_PIPESUBTASKEXECUTORMAXTHREADNUM_SHOULD_GREATER_THAN_0_CONFIGURING_IT_NOT_CHANGE_25E0CE6E = "pipeSubtaskExecutorMaxThreadNum 应大于 0，配置保持不变。";
  public static final String LOG_PIPERETRYLOCALLYFORPARALLELORUSERCONFLICT_SET_ARG_368926E5 = "pipeRetryLocallyForParallelOrUserConflict 已设置为 {}。";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALINITMS_SET_ARG_B8DCF143 = "pipeSinkSubtaskSleepIntervalInitMs 已设置为 {}。";
  public static final String LOG_PIPESINKSUBTASKSLEEPINTERVALMAXMS_SET_ARG_0010425D = "pipeSinkSubtaskSleepIntervalMaxMs 已设置为 {}。";
  public static final String LOG_PIPESUBTASKEXECUTORPENDINGQUEUEMAXBLOCKINGTIMEMS_SET_ARG_2F1A6865 = "pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs 已设置为 {}";
  public static final String LOG_PIPESUBTASKEXECUTORCRONHEARTBEATEVENTINTERVALSECONDS_SET_ARG_B5C9E195 = "pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds 已设置为 {}。";
  public static final String LOG_PIPEREALTIMEQUEUEPOLLHISTORICALTSFILETHRESHOLD_SET_ARG_FD88A384 = "pipeRealTimeQueuePollHistoricalTsFileThreshold 已设置为 {}";
  public static final String LOG_PIPEREALTIMEQUEUEMAXWAITINGTSFILESIZE_SET_ARG_7E0698AB = "pipeRealTimeQueueMaxWaitingTsFileSize 已设置为 {}。";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGTIME_SET_ARG_98A0F8AE = "pipeRealtimeForceDowngradingTime 已设置为 {}。";
  public static final String LOG_PIPEREALTIMEFORCEDOWNGRADINGPROPORTION_SET_ARG_92974D0B = "pipeRealtimeForceDowngradingProportion 已设置为 {}。";
  public static final String LOG_PIPERECEIVERLOGINPERIODICVERIFICATIONINTERVALMS_SET_ARG_158C791C = "pipeReceiverLoginPeriodicVerificationIntervalMs 已设置为 {}";
  public static final String LOG_PIPERECEIVERACTUALTOESTIMATEDMEMORYRATIO_SET_ARG_0D1F305D = "pipeReceiverActualToEstimatedMemoryRatio 已设置为 {}";
  public static final String LOG_PIPERECEIVERREQDECOMPRESSEDMAXLENGTHINBYTES_SET_ARG_9356E410 = "pipeReceiverReqDecompressedMaxLengthInBytes 已设置为 {}。";
  public static final String LOG_IGNORE_INVALID_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_ARG_BECAUSE_IT_MUST_GREATER_THAN_0_8ACA836C = "忽略无效的 pipeAirGapReceiverMaxPayloadSizeInBytes {}，原因：其必须大于 0。";
  public static final String LOG_PIPEAIRGAPRECEIVERMAXPAYLOADSIZEINBYTES_SET_ARG_9B21877F = "pipeAirGapReceiverMaxPayloadSizeInBytes 已设置为 {}。";
  public static final String LOG_PIPEPERIODICALLOGMININTERVALSECONDS_SET_ARG_5535C79E = "pipePeriodicalLogMinIntervalSeconds 已设置为 {}。";
  public static final String LOG_PIPEMETAREPORTMAXLOGINTERVALROUNDS_SET_ARG_0090AECB = "pipeMetaReportMaxLogIntervalRounds 已设置为 {}";
  public static final String LOG_PIPETSFILEPINMAXLOGINTERVALROUNDS_SET_ARG_FAFE1040 = "pipeTsFilePinMaxLogIntervalRounds 已设置为 {}";
  public static final String LOG_PIPEMEMORYALLOCATEFORTSFILESEQUENCEREADERINBYTES_SET_ARG_8A26960D = "pipeMemoryAllocateForTsFileSequenceReaderInBytes 已设置为 {}";
  public static final String LOG_PIPEMEMORYEXPANDERINTERVALSECONDS_SET_ARG_73F96BBC = "pipeMemoryExpanderIntervalSeconds 已设置为 {}";
  public static final String LOG_PIPEMEMORYALLOCATERETRYINTERVALMS_SET_ARG_39D52E47 = "pipeMemoryAllocateRetryIntervalMs 已设置为 {}";
  public static final String LOG_PIPELEADERCACHEMEMORYUSAGEPERCENTAGE_SET_ARG_E32DE64B = "pipeLeaderCacheMemoryUsagePercentage 已设置为 {}";
  public static final String LOG_PIPELISTENINGQUEUETRANSFERSNAPSHOTTHRESHOLD_SET_ARG_FD856477 = "pipeListeningQueueTransferSnapshotThreshold 已设置为 {}";
  public static final String LOG_PIPESNAPSHOTEXECUTIONMAXBATCHSIZE_SET_ARG_F1C5C62C = "pipeSnapshotExecutionMaxBatchSize 已设置为 {}";
  public static final String LOG_PIPEREMAININGTIMECOMMITRATEAUTOSWITCHSECONDS_SET_ARG_17E6C979 = "pipeRemainingTimeCommitRateAutoSwitchSeconds 已设置为 {}";
  public static final String LOG_PIPEREMAININGTIMECOMMITRATEAVERAGETIME_SET_ARG_D010BE98 = "pipeRemainingTimeCommitRateAverageTime 已设置为 {}";
  public static final String LOG_PIPEREMAININGINSERTEVENTCOUNTAVERAGE_SET_ARG_17C28F47 = "pipeRemainingInsertEventCountAverage 已设置为 {}";
  public static final String LOG_PIPEDYNAMICMEMORYADJUSTMENTTHRESHOLD_SET_ARG_2F008DB1 = "pipeDynamicMemoryAdjustmentThreshold 已设置为 {}";
  public static final String LOG_PIPETHRESHOLDALLOCATIONSTRATEGYMAXIMUMMEMORYINCREMENTRATIO_SET_ARG_BFAD04E0 = "pipeThresholdAllocationStrategyMaximumMemoryIncrementRatio 已设置为 {}";
  public static final String LOG_PIPEMEMORYBLOCKLOWUSAGETHRESHOLD_SET_ARG_DDF99D69 = "pipeMemoryBlockLowUsageThreshold 已设置为 {}";
  public static final String LOG_PIPETHRESHOLDALLOCATIONSTRATEGYFIXEDMEMORYHIGHUSAGETHRESHOLD_SET_ARG_82721CBE = "pipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold 已设置为 {}";
  public static final String LOG_PIPECHECKSYNCALLCLIENTLIVETIMEINTERVALMS_SET_ARG_246CE0EB = "pipeCheckSyncAllClientLiveTimeIntervalMs 已设置为 {}";
  public static final String LOG_PIPESENDTSFILERATELIMITBYTESPERSECOND_SET_ARG_653F2CC4 = "pipeSendTsFileRateLimitBytesPerSecond 已设置为 {}";
  public static final String LOG_PIPEALLSINKSRATELIMITBYTESPERSECOND_SET_ARG_EE3FE2A0 = "pipeAllSinksRateLimitBytesPerSecond 已设置为 {}";
  public static final String LOG_RATELIMITERHOTRELOADCHECKINTERVALMS_SET_ARG_E086A4F0 = "rateLimiterHotReloadCheckIntervalMs 已设置为 {}";
  public static final String LOG_PIPECONNECTORREQUESTSLICETHRESHOLDBYTES_SET_ARG_7FAA56F2 = "pipeConnectorRequestSliceThresholdBytes 已设置为 {}";
  public static final String LOG_TWOSTAGEAGGREGATEMAXCOMBINERLIVETIMEINMS_SET_ARG_F10B7C02 = "twoStageAggregateMaxCombinerLiveTimeInMs 已设置为 {}";
  public static final String LOG_TWOSTAGEAGGREGATEDATAREGIONINFOCACHETIMEINMS_SET_ARG_C7895888 = "twoStageAggregateDataRegionInfoCacheTimeInMs 已设置为 {}";
  public static final String LOG_TWOSTAGEAGGREGATESENDERENDPOINTSCACHEINMS_SET_ARG_A3CF42B2 = "twoStageAggregateSenderEndPointsCacheInMs 已设置为 {}";
  public static final String LOG_PIPEEVENTREFERENCETRACKINGENABLED_SET_ARG_98E9A640 = "pipeEventReferenceTrackingEnabled 已设置为 {}";
  public static final String LOG_PIPEEVENTREFERENCEELIMINATEINTERVALSECONDS_SET_ARG_62542387 = "pipeEventReferenceEliminateIntervalSeconds 已设置为 {}";

}
