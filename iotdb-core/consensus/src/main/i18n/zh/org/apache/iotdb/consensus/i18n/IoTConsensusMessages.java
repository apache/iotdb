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

package org.apache.iotdb.consensus.i18n;

/**
 * IoTConsensus（v1）特有消息。日志消息使用 SLF4J {@code {}} 占位符；异常消息使用 {@code %s}（String.format）或纯字符串。
 */
public final class IoTConsensusMessages {

  private IoTConsensusMessages() {}

  // ===================== IoTConsensus 生命周期 =====================

  public static final String INACTIVATE_NEW_PEER =
      "[IoTConsensus] 将新 peer 设为非活跃：{}";
  public static final String NOTIFY_PEERS_BUILD_SYNC_LOG =
      "[IoTConsensus] 通知当前 peer 建立同步日志通道...";
  public static final String START_TAKE_SNAPSHOT =
      "[IoTConsensus] 开始创建快照...";
  public static final String START_TRANSMIT_SNAPSHOT =
      "[IoTConsensus] 开始传输快照...";
  public static final String TRIGGER_LOAD_SNAPSHOT =
      "[IoTConsensus] 触发新 peer 加载快照...";
  public static final String ACTIVATE_NEW_PEER =
      "[IoTConsensus] 激活新 peer...";
  public static final String CLEANUP_REMOTE_SNAPSHOT =
      "[IoTConsensus] 清理远程快照...";
  public static final String FAILED_CLEANUP_REMOTE_SNAPSHOT =
      "[IoTConsensus] 清理远程快照失败";
  public static final String ADD_REMOTE_PEER_FAILED_CLEANUP =
      "[IoTConsensus] 添加远程 peer 失败，正在自动清理副作用...";
  public static final String CLEANUP_LOCAL_SNAPSHOT =
      "[IoTConsensus] 清理本地快照...";
  public static final String NOT_SUPPORT_LEADER_TRANSFER =
      "IoTConsensus 不支持 leader 切换";

  // ===================== IoTConsensusServerImpl =====================

  public static final String THROTTLE_DOWN = "[限流] index:{}, safeIndex:{}";
  public static final String DATA_REGION_INDEX_AFTER_BUILD =
      "DataRegion[{}]：构建后的 index：safeIndex:{}，searchIndex: {}，lastConsensusRequest: {}";
  public static final String FAILED_TO_THROTTLE_DOWN = "限流失败，原因 ";

  // ===================== 快照 =====================

  public static final String CANNOT_MKDIR_FOR_SNAPSHOT =
      "%s：无法为快照创建目录";
  public static final String UNKNOWN_ERROR_TAKING_SNAPSHOT =
      "创建快照时发生未知错误";
  public static final String ERROR_TAKING_SNAPSHOT =
      "创建快照时出错";
  public static final String CANNOT_FIND_SNAPSHOT_DIR =
      "为共识组 {} 创建新快照后找不到任何快照目录";
  public static final String DELETE_OLD_SNAPSHOT_FAILED =
      "删除旧快照目录 {} 失败";
  public static final String FILE_NOT_EXIST = "文件不存在：{}";
  public static final String CLEANUP_LOCAL_SNAPSHOT_FAIL =
      "清理本地快照失败。您可能需要手动删除 {}。";
  public static final String INVALID_SNAPSHOT_FILE =
      "无效的快照文件。snapshotId: %s，filePath: %s";
  public static final String ERROR_RECEIVING_SNAPSHOT =
      "接收快照 %s 时出错";
  public static final String INVALID_SNAPSHOT_RELATIVE_PATH =
      "无效的 snapshotRelativePath：";

  // ===================== 快照传输 =====================

  public static final String SNAPSHOT_TRANSMISSION_START =
      "[快照传输] 开始从目录 {} 传输快照（{} 个文件，总大小 {}）";
  public static final String SNAPSHOT_TRANSMISSION_ALL_FILES =
      "[快照传输] 以下所有文件将被传输：{}";
  public static final String SNAPSHOT_TRANSMISSION_ERROR =
      "[快照传输] 向 %s 传输快照片段时出错";
  public static final String SNAPSHOT_TRANSMISSION_PROGRESS =
      "[快照传输] 目录 {} 的整体进度：文件 {}/{} 已完成，大小 {}/{} 已完成，已耗时 {}。文件 {} 已完成。";
  public static final String SNAPSHOT_TRANSMISSION_SEND_ERROR =
      "[快照传输] 向 %s 发送快照文件时出错";
  public static final String SNAPSHOT_TRANSMISSION_COMPLETE =
      "[快照传输] 经过 {}，已成功从目录 {} 传输所有快照";

  // ===================== Peer 操作 =====================

  public static final String ERROR_INACTIVATING_PEER =
      "将 %s 设为非活跃时出错。%s";
  public static final String ERROR_INACTIVATING_PEER_SHORT =
      "将 %s 设为非活跃时出错";
  public static final String ERROR_TRIGGERING_SNAPSHOT_LOAD =
      "触发 %s 加载快照时出错。%s";
  public static final String ERROR_ACTIVATING_PEER =
      "激活 %s 时出错。%s";
  public static final String ERROR_ACTIVATING_PEER_SHORT =
      "激活 %s 时出错";
  public static final String CLEANUP_REMOTE_SNAPSHOT_FAILED =
      "清理 %s 的远程快照失败，状态为 %s";
  public static final String CLEANUP_REMOTE_SNAPSHOT_FAILED_SHORT =
      "清理 %s 的远程快照失败";

  // ===================== 同步日志 =====================

  public static final String NOTIFY_PEERS_BUILD_SYNC_LOG_DETAIL =
      "[IoTConsensus] 通知当前 peer 建立同步日志通道。组成员：{}，目标：{}";
  public static final String BUILD_SYNC_LOG_CHANNEL_FROM =
      "[IoTConsensus] 从 {} 建立同步日志通道";
  public static final String BUILD_SYNC_LOG_CHANNEL_FAILED =
      "从 %s 到 %s 建立同步日志通道失败";
  public static final String CANNOT_NOTIFY_BUILD_SYNC_LOG =
      "无法通知 {} 建立同步日志通道。请手动检查该节点状态";
  public static final String BUILD_SYNC_LOG_CHANNEL_SUCCESS =
      "[IoTConsensus] 成功建立到 {} 的同步日志通道，initialSyncIndex 为 {}。{}";
  public static final String SYNC_LOG_CHANNEL_STARTED =
      "同步日志通道已启动。";
  public static final String SYNC_LOG_CHANNEL_START_LATER =
      "同步日志通道可能稍后启动。";
  public static final String REMOVING_SYNC_LOG_CHANNEL_FAILED =
      "从 {} 到 {} 移除同步日志通道失败";
  public static final String EXCEPTION_REMOVING_SYNC_LOG_CHANNEL =
      "从 {} 到 {} 移除同步日志通道时发生异常";
  public static final String LOG_DISPATCHER_REMOVED_CLEANUP =
      "[IoTConsensus] 到 {} 的日志分发器已移除并清理";
  public static final String EXCEPTION_REMOVING_LOG_DISPATCHER =
      "[IoTConsensus] 移除日志分发器线程时发生异常，但 configuration.dat 仍将被移除。";
  public static final String SUGGEST_RESTART_DATANODE =
      "建议重启 DataNode 以移除日志分发器线程。";
  public static final String LOG_DISPATCHER_REMOVED_AND_CLEANUP =
      "[IoTConsensus] 到 {} 的日志分发器线程已移除并清理";
  public static final String CONFIGURATION_UPDATED =
      "[IoTConsensus 配置] 配置已更新为 {}。{}";

  // ===================== 等待同步日志 =====================

  public static final String WAIT_SYNC_LOG_COMPLETED =
      "[等待日志同步] {} SyncLog 已完成。TargetIndex: {}，CurrentSyncIndex: {}";
  public static final String WAIT_SYNC_LOG_IN_PROGRESS =
      "[等待日志同步] {} SyncLog 仍在进行中。TargetIndex: {}，CurrentSyncIndex: {}";
  public static final String ERROR_WAITING_SYNC_LOG_COMPLETE =
      "等待 %s 完成 SyncLog 时出错。%s";
  public static final String THREAD_INTERRUPTED_WAITING_SYNC_LOG =
      "等待 %s 完成 SyncLog 时线程被中断。%s";

  // ===================== Index 控制器 =====================

  public static final String UPDATE_INDEX =
      "更新 index：从 currentIndex {} 到 {}，文件前缀 {}，目录 {}";
  public static final String VERSION_FILE_UPDATED =
      "版本文件已更新，旧文件：{}，新文件：{}";
  public static final String FAILED_FLUSH_SYNC_INDEX =
      "刷新同步 index 失败，因为前一个版本文件 {} 不存在。"
          + "这可能是由于目标 Peer 已从当前组中移除。"
          + "目标文件为 {}";
  public static final String ERROR_FLUSHING_NEXT_VERSION =
      "刷新下一个版本时出错";
  public static final String VERSION_FILE_UPGRADE =
      "版本文件升级，旧文件：{}，新文件：{}";
  public static final String ERROR_UPGRADING_VERSION_FILE =
      "升级版本文件时出错";
  public static final String DELETE_OUTDATED_VERSION_FILE_FAILED =
      "删除过期版本文件 {} 失败";
  public static final String ERROR_CREATING_NEW_FILE =
      "创建新文件 {} 时出错";
  public static final String CONFIGURATION_EMPTY_UNEXPECTED =
      "配置为空，这是非预期的。本次不会更新安全删除搜索 index。";
  public static final String SEARCH_INDEX_SMALLER_THAN_SAFELY_DELETED =
      "此 region({}) 的 searchIndex 在节点重启时小于 safelyDeletedSearchIndex，"
          + "这意味着当前 region 的数据未通过 WAL 刷新，但已同步到其他节点。"
          + "此时不同副本已不一致且无法自动恢复。"
          + "为防止后续日志标记更小的 searchIndex 加剧不一致，"
          + "这里手动将 searchIndex({}) 设置为 safelyDeletedSearchIndex({}) "
          + "以减少此问题在未来的影响";

  // ===================== LogDispatcher =====================

  public static final String UNABLE_TO_SHUTDOWN_LOG_DISPATCHER =
      "在 {} 秒后仍无法关闭 LogDispatcher 服务";
  public static final String UNEXPECTED_INTERRUPTION_CLOSING_LOG_DISPATCHER =
      "关闭 LogDispatcher 服务时发生意外中断 ";
  public static final String DISPATCHER_STARTS =
      "{}：到 {} 的分发器启动";
  public static final String DISPATCHER_EXITS =
      "{}：到 {} 的分发器退出";
  public static final String DISPATCHER_DID_NOT_STOP =
      "{}：到 {} 的分发器在 30 秒后仍未停止。";
  public static final String UNEXPECTED_ERROR_IN_LOG_DISPATCHER =
      "peer {} 的日志分发器发生意外错误";
  public static final String PUSH_LOG_TO_QUEUE =
      "{}->{}：向队列推送一条日志，当前队列长度为 {}";
  public static final String LOG_QUEUE_FULL =
      "{}：{} 的日志队列已满，忽略此节点的日志，searchIndex：{}";
  public static final String GET_BATCH_START_INDEX =
      "{}：startIndex: {}，maxIndex: {}，pendingEntries 大小：{}，bufferedEntries 大小：{}";
  public static final String ACCUMULATED_FROM_WAL_WHEN_EMPTY =
      "{} ：空队列时从 WAL 累积了一个 {}";
  public static final String ACCUMULATED_FROM_WAL =
      "{} ：从 WAL 累积了一个 {}";
  public static final String ACCUMULATED_FROM_QUEUE =
      "{} ：从队列累积了一个 {}";
  public static final String ACCUMULATED_FROM_QUEUE_AND_WAL_GAP =
      "间隔 {} ：在存在间隔时从队列和 WAL 累积了一个 {}";
  public static final String ACCUMULATED_FROM_QUEUE_AND_WAL =
      "{} ：从队列和 WAL 累积了一个 {}";
  public static final String SEND_BATCH =
      "发送 Batch[startIndex:{}, endIndex:{}] 到共识组：{}";
  public static final String CANNOT_SYNC_LOGS_TO_PEER =
      "无法同步日志到 peer {}，原因";
  public static final String CONSTRUCT_FROM_WAL =
      "从 WAL 构造一条日志，index：{}";
  public static final String WAIT_NEXT_WAL_INTERRUPTED =
      "等待下一条 WAL 日志时被中断";
  public static final String SEARCH_ENTRY_FOUND_SMALLER =
      "搜索 index 为 {} 的日志，但找到一条更小的，index：{}";
  public static final String SEARCH_ENTRY_FOUND_LARGER =
      "搜索 index 为 {} 的日志，但找到一条更大的，index：{}。"
          + "WAL 文件可能已损坏，将跳过并选择更大的 index 进行复制";
  public static final String DATA_REGION_CONSTRUCT_FROM_WAL =
      "DataRegion[{}]->{}：currentIndex: {}，maxIndex: {}";

  // ===================== DispatchLogHandler =====================

  public static final String CANNOT_SEND_TO_PEER =
      "无法将 {} 发送到 peer {} ，已重试 {} 次，原因 {}";
  public static final String SEND_COMPLETE_BUT_CONTAINS_ERROR =
      "已将 {} 发送到 peer {} 但包含不成功的状态：{}";
  public static final String CANNOT_SEND_TO_PEER_ON_ERROR =
      "无法将 {} 发送到 peer，已重试 {} 次 {} ，原因 {}";
  public static final String SKIP_RETRY_TAPPLICATION_EXCEPTION =
      "由于 TApplicationException，跳过重试此 Batch {}。";
  public static final String LOG_DISPATCHER_STOPPED_NO_RETRY =
      "LogDispatcherThread {} 已停止，"
          + "不会在 {} 次后重试此 Batch {}";

  // ===================== SyncLogCacheQueue =====================

  public static final String CACHE_AND_INSERT_START =
      "缓存插入开始：source = {}，region = {}，队列大小 {}，startSyncIndex = {}，endSyncIndex = {}";
  public static final String CACHE_AND_INSERT_END =
      "缓存插入结束：source = {}，region = {}，队列大小 {}，startSyncIndex = {}，endSyncIndex = {}，sortTime = {}ms，applyTime = {}ms";
  public static final String WAITING_TARGET_REQUEST_TIMEOUT =
      "等待目标请求超时。当前 index：{}，目标 index：{}";
  public static final String CURRENT_WAITING_INTERRUPTED =
      "当前等待被中断。SyncIndex：{}。异常：";

  // ===================== SyncStatus =====================

  public static final String SYNC_STATUS_OFFER =
      "向 SyncStatus 提交 Batch[startIndex:{}, endIndex:{}]。"
          + "当前 SyncStatus 大小：{}。Pending 大小：{}";

  // ===================== AsyncClient =====================

  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT =
      "{} 中发生意外异常，错误信息为 {}";
  public static final String CLIENT_INVALIDATED = "此客户端已被标记为无效";

  // ===================== RPC 处理器执行日志同步 =====================

  public static final String EXECUTE_SYNC_LOG_ENTRIES =
      "执行 TSyncLogEntriesReq，共识组 {}，结果 {}";

  // ===================== 内存管理器 =====================

  public static final String RESERVING_BYTES_FOR_REQUEST_SUCCEEDS =
      "为请求 {} 预留 {} 字节成功，当前总使用量 {}";
  public static final String RESERVING_BYTES_FOR_REQUEST_FAILS =
      "为请求 {} 预留 {} 字节失败，当前总使用量 {}";
  public static final String SKIP_MEMORY_RESERVATION =
      "跳过 {} 的内存预留，因为其引用计数不为 0";
  public static final String RESERVING_BYTES_FOR_BATCH_SUCCEEDS =
      "为 batch {}-{} 预留 {} 字节成功，当前总使用量 {}";
  public static final String RESERVING_BYTES_FOR_BATCH_FAILS =
      "为 batch {}-{} 预留 {} 字节失败，当前总使用量 {}";
  public static final String FREED_BYTES_FOR_REQUEST =
      "为请求 {} 释放 {} 字节，当前总使用量 {}";
  public static final String FREED_BYTES_FOR_BATCH =
      "为 batch {}-{} 释放 {} 字节，当前总使用量 {}";
  public static final String FREE_MEMORY =
      "{} 释放 {} 字节，总内存大小：{} 字节。";
  public static final String INTERRUPTED_AFTER_POLLING_AND_SLEEPING =
      "轮询和等待后被中断";
  public static final String INTERRUPTED_AFTER_GETTING_A_BATCH =
      "获取批次后被中断";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_ONNEWPEERCREATED_CALLBACK_FAILED_GROUP_ARG_2671FCDA = "group {} 的 onNewPeerCreated 回调失败";
  public static final String LOG_ONPEERREMOVED_CALLBACK_FAILED_GROUP_ARG_9B79CBAF = "group {} 的 onPeerRemoved 回调失败";
  public static final String EXCEPTION_UNSUPPORTED_WRITER_META_VERSION_ARG_ARG_7D14E679 = "不支持 writer meta 版本 %d，位置 %s";
  public static final String LOG_WRITE_NO_SUBSCRIPTION_QUEUES_REGISTERED_0F4E697B = "write() 没有已注册的订阅队列，";
  public static final String LOG_GROUP_ARG_SEARCHINDEX_ARG_ARG_D5E034E6 = "group={}, searchIndex={}, 当前对象={}";
  public static final String LOG_CANNOT_NOTIFY_ARG_BUILD_SYNC_LOG_CHANNEL_490770FB = "无法通知 {} 构建同步日志通道。";
  public static final String LOG_PLEASE_CHECK_STATUS_NODE_MANUALLY_40FBC9B3 = "请手动检查该节点状态";
  public static final String LOG_FAILED_SYNC_WRITER_SAFE_TIME_BARRIER_PEER_ARG_GROUP_ARG_05A13E6A = "无法将 writer safe-time barrier 同步到 peer {}，group {}，";
  public static final String LOG_SAFEPT_ARG_WRITERNODEID_ARG_BARRIER_ARG_AC74618F = "safePt={}, writerNodeId={}, barrier={}";
  public static final String LOG_RECOVERED_WRITER_META_GROUP_ARG_ARG_RECOVEREDLOCALSEQ_ARG_9B989AAC = "已恢复 group {} 的 writer meta，来源 {}，recoveredLocalSeq={}，";
  public static final String LOG_PERSISTEDLOCALSEQ_ARG_5EBAA9A5 = "persistedLocalSeq={}";
  public static final String LOG_FAILED_LOAD_WRITER_META_GROUP_ARG_ARG_STARTING_FRESH_WRITER_A24EDEFE = "无法加载 group {} 的 writer meta，来源 {}。正在启动新的 writer 元数据。";
  public static final String LOG_INITIALIZED_FRESH_WRITER_META_GROUP_ARG_RECOVEREDLOCALSEQ_ARG_A7254C6E = "已初始化 group {} 的新 writer meta，recoveredLocalSeq={}";
  public static final String LOG_FAILED_PERSIST_WRITER_META_GROUP_ARG_AT_LOCALSEQ_ARG_PT_3502F119 = "无法持久化 group {} 的 writer meta，localSeq={}，pt={}";
  public static final String LOG_REGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_5102ABA0 = "已注册 group {} 的订阅队列，";
  public static final String LOG_TOTAL_SUBSCRIPTION_QUEUES_ARG_CURRENTSEARCHINDEX_ARG_ARG_9BF9006A = "订阅队列总数：{}，currentSearchIndex={}，当前对象={}";
  public static final String LOG_DEREGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_REMAINING_SUBSCRIPTION_QUEUES_ARG_B86E31AF = "已注销 group {} 的订阅队列，剩余订阅队列：{}";
  public static final String LOG_OBSERVED_INCOMPARABLE_WRITER_SAFE_TIME_BARRIER_WRITER_ARG_0F0C171D = "观察到 writer {} 存在不可比较的 writer safe-time barrier。";
  public static final String LOG_KEEP_PENDINGSAFEPHYSICALTIME_ARG_PENDINGSAFELOCALSEQ_ARG_7123FD95 = "保留 pendingSafePhysicalTime={}，pendingSafeLocalSeq={}，";
  public static final String LOG_IGNORE_SAFEPHYSICALTIME_ARG_SAFELOCALSEQ_ARG_A601C4D1 = "忽略 safePhysicalTime={}，safeLocalSeq={}";
  public static final String LOG_WRITE_OFFERING_ARG_SUBSCRIPTION_QUEUE_S_GROUP_ARG_SEARCHINDEX_ARG_A8489EDF = "write() 正在提交到 {} 个订阅队列，group={}，searchIndex={}，requestType={}";
  public static final String LOG_OFFER_RESULT_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_7ADC84C2 = "提交结果={}，queueSize={}，queueRemaining={}";
  public static final String LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ARG_ENTRY_S_LAST_ARG_MS_2AD8AB3D = "订阅队列已满，丢弃了 {} 个 entry，最近 {} ms，最新 ";
  public static final String LOG_SEARCHINDEX_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_2EA619ED = "searchIndex={}, queueSize={}, queueRemaining={}";
  public static final String LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ENTRY_SEARCHINDEX_ARG_DROPPEDCOUNT_ARG_61F126B8 = "订阅队列已满，丢弃 entry，searchIndex={}，droppedCount={}";
  public static final String LOG_RESERVED_ARG_BYTES_BATCH_ARG_ARG_CURRENT_TOTAL_USAGE_ARG_308AE9C2 = "预留 {} 字节给批次 {}-{}，当前总使用量 {}";
  public static final String LOG_ARG_FAILED_SEND_IDLE_WRITER_SAFE_TIME_BARRIER_ARG_STATUS_AE047EAD = "{}：无法向 {} 发送 idle writer safe-time barrier。状态={}";
  public static final String LOG_ARG_WRITE_OPERATION_FAILED_SEARCHINDEX_ARG_CODE_ARG_SUBSCRIPTIONQUEUES_ARG_THIS_ARG_F4B17576 = "{}：写入操作失败。searchIndex: {}。Code: {}，订阅队列：{}，当前对象：{}";

}
