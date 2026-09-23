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
 * 共享/通用共识消息，适用于所有共识实现。日志消息使用 SLF4J {@code {}} 占位符；异常消息使用 {@code %s}（String.format）或纯字符串。
 */
public final class ConsensusMessages {

  private ConsensusMessages() {}

  // ===================== ConsensusFactory =====================

  public static final String CONSTRUCT_FAILED_MSG =
      "构建共识实现失败，请检查共识类名 %s";
  public static final String COULD_NOT_CONSTRUCT_ICONSENSUS =
      "无法构建 IConsensus 类：{}";
  public static final String UTILITY_CLASS_CONSENSUS_FACTORY = "工具类 ConsensusFactory";

  // ===================== 异常消息（String.format %s）=====================

  public static final String CONSENSUS_GROUP_NOT_EXIST =
      "共识组 %s 不存在";
  public static final String CONSENSUS_GROUP_ALREADY_EXIST =
      "共识组 %d 已存在";
  public static final String ILLEGAL_PEER_NUM =
      "添加共识组时 peer 数量 %d 不合法";
  public static final String ILLEGAL_PEER_ENDPOINT =
      "添加共识组失败，当前节点 %s 不在共识组 %s 中";
  public static final String PEER_ALREADY_IN_GROUP =
      "Peer %s:%d 已在共识组 %d 中";
  public static final String PEER_NOT_IN_GROUP =
      "Peer %s 不在共识组 %d 中";

  // ===================== 通用日志消息（SLF4J {}）=====================

  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR =
      "无法在 {} 创建共识目录";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FMT =
      "无法在 %s 创建共识目录";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP =
      "无法为共识组 {} 在 {} 创建目录";
  public static final String UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP_FMT =
      "无法为共识组 %s 创建目录";
  public static final String CANNOT_CREATE_LOCAL_PEER =
      "无法为共识组 {} 创建本地 peer，peers 为 {}";
  public static final String FAILED_TO_RESET_PEER_LIST_WHILE_START =
      "启动时重置 peer 列表失败";
  public static final String RECORD_CORRECT_PEER_LIST =
      "记录正确的 peer 列表：{}";
  public static final String INTERRUPTED_WHEN_SHUTTING_DOWN_EXECUTOR =
      "{}：关闭 Executor 时被中断，异常 {}";
  public static final String INTERRUPTED_WHEN_SHUTTING_DOWN_EXECUTOR_RATIS =
      "{}：关闭 Executor 时被中断，异常 ";
  public static final String SET_ACTIVE_STATUS = "设置 {} 活跃状态为 {}";

  // ===================== Peer 重置日志消息（SLF4J {}）=====================

  public static final String RESET_PEER_LIST_NOT_IN_CORRECT =
      "[重置 PEER 列表] {} 本地 peer 不在正确的配置中，将其删除。";
  public static final String RESET_PEER_LIST_DELETE_LOCAL_PEER =
      "[重置 PEER 列表] 本地 peer 不在正确的 peer 列表中，删除本地 peer {}";
  public static final String RESET_PEER_LIST_REMOVE_SYNC_CHANNEL =
      "[重置 PEER 列表] {} 移除与 {} 的同步通道";
  public static final String RESET_PEER_LIST_FAILED_TO_REMOVE_SYNC_CHANNEL =
      "[重置 PEER 列表] {} 移除与 {} 的同步通道失败";
  public static final String RESET_PEER_LIST_BUILD_SYNC_CHANNEL =
      "[重置 PEER 列表] {} 建立与 {} 的同步通道";
  public static final String RESET_PEER_LIST_FAILED_TO_BUILD_SYNC_CHANNEL =
      "[重置 PEER 列表] {} 建立与 {} 的同步通道失败";
  public static final String RESET_PEER_LIST_RESET_RESULT =
      "[重置 PEER 列表] {} 本地 peer 列表已重置：{} -> {}";
  public static final String RESET_PEER_LIST_NOTHING_TO_RESET =
      "[重置 PEER 列表] {} 当前 peer 列表已正确，无需重置：{}";
  public static final String RESET_PEER_LIST_WILL_RESET =
      "[重置 PEER 列表] Peer 列表将从 {} 重置为 {}";
  public static final String RESET_PEER_LIST_RESET_SUCCESS =
      "[重置 PEER 列表] Peer 列表已重置为 {}";
  public static final String RESET_PEER_LIST_RESET_FAILED =
      "[重置 PEER 列表] Peer 列表重置为 {} 失败，回复为 {}";

  // ===================== SimpleConsensus 消息 =====================

  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_MEMBERSHIP_CHANGES =
      "SimpleConsensus 不支持成员变更";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_LEADER_TRANSFER =
      "SimpleConsensus 不支持 leader 切换";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_SNAPSHOT_TRIGGER =
      "SimpleConsensus 目前不支持触发快照";
  public static final String SIMPLE_CONSENSUS_NOT_SUPPORT_RESET_PEER_LIST =
      "SimpleConsensus 不支持重置 peer 列表";
  public static final String SIMPLE_CONSENSUS_NOOP_RECORD_PEER_LIST =
      "SimpleConsensus 调用 recordCorrectPeerListBeforeStarting 时不执行任何操作";

  // ===================== RPC 处理器通用消息 =====================

  public static final String UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST =
      "共识组 ID %s 与 %s 请求不匹配";
  public static final String UNEXPECTED_CONSENSUS_GROUP_ID_FOR_SYNC_LOG =
      "共识组 ID %s 与 TSyncLogEntriesReq 不匹配，大小为 %s";
  public static final String SYNC_LOG_SYSTEM_READ_ONLY =
      "系统为只读模式，无法同步日志。";
  public static final String PEER_INACTIVE_NOT_READY =
      "Peer 处于非活跃状态，无法接收同步日志请求，%s，DataNode ID：%s";
  public static final String PEER_INACTIVE_NOT_READY_WRITE =
      "Peer 处于非活跃状态，无法接收写入请求，%s，DataNode ID：%s";
  public static final String REMOVE_SYNC_LOG_CHANNEL_FAILED =
      "移除同步日志通道失败";
  public static final String FAILED_TO_CLEANUP_TRANSFERRED_SNAPSHOT =
      "清理已传输的快照 {} 失败";

  // ===================== 等待释放资源消息 =====================

  public static final String WAIT_RELEASE_HAS_RELEASED =
      "[等待释放] {} 已释放所有与 Region 相关的资源";
  public static final String WAIT_RELEASE_STILL_RELEASING =
      "[等待释放] {} 仍在释放与 Region 相关的资源";
  public static final String ERROR_WAITING_RELEASE_RESOURCE =
      "等待 %s 释放所有与 Region 相关的资源时出错。%s";
  public static final String THREAD_INTERRUPTED_WAITING_RELEASE_RESOURCE =
      "等待 %s 释放所有与 Region 相关的资源时线程被中断。%s";

  // ===================== 重复 peer 警告 =====================

  public static final String DUPLICATE_PEERS_IGNORED =
      "输入列表中存在重复的 peer，已忽略重复项。";

  // ===================== Consensus pipe name =====================

  public static final String INVALID_PIPE_NAME = "无效的 pipe 名称：";

  // ===================== 非活跃写入拒绝 =====================

  public static final String NODE_NOT_ACTIVE_REJECT_WRITE =
      "当前节点处于非活跃状态，无法接收用户写入请求。";

  // ===================== 工具类消息 =====================

  public static final String FAILED_TO_SERIALIZE_PEER = "序列化 Peer 失败";
  public static final String VISIT_FILE_FAILED = "访问文件 {} 失败，原因 {}";
  public static final String IO_EXCEPTION_LISTING_SNAPSHOT_DIR =
      "列出快照目录时发生 IOException：";
  public static final String FAILED_TO_LOAD_KEYSTORE =
      "加载 keystore 或 truststore 文件失败";
  public static final String KEYSTORE_FILE_NOT_FOUND = "keystore 或 truststore 文件未找到";
  public static final String FAILED_TO_READ_KEYSTORE =
      "读取 keystore 或 truststore 失败。";
  public static final String NOT_IMPLEMENTED_YET = "尚未实现";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_RAFT_SERVER_CANNOT_SERVE_READ_REQUESTS_NOW_LEADER_UNKNOWN_UNDER_B6D65373 = "Raft Server 当前无法处理读请求（leader 未知或正在恢复）。";
  public static final String EXCEPTION_PLEASE_TRY_READ_LATER_D8E0CDE1 = "请稍后重试读取：";
  public static final String EXCEPTION_RATIS_REQUEST_FAILED_52AF217F = "Ratis 请求失败 ";
  public static final String EXCEPTION_UNKNOWN_88183B94 = "未知";
  public static final String EXCEPTION_RATIS_REQUEST_FAILED_58107CDE = "Ratis 请求失败：";
  public static final String EXCEPTION_DOT_F779BA66 = ". ";

}
