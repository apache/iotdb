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
 * Ratis 共识特有消息。日志消息使用 SLF4J {@code {}} 占位符；异常消息使用 {@code %s}（String.format）或纯字符串。
 */
public final class RatisMessages {

  private RatisMessages() {}

  // ===================== RatisConsensus =====================

  public static final String INTERRUPTED_RETRYING_WRITE =
      "{}：重试写入请求 {} 时被中断";
  public static final String NULL_REPLY_IN_WRITE_WITH_RETRY =
      "writeWithRetry 中收到空回复，请求为 ";
  public static final String LEADER_READ_ONLY_STEP_DOWN_FAILED =
      "leader {} 处于只读模式，强制降级失败，原因 ";
  public static final String TRY_ADD_CONFLICTING_PEER =
      "{}：尝试添加 ID 或地址冲突的 peer {} 到 {}";
  public static final String IS_LEADER_REQUEST_FAILED =
      "isLeader 请求失败，异常：";
  public static final String IS_LEADER_READY_REQUEST_FAILED =
      "isLeaderReady 请求失败，异常：";
  public static final String GET_LOGICAL_CLOCK_REQUEST_FAILED =
      "getLogicalClock 请求失败，异常：";
  public static final String IS_LEADER_READY_CHECKING_FAILED =
      "isLeaderReady 检查失败，异常：";
  public static final String LEADER_STILL_NOT_READY =
      "{}：leader 在 {} 毫秒后仍未就绪";
  public static final String UNEXPECTED_INTERRUPTION_WAIT_LEADER_READY =
      "等待 leader 就绪时发生意外中断";
  public static final String FETCH_DIVISION_INFO_FAILED =
      "获取共识组 {} 的 division 信息失败，原因：";
  public static final String TRIGGER_SNAPSHOT_SUCCESS =
      "{} 共识组 {}：已在 index {} 成功创建快照，force = {}";
  public static final String GET_GROUP_FAILED =
      "获取共识组 {} 失败 ";
  public static final String BORROW_CLIENT_FROM_POOL_FAILED =
      "从连接池借用共识组 {} 的客户端失败。";
  public static final String TRANSFER_LEADER_FAILED_TIMEOUT =
      "共识组 %s 向 %s 切换 leader 失败。这可能是由于超时引起的，"
          + "特别是在磁盘使用率较高时。请考虑增大 "
          + "'ratis_transfer_leader_timeout_ms' 配置项。";
  public static final String TRANSFER_LEADER_FAILED_STARTUP =
      "共识组 %s 向 %s 切换 leader 失败。这可能是由于超时引起的，"
          + "特别是在初始启动期间。请考虑增大 "
          + "'ratis_rpc_transfer_leader_timeout_ms' 配置项。";

  // ===================== ApplicationStateMachineProxy =====================

  public static final String STATEMACHINE_RUNTIME_EXCEPTION =
      "应用状态机抛出运行时异常：";
  public static final String INTERNAL_ERROR_STATEMACHINE_RUNTIME_EXCEPTION =
      "内部错误。状态机抛出运行时异常：";
  public static final String INTERRUPTED_WAITING_SYSTEM_READY =
      "{}：等待系统就绪时被中断：";
  public static final String REQUEST_MESSAGE_REQUIRED =
      "需要 RequestMessage 但收到 {}";
  public static final String UNABLE_TO_CREATE_TEMP_SNAPSHOT_DIR =
      "无法在 {} 创建临时快照目录";
  public static final String ATOMIC_RENAME_FAILED =
      "{} 将 {} 原子重命名为 {} 失败，异常 {}";
  public static final String SNAPSHOT_DIR_INCOMPLETE_DELETING =
      "快照目录不完整，正在删除 {}";

  // ===================== RatisClient =====================

  public static final String CANNOT_CLOSE_RAFT_CLIENT =
      "无法关闭 Raft 客户端 ";
  public static final String RAFT_CLIENT_REQUEST_FAILED =
      "{}：Raft 客户端请求失败并捕获异常：";

  // ===================== DiskGuardian =====================

  public static final String ERROR_LISTING_FILES =
      "{}：列出 {} 在 {} 下的文件时出错：";
  public static final String CLEAR_SNAPSHOT_FLAG_FAILED =
      "{}：清除共识组 {} 的快照标志失败，请检查相关实现";
  public static final String TAKE_SNAPSHOT_FAILED =
      "{} 共识组 {} 创建快照失败，原因 {}。磁盘文件状态 {}";

  // ===================== SnapshotStorage =====================

  public static final String CANNOT_CONSTRUCT_SNAPSHOT_DIR_STREAM =
      "无法构建快照目录流 ";
  public static final String CANNOT_RESOLVE_REAL_PATH =
      "{} 无法解析 {} 的真实路径，原因 ";

  // ===================== ResponseMessage =====================

  public static final String SERIALIZE_TSSTATUS_FAILED =
      "序列化 TSStatus 失败 {}";

  // ===================== MetricRegistryManager =====================

  public static final String REPORTER_DISABLED =
      "Reporter 在 RatisMetricRegistries 中已禁用";
  public static final String JMX_REPORTER_DISABLED =
      "JMX Reporter 在 RatisMetricRegistries 中已禁用";
  public static final String CONSOLE_REPORTER_DISABLED =
      "Console Reporter 在 RatisMetricRegistries 中已禁用";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_RESET_PEER_LIST_CURRENT_PEER_LIST_CORRECT_NOTHING_NEED_RESET_0E009CDA = "[RESET PEER LIST] 当前 peer 列表正确，无需重置：{}";
  public static final String EXCEPTION_INTERNAL_GRPC_CONNECTION_ERROR_59404D15 = "内部 GRPC 连接错误：";
  public static final String LOG_FAILED_ARG_ATTEMPT_ARG_SLEEP_ARG_THEN_RETRY_36A9A0C2 = "{} 失败，第 {} 次尝试，休眠 {} 后重试";
  public static final String LOG_ARG_INTERRUPTED_WAITING_RETRY_38D69BCC = "{}：等待重试时被中断";
  public static final String EXCEPTION_SUPPLIER_EQUALS_EQUALS_NULL_13BACC3E = "supplier == null";
  public static final String EXCEPTION_CONDITION_EQUALS_EQUALS_NULL_E3A6C947 = "condition == null";
  public static final String EXCEPTION_ARG_FAILED_TO_LOAD_SNAPSHOT_FROM_ARG_A12E23D7 =
      "%s：从 %s 加载快照失败";

}
