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
 * IoTConsensusV2（基于 pipe 的共识）特有消息。日志消息使用 SLF4J {@code {}} 占位符；异常消息使用 {@code %s}（String.format）或纯字符串。
 */
public final class IoTConsensusV2Messages {

  private IoTConsensusV2Messages() {}

  // ===================== IoTConsensusV2 生命周期 =====================

  public static final String RECOVER_TASK_CANCELLED =
      "IoTV2 恢复任务已取消";
  public static final String RECOVER_FUTURE_EXCEPTION =
      "等待恢复 future 完成时发生异常";
  public static final String RECOVER_TASK_INTERRUPTED =
      "IoTV2 恢复任务被中断";
  public static final String FAILED_RECOVER_CONSENSUS =
      "从 {} 恢复共识组 {} 失败，忽略并继续恢复其他组，异步后台检查线程将自动注销该失败共识组的 pipe 副作用。";
  public static final String FAILED_RECOVER_CONSENSUS_READ_DIR =
      "从 {} 恢复共识失败，因为读取目录失败";
  public static final String FAILED_RECOVER_CONSENSUS_SHORT =
      "从 {} 恢复共识失败";

  // ===================== IoTConsensusV2 peer 操作 =====================

  public static final String START_DELETE_LOCAL_PEER =
      "[{}] 开始删除共识组 {} 的本地 peer";
  public static final String FINISH_DELETE_LOCAL_PEER =
      "[{}] 完成删除共识组 {} 的本地 peer";
  public static final String INACTIVATE_NEW_PEER =
      "[{}] 将新 peer 设为非活跃：{}";
  public static final String NOTIFY_CREATE_CONSENSUS_PIPES =
      "[{}] 通知当前 peer 创建 consensus pipe...";
  public static final String WAIT_PEERS_FINISH_TRANSFER =
      "[{}] 等待所有其他 peer 完成传输...";
  public static final String ACTIVATE_NEW_PEER =
      "[{}] 激活新 peer...";
  public static final String ADD_REMOTE_PEER_FAILED_CLEANUP =
      "[{}] 添加远程 peer 失败，正在自动清理副作用...";
  public static final String FAILED_CLEANUP_SIDE_EFFECTS =
      "[{}] 添加远程 peer 失败后清理副作用失败";
  public static final String NOTIFY_DROP_CONSENSUS_PIPES =
      "[{}] 通知其他 peer 删除 consensus pipe...";
  public static final String INACTIVATE_PEER =
      "[{}] 将 peer {} 设为非活跃";
  public static final String WAIT_TARGET_PEER_COMPLETE_TRANSFER =
      "[{}] 等待目标 peer{} 完成传输...";
  public static final String WAIT_PEER_RELEASE_RESOURCE =
      "[{}] 等待 {} 释放所有资源...";
  public static final String NOT_SUPPORT_LEADER_TRANSFER =
      "%s 不支持 leader 切换";

  // ===================== IoTConsensusV2ServerImpl =====================

  public static final String ERROR_SET_PEER_ACTIVE =
      "将 peer %s 设置为活跃状态 %s 时出错。结果状态：%s";
  public static final String ERROR_SET_PEER_ACTIVE_SHORT =
      "将 peer %s 设置为活跃状态 %s 时出错";
  public static final String TARGET_PEER_MAY_BE_DOWN =
      "目标 peer 可能已下线，将 peer {} 设置为活跃状态 {} 时出错";
  public static final String CANNOT_NOTIFY_PEER_CREATE_PIPE =
      "{} 无法通知 peer {} 创建 consensus pipe，该 peer 可能当前未知，请手动检查！";
  public static final String CANNOT_CREATE_CONSENSUS_PIPE =
      "{} 无法创建到 {} 的 consensus pipe，目标 peer 可能当前未知，请手动检查！";
  public static final String ERROR_NOTIFY_PEER_CREATE_PIPE =
      "通知 peer %s 创建 consensus pipe 时出错";
  public static final String CANNOT_NOTIFY_PEER_DROP_PIPE =
      "{} 无法通知 peer {} 删除 consensus pipe，该 peer 可能当前未知，请手动检查！";
  public static final String CANNOT_DROP_CONSENSUS_PIPE =
      "{} 无法删除到 {} 的 consensus pipe，目标 peer 可能当前未知，请手动检查！";
  public static final String ERROR_NOTIFY_PEER_DROP_PIPE =
      "通知 peer %s 删除 consensus pipe 时出错";
  public static final String INTERRUPTED_WAITING_TRANSFER =
      "{} 等待传输完成时被中断";
  public static final String INTERRUPTED_WAITING_TRANSFER_FMT =
      "%s 等待传输完成时被中断";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION =
      "{} 无法检查到 peer {} 的 consensus pipe 传输完成状态";
  public static final String ERROR_CHECK_PIPE_TRANSMISSION =
      "检查到 peer %s 的 consensus pipe 传输完成状态时出错";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION_SHORT =
      "{} 无法检查 consensus pipe 传输完成状态";

  // ===================== IoTConsensusV2RPCServiceProcessor =====================

  public static final String UNEXPECTED_GROUP_SET_ACTIVE =
      "共识组 ID %s 与设置活跃请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_CREATE_PIPE =
      "共识组 ID %s 与创建 consensus pipe 请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_DROP_PIPE =
      "共识组 ID %s 与删除 consensus pipe 请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_CHECK_TRANSFER =
      "共识组 ID %s 与检查传输完成请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_WAIT_RELEASE =
      "共识组 ID %s 与 TWaitReleaseAllRegionRelatedResourceRes 请求不匹配";
  public static final String FAILED_CREATE_CONSENSUS_PIPE =
      "创建到目标 peer 的 consensus pipe 失败，请求 {}";
  public static final String FAILED_DROP_CONSENSUS_PIPE =
      "删除到目标 peer 的 consensus pipe 失败，请求 {}";
  public static final String FAILED_CHECK_CONSENSUS_PIPE =
      "检查 consensus pipe 完成状态失败，请求 {}，将完成状态设为 {}";
}
