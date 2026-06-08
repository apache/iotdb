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
 * IoTConsensusV2（基于 pipe 的共识）特有消息。日志消息使用 SLF4J {@code {}} 占位符；异常消息使用 {@code %s}（String.format）或普通字符串。
 */
public final class IoTConsensusV2Messages {

  private IoTConsensusV2Messages() {}

  // ===================== IoTConsensusV2 生命周期 =====================

  public static final String RECOVER_TASK_CANCELLED =
      "IoTV2 恢复任务已取消";
  public static final String RECOVER_FUTURE_EXCEPTION =
      "等待恢复 Future 完成时发生异常";
  public static final String RECOVER_TASK_INTERRUPTED =
      "IoTV2 恢复任务被中断";
  public static final String FAILED_RECOVER_CONSENSUS =
      "从 {} 恢复共识组 {} 失败，将忽略该组并继续恢复其他共识组；异步后台检查线程会自动清理该失败共识组相关的 pipe 副作用。";
  public static final String FAILED_RECOVER_CONSENSUS_READ_DIR =
      "从 {} 恢复共识失败，原因：读取目录失败";
  public static final String FAILED_RECOVER_CONSENSUS_SHORT =
      "从 {} 恢复共识失败";

  // ===================== IoTConsensusV2 节点操作 =====================

  public static final String START_DELETE_LOCAL_PEER =
      "[{}] 开始删除共识组 {} 的本地节点";
  public static final String FINISH_DELETE_LOCAL_PEER =
      "[{}] 完成删除共识组 {} 的本地节点";
  public static final String INACTIVATE_NEW_PEER =
      "[{}] 停用新节点：{}";
  public static final String NOTIFY_CREATE_CONSENSUS_PIPES =
      "[{}] 通知当前节点创建共识管道...";
  public static final String WAIT_PEERS_FINISH_TRANSFER =
      "[{}] 等待其他节点完成传输...";
  public static final String ACTIVATE_NEW_PEER =
      "[{}] 激活新节点...";
  public static final String ADD_REMOTE_PEER_FAILED_CLEANUP =
      "[{}] 添加远程节点失败，正在自动清理副作用...";
  public static final String FAILED_CLEANUP_SIDE_EFFECTS =
      "[{}] 添加远程节点失败后清理副作用失败";
  public static final String NOTIFY_DROP_CONSENSUS_PIPES =
      "[{}] 通知其他节点删除共识管道...";
  public static final String INACTIVATE_PEER =
      "[{}] 停用节点 {}";
  public static final String WAIT_TARGET_PEER_COMPLETE_TRANSFER =
      "[{}] 等待目标节点 {} 完成传输...";
  public static final String WAIT_PEER_RELEASE_RESOURCE =
      "[{}] 等待 {} 释放所有资源...";
  public static final String NOT_SUPPORT_LEADER_TRANSFER =
      "%s 不支持主节点切换";

  // ===================== IoTConsensusV2ServerImpl =====================

  public static final String ERROR_SET_PEER_ACTIVE =
      "设置节点 %s 的活跃状态为 %s 时出错。结果状态：%s";
  public static final String ERROR_SET_PEER_ACTIVE_SHORT =
      "设置节点 %s 的活跃状态为 %s 时出错";
  public static final String TARGET_PEER_MAY_BE_DOWN =
      "目标节点可能已下线，设置节点 {} 的活跃状态为 {} 时出错";
  public static final String CANNOT_NOTIFY_PEER_CREATE_PIPE =
      "{} 无法通知节点 {} 创建共识管道，该节点当前可能未知，请手动检查。";
  public static final String CANNOT_CREATE_CONSENSUS_PIPE =
      "{} 无法创建到 {} 的共识管道，目标节点当前可能未知，请手动检查。";
  public static final String ERROR_NOTIFY_PEER_CREATE_PIPE =
      "通知节点 %s 创建共识管道时出错";
  public static final String CANNOT_NOTIFY_PEER_DROP_PIPE =
      "{} 无法通知节点 {} 删除共识管道，该节点当前可能未知，请手动检查。";
  public static final String CANNOT_DROP_CONSENSUS_PIPE =
      "{} 无法删除到 {} 的共识管道，目标节点当前可能未知，请手动检查。";
  public static final String ERROR_NOTIFY_PEER_DROP_PIPE =
      "通知节点 %s 删除共识管道时出错";
  public static final String INTERRUPTED_WAITING_TRANSFER =
      "{} 等待传输完成时被中断";
  public static final String INTERRUPTED_WAITING_TRANSFER_FMT =
      "%s 等待传输完成时被中断";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION =
      "{} 无法检查到节点 {} 的共识管道是否传输完成";
  public static final String ERROR_CHECK_PIPE_TRANSMISSION =
      "检查到节点 %s 的共识管道是否传输完成时出错";
  public static final String CANNOT_CHECK_PIPE_TRANSMISSION_SHORT =
      "{} 无法检查共识管道是否传输完成";

  // ===================== IoTConsensusV2RPCServiceProcessor =====================

  public static final String UNEXPECTED_GROUP_SET_ACTIVE =
      "共识组 ID %s 与设置活跃状态请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_CREATE_PIPE =
      "共识组 ID %s 与创建共识管道请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_DROP_PIPE =
      "共识组 ID %s 与删除共识管道请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_CHECK_TRANSFER =
      "共识组 ID %s 与检查传输完成请求 %s 不匹配";
  public static final String UNEXPECTED_GROUP_WAIT_RELEASE =
      "共识组 ID %s 与 TWaitReleaseAllRegionRelatedResourceRes 请求不匹配";
  public static final String FAILED_CREATE_CONSENSUS_PIPE =
      "创建到目标节点的共识管道失败，请求：{}";
  public static final String FAILED_DROP_CONSENSUS_PIPE =
      "删除到目标节点的共识管道失败，请求：{}";
  public static final String FAILED_CHECK_CONSENSUS_PIPE =
      "检查共识管道传输完成状态失败，请求：{}，将完成状态设为 {}";
}
