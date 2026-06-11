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
}
