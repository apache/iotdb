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

package com.timecho.iotdb.i18n;

/** 编译时国际化常量 - TimechoDB ConfigNode 子系统（中文）。 */
public final class TimechoConfigNodeMessages {

  private TimechoConfigNodeMessages() {}

  // ConfigNode 启动
  public static final String HARDWARE_GENERATION_FAILED = "硬件信息生成失败。";

  // CLI 激活
  public static final String CLI_ACTIVATION_SUCCESS =
      "[CLI 激活] 成功更新所有 ConfigNode 的 license";
  public static final String CLI_ACTIVATION_PREFIX = "[CLI 激活]";

  // 许可证管理
  public static final String ACTIVE_NODE_WATCHING_SERVICE_LAUNCHED =
      "活跃节点监听服务启动成功";
  public static final String EXPIRATION_WARNING_SERVICE_LAUNCHED =
      "过期告警服务启动成功";
  public static final String SUCCESSFULLY_CREATE_ACTIVATION_DIR =
      "成功创建激活目录 {}";
  public static final String LICENSE_FILE_DETECTED_DURING_STARTING =
      "ConfigNode 启动时检测到 license 文件。";
  public static final String LICENSE_FILE_NOT_DETECTED_DURING_STARTING =
      "ConfigNode 启动时未检测到 license 文件。";
  public static final String START_LICENSE_FILE_MONITOR_FAIL = "启动 licenseFileMonitor 失败";
  public static final String LICENSE_FILE_WATCHING_SERVICE_LAUNCHED =
      "license 文件监听服务启动成功";
  public static final String LICENSE_FILE_CREATION_DETECTED = "检测到 license 文件创建";
  public static final String LICENSE_FILE_MODIFICATION_DETECTED = "检测到 license 文件修改";
  public static final String LICENSE_FILE_DELETION_DETECTED = "检测到 license 文件删除";
  public static final String SLEEPING_WAS_INTERRUPTED = "休眠被中断";
  public static final String THIS_LICENSE_NOT_ALLOWED_TO_ACTIVATE_THIS_CONFIGNODE =
      "此 license 不允许激活该 ConfigNode。";
  public static final String LOAD_LICENSE_SUCCESS = "加载 license 成功。";
  public static final String LOAD_LICENSE_FAIL = "加载 license 失败。";
  public static final String LOADING_LICENSE_WITH_CONTENT = "正在加载 license：\n{}";
  public static final String LICENSE_VERSION_NOT_SUPPORTED = "license 版本 ";
  public static final String LICENSE_VERSION_NOT_SUPPORTED_SUFFIX = " 不支持";
  public static final String LOADING_REMOTE_LICENSE = "正在加载远端 license...";
  public static final String LOADING_REMOTE_LICENSE_FAIL = "加载远端 license 失败。";
  public static final String LICENSE_UPDATED_BECAUSE_RECEIVE_REMOTE_LICENSE =
      "由于收到远端 license，本地 license 已更新";
  public static final String LICENSE_HAS_BEEN_GIVEN_UP_BECAUSE =
      "已放弃当前 license，原因：{}";

  // 系统信息文件
  public static final String SYSTEM_INFO_FILE_GENERATED_SUCCESSFULLY =
      "{} 文件生成成功。内容为 {}";
  public static final String SYSTEM_INFO_FILE_GENERATED_FAIL = "{} 文件生成失败。";
  public static final String CANNOT_REMOVE_VERSION_FROM_SYSTEM_INFO =
      "无法从系统信息中移除版本号 ";
  public static final String GENERATE_SYSTEM_INFO_CONTENT_FAIL = "生成系统信息内容失败";
  public static final String LICENSE_SYSTEM_INFO_VERSION_UNSUPPORTED =
      "license 系统信息版本 {} 不支持";
  public static final String VERIFY_SYSTEM_INFO_FAIL = "校验系统信息失败。";

  // license 文件管理
  public static final String SET_LICENSE_FILE_SUCCESS = "设置 license 文件成功：{}";
  public static final String SET_LICENSE_FILE_FAIL = "设置 license 文件 {} 失败";
  public static final String DELETE_LICENSE_FILE_FAIL = "删除 license 文件 {} 失败：{}";
  public static final String DELETE_LICENSE_FILE = "删除 license 文件：{}";
  public static final String READ_LICENSE_FILE_FAIL = "读取 license 文件 {} 失败：{}";
}
