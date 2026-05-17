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

/** 编译时国际化常量 - TimechoDB server 子系统（中文）。 */
public final class TimechoServerMessages {

  private TimechoServerMessages() {}

  // DataNode 启动
  public static final String IOTDB_DATANODE_ENVIRONMENT_VARIABLES =
      "IoTDB-DataNode 环境变量：{}";
  public static final String IOTDB_DATANODE_DEFAULT_CHARSET = "IoTDB-DataNode 默认字符集为：{}";
  public static final String HARDWARE_GENERATION_FAILED = "硬件信息生成失败。";

  // 权限（管理员权限分离）
  public static final String UNSUPPORTED_AUTHOR_TYPE = "不支持的 authorType：";
  public static final String ONLY_BUILTIN_ADMIN_CAN_GRANT_REVOKE_ADMIN =
      "仅内置管理员可授予/收回管理员权限";

  // 会话
  public static final String CANNOT_DISCONNECT_EXPIRED_SESSION = "无法断开已过期的会话 {}";

  // 共享存储 compaction
  public static final String FAILED_TO_SELECT_SHARED_STORAGE_COMPACTION_TASK =
      "选择共享存储 compaction 任务失败。";
  public static final String CANNOT_GET_REMOTE_STORAGE_BLOCK_OF_TSFILE =
      "无法获取 TsFile {} 的远端存储块。";
  public static final String FAIL_TO_DELETE_REMOTE_TMP_FILES_IN_DIR =
      "删除目录 {} 下的远端临时文件失败";
  public static final String TSFILE_RESOURCE_CANNOT_BE_DELETED = "TsFileResource {} 无法被删除：";
  public static final String STOP_COMPACTION_BECAUSE_OF_EXCEPTION_DURING_RECOVERING =
      "由于恢复过程中出现异常，停止 compaction";
  public static final String FAIL_TO_DELETE_OLD_LOG_FILE = "删除旧日志文件 {} 失败";
  public static final String FAIL_TO_PULL_REMOTE_REPLICA_FROM_ENDPOINT =
      "从端点 {} 拉取远端副本失败";
  public static final String FAIL_TO_PERSIST_REMOTE_REPLICA_OF_ENDPOINT =
      "持久化端点 {} 的远端副本失败";

  // 对象表大小索引
  public static final String FAILED_TO_EXECUTE_COMPACTION_FOR_OBJECT_TABLE_SIZE_INDEX_FILE =
      "执行对象表大小索引文件的 compaction 失败";
  public static final String FAILED_TO_SYNC_OBJECT_TABLE_SIZE_INDEX_FILE =
      "同步对象表大小索引文件 {} 失败";

  // 迁移任务
  public static final String FAIL_TO_COPY_TSFILE_FROM_LOCAL_TO_LOCAL =
      "将 TsFile 从本地 {} 拷贝到本地 {} 失败";
  public static final String FAIL_TO_SERIALIZE_REMOTE_STORAGE_INFO_INTO_FILE =
      "将远端存储信息序列化到文件 {} 失败";
  public static final String FAIL_TO_MIGRATE_RESOURCE_FROM_LOCAL_TO_REMOTE =
      "将资源从本地 {} 迁移到远端 {} 失败";
  public static final String FAIL_TO_DELETE_LOCAL_TSFILE = "删除本地 TsFile {} 失败";
  public static final String SUCCESSFULLY_DELETE_TSFILE_BY_SPACE_TL =
      "通过 SpaceTL 成功删除 TsFile {}。";
  public static final String MIGRATE_TASK_ERROR = "迁移任务发生错误";

  // RPC / IPFilter
  public static final String CANNOT_INSTANTIATE_THIS_CLASS = "无法实例化此类";
  public static final String INITIALIZING_WHITE_BLACK_LIST_UPDATE_CALLBACK =
      "正在初始化白/黑名单更新回调";
}
