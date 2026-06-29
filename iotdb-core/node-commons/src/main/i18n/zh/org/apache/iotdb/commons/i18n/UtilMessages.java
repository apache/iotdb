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

public final class UtilMessages {

  // ======================== AuthUtils ========================

  public static final String FAILED_TO_SERIALIZE_PARTIAL_PATH_LIST =
      "序列化 PartialPath 列表失败";
  public static final String UNSUPPORTED_POSITION = "不支持的位置";
  public static final String UNSUPPORTED_PRIVILEGE_TYPE = "不支持的权限类型 ";

  // ======================== BasicStructureSerDeUtil ========================

  public static final String STRING_LIST_MUST_NOT_BE_NULL = "stringList 不能为 null！";

  // ======================== BlobUtils ========================

  public static final String BLOB_MUST_BE_HEX_STRING =
      "二进制字面量必须采用 X'hexstring' 格式";
  public static final String BLOB_ONLY_HEX_DIGITS = "二进制字面量只能包含十六进制数字";
  public static final String BLOB_EVEN_NUMBER_OF_DIGITS =
      "二进制字面量必须包含偶数个数字";

  // ======================== CommonDateTimeUtils ========================

  public static final String INTEGER_OVERFLOW_CONVERTING_TIME =
      "将 {}ms 转换为 {}{} 时发生整数溢出。";

  // ======================== FileUtils ========================

  public static final String DELETE_FOLDER_FAILED = "删除文件夹失败：{}";
  public static final String COPY_FOLDER_SOURCE_NOT_EXIST =
      "复制文件夹失败，源文件夹 [{}] 不存在。";
  public static final String COPY_FOLDER_CREATE_TARGET_FAILED =
      "复制文件夹失败，无法创建目标文件夹 [{}]。";
  public static final String COPY_FOLDER_TARGET_ALREADY_EXISTS =
      "复制文件夹失败，目标文件夹 [{}] 已存在。";
  public static final String IO_EXCEPTION_ON_FILE = "文件 {} 发生 IO 异常";
  public static final String MOVE_FILE_TARGET_ALREADY_EXISTS =
      "目标文件已存在，跳过移动：{}";
  public static final String MOVE_FILE_DELETE_SOURCE_HINT =
      "如有需要请手动删除源文件：{}";
  public static final String MOVE_FILE_START = "开始移动文件，{}";
  public static final String DELETE_UNFINISHED_TARGET_FAILED = "删除未完成的目标文件失败：{}";
  public static final String UNFINISHED_TARGET_DELETED =
      "已删除上次遗留的未完成目标文件：{}";
  public static final String FILE_COPY_FAIL = "文件复制失败";
  public static final String FILE_RENAME_FAIL = "文件重命名失败";
  public static final String DELETE_SOURCE_FILE_FAIL = "删除源文件失败：{}";
  public static final String MOVE_FILE_SUCCESS = "移动文件成功，{}";
  public static final String HARDLINK_ALREADY_EXISTS =
      "硬链接 {} 已存在，不再重复创建。源文件：{}";
  public static final String HARDLINK_MISMATCH_RETRY =
      "硬链接 {} 已存在但与源文件 {} 不一致，将尝试重新创建。";
  public static final String FAILED_TO_CREATE_HARDLINK =
      "为文件 {} 创建硬链接 {} 失败：{}";
  public static final String FAILED_TO_CREATE_HARDLINK_PARENT_DIR =
      "创建硬链接 %s 失败（源文件 %s）：无法创建父目录 %s";
  public static final String FAILED_TO_COPY_FILE_PARENT_DIR =
      "复制文件 %s 到 %s 失败：无法创建父目录 %s";
  public static final String DELETED_DUPLICATE_FILE =
      "已删除文件 {}，因为目标目录 {} 中已存在同名文件";
  public static final String FAILED_TO_CREATE_TARGET_DIRECTORY =
      "创建目标目录失败：{}";
  public static final String RENAMED_FILE_ALREADY_EXISTS =
      "已将文件 {} 重命名为 {}，因为目标目录 {} 中已存在同名文件";
  public static final String COPIED_FILE_ALREADY_EXISTS =
      "已将文件 {} 复制为 {}，因为目标目录 {} 中已存在同名文件";
  public static final String ILLEGAL_EMPTY_PATH = "路径不能为空。 ";
  public static final String ILLEGAL_PATH_DOTS_OR_SEPARATORS =
      "路径不能为 '.'、'..'、'./' 或 '.\\\\'. ";

  // ======================== IOUtils ========================

  public static final String CANNOT_DELETE_OLD_USER_FILE = "无法删除旧用户文件：%s";
  public static final String CANNOT_REPLACE_OLD_USER_FILE =
      "无法用新文件替换旧用户文件：%s";

  // ======================== JVMCommonUtils ========================

  public static final String UNEXPECTED_ERROR_CHECKING_DISK_SPACE_FOR_DIR =
      "检查目录 {} 的磁盘空间时发生意外错误";
  public static final String CANNOT_GET_FREE_SPACE =
      "多次重试后仍无法获取 {} 的可用空间，请检查磁盘状态";
  public static final String DISK_ABOVE_WARNING_THRESHOLD =
      "{} 的磁盘使用率超过告警阈值，可用空间 {}，总空间 {}";
  public static final String UNEXPECTED_ERROR_CHECKING_DISK_SPACE =
      "检查 {} 的磁盘空间时发生意外错误";

  // ======================== FolderManager ========================

  public static final String ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY =
      "所有文件夹空间均已耗尽，系统切换为只读模式。";
  public static final String FAILED_TO_PROCESS_FOLDER = "处理文件夹 {} 失败";
  public static final String FAILED_TO_READ_FILE_STORE_PATH =
      "读取文件存储路径 '{}' 失败";
  public static final String DISK_SPACE_INSUFFICIENT_READ_ONLY =
      "磁盘空间不足，系统切换为只读模式。";
  public static final String CANNOT_CALCULATE_OCCUPIED_SPACE =
      "无法计算文件夹 {} 的已占用空间";
  public static final String UNRECOGNIZED_MULTI_DIR_STRATEGY =
      "无法识别的多目录策略 '{}'，回退为 {}。";

  // ======================== NodeUrlUtils ========================

  public static final String BAD_CONFIG_NODE_URL = "ConfigNode URL 格式错误：{}";
  public static final String BAD_NODE_URL = "节点 URL 格式错误：%s";
  public static final String ENDPOINT_URLS_IS_NULL = "endPointUrls 为 null";

  // ======================== RetryUtils ========================

  public static final String OPERATION_SUCCEEDED_AFTER_RETRIES =
      "操作 '{}' 在第 {} 次尝试后成功";
  public static final String OPERATION_FAILED_RETRYING =
      "操作 '{}' 失败（第 {} 次尝试）。将在 {}ms 后重试...";
  public static final String RETRY_WAIT_INTERRUPTED =
      "操作 '{}' 的重试等待被中断，停止重试。";

  // ======================== KillPoint ========================

  public static final String KILL_POINT_SET = "Kill point 集合：{}";

  // ======================== ThriftCommonsSerDeUtils ========================

  public static final String WRITE_T_ENDPOINT_FAILED = "写入 TEndPoint 失败：";
  public static final String READ_T_ENDPOINT_FAILED = "读取 TEndPoint 失败：";
  public static final String WRITE_T_DATA_NODE_CONFIGURATION_FAILED =
      "写入 TDataNodeConfiguration 失败：";
  public static final String READ_T_DATA_NODE_CONFIGURATION_FAILED =
      "读取 TDataNodeConfiguration 失败：";
  public static final String WRITE_T_DATA_NODE_LOCATION_FAILED =
      "写入 TDataNodeLocation 失败：";
  public static final String READ_T_DATA_NODE_LOCATION_FAILED =
      "读取 TDataNodeLocation 失败：";
  public static final String WRITE_T_CREATE_CQ_REQ_FAILED = "写入 TCreateCQReq 失败：";
  public static final String READ_T_CREATE_CQ_REQ_FAILED = "读取 TCreateCQReq 失败：";
  public static final String WRITE_T_DATA_NODE_INFO_FAILED = "写入 TDataNodeInfo 失败：";
  public static final String READ_T_DATA_NODE_INFO_FAILED = "读取 TDataNodeInfo 失败：";
  public static final String WRITE_T_SERIES_PARTITION_SLOT_FAILED =
      "写入 TSeriesPartitionSlot 失败：";
  public static final String READ_T_SERIES_PARTITION_SLOT_FAILED =
      "读取 TSeriesPartitionSlot 失败：";
  public static final String WRITE_T_TIME_SLOT_LIST_FAILED = "写入 TTimeSlotList 失败：";
  public static final String READ_T_TIME_SLOT_LIST_FAILED = "读取 TTimeSlotList 失败：";
  public static final String WRITE_T_TIME_PARTITION_SLOT_FAILED =
      "写入 TTimePartitionSlot 失败：";
  public static final String READ_T_TIME_PARTITION_SLOT_FAILED =
      "读取 TTimePartitionSlot 失败：";
  public static final String WRITE_T_CONSENSUS_GROUP_ID_FAILED =
      "写入 TConsensusGroupId 失败：";
  public static final String READ_T_CONSENSUS_GROUP_ID_FAILED =
      "读取 TConsensusGroupId 失败：";
  public static final String WRITE_T_REGION_REPLICA_SET_FAILED =
      "写入 TRegionReplicaSet 失败：";
  public static final String READ_T_REGION_REPLICA_SET_FAILED =
      "读取 TRegionReplicaSet 失败：";
  public static final String WRITE_T_SCHEMA_NODE_FAILED = "写入 TSchemaNode 失败：";
  public static final String READ_T_SCHEMA_NODE_FAILED = "读取 TSchemaNode 失败：";
  public static final String WRITE_T_AI_NODE_INFO_FAILED = "写入 TAINodeInfo 失败：";
  public static final String READ_T_AI_NODE_INFO_FAILED = "读取 TAINodeInfo 失败：";
  public static final String WRITE_T_AI_NODE_LOCATION_FAILED = "写入 TAINodeLocation 失败：";
  public static final String READ_T_AI_NODE_LOCATION_FAILED = "读取 TDataNodeLocation 失败：";
  public static final String READ_T_AI_NODE_CONFIGURATION_FAILED =
      "读取 TAINodeConfiguration 失败：";

  // ======================== ThriftConfigNodeSerDeUtils ========================

  public static final String WRITE_T_STORAGE_GROUP_SCHEMA_FAILED =
      "写入 TStorageGroupSchema 失败：";
  public static final String READ_T_STORAGE_GROUP_SCHEMA_FAILED =
      "读取 TStorageGroupSchema 失败：";
  public static final String WRITE_T_CONFIG_NODE_LOCATION_FAILED =
      "写入 TConfigNodeLocation 失败：";
  public static final String READ_T_CONFIG_NODE_LOCATION_FAILED =
      "读取 TConfigNodeLocation 失败：";
  public static final String WRITE_T_PIPE_SINK_INFO_FAILED = "写入 TPipeSinkInfo 失败：";
  public static final String READ_T_PIPE_SINK_INFO_FAILED = "读取 TPipeSinkInfo 失败：";

  private UtilMessages() {}
}
