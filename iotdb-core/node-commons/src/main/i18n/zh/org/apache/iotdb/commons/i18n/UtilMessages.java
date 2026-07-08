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
      "路径不能为 '.'、'..'、'./' 或 '.\\'. ";

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
  public static final String CANNOT_SELECT_FOLDER_BUT_DISK_HAS_SPACE =
      "无法选择文件夹但磁盘仍有可用空间，不切换为只读模式。";
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
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_INTERNAL_USER_NAMES_EXCEPT_DEFAULT_SUPERUSER_SEPARATION_DUTIES_ADMINS_MUST_3F045FC1 = "内部用户名（默认 superuser 和职责分离管理员除外）必须";
  public static final String EXCEPTION_START_01CCD2EF = "以 \" 开头";
  public static final String EXCEPTION_USER_NAMES_STARTING_8FDC637E = "以 \" 开头的用户名";
  public static final String EXCEPTION_RESERVED_SYSTEM_USE_CANNOT_USED_NEW_USERS_AS_RENAME_CEB73835 = "\" 保留供系统使用，不能用于新用户或重命名 ";
  public static final String EXCEPTION_TARGET_42AEFBAE = "target";
  public static final String EXCEPTION_LENGTH_NAME_MUST_GREATER_THAN_EQUAL_0387C3A5 = "name 长度必须大于等于 ";
  public static final String EXCEPTION_LENGTH_NAME_MUST_LESS_THAN_EQUAL_B7B31C94 = "name 长度必须小于等于 ";
  public static final String EXCEPTION_NAME_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_A313856E = "name 只能包含字母、数字或 !@#$%^*()_+-=";
  public static final String EXCEPTION_LENGTH_PASSWORD_MUST_GREATER_THAN_EQUAL_F95F3E8F = "密码长度必须大于等于 ";
  public static final String EXCEPTION_LENGTH_PASSWORD_MUST_LESS_THAN_EQUAL_C822ECBE = "密码长度必须小于等于 ";
  public static final String EXCEPTION_PASSWORD_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_D84EE152 = "密码只能包含字母、数字或 !@#$%^*()_+-=";
  public static final String EXCEPTION_ILLEGAL_SERIESPATH_ARG_SERIESPATH_SHOULD_START_ARG_FB9E3C07 = "非法 seriesPath %s，seriesPath 应以 \"%s\" 开头";
  public static final String EXCEPTION_ILLEGAL_PATTERN_PATH_ARG_ONLY_PATTERN_PATH_END_SUPPORTED_16CBB77D = "非法 pattern path：%s，仅支持以 ** 结尾的 pattern path。";
  public static final String EXCEPTION_ILLEGAL_PATTERN_PATH_ARG_ONLY_PATTERN_PATH_END_WILDCARDS_SUPPORTED_7183A896 = "非法 pattern path：%s，仅支持以通配符结尾的 pattern path。";
  public static final String EXCEPTION_NO_SUCH_PRIVILEGE_62644205 = "无此权限 ";
  public static final String MESSAGE_EXECUTED_SUCCESSFULLY_1EAF1169 = "执行成功。";
  public static final String MESSAGE_REQUEST_TIMED_OUT_FD587FC4 = "请求超时。";
  public static final String MESSAGE_INCOMPATIBLE_VERSION_0C5CB2AF = "版本不兼容。";
  public static final String MESSAGE_FAILED_REMOVING_DATANODE_B4B7F050 = "移除 DataNode 失败。";
  public static final String MESSAGE_ALIAS_ALREADY_EXISTS_C05A2E5A = "别名已存在。";
  public static final String MESSAGE_PATH_ALREADY_EXIST_56F8BFF9 = "路径 已存在.";
  public static final String MESSAGE_PATH_DOES_NOT_EXIST_93310498 = "路径 不存在.";
  public static final String MESSAGE_MEET_ERROR_DEALING_METADATA_1C4A38B9 = "处理元数据时发生错误。";
  public static final String MESSAGE_INSERTION_TIME_LESS_THAN_TTL_TIME_BOUND_0F1BB861 = "插入时间小于 TTL 时间边界。";
  public static final String MESSAGE_MEET_ERROR_MERGING_28424A77 = "合并时发生错误。";
  public static final String MESSAGE_MEET_ERROR_DISPATCHING_73E4FD5E = "分发时发生错误。";
  public static final String MESSAGE_DATABASE_PROCESSOR_RELATED_ERROR_C58690B1 = "数据库处理器相关错误。";
  public static final String MESSAGE_STORAGE_ENGINE_RELATED_ERROR_94DEBBCF = "存储引擎相关错误。";
  public static final String MESSAGE_TSFILE_PROCESSOR_RELATED_ERROR_B6C57C3E = "TsFile 处理器相关错误。";
  public static final String MESSAGE_ILLEGAL_PATH_020B26BC = "非法路径。";
  public static final String MESSAGE_MEET_ERROR_LOADING_FILE_A90EBC21 = "加载文件时发生错误。";
  public static final String MESSAGE_EXECUTE_STATEMENT_ERROR_54E4A395 = "执行语句错误。";
  public static final String MESSAGE_MEET_ERROR_PARSING_SQL_3C5A3B80 = "解析 SQL 时发生错误。";
  public static final String MESSAGE_MEET_ERROR_GENERATING_TIME_ZONE_94E03CA3 = "生成时区时发生错误。";
  public static final String MESSAGE_MEET_ERROR_SETTING_TIME_ZONE_CBE88DCF = "设置时区时发生错误。";
  public static final String MESSAGE_QUERY_STATEMENTS_NOT_ALLOWED_ERROR_58D30C99 = "不允许执行查询语句。";
  public static final String MESSAGE_LOGICAL_OPERATOR_RELATED_ERROR_D94D5972 = "逻辑运算符相关错误。";
  public static final String MESSAGE_LOGICAL_OPTIMIZE_RELATED_ERROR_36D63CB3 = "逻辑优化相关错误。";
  public static final String MESSAGE_UNSUPPORTED_FILL_TYPE_RELATED_ERROR_67BE2CF2 = "不支持的填充类型相关错误。";
  public static final String MESSAGE_QUERY_PROCESS_RELATED_ERROR_93FA1016 = "查询处理相关错误。";
  public static final String MESSAGE_WRITING_DATA_RELATED_ERROR_0CA06C4D = "写入数据相关错误。";
  public static final String MESSAGE_INTERNAL_SERVER_ERROR_12F61DF7 = "内部服务器错误。";
  public static final String MESSAGE_MEET_ERROR_CLOSE_OPERATION_1C7D0589 = "关闭操作中发生错误。";
  public static final String MESSAGE_FAIL_DO_NON_QUERY_OPERATIONS_BECAUSE_SYSTEM_READ_ONLY_10CA1ED2 = "系统只读，无法执行非查询操作。";
  public static final String MESSAGE_DISK_SPACE_INSUFFICIENT_DF6205B0 = "磁盘空间不足。";
  public static final String MESSAGE_MEET_ERROR_STARTING_UP_22A4CBFE = "启动时发生错误。";
  public static final String MESSAGE_USERNAME_PASSWORD_WRONG_C44C4AF0 = "用户名或密码错误。";
  public static final String MESSAGE_HAS_NOT_LOGGED_A2BA0267 = "尚未登录。";
  public static final String MESSAGE_NO_PERMISSIONS_OPERATION_PLEASE_ADD_PRIVILEGE_64047D1E = "没有此操作的权限，请添加权限。";
  public static final String MESSAGE_FAILED_INIT_AUTHORIZER_1E2B017E = "初始化授权器失败。";
  public static final String MESSAGE_UNSUPPORTED_OPERATION_295CDB21 = "不支持的操作。";
  public static final String MESSAGE_NODE_CANNOT_REACHED_D3FD04A8 = "节点无法访问。";
  public static final String LOG_ARG_ABOVE_WARNING_THRESHOLD_NOT_ACCESSIBLE_FREE_SPACE_ARG_TOTAL_87DAD16A = "{} 超过警告阈值或不可访问，可用空间 {}，总空间 {}";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value 不能为空";
  public static final String LOG_ARG_COLON_ARG_DCE519A1 = "{}: {}";
  public static final String EMPTY_MESSAGE = "";

}
