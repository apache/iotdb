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

package org.apache.iotdb.cli.i18n;

public final class CliMessages {

  // CliContext
  public static final String EXITING_WITH_CODE = "正在退出，退出码 %d";

  // Cli
  public static final String SUCCESSFULLY_LOGIN_AT = "成功登录到 %s";

  // IoTDBDataBackTool
  public static final String TARGET_DIR_EMPTY =
      " -targetdir 不能为空，必须指定备份目录";
  public static final String TARGET_DIR_USE_ABSOLUTE_PATH =
      "-targetdir 参数异常，请使用绝对路径";
  public static final String TARGET_DATA_DIR_USE_ABSOLUTE_PATH =
      "-targetdatadir 参数异常，请使用绝对路径";
  public static final String TARGET_WAL_DIR_USE_ABSOLUTE_PATH =
      "-targetwaldir 参数异常，请使用绝对路径";
  public static final String BACKUP_FOLDER_EXISTS = "备份目录已存在：{}";
  public static final String ALL_OPERATIONS_COMPLETE = "所有操作已完成";
  public static final String COPY_FILE_ERROR = "复制文件错误";
  public static final String COPY_FILE_ERROR_WITH_PATH = "复制文件错误 {}";
  public static final String START_READ_CONFIG = "开始读取配置文件 {}";
  public static final String READ_CONFIG_ERROR = "读取配置文件 {} 错误";
  public static final String DIRECTORY_CREATED = "目录创建成功：{}";
  public static final String FAILED_TO_CREATE_DIRECTORY = "创建目录失败：{}";
  public static final String LINK_FILE_ERROR = "创建文件链接错误 {}";
  public static final String PROPERTIES_FILE_UPDATE_ERROR = "属性文件更新错误。";
  public static final String FAILED_TO_READ_DATA = "从文件读取数据失败：{}";
  public static final String FAILED_TO_WRITE_DATA = "向文件写入数据失败：{}";
  public static final String FAILED_TO_CREATE_FILE = "创建文件失败：{}";

  // AbstractCli
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E =
      "不支持的数据类型：%s";

  // AbstractDataTool
  public static final String USE_HELP_FOR_MORE = "使用 -help 获取更多信息";

  // ImportTsFileRemotely
  public static final String SYNC_CLIENT_INIT_ERROR = "同步客户端初始化失败，原因：%s";

  // UnsupportedOperationException
  public static final String NOT_SUPPORTED_YET = "尚不支持此操作。";

  // ImportData
  public static final String UNKNOWN_TYPE_INFER_KEY = "未知的类型推断键：%s";
  public static final String UNKNOWN_TYPE_INFER_VALUE = "未知的类型推断值：%s";
  public static final String NAN_CANNOT_CONVERT = "NaN 无法转换为 %s";
  public static final String BOOLEAN_CANNOT_CONVERT = "Boolean 无法转换为 %s";
  public static final String DATE_CANNOT_CONVERT = "Date 无法转换为 %s";
  public static final String TIMESTAMP_CANNOT_CONVERT = "Timestamp 无法转换为 %s";
  public static final String BLOB_CANNOT_CONVERT = "Blob 无法转换为 %s";
  public static final String CANNOT_CONVERT = "%s 无法转换为 %s";
  public static final String
      MESSAGE_INVALID_ARGS_REQUIRED_VALUES_FOR_OPTION_TABLE_NOT_PROVIDED_4BC3FCFA =
          "参数无效：未提供 table 选项的必填值。";

  // Data import/export
  public static final String EXCEPTION_FAILED_TO_INSERT_CSV_BATCH_ARG_159E0269 =
      "CSV 批次写入失败：%s";
  public static final String MESSAGE_ARG_UNEXPECTED_ERROR_OCCURRED_ARG_E7255EE6 =
      "[%s] - 发生意外错误：%s";
  public static final String
      EXCEPTION_SQL_TSFILE_EXPORT_REQUIRES_A_QUERY_FROM_A_SINGLE_TABLE_IN_DATABASE_ARG_C9EB3768 =
          "SQL/TsFile 导出要求查询仅来自数据库 %s 中的一张表。";
  public static final String
      MESSAGE_QUERY_TO_EXPORT_OPTIONAL_TABLE_SQL_TSFILE_STORED_COLUMNS_OF_ONE_TABLE_6D79406B =
          "导出查询（可选）。表模型 SQL/TsFile 导出只能查询一张表的原始列。";
  public static final String
      MESSAGE_MULTIPLE_CSV_HEADERS_RESOLVE_TO_THE_SAME_TARGET_COLUMN_99189563 =
          "多个 CSV 表头映射到同一目标列。";

  private CliMessages() {}

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_HANDSHAKE_ERROR_TARGET_SERVER_IP_ARG_PORT_ARG_BECAUSE_ARG_9D522E62 = "与目标服务器握手失败，IP：%s，端口：%s，原因：%s。";
  public static final String EXCEPTION_NETWORK_ERROR_SEAL_FILE_ARG_BECAUSE_ARG_62E92EE8 = "封存文件 %s 时发生网络错误，原因：%s。";
  public static final String EXCEPTION_SEAL_FILE_ARG_ERROR_RESULT_STATUS_ARG_FE3B82AC = "封存文件 %s 出错，结果状态 %s。";
  public static final String EXCEPTION_NETWORK_ERROR_TRANSFER_FILE_ARG_BECAUSE_ARG_BC25323C = "传输文件 %s 时发生网络错误，原因：%s。";
  public static final String EXCEPTION_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD = "传输文件 %s 出错，结果状态 %s。";
  public static final String LOG_TARGETDATADIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_8B31BF59 = "-targetdatadir 参数异常，原始路径数量与指定路径数量不匹配";
  public static final String LOG_TARGETWALDIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_94AFE885 = "-targetwaldir 参数异常，原始路径数量与指定路径数量不匹配";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_371383B7 = "待备份目录不能位于源目录中，请检查：{},{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_6DA7D5DA = "待备份目录不能位于源目录中，请检查：{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_CFA67674 = "待备份目录不能位于源目录中，请检查：{}";
  public static final String LOG_TOTAL_FILE_NUMBER_A1554ADC = "文件总数：";
  public static final String LOG_VERIFY_NUMBER_FILES_E171592C = "，校验文件数量：";
  public static final String LOG_BACKUP_FILE_NUMBER_72FC1312 = "，备份文件数量：";
  public static final String LOG_INPUT_TIME_FORMAT_ARG_NOT_SUPPORTED_00172A7B = "不支持输入时间格式 {}，";
  public static final String LOG_PLEASE_INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_SSS_9318BFC7 = "请输入类似 yyyy-MM-dd\\ HH:mm:ss.SSS 或 yyyy-MM-dd'T'HH:mm:ss.SSS 的格式%n";

  // Export data
  public static final String MESSAGE_EXPORT_LOG_FILE_DEFAULT_TARGET_LOG_EXPORT_DATA_YYYYMMDDHHMMSS_LOG_B29FC35F =
      "导出日志文件（默认：<target>/log_export_data_yyyyMMddHHmmss.log）。";
  public static final String MESSAGE_INCLUSIVE_START_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_69FE75CF =
      "测点文件或 Table 自动导出的开始时间（含边界）。";
  public static final String MESSAGE_INCLUSIVE_END_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_D9C24D95 =
      "测点文件或 Table 自动导出的结束时间（含边界）。";
  public static final String MESSAGE_UTF_8_IOTDB_PATH_PATTERN_FILE_FOR_TREE_EXPORT_MUTUALLY_EXCLUSIVE_WITH_Q_DD1F2CDA =
      "Tree 导出的 UTF-8 IoTDB 路径 pattern 文件，与 -q 互斥。";
  public static final String MESSAGE_TABLE_TO_EXPORT_OPTIONAL_IGNORED_WHEN_Q_IS_GIVEN_4F37C0B5 =
      "要导出的表（可选）。指定 -q 时忽略。";
  public static final String
      MESSAGE_DATABASE_TO_EXPORT_REQUIRED_WITHOUT_TABLE_OR_Q_EXPORTS_ALL_ITS_TABLES_704F6B55 =
          "要导出的数据库（必填）。不带 -table 或 -q 时导出其中所有表。";
  public static final String MESSAGE_MAXIMUM_DATA_ROWS_PER_CSV_OR_SQL_FILE_DEFAULT_10000_000CEFB9 =
      "每个 CSV 或 SQL 文件的最大数据行数（默认 10000）。";
  public static final String EXCEPTION_FILE_PREFIX_MUST_NOT_CONTAIN_PATH_SEPARATORS_ARG_C89823EE =
      "文件名前缀不能包含路径分隔符：%s";
  public static final String EXCEPTION_INVALID_OUTPUT_TIME_FORMAT_ARG_9C690675 =
      "输出时间格式无效：%s";
  public static final String EXCEPTION_EXPORTING_INFORMATION_SCHEMA_IS_NOT_SUPPORTED_6FD0FB85 =
      "不支持导出 information_schema。";
  public static final String EXCEPTION_THE_EXPORT_LOG_MUST_DIFFER_FROM_THE_MEASUREMENT_INPUT_FILE_088A542C =
      "导出日志不能与测点输入文件相同。";
  public static final String LOG_PLEASE_INPUT_YOUR_PASSWORD_AEE49DC5 =
      "请输入密码：";
  public static final String LOG_PLEASE_INPUT_YOUR_TRUST_STORE_4C50D824 =
      "请输入 trust_store：";
  public static final String LOG_PLEASE_INPUT_YOUR_TRUST_STORE_PASSWORD_86ED5CB6 =
      "请输入 trust_store_password：";
  public static final String LOG_PLEASE_INPUT_YOUR_KEY_STORE_AC5DB91F =
      "请输入 key_store：";
  public static final String LOG_PLEASE_INPUT_YOUR_KEY_STORE_PASSWORD_19ED16D4 =
      "请输入 key_store_password：";
  public static final String EXCEPTION_UNSUPPORTED_SQL_DIALECT_ARG_C4D0D97C =
      "不支持的 sql_dialect：%s";
  public static final String EXCEPTION_SPECIFY_AN_EXPORT_FORMAT_CSV_SQL_OR_TSFILE_RECEIVED_ARG_5B5211B5 =
      "请指定导出格式 csv、sql 或 tsfile（收到：%s）。";
  public static final String EXCEPTION_MEASUREMENT_FILE_IS_SUPPORTED_ONLY_IN_THE_TREE_MODEL_1ED604B5 =
      "-measurement_file 仅支持 Tree 模型。";
  public static final String EXCEPTION_MEASUREMENT_FILE_AND_Q_ARE_MUTUALLY_EXCLUSIVE_7A7457D6 =
      "-measurement_file 和 -q 互斥。";
  public static final String EXCEPTION_UNEXPECTED_POSITIONAL_ARGUMENTS_ARG_027F44CB =
      "不支持的位置参数：%s";
  public static final String EXCEPTION_INVALID_POSITIVE_INTEGER_FOR_OPTION_ARG_ARG_30FB488D =
      "选项 %s 的正整数值无效：%s";
  public static final String EXCEPTION_OPTION_ARG_REQUIRES_TRUE_OR_FALSE_ARG_0DC9BE3D =
      "选项 %s 必须为 true 或 false：%s";
  public static final String EXCEPTION_START_TIME_MUST_BE_LESS_THAN_OR_EQUAL_TO_END_TIME_3551199C =
      "start_time 必须小于或等于 end_time。";
  public static final String EXCEPTION_CANNOT_PARSE_TIME_ARG_ARG_40257DF7 =
      "无法解析时间 %s：%s";
  public static final String EXCEPTION_MEASUREMENT_INPUT_MUST_BE_A_READABLE_REGULAR_FILE_ARG_43D4A272 =
      "测点输入必须是可读的普通文件：%s";
  public static final String EXCEPTION_A_COMPLETE_IOTDB_PATH_PATTERN_STARTING_WITH_ROOT_IS_REQUIRED_7D000A79 =
      "必须提供以 root 开头的完整 IoTDB 路径 pattern。";
  public static final String EXCEPTION_INVALID_MEASUREMENT_INPUT_ARG_AT_LINE_ARG_CONTENT_ARG_ARG_27A0F948 =
      "测点输入 %s 的第 %d 行无效，原文=%s：%s";
  public static final String EXCEPTION_CANNOT_READ_UTF_8_MEASUREMENT_FILE_ARG_AT_LINE_ARG_ARG_A5739D3F =
      "无法读取 UTF-8 测点文件 %s 的第 %d 行：%s";
  public static final String EXCEPTION_MEASUREMENT_INPUT_CONTAINS_NO_PATH_PATTERNS_ARG_96760070 =
      "测点输入没有有效路径 pattern：%s";
  public static final String EXCEPTION_MEASUREMENT_OUTPUT_FILENAME_EXCEEDS_THE_SUPPORTED_LENGTH_ARG_FF01275D =
      "测点输出文件名超过支持的长度：%s";
  public static final String EXCEPTION_MEASUREMENTS_ARG_AND_ARG_MAP_TO_THE_SAME_OUTPUT_FILENAME_ARG_58E1FD4D =
      "测点 %s 和 %s 对应同一个输出文件名：%s";
  public static final String EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_LOG_FILE_ARG_F01BE79D =
      "导出输出与日志文件冲突：%s";
  public static final String LOG_EXPORT_STARTED_45465087 =
      "导出已开始。";
  public static final String LOG_EXPORT_COMPLETELY_34200D19 =
      "导出完成！";
  public static final String EXCEPTION_EXPORT_CANCELLED_85D44F54 =
      "导出已取消。";
  public static final String LOG_EXPORT_FAILED_77EE72CD =
      "导出失败。";
  public static final String LOG_CANNOT_WRITE_OR_CLOSE_THE_EXPORT_LOG_32579AC6 =
      "无法写入或关闭导出日志。";
  public static final String LOG_EXPORT_STARTED_ARG_ARG_TARGET_ARG_LOG_ARG_BD58AECB =
      "导出已开始（%s，%s）：输出目录 %s，日志 %s";
  public static final String LOG_MEASUREMENTS_TO_EXPORT_ARG_PATH_PATTERNS_ARG_A0AC5F31 =
      "待导出测点：%d 个（路径 pattern：%d 个）。";
  public static final String LOG_EXPORTING_ARG_ARG_ARG_ARG_ROWS_ARG_ETA_ARG_C41CA790 =
      "导出中 %s  %d/%d  %s 行  %s  剩余 %s";
  public static final String LOG_ARG_ARG_ARG_MEASUREMENTS_ARG_ROWS_ELAPSED_ARG_ETA_ARG_CURRENT_ARG_B6DEA0BD =
      "[%s] %d/%d 个测点，%s 行，已用 %s，剩余 %s，当前 %s";
  public static final String LOG_EXPORTING_ARG_ROWS_ARG_ARG_ROWS_S_56BD441C =
      "导出中 %s 行  %s  %s 行/秒";
  public static final String LOG_ARG_ROWS_ELAPSED_ARG_ARG_ROWS_S_AE3CF660 =
      "%s 行，已用 %s，%s 行/秒";
  public static final String LOG_WARNING_ARG_05BE32E9 =
      "警告：%s";
  public static final String LOG_EXPORT_COMPLETELY_MEASUREMENTS_ARG_ARG_ROWS_ARG_FILES_ARG_ELAPSED_ARG_36D9F8E5 =
      "导出完成！测点：%d/%d，行数：%s，文件：%s 个，耗时：%s。";
  public static final String LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B =
      "导出完成！行数：%s，文件：%s 个，耗时：%s。";
  public static final String LOG_EXPORT_FAILED_AFTER_ARG_ARG_C123F0EF =
      "导出失败（已用 %s）：%s";
  public static final String LOG_EXPORT_FAILED_AFTER_ARG_DA6D56F9 =
      "导出失败（已用 %s）。";
  public static final String LOG_EXPORT_CANCELLED_AFTER_ARG_ROWS_EXPORTED_ARG_0999F99F =
      "导出已取消（已用 %s），已导出 %s 行。";
  public static final String LOG_LOG_ARG_FBF61F63 =
      "日志：%s";
  public static final String LOG_ARG_82AC62F7 =
      "%.2f%%";
  public static final String EXCEPTION_AGGREGATE_QUERIES_CANNOT_BE_EXPORTED_AS_SQL_OR_TSFILE_48BC1932 =
      "聚合查询不能导出为 SQL 或 TsFile。";
  public static final String LOG_NO_MEASUREMENTS_MATCHED_ARG_MEASUREMENT_FILE_LINE_ARG_94C4EF10 =
      "%s 未匹配到任何测点（测点文件第 %d 行）。";
  public static final String LOG_START_TIME_AND_END_TIME_DO_NOT_APPLY_TO_QUERIES_AND_ARE_IGNORED_02DC37CC =
      "-start_time 和 -end_time 对查询不生效，已忽略。";
  public static final String LOG_TABLE_DOES_NOT_APPLY_TO_QUERIES_AND_IS_IGNORED_DBA31209 =
      "-table 对查询不生效，已忽略。";
  public static final String EXCEPTION_NO_MEASUREMENTS_REMAIN_AFTER_PATTERN_EXPANSION_AND_DEDUPLICATION_40D930B7 =
      "pattern 展开并去重后没有实际测点。";
  public static final String EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_80E096EB =
      "目标数据库 %s 不存在。";
  public static final String EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_E5F88A98 =
      "没有可导出的表，或目标表 %s 不存在。";
  public static final String LOG_PLEASE_INPUT_QUERY_EEAD5433 =
      "请输入查询：";
  public static final String EXCEPTION_NO_EXPORT_QUERY_WAS_PROVIDED_09E24A92 =
      "没有提供导出查询。";
  public static final String LOG_EXPORT_FAILED_ARG_57F9420F =
      "导出失败：%s";
  public static final String EXCEPTION_CANNOT_OBTAIN_SCHEMA_FOR_MEASUREMENT_ARG_D3F1DAF3 =
      "无法获取测点 %s 的 schema。";
  public static final String
      EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_COLUMNS_OF_TABLE_ARG_14B7FB67 =
          "SQL/TsFile 输出需要 time 列以及表 %s 中的列。";
  public static final String
      EXCEPTION_SQL_TSFILE_EXPORT_CAN_ONLY_SELECT_OR_COLUMN_NAMES_WITHOUT_FILL_EC83126D =
          "SQL/TsFile 导出只能选择 * 或列名，且不能使用 FILL。";
  public static final String
      EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_A_FIELD_COLUMN_FROM_TABLE_ARG_1C08B044 =
          "SQL/TsFile 输出需要表 %s 的至少一个 FIELD 列。";
  public static final String
      EXCEPTION_TSFILE_OUTPUT_REQUIRES_EVERY_TAG_COLUMN_FROM_TABLE_ARG_901F420F =
          "TsFile 输出需要表 %s 的全部 TAG 列。";
  public static final String EXCEPTION_SQL_OUTPUT_REQUIRES_ORIGINAL_MEASUREMENT_PATHS_OR_AN_ALIGN_BY_DEVICE_QUERY_ADA72EA8 =
      "SQL 输出需要原始测点路径或 ALIGN BY DEVICE 查询。";




  public static final String EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_MEASUREMENT_INPUT_FILE_ARG_1CBEBB6C =
      "导出输出与测点输入文件冲突：%s";
  public static final String EXCEPTION_TREE_TSFILE_OUTPUT_REQUIRES_A_TIME_COLUMN_AND_ORIGINAL_MEASUREMENT_PATHS_1DCEE8EA =
      "Tree TsFile 输出需要 time 列及原始测点路径。";

}
