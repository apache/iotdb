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

  public static final String LOG_UNKNOWN_696B0310 = "未知";

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

  public static final String EXCEPTION_ARGS_ARGS_ARG_C39A089A = "参数：%s";
  public static final String EXCEPTION_ARGS_ERROR_ARG_E49DCA27 = "参数错误：%s";
  public static final String EXCEPTION_ARG_CSV_FILE_READ_EXCEPTION_BECAUSE_ARG_4F31CBC1 =
      "%s : 读取 CSV 文件异常，原因是：%s";
  public static final String EXCEPTION_CANNOT_DUMP_FAIL_RESULT_BECAUSE_ARG_5C6E82E0 =
      "无法导出失败记录，原因是：%s";
  public static final String EXCEPTION_CANNOT_DUMP_RESULT_BECAUSE_ARG_F78D917B = "无法导出结果，原因是：%s";
  public static final String EXCEPTION_ENCOUNTER_AN_ERROR_BECAUSE_ARG_484F4B80 = "发生错误，原因是 %s";
  public static final String EXCEPTION_ENCOUNTER_AN_ERROR_BECAUSE_ARG_49284363 = "发生错误，原因是：%s";
  public static final String EXCEPTION_IMPORTDATA_THREAD_JOIN_INTERRUPTED_ARG_B94FC04B =
      "等待 ImportData 线程结束时被中断：%s";
  public static final String EXCEPTION_PARSE_ERROR_ARG_DA103556 = "解析错误：%s";
  public static final String EXCEPTION_SQL_FILE_READ_EXCEPTION_BECAUSE_ARG_1626B867 =
      "读取 SQL 文件异常，原因是：%s";
  public static final String EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331 = "发生意外错误：%s";
  public static final String MESSAGE_ARG_EMPTY_FILE_4366EB0C = "%s : 空文件！";
  public static final String MESSAGE_ARG_IMPORT_COMPLETELY_FAIL_9B6F42FD = "%s : 导入全部失败！";
  public static final String MESSAGE_ARG_IMPORT_COMPLETELY_SUCCESSFUL_BB17250E = "%s : 导入全部成功！";
  public static final String MESSAGE_ARG_NO_RECORDS_5672039C = "%s : 没有记录！";
  public static final String MESSAGE_ARG_PLEASE_INPUT_PATH_PATTERN_D784AEFD = "%s> 请输入路径模式： ";
  public static final String
      MESSAGE_ARG_THE_FILE_NAME_MUST_END_WITH_CSV_WHEN_SQL_DIALECT_TREE_OR_SQL_WHEN_SQL_DIALECT_TABLE_7A2FD49A =
          "%s : sql_dialect 为 tree 时文件名必须以 \"csv\" 结尾，为 table 时必须以 \"sql\" 结尾！";
  public static final String MESSAGE_DOES_NOT_SUPPORT_EXPORTING_SYSTEM_DATABASES_ARG_D86F9100 =
      "不支持导出系统数据库 %s";
  public static final String MESSAGE_EXPORT_SCHEMA_FAIL_ARG_75FE5946 = "导出元数据失败：%s";
  public static final String MESSAGE_IMPORT_SCHEMA_FAIL_ARG_609A6D30 = "导入元数据失败：%s";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_COMPRESSIONTYPE_N_8E111BB4 =
      "行 '%s'，列 '%s'：未知的 compressionType '%s' %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_DATATYPE_N_81E56571 =
      "行 '%s'，列 '%s'：未知的 dataType '%s' %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_ENCODINGTYPE_N_066E2D0D =
      "行 '%s'，列 '%s'：未知的 encodingType '%s' %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ILLEGAL_PATH_ARG_D5C1EBD6 =
      "行 '%s'，列 '%s'：路径 %s 不合法";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_F2FBBF40 = "请输入 key_store：";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_PWD_D18797A4 =
      "请输入 key_store_pwd：";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_PASSWORD_72B23434 = "请输入密码：";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_387CF139 = "请输入 trust_store：";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_PWD_F402D8D9 =
      "请输入 trust_store_pwd：";
  public static final String MESSAGE_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_86467CA2 =
      "未提供选项 '%s' 所需的值";
  public static final String MESSAGE_SOURCE_FILE_OR_DIRECTORY_ARG_DOES_NOT_EXIST_0299DF95 =
      "源文件或目录 %s 不存在";
  public static final String
      MESSAGE_THE_DATABASE_PARAM_IS_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_1EFE6DAF =
          "sql_dialect 为 table 时必须提供 database 参数 ";
  public static final String
      MESSAGE_THE_HEADER_OF_THE_CSV_FILE_TO_BE_IMPORTED_IS_ILLEGAL_THE_CORRECT_FORMAT_IS_TIMESERIES_ALIBABA_DATATYPE_ENCODING_COMPRESSION_C429E7C6 =
          "待导入的 CSV 文件表头不合法，正确格式为 \"Timeseries, Alibaba, DataType, Encoding, Compression\"！";

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

  public static final String LOG_CONNECT_IS_ABORT_TRY_TO_RECONNECT_MAX_RETRY_COUNT_ARG_EBF7D348 =
      "连接中断，尝试重新连接，最大重试次数：%s";

  public static final String LOG_THE_ARG_TH_RETRY_WILL_AFTER_ARG_SECONDS_F4AECD33 =
      "第 %s 次重试将在 %s 秒后进行。";

  public static final String LOG_RECONNECT_SUCCESSFUL_B741C2DE = "重新连接成功。";

  public static final String LOG_THE_ARG_TH_RECONNECT_FAILED_F5DEEF49 = "第 %s 次重新连接失败";

  public static final String
      LOG_HANDSHAKE_ERROR_WITH_TARGET_SERVER_IP_ARG_PORT_ARG_BECAUSE_ARG_RETRY_TO_HANDSHAKE_BY_PIPETRANSFERHANDSHAKEV1REQ_42529EF7 =
          "与目标服务器握手失败，IP：%s，端口：%s，原因：%s。使用 PipeTransferHandshakeV1Req 重试握手。";

  public static final String LOG_HANDSHAKE_SUCCESS_TARGET_SERVER_IP_ARG_PORT_ARG_8FA76B58 =
      "握手成功。目标服务器 IP：%s，端口：%s";

  public static final String LOG_SUCCESSFULLY_TRANSFERRED_FILE_ARG_303718B9 = "文件传输成功：%s";

  public static final String LOG_REDIRECT_FILE_POSITION_TO_ARG_2690B376 = "将文件位置重定向到 %s。";

  public static final String LOG_FAILED_TO_CLOSE_CLIENT_BECAUSE_ARG_8EBA5673 = "关闭客户端失败，原因：%s";

  public static final String
      LOG_REJECTING_FILE_ARG_DUE_TO_MEMORY_CONSTRAINTS_WILL_RETRY_LATER_AE83A15F =
          "因内存限制拒绝文件 [ %s ]，稍后重试。";

  public static final String LOG_FAILED_TO_IMPORT_ARG_FILE_ARG_F085D907 = "导入文件 [ %s ] 失败：%s";

  public static final String LOG_PROCESSED_FAIL_FILE_ARG_SUCCESSFULLY_B430F7F0 =
      "成功处理导入失败的文件 [ %s ]！";

  public static final String LOG_FAILED_TO_PROCESS_FAIL_FILE_ARG_ARG_D01A0F75 =
      "处理导入失败的文件 [ %s ] 失败：%s";

  public static final String LOG_IMPORTED_ARG_FILE_SUCCESSFULLY_B323C7DC = "文件 [ %s ] 导入成功！";

  public static final String LOG_PROCESSED_SUCCESS_FILE_ARG_SUCCESSFULLY_B2CCF2E5 =
      "成功处理导入成功的文件 [ %s ]！";

  public static final String LOG_FAILED_TO_PROCESS_SUCCESS_FILE_ARG_ARG_8E0E4B9C =
      "处理导入成功的文件 [ %s ] 失败：%s";

  public static final String
      LOG_SUCCESSFULLY_LOAD_ARG_TSFILE_S_ON_SUCCESS_OPERATION_S_ARG_SUCCEED_ARG_FAILED_A04B748F =
          "成功加载 %s 个 TsFile（--on_success 操作：%s 次成功，%s 次失败）";

  public static final String
      LOG_FAILED_TO_LOAD_ARG_FILE_S_ON_FAIL_OPERATION_S_ARG_SUCCEED_ARG_FAILED_F5B202E2 =
          "加载失败 %s 个文件（--on_fail 操作：%s 次成功，%s 次失败）";

  public static final String LOG_UNPROCESSED_ARG_FILE_S_DUE_TO_UNEXPECTED_EXCEPTIONS_0950B97D =
      "因意外异常未处理 %s 个文件";

  public static final String LOG_FOR_MORE_DETAILS_PLEASE_CHECK_THE_LOG_D0597775 = "更多详情请查看日志。";

  public static final String LOG_TOTAL_OPERATION_TIME_ARG_MS_D8632871 = "总操作耗时：%s ms。";

  public static final String LOG_WORK_HAS_BEEN_COMPLETED_B4F48618 = "操作已完成！";

  public static final String LOG_TOO_FEW_ARGUMENTS_PLEASE_CHECK_THE_FOLLOWING_HINT_7EB254F2 =
      "参数不足，请查看以下提示。";

  public static final String LOG_FAILED_TO_PARSE_THE_PROVIDED_OPTIONS_ARG_5E552330 =
      "解析所提供的选项失败：%s";

  public static final String LOG_ENCOUNTER_AN_ERROR_WHEN_PARSING_THE_PROVIDED_OPTIONS_ARG_D74C6F72 =
      "解析所提供的选项时发生错误：%s";

  public static final String LOG_LOAD_REMOTELY_87E711F6 = "使用远程加载。";

  public static final String LOG_LOAD_LOCALLY_F1E20946 = "使用本地加载。";

  public static final String
      LOG_ENCOUNTER_AN_ERROR_WHEN_GET_IOTDB_SERVER_TIMESTAMPPRECISION_ARG_DECCC828 =
          "获取 IoTDB 服务器 timestampPrecision 时发生错误：%s";

  public static final String
      LOG_ENCOUNTER_AN_ERROR_THE_TIME_ACCURACY_OF_THE_IOTDB_SERVER_IS_ARG_BUT_THE_CLIENT_TIME_ACCURACY_IS_ARG_8A881838 =
          "发生错误：IoTDB 服务器的时间精度为 \"%s\"，客户端的时间精度为 %s";

  public static final String LOG_ARGS_ERROR_OS_OF_MUST_BE_ONE_OF_NONE_MV_CP_DELETE_6E478D75 =
      "参数错误：os/of 必须为 none、mv、cp、delete 之一";

  public static final String LOG_UNKNOWN_HOST_ARG_EXCEPTION_ARG_WILL_USE_REMOTE_LOAD_7E2F7E65 =
      "未知主机：%s。异常：%s。将使用远程加载。";

  public static final String LOG_IOEXCEPTION_WHEN_CHECKING_FILE_STORE_ARG_B5302D40 =
      "检查文件存储时发生 IOException：%s";

  public static final String LOG_FAILED_TO_CREATE_ARG_ARG_3A4A1CFA = "创建 %s %s 失败";

  public static final String LOG_IMPORT_TSFILE_FAIL_ARG_6A83990E = "导入 TsFile 失败：%s";

  public static final String LOG_IMPORTTSFILE_THREAD_JOIN_INTERRUPTED_ARG_E4141B99 =
      "等待 ImportTsFile 线程结束时被中断：%s";

  public static final String LOG_EXPORT_TSFILE_COUNT_ARG_C216A69B = "导出的 TsFile 数量：%s";

  public static final String LOG_EMPTY_FILE_83778141 = "空文件！";

  public static final String LOG_THE_FIRST_FIELD_OF_HEADER_MUST_BE_TIME_F48E3999 =
      "表头的第一个字段必须为 `Time`！";

  public static final String LOG_CSV_FILE_READ_EXCEPTION_BECAUSE_ARG_5EE7AFBF = "读取 CSV 文件异常，原因：%s";

  public static final String LOG_THE_FILE_NAME_MUST_END_WITH_CSV_OR_TXT_0C020AB3 =
      "文件名必须以 \"csv\" 或 \"txt\" 结尾！";

  public static final String LOG_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_TYPE_ARG_93D3B31D =
      "第 '%s' 行，第 '%s' 列：'%s' 类型未知%n";

  public static final String LOG_LINE_ARG_COLUMN_ARG_ARG_CAN_T_CONVERT_TO_ARG_ARG_F85541A2 =
      "第 '%s' 行，第 '%s' 列：'%s' 无法转换为 '%s'%n";

  public static final String LOG_MEET_ERROR_WHEN_QUERY_THE_TYPE_OF_TIMESERIES_BECAUSE_ARG_21949119 =
      "查询时间序列类型时发生错误，原因：%s";

  public static final String
      LOG_THE_EXTRACTED_DATABASE_ARG_IN_SQL_STATEMENT_DOES_NOT_MATCH_THE_TARGET_DATABASE_ARG_2A442AEB =
          "从 SQL 语句中提取的数据库 '%s' 与目标数据库 '%s' 不匹配";

  public static final String LOG_THE_FIRST_FIELD_OF_HEADER_MUST_BE_TIME_2D6FAABE =
      "表头的第一个字段必须为 `time`！";

  public static final String
      LOG_THESE_COLUMNS_DO_NOT_MATCH_THE_COLUMN_TYPES_IN_THE_TARGET_TABLE_ARG_FC76A705 =
          "以下列与目标表中的列类型不匹配：%s";

  public static final String LOG_SQL_DIALECT_ARG_IS_NOT_SUPPORT_5301233A = "不支持 sql_dialect %s";

  public static final String LOG_FILE_TYPE_ARG_IS_NOT_SUPPORT_D210E109 = "不支持文件类型 %s";

  public static final String
      LOG_ERROR_INVALID_THREAD_NUMBER_ARG_PLEASE_SET_A_POSITIVE_INTEGER_AFD679CF =
          "错误：线程数 '%s' 无效。请设置为正整数。";

  public static final String
      LOG_HOST_ARG_IS_REMOTE_LOAD_ONLY_LOCAL_LOAD_IS_SUPPORTED_IN_TABLE_MODEL_AB4AEC31 =
          "主机 %s 需要远程加载，表模型仅支持本地加载";

  public static final String LOG_UNKNOWN_HOST_ARG_EXCEPTION_ARG_WILL_USE_LOCAL_LOAD_28E4A226 =
      "未知主机：%s。异常：%s。将使用本地加载。";

  public static final String LOG_FILE_NOT_FOUND_D5ED74DC = "文件不存在！";

  public static final String LOG_ENCOUNTER_AN_ERROR_WHEN_CONNECTING_TO_SERVER_BECAUSE_ARG_68D0B497 =
      "连接服务器时发生错误，原因：%s";

  public static final String LOG_ARG_IMPORT_COMPLETELY_790B9524 = "%s 导入完成！";

  public static final String LOG_IMPORT_COMPLETELY_B7451B2C = "导入完成！";

  public static final String LOG_NO_RECORDS_05AB97C4 = "没有记录！";

  public static final String EXCEPTION_EMPTY_SET_A4A8C90D = "空结果集。";

  public static final String EXCEPTION_TOTAL_LINE_NUMBER_ARG_43CC83F4 = "总行数 = %s";

  public static final String
      LOG_REQUIRE_MORE_PARAMS_INPUT_EG_START_CLI_SH_START_CLI_BAT_IF_WINDOWS_H_XXX_XXX_XXX_XXX_P_XXXX_U_XXX_AE58F13C =
          "请输入更多参数，例如 ./start-cli.sh（Windows 下为 start-cli.bat）-h xxx.xxx.xxx.xxx -p xxxx -u xxx。";

  public static final String LOG_FOR_MORE_INFORMATION_PLEASE_CHECK_THE_FOLLOWING_HINT_85DAC74B =
      "更多信息请查看以下提示。";

  public static final String LOG_ARG_INPUT_PARAMS_ERROR_BECAUSE_ARG_4CA51653 = "%s：输入参数错误，原因：%s";

  public static final String LOG_ARG_EXIT_CLI_WITH_ERROR_ARG_4BAF4EB2 = "%s：CLI 因错误退出：%s";

  public static final String
      LOG_ARG_ERROR_FORMAT_OF_MAX_PRINT_ROW_COUNT_IT_SHOULD_BE_AN_INTEGER_NUMBER_F7E9B25D =
          "%s：最大打印行数格式错误，应为整数";

  public static final String LOG_ARG_CAN_T_EXECUTE_SQL_BECAUSE_ARG_9C05D8B0 = "%s：无法执行 SQL，原因：%s";

  public static final String EXCEPTION_ARG_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_1593074C =
      "%s：未提供选项 '%s' 的必需值";

  public static final String EXCEPTION_ARG_REQUIRED_VALUES_FOR_OPTION_ARG_IS_NULL_E4215AA7 =
      "%s：选项 '%s' 的必需值为 null。";

  public static final String LOG_FETCH_SIZE_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_10000_F43DF137 =
      "读取行数格式错误，请按 %s=10000 的格式输入";

  public static final String LOG_FETCH_SIZE_FORMAT_ERROR_ARG_C2A64235 = "读取行数格式错误，%s";

  public static final String LOG_FETCH_SIZE_HAS_SET_TO_ARG_1EC2519D = "读取行数已设置为 %s";

  public static final String LOG_ARG_VERSION_ARG_BUILD_ARG_73F1169F =
      "%s版本 %s（构建：%s）\n                                           \n";

  public static final String LOG_STARTING_IOTDB_CLI_D08E81B1 = "正在启动 IoTDB CLI";

  public static final String LOG_CURRENT_TIME_FORMAT_ARG_4AF56328 = "当前时间格式：%s";

  public static final String LOG_CURRENT_FETCH_SIZE_ARG_D0149220 = "当前读取行数：%s";

  public static final String LOG_YOUR_SQL_EXECUTE_YOUR_SQL_STATMENT_BFC89C6C =
      "    <your-sql>\t\t\t 执行 SQL 语句";

  public static final String LOG_ARG_SHOW_HOW_MANY_TIMESERIES_ARE_IN_IOTDB_359C5CCB =
      "    %s\t\t 显示 IoTDB 中的时间序列数量";

  public static final String LOG_ARG_XXX_EG_LONG_DEFAULT_ISO8601_YYYY_MM_DD_HH_MM_SS_AB86BCB9 =
      "    %s=xxx\t 例如 long、default、ISO8601、yyyy-MM-dd HH:mm:ss。";

  public static final String LOG_ARG_SHOW_TIME_DISPLAY_TYPE_F4FDBA29 = "    %s\t 显示时间显示类型";

  public static final String LOG_ARG_XXX_EG_08_00_ASIA_SHANGHAI_C03D8EBF =
      "    %s=xxx\t\t 例如 +08:00、Asia/Shanghai。";

  public static final String LOG_ARG_SHOW_CLI_TIME_ZONE_9F84BADC = "    %s\t\t 显示 CLI 时区";

  public static final String LOG_ARG_XXX_SET_FETCH_SIZE_WHEN_QUERYING_DATA_FROM_SERVER_4B58C233 =
      "    %s=xxx\t\t 设置从服务器查询数据时的读取行数。";

  public static final String LOG_ARG_SHOW_FETCH_SIZE_58D138A1 = "    %s\t\t 显示读取行数";

  public static final String LOG_TIME_DISPLAY_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_ISO8601_A0F723FB =
      "时间显示格式错误，请按 %s=ISO8601 的格式输入";

  public static final String LOG_TIME_DISPLAY_FORMAT_ERROR_ARG_2BE8FA7D = "时间显示格式错误，%s";

  public static final String LOG_TIME_DISPLAY_TYPE_HAS_SET_TO_ARG_1E0B2163 = "时间显示类型已设置为 %s";

  public static final String LOG_TIME_ZONE_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_08_00_26C955E6 =
      "时区格式错误，请按 %s=+08:00 的格式输入";

  public static final String LOG_TIME_ZONE_FORMAT_ERROR_ARG_F356979E = "时区格式错误：%s";

  public static final String LOG_TIME_ZONE_HAS_SET_TO_ARG_4D07E82F = "时区已设置为 %s";

  public static final String LOG_CURRENT_TIME_ZONE_ARG_A0221687 = "当前时区：%s";

  public static final String LOG_CANNOT_GET_TIME_ZONE_FROM_SERVER_SIDE_BECAUSE_ARG_E3FE596C =
      "无法从服务器获取时区，原因：%s";

  public static final String
      LOG_PLEASE_INPUT_LIKE_IMPORT_USER_MYFILE_NOTED_THAT_YOUR_FILE_PATH_CANNOT_CONTAIN_ANY_SPACE_CHARACTER_F0E1F5B8 =
          "请按以下格式输入：import /User/myfile。请注意，文件路径不能包含空格字符）";

  public static final String LOG_IT_COSTS_ARG_S_B7D08F3A = "耗时 %.3fs";

  public static final String
      LOG_THIS_DISPLAY_1000_ROWS_PRESS_ENTER_TO_SHOW_MORE_INPUT_Q_TO_QUIT_F674ED97 =
          "已显示 1000 行。按 ENTER 显示更多，输入 'q' 退出。";

  public static final String LOG_MSG_ARG_240992E0 = "消息：%s";

  public static final String LOG_TRACING_ACTIVITIES_CE1F6C12 = "跟踪活动：";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_5DC07992 = "时间戳精度（ms/us/ns）";

  public static final String
      EXCEPTION_THE_SOURCE_FILE_OR_DIRECTORY_PATH_WHICH_CAN_BE_A_TSFILE_OR_A_DIRECTORY_CONTAINING_TSFILES_REQUIRED_5C5AC0CC =
          "源文件或目录路径，可以是 TsFile 或包含 TsFile 的目录。（必需）";

  public static final String
      EXCEPTION_WHEN_LOADING_TSFILE_SUCCESSFULLY_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_601E75DC =
          "成功加载 TsFile 后，对 TsFile（及其 .resource 和 .mods 文件）执行的操作，可选值为 none、mv、cp、delete。（必需）";

  public static final String
      EXCEPTION_WHEN_LOADING_TSFILE_FAIL_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_7BA55A84 =
          "加载 TsFile 失败后，对 TsFile（及其 .resource 和 .mods 文件）执行的操作，可选值为 none、mv、cp、delete。（必需）";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OS_IS_MV_OR_CP_C2F33AE4 =
      "'os' 为 'mv' 或 'cp' 时的目标文件夹。";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OF_IS_MV_OR_CP_6F43F81D =
      "'of' 为 'mv' 或 'cp' 时的目标文件夹。";

  public static final String
      EXCEPTION_THE_NUMBER_OF_THREADS_USED_TO_IMPORT_TSFILE_DEFAULT_IS_8_4623E27F =
          "导入 TsFile 使用的线程数，默认为 8。";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_OPTIONAL_E312B661 =
      "时间戳精度（ms/us/ns）（可选）";

  public static final String EXCEPTION_DISPLAY_HELP_INFORMATION_3BBD4899 = "显示帮助信息";

  public static final String EXCEPTION_HOST_NAME_OPTIONAL_FD001BC0 = "主机名（可选）";

  public static final String EXCEPTION_USE_SSL_STATEMENT_OPTIONAL_92E1AA92 = "使用 SSL。（可选）";

  public static final String EXCEPTION_TRUST_STORE_OPTIONAL_EA106F8E = "信任库。（可选）";

  public static final String EXCEPTION_TRUST_STORE_PASSWORD_OPTIONAL_B017719D = "信任库密码。（可选）";

  public static final String EXCEPTION_KEY_STORE_FOR_MUTUAL_SSL_OPTIONAL_A617339F =
      "双向 SSL 的密钥库。（可选）";

  public static final String EXCEPTION_KEY_STORE_PASSWORD_FOR_MUTUAL_SSL_OPTIONAL_CA3006DD =
      "双向 SSL 的密钥库密码。（可选）";

  public static final String EXCEPTION_SSL_PROTOCOL_OPTIONAL_F7C84E29 = "SSL 协议。（可选）";

  public static final String
      EXCEPTION_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_046E0304 =
          "文件类型，可选 tsfile、csv 或 sql。（必需）";

  public static final String
      EXCEPTION_EXPORT_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_04770009 =
          "导出文件类型，可选 tsfile、csv 或 sql。（必需）";

  public static final String EXCEPTION_TYPES_OF_IMPORTED_FILES_CSV_SQL_TSFILE_REQUIRED_E2678FF9 =
      "导入文件类型：csv、sql、tsfile。（必需）";

  public static final String
      EXCEPTION_OUTPUT_TIME_FORMAT_IN_CSV_FILE_YOU_CAN_CHOOSE_1_TIMESTAMP_NUMBER_LONG_2_ISO8601_DEFAULT_3_USER_DEFINED_PATTERN_LIKE_YYYY_MM_DD_HH_MM_SS_DEFAULT_ISO8601_OUTPUT_TIMESTAMP_IN_SQL_FILE_NO_MATTER_WHAT_TIME_FORMAT_IS_SET_OPTIONAL_D766AEFB =
          "CSV 文件的输出时间格式。可选：1) timestamp、number、long；2) ISO8601、default；3) 自定义格式，例如 yyyy-MM-dd"
              + " HH:mm:ss，默认为 ISO8601。\n"
              + "无论设置何种时间格式，SQL 文件均输出时间戳（可选）";

  public static final String EXCEPTION_TIME_ZONE_EG_08_00_OR_01_00_OPTIONAL_7BEA75F1 =
      "时区，例如 +08:00 或 -01:00。（可选）";

  public static final String EXCEPTION_TIMEOUT_FOR_SESSION_QUERY_OPTIONAL_69034017 =
      "Session 查询超时时间。（可选）";

  public static final String EXCEPTION_EXPORT_ALIGNED_INSERT_SQL_EA4543D3 = "导出对齐的 INSERT SQL";

  public static final String EXCEPTION_USE_THE_ALIGNED_INTERFACE_F915187B = "使用对齐接口";

  public static final String EXCEPTION_WHETHER_EXPORT_TO_SQL_OF_ALIGNED_OPTIONAL_CF4C6EA0 =
      "是否导出对齐的 SQL。（可选）";

  public static final String EXCEPTION_WHETHER_TO_USE_THE_INTERFACE_OF_ALIGNED_OPTIONAL_440B01C4 =
      "是否使用对齐接口。（可选）";

  public static final String
      EXCEPTION_CURRENTLY_SUPPORTS_TREE_AND_TABLE_MODEL_DEFAULT_TREE_OPTIONAL_FB4F4486 =
          "当前支持 tree 和 table 模型，默认为 tree。（可选）";

  public static final String
      EXCEPTION_THE_DATABASE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_AND_REQUIRED_WHEN_FILE_TYPE_IS_CSV_AND_TSFILE_OPTIONAL_DCB4863E =
          "待导出的数据库，仅当 sql_dialect 为 table 时生效，file_type 为 csv 或 tsfile 时必填。（可选）";

  public static final String
      EXCEPTION_THE_TABLE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_OPTIONAL_718386C9 =
          "待导出的表，仅当 sql_dialect 为 table 时生效。（可选）";

  public static final String
      EXCEPTION_ARG_IF_THE_Q_PARAMETER_IS_SPECIFIED_THIS_PARAMETER_DOES_NOT_TAKE_EFFECT_IF_THE_EXPORT_TYPE_IS_TSFILE_OR_SQL_THIS_PARAMETER_IS_REQUIRED_OPTIONAL_8F65937E =
          "%s。如果指定了 '-q' 参数，则此参数不生效。导出类型为 tsfile 或 sql 时，此参数为必需。（可选）";

  public static final String EXCEPTION_ARG_AND_FILE_TYPE_IS_CSV_OPTIONAL_695FD8E8 =
      "%s 且 file_type 为 csv。（可选）";

  public static final String EXCEPTION_EXPORT_DATA_2F1FB3D9 = "导出数据";

  public static final String
      EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_EXPORT_DATA_SH_HELP_TSFILE_EXPORT_DATA_SH_HELP_SQL_EXPORT_DATA_SH_HELP_CSV_0D410E8A =
          "请通过不同参数获取相应数据类型的帮助信息，例如：\n"
              + "./export_data.sh -help tsfile\n"
              + "./export_data.sh -help sql\n"
              + "./export_data.sh -help csv";

  public static final String
      EXCEPTION_TOO_FEW_PARAMS_INPUT_PLEASE_CHECK_THE_FOLLOWING_HINT_E6D41E98 = "输入参数不足，请查看以下提示。";

  public static final String EXCEPTION_THE_START_TIME_TO_BE_EXPORTED_OPTIONAL_8133559A =
      "待导出的开始时间（可选）";

  public static final String EXCEPTION_THE_END_TIME_TO_BE_EXPORTED_OPTIONAL_355E448D =
      "待导出的结束时间。（可选）";

  public static final String EXCEPTION_TARGET_FILE_DIRECTORY_REQUIRED_9A407F06 = "目标文件目录（必需）";

  public static final String EXCEPTION_TARGET_FILE_DIRECTORY_DEFAULT_TARGET_OPTIONAL_52390574 =
      "目标文件目录，默认为 ./target（可选）";

  public static final String EXCEPTION_EXPORT_PATH_PATTERN_OPTIONAL_8E9C0DE8 = "导出路径模式（可选）";

  public static final String EXCEPTION_EXPORT_FILE_NAME_OPTIONAL_BBFDC3AE = "导出文件名。（可选）";

  public static final String
      EXCEPTION_THE_MAX_FRAME_SIZE_OF_RPC_DEFAULT_IS_536870912_BYTES_OPTIONAL_DDC009D1 =
          "RPC 最大帧大小，默认为 536870912 字节。（可选）";

  public static final String
      EXCEPTION_WILL_THE_DATA_TYPE_OF_TIMESERIES_BE_PRINTED_IN_THE_HEAD_LINE_OF_THE_CSV_FILE_ARG_YOU_CAN_CHOOSE_TRUE_OR_FALSE_OPTIONAL_D1409A80 =
          "是否在 CSV 文件表头中打印时间序列的数据类型？%s可选 true 或 false。（可选）";

  public static final String
      EXCEPTION_LINES_PER_DUMP_FILE_ONLY_EFFECTIVE_IN_TREE_MODEL_OPTIONAL_8138FC83 =
          "每个导出文件的行数，仅在树模型下生效。（可选）";

  public static final String
      EXCEPTION_EITHER_Q_OR_TABLE_IS_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_378EE68E =
          "'sql-dialect' 为 'table' 时，必须指定 '-q' 或 '-table'";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INSERT_CSV_BECAUSE_F4B95972 =
      "插入 CSV 时发生错误，原因：";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INSERT_SQL_BECAUSE_54810B8F =
      "插入 SQL 时发生错误，原因：";

  public static final String
      EXCEPTION_MEET_ERROR_WHEN_GET_TABLE_COLUMNS_INFORMATION_BECAUSE_3E210150 = "获取表列信息时发生错误，原因：";

  public static final String EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_342912D0 =
      "目标数据库 %s 不存在";

  public static final String
      EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_23BCB7AF =
          "没有表或目标表 %s 不存在";

  public static final String
      EXCEPTION_THERE_ARE_NO_TABLES_TO_EXPORT_PLEASE_CHECK_IF_THE_TABLES_IN_THE_TARGET_DATABASE_EXIST_AND_IF_YOU_HAVE_PERMISSION_TO_ACCESS_THEM_1592463E =
          "没有可导出的表。请检查目标数据库中是否存在表，以及是否具有访问权限。";

  public static final String
      EXCEPTION_THE_PATH_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TREE_OPTIONAL_99A39D5E =
          "待导出的路径，仅当 sql_dialect 为 tree 时生效。（可选）";

  public static final String EXCEPTION_EXPORT_TSFILE_201F2F41 = "导出 TsFile";

  public static final String EXCEPTION_IMPORT_DATA_446C4D18 = "导入数据";

  public static final String
      EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_IMPORT_DATA_SH_HELP_TSFILE_IMPORT_DATA_SH_HELP_SQL_IMPORT_DATA_SH_HELP_CSV_F9320898 =
          "请通过不同参数获取相应数据类型的帮助信息，例如：\n"
              + "./import_data.sh -help tsfile\n"
              + "./import_data.sh -help sql\n"
              + "./import_data.sh -help csv";

  public static final String
      EXCEPTION_THE_LOCAL_DIRECTORY_PATH_OF_THE_SCRIPT_FILE_FOLDER_TO_BE_LOADED_REQUIRED_AC5278C5 =
          "待加载的脚本文件（文件夹）的本地目录路径。（必需）";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_DADE5B55 =
          "用于保存失败文件的目录，默认为 YOUR_CSV_FILE_PATH（可选）";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OS_IS_MV_OR_CP_OPTIONAL_8AFE2264 =
      "'os' 为 'mv' 或 'cp' 时的目标文件夹。（可选）";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_EA6E7DF7 =
          "用于保存失败文件的目录，默认为 YOUR_CSV_FILE_PATH。（可选）";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_SQL_FILE_PATH_OPTIONAL_8CF7BBC4 =
          "用于保存失败文件的目录，默认为 YOUR_SQL_FILE_PATH。（可选）";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OF_IS_MV_OR_CP_OPTIONAL_E72A0E75 =
      "'of' 为 'mv' 或 'cp' 时的目标文件夹。（可选）";

  public static final String
      EXCEPTION_THE_NUMBER_OF_THREADS_USED_TO_IMPORT_TSFILE_DEFAULT_IS_8_OPTIONAL_EAAFD75C =
          "导入 TsFile 使用的线程数，默认为 8。（可选）";

  public static final String
      EXCEPTION_10000_ONLY_NOT_ALIGNED_AND_SQL_DIALECT_TREE_OPTIONAL_75B83039 =
          "10000（仅在非对齐且 sql_dialect 为 tree 时可选）";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_OPTIONAL_10C6809A =
      "时间戳精度（ms/us/ns）。（可选）";

  public static final String
      EXCEPTION_DEFINE_TYPE_INFO_BY_OPTION_BOOLEAN_TEXT_INT_LONG_OPTIONAL_CF9F51B3 =
          "通过选项定义类型信息：\"boolean=text,int=long, ...（可选）";

  public static final String
      EXCEPTION_LINES_PER_FAILED_FILE_ONLY_TAKES_EFFECT_AND_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_OPTION_55D8B932 =
          "每个失败文件的行数，仅当 sql_dialect 为 table 时生效且必需。（可选）";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INIT_IMPORT_BECAUSE_A38F65C6 =
      "初始化导入时发生错误，原因：";

  public static final String
      EXCEPTION_INVALID_ARGS_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_601B17E3 =
          "参数无效：未提供选项 '%s' 的必需值";

  public static final String
      EXCEPTION_PRESS_CTRL_C_AGAIN_TO_EXIT_OR_PRESS_ENTER_TO_CONTINUE_8CA49A92 =
          "再次按 CTRL+C 退出，或按 ENTER 继续";

  public static final String EXCEPTION_THE_STATEMENT_IS_EXECUTED_SUCCESSFULLY_B07F8CCB = "语句执行成功。";

  public static final String EXCEPTION_DISPLAY_HELP_INFORMATION_OPTIONAL_BF7EAC3D = "显示帮助信息。（可选）";

  public static final String EXCEPTION_DISPLAY_TIMESTAMP_IN_NUMERIC_FORMAT_OPTIONAL_74353C35 =
      "以数字格式显示时间戳。（可选）";

  public static final String EXCEPTION_HOST_NAME_DEFAULT_IS_127_0_0_1_OPTIONAL_487641A0 =
      "主机名，默认为 127.0.0.1。（可选）";

  public static final String EXCEPTION_PORT_DEFAULT_IS_6667_OPTIONAL_97563E4C = "端口，默认为 6667。（可选）";

  public static final String EXCEPTION_USER_NAME_REQUIRED_45948411 = "用户名。（必需）";

  public static final String EXCEPTION_PASSWORD_DEFAULT_IS_ROOT_OPTIONAL_E6E2197E =
      "密码，默认为 root。（可选）";

  public static final String EXCEPTION_ENABLE_OR_DISABLE_RPC_COMPRESSION_OPTIONAL_9F1F97AB =
      "启用或禁用 RPC 压缩。（可选）";

  public static final String
      EXCEPTION_THE_TIMEOUT_IN_SECONDS_USES_THE_SERVER_CONFIGURATION_IF_NOT_SET_OPTIONAL_49A917F7 =
          "超时时间，单位为秒。未设置时使用服务器配置。（可选）";

  public static final String
      EXCEPTION_CURRENTLY_SUPPORTS_TREE_AND_TABLE_USES_TREE_IF_NOT_SET_OPTIONAL_9D030956 =
          "当前支持 tree 和 table；未设置时使用 tree。（可选）";

  public static final String EXCEPTION_ELAPSED_TIME_7083D10C = "已耗时间";
}
