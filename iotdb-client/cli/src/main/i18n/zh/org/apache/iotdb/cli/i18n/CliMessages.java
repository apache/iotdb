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


  // Filesystem command execution
  public static final String MESSAGE_ARG_ARG_NO_SUCH_FILE_OR_DIRECTORY_ABDC5A9C = "%s：%s：文件或目录不存在";
  public static final String MESSAGE_ARG_ARG_NOT_A_DIRECTORY_CF18DCA5 = "%s：%s：不是目录";
  public static final String MESSAGE_ARG_ARG_READ_ONLY_FILE_SYSTEM_A86EB99C = "%s：%s：只读文件系统";
  public static final String MESSAGE_UNSUPPORTED_FILESYSTEM_COMMAND_ARG_428768D0 = "不支持的文件系统命令：%s";
  public static final String MESSAGE_FAILED_TO_WRITE_STANDARD_OUTPUT_C1A5CCF7 = "写入标准输出失败";
  public static final String MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E = "读取标准输入失败";
  public static final String MESSAGE_CANNOT_EXECUTE_FILESYSTEM_COMMAND_ARG_C61FAE4B = "无法执行文件系统命令：%s";
  public static final String MESSAGE_TEE_USE_WQ_TO_WRITE_OR_Q_TO_QUIT_WITHOUT_WRITING_C46EFD2C = "tee：使用 :wq 写入，或使用 :q! 放弃写入并退出";

  // Filesystem command validation
  public static final String MESSAGE_EMPTY_COMMAND_943E8DA9 = "命令为空";
  public static final String MESSAGE_UNCLOSED_QUOTE_OR_ESCAPE_IN_FILESYSTEM_COMMAND_42C74084 = "文件系统命令中的引号或转义未闭合";
  public static final String MESSAGE_UNKNOWN_COMMAND_ARG_00157142 = "未知命令：%s";
  public static final String MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5 = "请单独使用 help [command] 或 <command> --help，不要添加其他参数";
  public static final String MESSAGE_SQL_STATEMENT_IS_EMPTY_676FCD59 = "SQL 语句为空";
  public static final String EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D = "%s：不支持的选项：%s";
  public static final String EXCEPTION_ARG_OPTION_SPECIFIED_MORE_THAN_ONCE_ARG_CEB275DB = "%s：重复指定选项：%s";
  public static final String EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB = "%s：%s 缺少参数值";
  public static final String EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F = "%s：多余的参数：%s";
  public static final String EXCEPTION_ARG_EXPECTED_AT_LEAST_ARG_PATH_ARGUMENT_S_2040D496 = "%s：至少需要 %d 个路径参数";
  public static final String EXCEPTION_ARG_PATH_MUST_NOT_BE_EMPTY_FEC583BE = "%s：路径不能为空";
  public static final String EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04 = "%s：%s 的无符号整数无效：%s";
  public static final String EXCEPTION_INVALID_TREE_DEPTH_ARG_EF544DD4 = "无效的 tree 深度：%s";
  public static final String EXCEPTION_ARG_DELIMITER_MUST_BE_A_SINGLE_CHARACTER_23C3CA5E = "%s：分隔符必须是单个字符";
  public static final String EXCEPTION_INVALID_CUT_FIELDS_ARG_USE_POSITIVE_FIELD_NUMBERS_OR_ASCENDING_RANGES_95F4C873 = "无效的 cut 字段：%s；请使用正整数字段编号或递增范围";

  // Filesystem command help
  public static final String MESSAGE_USAGE_ARG_RESULT_ARG_DEFAULT_ARG_EXAMPLES_ARG_05BEA07B = "用法：%s\n结果：%s\n默认：%s\n示例：\n  %s";
  public static final String MESSAGE_FILESYSTEM_COMMANDS_USE_HELP_COMMAND_FOR_DETAILS_38FE89C6 = "文件系统命令（使用 help <command> 查看详情）：";
  public static final String MESSAGE_META_PATH_USAGE = "meta [path]";
  public static final String MESSAGE_META_PATH_RESULT = "输出表或时间序列路径的对象元数据。";
  public static final String MESSAGE_META_PATH_DEFAULT = "默认使用当前目录；表路径可使用 /database/table 或 /database/table.csv。";
  public static final String MESSAGE_META_PATH_EXAMPLE = "meta /db1/table1.csv";
  public static final String MESSAGE_SCHEMA_PATH_USAGE = "schema [path]";
  public static final String MESSAGE_SCHEMA_PATH_RESULT = "输出表或时间序列路径的模式信息。";
  public static final String MESSAGE_SCHEMA_PATH_DEFAULT = "默认使用当前目录；表路径可使用 /database/table 或 /database/table.csv。";
  public static final String MESSAGE_SCHEMA_PATH_EXAMPLE = "schema /db1/table1.csv";
  public static final String MESSAGE_QUOTE_PATHS_AND_PATTERNS_CONTAINING_SPACES_USE_BEFORE_OPERANDS_BEGINNING_WITH_COUNTS_USE_UNSIGNED_DECIMAL_INTEGERS_WITHOUT_LEADING_ZEROS_FIELDS_START_AT_1_OPTIONS_MAY_PRECEDE_OR_FOLLOW_PATHS_SINGLETON_OPTIONS_MUST_NOT_REPEAT_WRITES_REQUIRE_FS_WRITE_MODE_ENABLED_BATCH_OUTPUT_GOES_TO_STDOUT_ERRORS_GO_TO_STDERR_EXIT_STATUS_0_SUCCESS_1_USAGE_ERROR_2_INPUT_ERROR_3_RUNTIME_ERROR_832F0BFC = "包含空格的路径和模式需加引号；以 - 开头的参数前需加 --。\n计数使用不带前导零的无符号十进制整数；字段编号从 1 开始。\n选项可放在路径前或后；单值选项不能重复。\n写入需要 --fs_write_mode enabled。批处理结果写入 stdout，错误写入 stderr。\n退出码：0 成功，1 参数错误，2 输入错误，3 运行错误。";
  public static final String MESSAGE_PWD_9003D1DF = "pwd";
  public static final String MESSAGE_ABSOLUTE_VIRTUAL_WORKING_DIRECTORY_A179DC18 = "当前虚拟工作目录的绝对路径。";
  public static final String MESSAGE_NO_OPTIONS_2420248A = "无选项。";
  public static final String MESSAGE_LS_LAR_PATH_B103CAFF = "ls [-laR] [path]";
  public static final String MESSAGE_ENTRY_NAMES_L_ADDS_MODE_LINK_COUNT_OWNER_GROUP_AND_PLACEHOLDER_SIZE_A_INCLUDES_THE_CURRENT_AND_PARENT_DIRECTORIES_83802D00 = "条目名称；-l 增加模式、链接数、所有者、组和占位大小。-a 包含当前目录和父目录。";
  public static final String MESSAGE_CURRENT_DIRECTORY_R_PRINTS_THE_RECURSIVE_TREE_F35CB2DB = "当前目录；-R 输出递归目录树。";
  public static final String MESSAGE_LS_LS_LA_DB1_0A5F54F4 = "ls /\n  ls -la /db1";
  public static final String MESSAGE_LL_LAR_PATH_60CAF30F = "ll [-laR] [path]";
  public static final String MESSAGE_LONG_LISTING_AS_WITH_LS_L_1817C31A = "长列表，与 ls -l 相同。";
  public static final String MESSAGE_CURRENT_DIRECTORY_4B3788F6 = "当前目录（.）。";
  public static final String MESSAGE_LL_A_DB1_13B82162 = "ll -a /db1";
  public static final String MESSAGE_CD_PATH_3F25118B = "cd [path]";
  public static final String MESSAGE_CHANGES_THE_VIRTUAL_WORKING_DIRECTORY_NO_OUTPUT_ON_SUCCESS_2F56A52C = "切换虚拟工作目录；成功时不输出内容。";
  public static final String MESSAGE_CD_DB1_CD_9B32B3E0 = "cd /db1\n  cd ..";
  public static final String MESSAGE_STAT_PATH_09D48F35 = "stat [path]";
  public static final String MESSAGE_PATH_VIRTUAL_FILE_TYPE_AND_AVAILABLE_METADATA_7EC74779 = "路径、虚拟文件类型和可用元数据。";
  public static final String MESSAGE_STAT_DB1_TABLE1_CSV_411B4588 = "stat /db1/table1.csv";
  public static final String MESSAGE_CAT_PATH_C889DFB2 = "cat [path ...]";
  public static final String MESSAGE_SIDECAR_TEXT_IS_PRINTED_UNCHANGED_DATA_ROWS_ARE_TAB_SEPARATED_WITHOUT_AN_ADDED_HEADER_2E8E51C3 = "原样输出附属文件文本；数据行以制表符分隔，不额外添加表头。";
  public static final String MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_FROM_EACH_PATH_IN_ORDER_9BD68A9A = "当前目录；按路径顺序读取每个路径的前 20 行。";
  public static final String MESSAGE_CAT_DB1_TABLE1_CSV_CAT_REPORT_CSV_F9359B28 = "cat /db1/table1.csv\n  cat -- -report.csv";
  public static final String MESSAGE_HEAD_N_COUNT_COUNT_PATH_2A61E54C = "head [-n count | -count] [path]";
  public static final String MESSAGE_FIRST_COUNT_TEXT_LINES_OR_DATA_ROWS_8ABCCAD5 = "前 count 行文本或数据。";
  public static final String MESSAGE_CURRENT_DIRECTORY_COUNT_IS_10_INCLUDING_ANY_SIDECAR_HEADER_6C7FD37F = "当前目录；count 默认为 10，包含附属文件表头。";
  public static final String MESSAGE_HEAD_N_5_DB1_TABLE1_CSV_4DE75834 = "head -n 5 /db1/table1.csv";
  public static final String MESSAGE_TAIL_N_COUNT_COUNT_PATH_07ADC736 = "tail [-n count | -count] [path]";
  public static final String MESSAGE_LAST_COUNT_TEXT_LINES_OR_DATA_ROWS_EF2CC0FC = "后 count 行文本或数据。";
  public static final String MESSAGE_TAIL_N_5_DB1_TABLE1_CSV_E75ED46B = "tail -n 5 /db1/table1.csv";
  public static final String MESSAGE_GREP_PATTERN_PATH_3EF6BB72 = "grep <pattern> <path>";
  public static final String MESSAGE_LINES_CONTAINING_THE_LITERAL_PATTERN_REGULAR_EXPRESSIONS_ARE_NOT_USED_47F2D493 = "包含指定字面模式的行；不使用正则表达式。";
  public static final String MESSAGE_BOTH_PATTERN_AND_PATH_ARE_REQUIRED_SEARCHES_AT_MOST_20_ROWS_0B801CE1 = "必须指定模式和路径；最多搜索 20 行。";
  public static final String MESSAGE_GREP_DEVICE_1_DB1_TABLE1_CSV_6B7DEB82 = "grep \"device 1\" /db1/table1.csv";
  public static final String MESSAGE_FIND_PATH_NAME_PATTERN_D67E4643 = "find [path] [-name pattern]";
  public static final String MESSAGE_MATCHING_ABSOLUTE_PATHS_VISITED_RECURSIVELY_NAME_MATCHES_THE_EXACT_ENTRY_NAME_5B1560AA = "递归遍历并输出匹配的绝对路径；-name 精确匹配条目名称。";
  public static final String MESSAGE_CURRENT_DIRECTORY_INCLUDES_ALL_NAMES_IF_NAME_IS_OMITTED_B5541942 = "当前目录；省略 -name 时包含所有名称。";
  public static final String MESSAGE_FIND_DB1_NAME_TABLE1_CSV_9B02F992 = "find /db1 -name table1.csv";
  public static final String MESSAGE_LESS_PATH_8196C183 = "less [path]";
  public static final String MESSAGE_TEXT_LINES_OR_DATA_ROWS_PRINTED_WITHOUT_INTERACTIVE_PAGING_6C652AEF = "输出文本行或数据行，不进行交互式分页。";
  public static final String MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_29770E37 = "当前目录；最多读取 20 行。";
  public static final String MESSAGE_LESS_DB1_TABLE1_CSV_1C486298 = "less /db1/table1.csv";
  public static final String MESSAGE_MORE_PATH_75C477B2 = "more [path]";
  public static final String MESSAGE_MORE_DB1_TABLE1_CSV_63580724 = "more /db1/table1.csv";
  public static final String MESSAGE_FILE_PATH_4928CBD2 = "file [path]";
  public static final String MESSAGE_ABSOLUTE_PATH_AND_VIRTUAL_FILE_TYPE_C3A88F3C = "绝对路径和虚拟文件类型。";
  public static final String MESSAGE_FILE_DB1_TABLE1_CSV_A4914994 = "file /db1/table1.csv";
  public static final String MESSAGE_MKDIR_PATH_76FAFA85 = "mkdir [path]";
  public static final String MESSAGE_CREATES_A_TABLE_MODEL_DATABASE_NO_OUTPUT_ON_SUCCESS_DAE4AAC6 = "创建表模型数据库；成功时不输出内容。";
  public static final String MESSAGE_CURRENT_DIRECTORY_REQUIRES_FS_WRITE_MODE_ENABLED_5A485B47 = "当前目录；需要 --fs_write_mode enabled。";
  public static final String MESSAGE_MKDIR_DB1_FD8E7AF9 = "mkdir /db1";
  public static final String MESSAGE_RMDIR_PATH_A23525AE = "rmdir [path]";
  public static final String MESSAGE_DROPS_A_TABLE_MODEL_DATABASE_AND_ITS_TABLES_NO_OUTPUT_ON_SUCCESS_1F281CFB = "删除表模型数据库及其中的表；成功时不输出内容。";
  public static final String MESSAGE_RMDIR_DB1_40BDEEB7 = "rmdir /db1";
  public static final String MESSAGE_RM_R_PATH_B96CAAA7 = "rm [-r] <path>";
  public static final String MESSAGE_DROPS_THE_SELECTED_CSV_TABLE_R_DROPS_A_DATABASE_AND_ITS_TABLES_C499AA6A = "删除指定的 .csv 表；-r 删除数据库及其中的表。";
  public static final String MESSAGE_PATH_IS_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_4429FD1C = "必须指定路径；需要 --fs_write_mode enabled。";
  public static final String MESSAGE_RM_DB1_TABLE1_CSV_RM_R_DB1_5F30D7BC = "rm /db1/table1.csv\n  rm -r /db1";
  public static final String MESSAGE_MV_SOURCE_TARGET_A3FDF16A = "mv <source> <target>";
  public static final String MESSAGE_RENAMES_A_CSV_TABLE_WITHIN_THE_SAME_DATABASE_NO_OUTPUT_ON_SUCCESS_214800C4 = "在同一数据库内重命名 .csv 表；成功时不输出内容。";
  public static final String MESSAGE_BOTH_PATHS_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_0CF793C2 = "必须指定两个路径；需要 --fs_write_mode enabled。";
  public static final String MESSAGE_MV_DB1_TABLE1_CSV_DB1_TABLE2_CSV_E8D0CD22 = "mv /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_CP_SOURCE_TARGET_AF9799A3 = "cp <source> <target>";
  public static final String MESSAGE_COPIES_A_SCHEMA_TABLE_DEFINITION_USING_CREATE_TABLE_LIKE_NO_DATA_IS_COPIED_4B219F3F = "使用 CREATE TABLE LIKE 复制 .schema 表定义；不复制数据。";
  public static final String MESSAGE_CP_DB1_TABLE1_SCHEMA_DB1_TABLE2_SCHEMA_DD111A6B = "cp /db1/table1.schema /db1/table2.schema";
  public static final String MESSAGE_CUT_D_DELIMITER_F_FIELDS_PATH_9FE10722 = "cut [-d delimiter] -f fields <path>";
  public static final String MESSAGE_SELECTED_FIELDS_IN_SOURCE_ORDER_FIELDS_ACCEPT_COMMA_SEPARATED_POSITIVE_NUMBERS_AND_CLOSED_ASCENDING_RANGES_E5FF82DC = "按源字段顺序输出选定字段；fields 接受逗号分隔的正整数或闭合递增范围。";
  public static final String MESSAGE_TAB_DELIMITER_FIELDS_AND_PATH_ARE_REQUIRED_READS_AT_MOST_20_ROWS_46932C89 = "默认以制表符分隔；必须指定字段和路径；最多读取 20 行。";
  public static final String MESSAGE_CUT_D_F1_3_5_DB1_TABLE1_CSV_D096FEBD = "cut -d, -f1-3,5 /db1/table1.csv";
  public static final String MESSAGE_PASTE_PATH_PATH_2EBAB7CB = "paste <path> [path ...]";
  public static final String MESSAGE_CORRESPONDING_LINES_JOINED_WITH_TABS_SHORTER_INPUTS_CONTRIBUTE_EMPTY_COLUMNS_A4A4DA9C = "用制表符连接对应行；较短输入补充空列。";
  public static final String MESSAGE_AT_LEAST_ONE_PATH_IS_REQUIRED_READS_AT_MOST_20_ROWS_PER_PATH_2BF93D65 = "至少需要一个路径；每个路径最多读取 20 行。";
  public static final String MESSAGE_PASTE_DB1_TABLE1_CSV_DB1_TABLE2_CSV_35614E37 = "paste /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_JOIN_T_DELIMITER_1_FIELD_2_FIELD_PATH1_PATH2_2425772D = "join [-t delimiter] [-1 field] [-2 field] <path1> <path2>";
  public static final String MESSAGE_MATCHING_ROWS_JOINED_BY_KEY_FOLLOWED_BY_NON_KEY_FIELDS_FROM_EACH_INPUT_46F574EA = "按键匹配并连接行，随后输出各输入的非键字段。";
  public static final String MESSAGE_WHITESPACE_DELIMITER_FIELD_1_IS_THE_KEY_READS_AT_MOST_20_ROWS_PER_INPUT_7C11E3C7 = "默认以空白字符分隔；第 1 个字段为连接键；每个输入最多读取 20 行。";
  public static final String MESSAGE_JOIN_T_1_2_2_1_DB1_TABLE1_CSV_DB1_TABLE2_CSV_7E3608F9 = "join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_TEE_A_PATH_9071FE69 = "tee -a <path>";
  public static final String MESSAGE_APPENDS_STDIN_LINES_IN_BATCH_MODE_INTERACTIVELY_USE_WQ_TO_WRITE_OR_Q_TO_DISCARD_DD0C826A = "批处理模式下追加标准输入行；交互模式下使用 :wq 写入或 :q! 放弃。";
  public static final String MESSAGE_A_AND_PATH_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_CA1D5E95 = "必须指定 -a 和路径；需要 --fs_write_mode enabled。";
  public static final String MESSAGE_TEE_A_DB1_TABLE1_CSV_DEFFDD8A = "tee -a /db1/table1.csv";
  public static final String MESSAGE_TREE_L_DEPTH_PATH_80213473 = "tree [-L depth] [path]";
  public static final String MESSAGE_ENTRY_NAMES_WITH_INDENTATION_FOR_EACH_DIRECTORY_LEVEL_0A2D420E = "条目名称，按目录层级缩进。";
  public static final String MESSAGE_CURRENT_DIRECTORY_UNLIMITED_DEPTH_DEPTH_0_PRINTS_NO_DESCENDANTS_7B9643C9 = "当前目录；深度不限。深度 0 不输出子条目。";
  public static final String MESSAGE_TREE_L_2_DB1_4857586B = "tree -L 2 /db1";
  public static final String MESSAGE_SQL_STATEMENT_635619E5 = "sql <statement>";
  public static final String MESSAGE_SQL_PASSTHROUGH_IS_NOT_SUPPORTED_IN_FILESYSTEM_MODE_USE_THE_DEFAULT_SQL_ACCESS_MODE_6B83ED75 = "文件系统模式不支持 SQL 透传；请使用默认的 SQL 访问模式。";
  public static final String MESSAGE_THE_STATEMENT_IS_REQUIRED_AND_KEEPS_ITS_ORIGINAL_QUOTING_D8A652DF = "必须指定语句，并保留其原始引号。";
  public static final String MESSAGE_SQL_SELECT_FROM_ROOT_SG_D1_74E0542D = "sql SELECT * FROM root.sg.d1";
  public static final String MESSAGE_HELP_COMMAND_D620EA8F = "help [command]";
  public static final String MESSAGE_GENERAL_HELP_OR_HELP_FOR_ONE_KNOWN_COMMAND_COMMAND_HELP_IS_EQUIVALENT_E6ADD9ED = "输出通用帮助或指定命令的帮助；等价形式为 <command> --help。";
  public static final String MESSAGE_GENERAL_HELP_HELP_MUST_BE_USED_WITHOUT_OTHER_ARGUMENTS_67857584 = "默认显示通用帮助；--help 不能与其他参数混用。";
  public static final String MESSAGE_HELP_HELP_HEAD_HEAD_HELP_77DB2FED = "help\n  help head\n  head --help";
  public static final String MESSAGE_EXIT_F24F62EE = "exit";
  public static final String MESSAGE_LEAVES_FILESYSTEM_MODE_QUIT_IS_AN_ALIAS_FOR_EXIT_B456121F = "退出文件系统模式；quit 是 exit 的别名。";
  public static final String MESSAGE_QUIT_DBD73C2B = "quit";
  public static final String EXCEPTION_WRITE_COMMAND_IS_REQUIRED_6DD7F72C = "需要 write 命令";
  public static final String EXCEPTION_MISSING_VALUE_FOR_ARG_0AF4A1C7 = "%s 缺少参数值";
  public static final String EXCEPTION_ARG_SPECIFIED_MORE_THAN_ONCE_255E2870 = "%s 重复指定";
  public static final String EXCEPTION_CHOOSE_EXACTLY_ONE_OF_INPUT_OR_STDIN_966F4870 = "必须且只能选择 --input 或 --stdin";
  public static final String EXCEPTION_UNKNOWN_WRITE_OPTION_ARG_EAF8B4F9 = "未知的 write 选项：%s";
  public static final String EXCEPTION_WRITE_REQUIRES_T_TABLE_4B9990EB = "write 需要 -t/--table";
  public static final String EXCEPTION_WRITE_REQUIRES_AT_LEAST_ONE_FIELD_COLUMN_E714D04C = "write 至少需要一个 --field 列";
  public static final String EXCEPTION_WRITE_REQUIRES_O_OUTPUT_25A5E793 = "write 需要 -o/--output";
  public static final String EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704 = "无效名称 '%s'：名称必须为非空 UTF-8，且不能包含 BOM 或控制字符";
  public static final String EXCEPTION_NAME_ARG_IS_RESERVED_59CAD66D = "名称 '%s' 为保留名称";
  public static final String EXCEPTION_DUPLICATE_COLUMN_NAME_ARG_AB717F15 = "列名 '%s' 重复";
  public static final String EXCEPTION_UNKNOWN_TYPE_ARG_0FCF53E3 = "未知类型 '%s'";
  public static final String EXCEPTION_TAG_COLUMN_ARG_MUST_USE_STRING_93F86185 = "TAG 列 '%s' 必须使用 STRING 类型";
  public static final String EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_MUST_BE_A_USED_CANONICAL_DATA_TYPE_C36A91EC = "物理配置覆盖类型 '%s' 必须是已使用的规范数据类型";
  public static final String EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_IS_NOT_USED_BY_ANY_DECLARED_TAG_OR_FIELD_E0A808E7 = "物理配置覆盖类型 %s 未被任何声明的 TAG 或 FIELD 使用";
  public static final String EXCEPTION_ARG_FOR_DATA_TYPE_ARG_SPECIFIED_MORE_THAN_ONCE_6D9687F3 = "%s 被重复指定，数据类型为 %s";
  public static final String EXCEPTION_ENCODING_ARG_IS_NOT_SUPPORTED_FOR_DATA_TYPE_ARG_218D4FB0 = "不支持编码 %s，数据类型为 %s";
  public static final String EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_18307F13 = "不支持压缩方式 %s";
  public static final String MESSAGE_WRITE_TABLE_NAME_TAG_NAME_STRING_FIELD_NAME_TYPE_ENCODING_TYPE_ENCODING_COMPRESSION_TYPE_COMPRESSION_I_INPUT_INPUT_CSV_STDIN_O_OUTPUT_OUT_TSFILE_V_VERBOSE_AE31E0C1 = "write --table <name> (--tag <name> STRING)* (--field <name> <type>)+ [--encoding <type> <encoding>] [--compression <type> <compression>] (-i/--input <input.csv> | --stdin) -o/--output <out.tsfile> [-v/--verbose]";
  public static final String MESSAGE_CREATE_A_NEW_LOCAL_TABLE_MODEL_TSFILE_FROM_STRICT_CSV_SUCCESS_IS_SILENT_V_PRINTS_DETAILS_TO_STDERR_2ADF8D40 = "从严格 CSV 创建新的本地表模型 TsFile。成功时不输出内容；-v 向 stderr 输出详情。";
  public static final String MESSAGE_REQUIRES_FS_WRITE_MODE_ENABLED_THE_TARGET_MUST_NOT_EXIST_CSV_REQUIRES_TIME_AND_EXACTLY_THE_DECLARED_COLUMNS_UNQUOTED_N_IS_NULL_TIME_MUST_STRICTLY_INCREASE_PER_TAG_DEVICE_LOCAL_PATHS_ARE_RELATIVE_TO_THE_PROCESS_WORKING_DIRECTORY_43E64C5C = "需要 --fs_write_mode enabled。目标不得存在。CSV 必须包含 time 以及所有声明的列；未加引号的 \\N 表示空值。每个 TAG 设备的时间必须严格递增。本地路径相对于进程工作目录。";
  public static final String MESSAGE_WRITE_TABLE_SENSORS_TAG_SITE_STRING_FIELD_TEMPERATURE_DOUBLE_I_INPUT_CSV_O_OUTPUT_TSFILE_D9241DEC = "write --table sensors --tag site STRING --field temperature DOUBLE -i input.csv -o output.tsfile";
  public static final String EXCEPTION_CSV_HEADER_IS_MISSING_REQUIRED_COLUMN_ARG_27DD7D74 = "CSV 表头缺少必需的列 '%s'";
  public static final String EXCEPTION_INVALID_CSV_HEADER_NAME_ARG_FE536B69 = "无效的 CSV 表头列名 '%s'";
  public static final String EXCEPTION_CSV_HEADER_NAME_ARG_CONFLICTS_CASE_INSENSITIVELY_CB40F241 = "CSV 表头列名 '%s' 忽略大小写后重复";
  public static final String EXCEPTION_CSV_CONTAINS_UNDECLARED_COLUMN_ARG_2E8E0B09 = "CSV 包含未声明的列 '%s'";
  public static final String EXCEPTION_EXPECTED_ARG_FIELDS_GOT_ARG_LINE_ARG_848602BC = "应有 %d 个字段，实际有 %d 个（第 %d 行）";
  public static final String EXCEPTION_BAD_TIMESTAMP_ARG_LINE_ARG_EA69CAC9 = "无效的时间戳 '%s'（第 %d 行）";
  public static final String EXCEPTION_TIMESTAMPS_MUST_BE_STRICTLY_INCREASING_PER_DEVICE_LINE_ARG_ARG_PREVIOUS_ARG_ECC72725 = "每个设备的时间戳必须严格递增（第 %d 行：%d <= 前一个值 %d）";
  public static final String EXCEPTION_UNTERMINATED_QUOTED_CSV_FIELD_LINE_ARG_24E8F1B9 = "CSV 字段的引号未闭合（第 %d 行）";
  public static final String EXCEPTION_INVALID_OR_OUT_OF_RANGE_ARG_VALUE_ARG_LINE_ARG_341A3B62 = "无效或超出范围的 %s 值 '%s'（第 %d 行）";
  public static final String EXCEPTION_INVALID_UTF_8_OR_MISPLACED_BOM_LINE_ARG_16664A56 = "无效的 UTF-8 或位置错误的 BOM（第 %d 行）";
  public static final String EXCEPTION_OUTPUT_TARGET_ALREADY_EXISTS_ARG_238A4D83 = "输出目标已存在：%s";
  public static final String MESSAGE_CANNOT_WRITE_OUTPUT_ARG_ARG_AB420A33 = "无法写入输出文件 '%s'：%s";
  public static final String MESSAGE_CANNOT_REMOVE_TEMPORARY_OUTPUT_ARG_ARG_187E734C = "无法删除临时输出文件 '%s'：%s";
  public static final String EXCEPTION_INPUT_MUST_BE_A_REGULAR_CSV_FILE_ARG_AEB02C75 = "输入必须是普通 CSV 文件：%s";
  public static final String EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_BY_THE_BUNDLED_TSFILE_WRITER_03C0806D = "内置 TsFile 写入器不支持压缩算法 %s";
  public static final String MESSAGE_CREATED_MODEL_TABLE_OBJECT_ARG_ROWS_ARG_OUTPUT_ARG_70FEA8AE = "created model=table object=%s rows=%d output=%s";
  public static final String MESSAGE_COLUMN_ARG_CATEGORY_ARG_DATA_TYPE_ARG_ENCODING_ARG_SOURCE_ARG_COMPRESSION_ARG_SOURCE_ARG_4AA9D567 = "column=%s category=%s data_type=%s encoding=%s source=%s compression=%s source=%s";
  public static final String MESSAGE_ERROR_ARG_10E10A81 = "错误：%s";
  public static final String EXCEPTION_INVALID_CSV_SYNTAX_LINE_ARG_46CAD7A0 = "无效的 CSV 语法（第 %d 行）";
  public static final String EXCEPTION_NAME_ARG_CANNOT_BE_PRESERVED_BY_THE_BUNDLED_TSFILE_WRITER_RESOLVED_AS_ARG_697B06A9 = "内置 TsFile 写入器无法保留名称 '%s'（会解析为 '%s'）";
}
