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

  // Pipe 逻辑备份工具
  public static final String EXCEPTION_LOGICAL_BACKUP_COMMAND_FAILED_ARG_9973B0C0 =
      "逻辑备份命令执行失败：%s";
  public static final String EXCEPTION_UNKNOWN_LOGICAL_BACKUP_COMMAND_ARG_79275619 =
      "未知的逻辑备份命令：%s";
  public static final String EXCEPTION_OUTPUT_IS_REQUIRED_FOR_LOGICAL_BACKUP_EXPORT_603340B1 =
      "逻辑备份导出必须指定 --output";
  public static final String LOG_STREAM_ARG_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_DE6BBAD0 =
      "流 %s：%d 条记录，%d 个已提交事件组";
  public static final String
      LOG_LOGICAL_BACKUP_VERIFIED_ARG_STREAMS_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_5210360B =
          "逻辑备份校验通过：%d 个流，%d 条记录，%d 个已提交事件组";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_TARGET_MUST_NOT_BE_INSIDE_SOURCE_A499F994 =
          "逻辑备份导出目标不能位于源目录内";
  public static final String LOG_LOGICAL_BACKUP_EXPORTED_FROM_ARG_TO_ARG_B3E8D280 =
      "逻辑备份已从 %s 导出到 %s";
  public static final String LOG_DRY_RUN_COMPLETED_NO_DATA_WAS_WRITTEN_38AE244B =
      "试运行完成，未写入数据";
  public static final String
      LOG_LOGICAL_BACKUP_IMPORT_COMPLETED_ARG_EVENT_GROUPS_CHECKPOINT_ARG_16F6A72D =
          "逻辑备份导入完成：%d 个事件组，checkpoint 为 %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA =
      "逻辑备份选项 --%s 为必填项";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_PASSWORD_ENVIRONMENT_VARIABLE_IS_NOT_SET_616738A2 =
          "未设置逻辑备份密码环境变量";
  public static final String EXCEPTION_LOGICAL_BACKUP_HANDSHAKE_FAILED_ARG_7CDD4697 =
      "逻辑备份握手失败：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_FAILED_ARG_75EE2D11 =
      "逻辑备份请求类型 %d 执行失败：%s";
  public static final String
      LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_EXPORT_CAN_USE_BACKUP_DIR_TO_CREATE_MONITOR_AND_CLEAN_UP_A_SNAPSHOT_PIPE_AUTOMATICALLY_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_F5B223CB =
          "使用 --input 指定输入。导出还需要 --output；导出也可使用 --backup-dir 自动创建、监控并清理快照 Pipe。导入需要 --host 和 --port。请使用 --password-stdin 或 --password-env，避免密码出现在命令行中。";
  public static final String LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_6D62F9CE =
      "logical-backup <inspect|verify|export|import>";
  public static final String EXCEPTION_LOGICAL_BACKUP_ARCHIVE_ENTRY_IS_UNSAFE_ARG_3E548152 =
      "逻辑备份归档项路径不安全：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432 =
      "逻辑备份归档超过安全限制";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_SOURCE_MUST_BE_A_DIRECTORY_OR_MANIFEST_ARG_0BBDE9F0 =
          "逻辑备份导出源必须是目录或 manifest：%s";
  public static final String
      EXCEPTION_SPECIFY_EXACTLY_ONE_OF_PASSWORD_STDIN_AND_PASSWORD_ENV_FOR_LOGICAL_BACKUP_IMPORT_A96813D9 =
          "逻辑备份导入必须且只能指定 --password-stdin 或 --password-env 中的一项";
  public static final String EXCEPTION_NO_PASSWORD_WAS_READ_FROM_STANDARD_INPUT_6294AB8E =
      "未能从标准输入读取密码";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_DOES_NOT_MATCH_THE_SOURCE_OR_TARGET_ARG_B978184D =
          "逻辑备份 checkpoint 与源备份或目标实例不匹配：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_IS_INVALID_ARG_71E82F4C =
      "逻辑备份 checkpoint 无效：%s";
  public static final String EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EXPORT_FORMAT_ARG_A6D7DEB1 =
      "不支持的逻辑备份导出格式：%s";

  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_TIMEOUT_AND_POLL_INTERVAL_MUST_BE_POSITIVE_38622769 =
      "逻辑备份导出 timeout 和轮询间隔必须为正数";
  public static final String
      LOG_LOGICAL_BACKUP_EXPORT_ARG_STATE_ARG_REMAINING_ARG_ESTIMATED_SECONDS_ARG_6256B85B =
      "逻辑备份导出 %s：状态=%s，剩余=%s，预计秒数=%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_FAILED_ARG_F52EB190 =
      "逻辑备份导出 pipe %s 失败：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_TIMED_OUT_AFTER_ARG_SECONDS_76CF4B4B =
      "逻辑备份导出 pipe %s 在 %d 秒后超时";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_MUST_BE_AN_INTEGER_C9B397BE =
      "逻辑备份选项 --%s 必须是整数";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_HAS_INVALID_IDENTIFIER_ARG_BB015204 =
      "逻辑备份选项 --%s 的标识符无效：%s";
  public static final String LOG_EXPORT_IS_DEPRECATED_USE_DUMP_TO_GENERATE_A_BACKUP_AND_PACK_TO_CREATE_A_ZIP_47709D2D =
      "export 已弃用；请使用 dump 生成备份，使用 pack 创建 ZIP。";
  public static final String LOG_IMPORT_IS_DEPRECATED_USE_RESTORE_2F1AE2CE =
      "import 已弃用；请使用 restore。";
  public static final String LOG_LOGICAL_BACKUP_ARG_1E5C23CE =
      "逻辑备份：%s";
  public static final String LOG_LOGICAL_BACKUP_PACKED_FROM_ARG_TO_ARG_CB9D5304 =
      "已将逻辑备份从 %s 打包至 %s";
public static final String EXCEPTION_STOP_SEQUENCE_REQUIRES_A_STABLE_SOURCE_SEQUENCE_CAPABILITY_USE_UNTIL_WITH_THIS_SERVER_VERSION_8323B09E =
      "--stop-sequence 需要源端提供稳定序号能力；当前服务端版本请使用 --until。";
  public static final String EXCEPTION_OBJECT_FILTERS_REQUIRE_MODEL_TREE_OR_MODEL_TABLE_SPLIT_MIXED_MODEL_FILTERED_DUMPS_INTO_SEPARATE_BACKUPS_0488699E =
      "对象过滤需要 --model tree 或 --model table；请将混合模型过滤拆为独立备份。";
  public static final String EXCEPTION_TABLE_FILTERS_USE_DATABASE_REGEX_TABLE_REGEX_EXCLUSIONS_MUST_USE_FOR_EITHER_DATABASE_OR_TABLE_42F0FA23 =
      "Table 过滤使用数据库正则/表名正则；排除过滤中数据库或表名必须有一项为 .*。";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_DUMP_OPTION_ARG_894E11E0 =
      "逻辑备份 dump 选项无效：--%s";
  public static final String EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81 =
      "逻辑备份参数无效或相互冲突。";
  public static final String EXCEPTION_ONLY_CONFLICT_FAIL_IS_SUPPORTED_RESTORE_NEVER_IMPLICITLY_OVERWRITES_OR_DELETES_TARGET_OBJECTS_70C9523E =
      "当前仅支持 --conflict fail；restore 不会隐式覆盖或删除目标对象。";
  public static final String EXCEPTION_THE_EXISTING_BACKUP_TASK_DOES_NOT_MATCH_THIS_BACKUP_IDENTITY_OR_CAPTURE_OPTIONS_50E6388F =
      "已有备份任务与本次备份身份或捕获选项不一致。";
  public static final String EXCEPTION_ANOTHER_DUMP_PROCESS_IS_MANAGING_THIS_BACKUP_FE8EE828 =
      "其他 dump 进程正在管理此备份。";
  public static final String EXCEPTION_BACKUP_ARG_TASK_ARG_DIRECTORY_ARG_ARG_THE_TASK_IS_RETAINED_QUERY_SHOW_PIPE_OR_REPEAT_DUMP_WITH_THE_SAME_OPTIONS_AND_RESUME_APPEND_6F8C1E87 =
      "备份 %s，任务 %s，目录 %s：%s。任务已保留；可使用 SHOW PIPE 查询，或使用相同选项和 --resume append 继续 dump。";
  public static final String EXCEPTION_THE_BACKUP_TASK_HAS_NOT_PRODUCED_SEALED_STREAMS_YET_RESUME_THE_SAME_TASK_TO_FINISH_SEALING_87C9B599 =
      "备份任务尚未产出封存的流；请继续原任务以完成封存。";
  public static final String EXCEPTION_AN_EXISTING_CHECKPOINT_REQUIRES_RESUME_RESUME_REQUIRES_AN_EXISTING_CHECKPOINT_FB6D0B7D =
      "使用已有 checkpoint 必须指定 --resume；--resume 要求 checkpoint 已存在。";
  public static final String EXCEPTION_THE_TARGET_TIMESTAMP_PRECISION_DOES_NOT_MATCH_THE_BACKUP_70B225CB =
      "目标时间精度与备份不一致。";
  public static final String EXCEPTION_THE_TARGET_VERSION_OR_DATA_MODEL_IS_INCOMPATIBLE_WITH_THE_BACKUP_220EB40C =
      "目标版本或数据模型与备份不兼容。";
  public static final String EXCEPTION_THE_CHECKPOINT_MUST_BE_OUTSIDE_THE_INPUT_BACKUP_15B70F59 =
      "checkpoint 必须位于输入备份之外。";
  public static final String EXCEPTION_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_INSPECT_THE_BACKUP_OR_USE_DRY_RUN_FIRST_83DDCD79 =
      "恢复权限对象必须显式指定 --include-auth true；请先 inspect 备份或执行 --dry-run。";
  public static final String LOG_LOGICAL_BACKUP_DUMP_VERIFY_INSPECT_PACK_RESTORE_OPTIONS_DUMP_CONNECT_TO_A_SOURCE_AND_SEAL_A_BACKUP_DIRECTORY_DOES_NOT_CREATE_A_ZIP_INSPECT_SHOW_BACKUP_IDENTITY_SCOPE_STREAMS_SEGMENTS_COUNTS_AND_STATUS_VERIFY_CHECK_STRUCTURE_CHECKSUMS_AND_SEMANTICS_WITHOUT_CHANGING_THE_INPUT_PACK_VERIFY_AND_PACKAGE_AN_EXISTING_DIRECTORY_AS_ZIP_ACCEPTS_NO_CONNECTION_OPTIONS_RESTORE_PREFLIGHT_AND_REPLAY_A_DIRECTORY_ROOT_MANIFEST_JSON_OR_ZIP_INSPECT_VERIFY_INPUT_PATH_JSON_ALLOW_INCOMPLETE_PACK_INPUT_DIR_OUTPUT_ZIP_JSON_ALLOW_INCOMPLETE_DUMP_HOST_HOST_PORT_PORT_USER_ROOT_PASSWORD_STDIN_PASSWORD_ENV_NAME_BACKUP_DIR_DIR_BACKUP_ID_ID_SCOPE_CONFIG_SCHEMA_DATA_MODEL_TREE_TABLE_INCLUDE_PATTERN_EXCLUDE_PATTERN_START_TIME_TIME_END_TIME_TIME_INCLUDE_AUTH_TRUE_FALSE_MODE_SNAPSHOT_CONTINUOUS_UNTIL_ISO_8601_RESUME_FAIL_IF_EXISTS_APPEND_NEW_PIPE_NAME_NAME_SEGMENT_SIZE_BYTES_268435456_MAX_RECORD_BYTES_67108864_FSYNC_POLICY_ALWAYS_BATCH_PERIODIC_NONE_FSYNC_BATCH_OPERATIONS_N_FSYNC_PERIOD_MS_N_ON_UNSUPPORTED_FAIL_SKIP_INCLUDE_HEARTBEAT_TRUE_FALSE_TIMEOUT_SECONDS_3600_POLL_INTERVAL_SECONDS_2_JSON_ALLOW_INCOMPLETE_CONTINUOUS_REQUIRES_UNTIL_STOP_SEQUENCE_IS_RESERVED_AND_CURRENTLY_REJECTED_OBJECT_FILTERS_REQUIRE_ONE_MODEL_TREE_USES_PATH_LISTS_TABLE_USES_DATABASE_REGEX_TABLE_REGEX_TREE_CONFIG_SCHEMA_CAPTURE_ACCEPTS_A_SINGLE_PREFIX_FULL_PATH_WITHOUT_EXCLUSIONS_TABLE_EXCLUSIONS_MUST_SELECT_WHOLE_DATABASES_OR_WHOLE_TABLE_NAME_PATTERNS_RESTORE_INPUT_PATH_HOST_HOST_PORT_PORT_USER_USER_PASSWORD_STDIN_PASSWORD_ENV_NAME_DRY_RUN_CHECKPOINT_FILE_RESUME_CONFLICT_FAIL_INCLUDE_AUTH_TRUE_FALSE_JSON_ALLOW_INCOMPLETE_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_LEGACY_CONFIG_IS_CONSERVATIVE_A_TARGET_CONNECTION_IS_REQUIRED_FOR_REPLAY_LOCAL_DRY_RUN_NEEDS_NO_CONNECTION_TARGET_PREFLIGHT_CHECKS_VERSION_PRECISION_AUTHENTICATION_AND_HANDSHAKE_SUPPORT_OBJECT_CONFLICTS_AND_PER_OBJECT_PRIVILEGES_ARE_CHECKED_DURING_REPLAY_DEPRECATED_ALIASES_EXPORT_INPUT_DIR_OUTPUT_ZIP_PACK_EXPORT_WITH_SOURCE_CONNECTION_OPTIONS_DUMP_THEN_PACK_IMPORT_RESTORE_EXIT_CODE_0_SUCCESS_1_FAILURE_ERRORS_AND_DEPRECATION_NOTICES_GO_TO_STDERR_B1983BE8 =
      "logical-backup <dump|verify|inspect|pack|restore> [选项]\n  dump     连接源端并封存备份目录，不生成 ZIP。\n  inspect  查看备份身份、范围、流、segment、计数和状态。\n  verify   检查结构、校验和与语义，不修改输入。\n  pack     校验已有目录并打包为 ZIP，不接受数据库连接参数。\n  restore  预检并回放备份目录、根 manifest.json 或 ZIP。\n\ninspect/verify: --input PATH [--json] [--allow-incomplete]\npack: --input DIR --output ZIP [--json] [--allow-incomplete]\ndump: --host HOST --port PORT [--user root] (--password-stdin|--password-env NAME)\n  --backup-dir DIR --backup-id ID --scope config,schema,data [--model tree,table]\n  [--include PATTERN] [--exclude PATTERN] [--start-time TIME] [--end-time TIME]\n  [--include-auth true|false] [--mode snapshot|continuous] [--until ISO-8601]\n  [--resume fail-if-exists|append|new] [--pipe-name NAME]\n  [--segment-size-bytes 268435456] [--max-record-bytes 67108864]\n  [--fsync-policy always|batch|periodic|none] [--fsync-batch-operations N|--fsync-period-ms N]\n  [--on-unsupported fail|skip] [--include-heartbeat true|false]\n  [--timeout-seconds 3600] [--poll-interval-seconds 2] [--json] [--allow-incomplete]\n  continuous 必须提供 --until；--stop-sequence 是预留参数，当前会被拒绝。\n  对象过滤只允许选择一个模型：Tree 使用路径列表，Table 使用数据库正则/表名正则。\n  Tree config/schema 捕获只接受单个前缀或完整路径，不支持排除过滤。\n  Table 排除过滤必须覆盖整个数据库或整个表名模式。\nrestore: --input PATH [--host HOST --port PORT --user USER\n  (--password-stdin|--password-env NAME)] [--dry-run] [--checkpoint FILE] [--resume]\n  [--conflict fail] [--include-auth true|false] [--json] [--allow-incomplete]\n  恢复权限对象必须指定 --include-auth true；旧 config 备份按包含权限对象处理。\n  实际恢复需要目标连接；本地 dry-run 无需连接。\n  目标预检检查版本、时间精度、认证和握手能力；\n  对象冲突及对象级权限在回放时检查。\n\n弃用别名：export --input DIR --output ZIP = pack；\n  带源端连接参数的 export = dump 后接 pack；import = restore。\n退出码：0 成功，1 失败。错误及弃用提示输出到标准错误。";
}
