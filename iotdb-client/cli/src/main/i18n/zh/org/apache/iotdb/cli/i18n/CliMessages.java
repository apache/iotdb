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
      LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_4677380E =
          "使用 --input 指定输入。导出还需要 --output；导入需要 --host 和 --port。请使用 --password-stdin 或 --password-env，避免密码出现在命令行中。";
  public static final String
      LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_RESTORE_STATS_BFF9FDC2 =
          "pipe-logical-backup <inspect|verify|export|import|restore|stats>";
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

}
