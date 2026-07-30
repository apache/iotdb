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

}
