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

package org.apache.iotdb.db.i18n;

/** WAL 导入工具的编译期国际化常量（中文）。 */
public final class ImportWALMessages {

  public static final String MESSAGE_IMPORT_WAL_5E42804E = "import-wal";
  public static final String
      MESSAGE_PATH_OF_A_WAL_FILE_OR_A_DIRECTORY_CONTAINING_WAL_FILES_473D0554 =
          "WAL 文件或包含 WAL 文件的目录路径。";
  public static final String MESSAGE_TARGET_IOTDB_HOST_DEFAULT_127_0_0_1_3729156F =
      "目标 IoTDB 主机。默认：127.0.0.1。";
  public static final String MESSAGE_TARGET_IOTDB_RPC_PORT_DEFAULT_6667_FC0D345D =
      "目标 IoTDB RPC 端口。默认：6667。";
  public static final String MESSAGE_TARGET_IOTDB_USERNAME_DEFAULT_ROOT_EB91453B =
      "目标 IoTDB 用户名。默认：root。";
  public static final String
      MESSAGE_TARGET_IOTDB_PASSWORD_PROMPTED_INTERACTIVELY_IF_OMITTED_29681961 =
      "目标 IoTDB 密码。未提供时将交互式询问。";
  public static final String MESSAGE_PASSWORD_PROMPT_F2D0E794 = "密码：";
  public static final String MESSAGE_TARGET_DATABASE_FOR_TABLE_MODEL_WAL_ENTRIES_27BACD1C =
      "表模型 WAL 条目的目标数据库。";
  public static final String MESSAGE_PRINT_THIS_HELP_MESSAGE_E800AF7A = "打印帮助信息。";
  public static final String MESSAGE_ARGUMENT_ERROR_ARG_A9767F62 = "参数错误：%s";
  public static final String MESSAGE_WAL_IMPORT_FAILED_ARG_55C014BA = "WAL 导入失败：%s";
  public static final String EXCEPTION_SOURCE_PATH_DOES_NOT_EXIST_ARG_7C806CA2 =
      "源路径不存在：%s";
  public static final String EXCEPTION_SOURCE_FILE_IS_NOT_A_WAL_FILE_ARG_14A43F76 =
      "源文件不是 WAL 文件：%s";
  public static final String EXCEPTION_NO_WAL_FILES_FOUND_UNDER_ARG_45F7FA22 =
      "路径下未找到 WAL 文件：%s";
  public static final String EXCEPTION_INVALID_PORT_ARG_A7CDD5AC = "无效端口：%s";
  public static final String
      MESSAGE_REPLAYED_ARG_OPERATIONS_FROM_ARG_WAL_FILES_SKIPPED_ARG_ENTRIES_F0D37E3A =
          "已重放 %d 个操作（来自 %d 个 WAL 文件）；跳过 %d 个条目。";
  public static final String
      MESSAGE_PROGRESS_ARG_COMPLETED_FILES_ARG_TOTAL_FILES_ARG_PROCESSED_BYTES_ARG_TOTAL_BYTES_ARG_PERCENT_ARG_ELAPSED_SECONDS_ARG_RATE_ARG_MB_PER_SECOND_F1C1356F =
          "进度：已完成 %d/%d 个 WAL 文件，已处理 %d/%d 字节（%.1f%%），耗时 %.1f 秒，速率 %.1f MB/s。";
  public static final String
      MESSAGE_IMPORT_DURATION_ARG_SECONDS_TOTAL_SIZE_ARG_BYTES_AVERAGE_RATE_ARG_MB_PER_SECOND_4B4EA58D =
          "导入耗时：%.1f 秒；文件总大小：%d 字节；平均速率：%.1f MB/s。";
  public static final String EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9 =
      "重放 WAL 文件 %s 时失败，偏移量 %d：%s";
  public static final String EXCEPTION_TABLE_MODEL_WAL_ENTRIES_REQUIRE_DB_DATABASE_F7597726 =
      "表模型 WAL 条目要求指定 -db/--database。";
  public static final String EXCEPTION_UNSUPPORTED_WAL_OPERATION_ARG_ABD227A0 =
      "不支持的 WAL 操作：%s";
  public static final String MESSAGE_UNSUPPORTED_WAL_OPERATION_ARG_SKIP_THIS_ENTRY_Y_N_DAFBE650 =
      "不支持的 WAL 操作：%s。是否跳过此条目？[y/N]：";
  public static final String EXCEPTION_INSERT_NODE_ARG_CONTAINS_NO_REPLAYABLE_DATA_5DA13453 =
      "Insert node %s 不包含可重放数据。";
  public static final String EXCEPTION_UNSUPPORTED_SNAPSHOT_DATA_TYPE_ARG_7A32D312 =
      "Unsupported snapshot data type: %s";
  public static final String EXCEPTION_THE_WAL_FILE_IS_TRUNCATED_OR_CORRUPTED_6B0734C5 =
      "WAL 文件被截断或已损坏。";
  public static final String
      EXCEPTION_PASSWORD_WAS_NOT_PROVIDED_AND_INTERACTIVE_INPUT_IS_UNAVAILABLE_40F42BCD =
          "未提供密码且当前环境不支持交互式输入，请指定 -pw/--password。";

  private ImportWALMessages() {}
}
