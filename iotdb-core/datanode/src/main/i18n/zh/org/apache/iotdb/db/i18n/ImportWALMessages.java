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
  public static final String
      MESSAGE_WHEN_ALL_WAL_FILES_ARE_REPLAYED_SUCCESSFULLY_DO_OPERATION_ON_SOURCE_WAL_FILES_OPTIONAL_PARAMETERS_ARE_NONE_DEFAULT_AND_DELETE_41963A66 =
          "所有 WAL 文件成功重放后，对源 WAL 文件执行操作。可选参数为 none（默认）和 delete。";
  public static final String
      MESSAGE_NUMBER_OF_THREADS_USED_TO_REPLAY_WAL_DIRECTORIES_IN_PARALLEL_DEFAULT_1_6AEF4F50 =
          "并行重放 WAL 目录所用的线程数。默认：1。";
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
      EXCEPTION_INVALID_THREAD_COUNT_ARG_EXPECTED_A_POSITIVE_INTEGER_F3AE2CFD =
          "无效线程数：%s。应为正整数。";
  public static final String EXCEPTION_WAL_REPLAY_WAS_INTERRUPTED_770BA8AD =
      "WAL 重放被中断。";
  public static final String
      MESSAGE_REPLAYED_ARG_OPERATIONS_FROM_ARG_WAL_FILES_SKIPPED_ARG_ENTRIES_F0D37E3A =
          "已重放 %d 个操作（来自 %d 个 WAL 文件）；跳过 %d 个条目。";
  public static final String
      MESSAGE_PROGRESS_ARG_COMPLETED_FILES_ARG_TOTAL_FILES_ARG_PROCESSED_BYTES_ARG_TOTAL_BYTES_ARG_PERCENT_ARG_ELAPSED_SECONDS_ARG_RATE_ARG_MB_PER_SECOND_F1C1356F =
          "进度：已完成 %d/%d 个 WAL 文件，已处理 %d/%d 字节（%.1f%%），耗时 %.1f 秒，速率 %.1f MB/s。";
  public static final String
      MESSAGE_IMPORT_DURATION_ARG_SECONDS_TOTAL_SIZE_ARG_BYTES_AVERAGE_RATE_ARG_MB_PER_SECOND_4B4EA58D =
          "导入耗时：%.1f 秒；文件总大小：%d 字节；平均速率：%.1f MB/s。";
  public static final String MESSAGE_DELETED_ARG_SOURCE_WAL_FILES_C7A5AA1B =
      "已删除 %d 个源 WAL 文件。";
  public static final String EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9 =
      "重放 WAL 文件 %s 时失败，偏移量 %d：%s";
  public static final String EXCEPTION_FAILED_TO_DELETE_SOURCE_WAL_FILE_ARG_ARG_236AF580 =
      "删除源 WAL 文件 %s 失败：%s";
  public static final String
      EXCEPTION_UNSUPPORTED_ON_SUCCESS_VALUE_ARG_EXPECTED_NONE_OR_DELETE_F1C8EACE =
          "不支持的 on_success 值：%s。应为 none 或 delete。";
  public static final String EXCEPTION_TABLE_MODEL_WAL_ENTRIES_REQUIRE_DB_DATABASE_F7597726 =
      "表模型 WAL 条目要求指定 -db/--database。";
  public static final String EXCEPTION_UNSUPPORTED_WAL_OPERATION_ARG_ABD227A0 =
      "不支持的 WAL 操作：%s";
  public static final String
      MESSAGE_TREE_MODEL_DELETE_OPERATION_DETECTED_ARG_CHOOSE_E_EXECUTE_S_SKIP_A_EXECUTE_ALL_L_SKIP_ALL_Q_QUIT_11E39FD7 =
          "检测到树模型删除操作：%s。请选择 e=执行、s=跳过、a=全部执行、l=全部跳过、q=终止重放：";
  public static final String
      MESSAGE_UNSUPPORTED_WAL_OPERATION_ARG_CHOOSE_S_SKIP_L_SKIP_ALL_Q_QUIT_0A734E52 =
          "不支持的 WAL 操作：%s。请选择 s=跳过、l=全部跳过、q=终止重放：";
  public static final String EXCEPTION_WAL_REPLAY_WAS_TERMINATED_BY_THE_USER_E0BD6197 =
      "用户终止了 WAL 重放。";
  public static final String EXCEPTION_INSERT_NODE_ARG_CONTAINS_NO_REPLAYABLE_DATA_5DA13453 =
      "Insert node %s 不包含可重放数据。";
  public static final String EXCEPTION_UNSUPPORTED_SNAPSHOT_DATA_TYPE_ARG_7A32D312 =
      "Unsupported snapshot data type: %s";
  public static final String EXCEPTION_THE_WAL_FILE_IS_TRUNCATED_OR_CORRUPTED_6B0734C5 =
      "WAL 文件被截断或已损坏。";
  public static final String
      EXCEPTION_PASSWORD_WAS_NOT_PROVIDED_AND_INTERACTIVE_INPUT_IS_UNAVAILABLE_40F42BCD =
          "未提供密码且当前环境不支持交互式输入，请指定 -pw/--password。";

  public static final String MESSAGE_TABLE_MODEL_DELETE_OPERATION_DETECTED_ARG_CHOOSE_E_EXECUTE_S_SKIP_A_EXECUTE_ALL_L_SKIP_ALL_Q_QUIT_0C8D178A =
      "检测到表模型删除操作：%s。请选择 e=执行、s=跳过、a=全部执行、l=全部跳过、q=退出：";

  public static final String EXCEPTION_CANNOT_REPLAY_COLUMN_SPECIFIC_DELETION_FOR_TABLE_ARG_AS_DELETE_FROM_4A7ACC93 =
      "无法将表 %s 的指定列删除重放为 DELETE FROM。";

  public static final String EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_TARGET_TABLE_ARG_HAS_NO_TIME_COLUMN_55B1C25C =
      "无法重放表模型删除：目标表 %s 没有 TIME 列。";

  public static final String EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_TAG_SEGMENT_INDEX_ARG_IS_INCOMPATIBLE_WITH_TARGET_TABLE_ARG_D5E3CCEE =
      "无法重放表模型删除：TAG 段索引 %d 与目标表 %s 不兼容。";

  public static final String EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_DEVICE_ARG_IS_INCOMPATIBLE_WITH_TARGET_TABLE_ARG_A6320535 =
      "无法重放表模型删除：设备 %s 与目标表 %s 不兼容。";

  public static final String EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_UNSUPPORTED_TAG_PREDICATE_ARG_AD0753A8 =
      "无法重放表模型删除：不支持 TAG 条件 %s。";

  public static final String MESSAGE_POLICY_FOR_TREE_TABLE_DELETIONS_ASK_DEFAULT_EXECUTE_SKIP_TERMINATE_C485B75F =
      "树模型和表模型删除操作的策略：ask（默认）、execute、skip、terminate。";
  public static final String MESSAGE_POLICY_FOR_OBJECTNODE_ENTRIES_ASK_DEFAULT_SKIP_TERMINATE_7C94280F =
      "ObjectNode 条目的策略：ask（默认）、skip、terminate。";
  public static final String MESSAGE_POLICY_FOR_UNSUPPORTED_OPERATIONS_INCLUDING_UNCONVERTIBLE_TABLE_DELETIONS_ASK_DEFAULT_SKIP_TERMINATE_46ED9D3B =
      "不支持的操作（包括无法转换的表模型删除）的策略：ask（默认）、skip、terminate。";
  public static final String EXCEPTION_INVALID_VALUE_FOR_ARG_ARG_EXPECTED_ARG_73B78522 =
      "--%s 的值无效：%s。应为 %s。";

  public static final String MESSAGE_POLICY_FOR_CORRUPTED_WAL_FILES_ASK_DEFAULT_SKIP_TERMINATE_SKIPS_THE_REST_OF_THE_FILE_ALREADY_REPLAYED_OPERATIONS_ARE_NOT_ROLLED_BACK_BF097791 =
      "损坏 WAL 文件的处理策略：ask（默认）、skip、terminate。跳过文件的剩余内容；已重放的操作不会回滚。";
  public static final String MESSAGE_WAL_CORRUPTION_DETECTED_ARG_ALREADY_REPLAYED_OPERATIONS_ARE_NOT_ROLLED_BACK_CHOOSE_S_SKIP_FILE_L_SKIP_ALL_CORRUPTED_FILES_Q_QUIT_BFED14E4 =
      "检测到 WAL 损坏：%s。已重放的操作不会回滚。请选择 s=跳过当前文件、l=跳过所有损坏文件、q=退出：";
  public static final String MESSAGE_SKIPPED_CORRUPTED_WAL_FILE_ARG_SOURCE_FILE_RETAINED_C20FF968 =
      "已跳过损坏的 WAL 文件：%s。源文件已保留。";
  public static final String MESSAGE_SKIPPED_ARG_CORRUPTED_WAL_FILES_SOURCE_FILES_RETAINED_A889CCE2 =
      "已跳过 %d 个损坏的 WAL 文件；源文件已保留。";

  public static final String MESSAGE_TARGET_DATABASE_FOR_TABLE_MODEL_WAL_ENTRIES_IF_OMITTED_INFER_FROM_THE_WAL_PARENT_DIRECTORY_AND_ASK_FOR_CONFIRMATION_4B1E409D =
      "表模型 WAL 条目的目标数据库。省略时尝试从 WAL 文件的父目录名推断，并请求确认。";
  public static final String MESSAGE_INFERRED_TABLE_DATABASE_ARG_FROM_WAL_DIRECTORY_ARG_REPLAY_INTO_THIS_DATABASE_Y_YES_A_ACCEPT_ALL_INFERRED_DATABASES_N_QUIT_5B59D833 =
      "推断目标表模型数据库为 %s，来源 WAL 目录为 %s。是否向此数据库重放？[y] 同意，[a] 全部同意推断的数据库，[N] 退出：";
  public static final String EXCEPTION_DATABASE_CONFIRMATION_REQUIRED_FOR_WAL_DIRECTORY_ARG_INFERRED_DATABASE_ARG_SPECIFY_DB_DATABASE_OR_SKIP_DB_CONFIRMATION_WHEN_INTERACTIVE_INPUT_IS_UNAVAILABLE_14DF6D36 =
      "需要确认 WAL 目录 %s 的目标数据库（推断结果：%s）。无交互终端时请指定 -db/--database 或 --skip_db_confirmation。";
  public static final String EXCEPTION_REPLAY_INTO_INFERRED_DATABASE_ARG_WAS_NOT_CONFIRMED_SPECIFY_DB_DATABASE_TO_SELECT_THE_TARGET_EXPLICITLY_86F81190 =
      "未确认向推断出的数据库 %s 重放。请使用 -db/--database 显式选择目标。";

  public static final String MESSAGE_ACCEPT_ALL_INFERRED_DATABASE_NAMES_WITHOUT_CONFIRMATION_DB_DATABASE_STILL_TAKES_PRECEDENCE_FA49A73C =
      "自动接受所有推断出的数据库名，不再询问确认；-db/--database 仍优先。";

  private ImportWALMessages() {}
}
