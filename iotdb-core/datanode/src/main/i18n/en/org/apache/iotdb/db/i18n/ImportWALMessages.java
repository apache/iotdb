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

/** Compile-time i18n constants for the WAL import tool (English). */
public final class ImportWALMessages {

  public static final String MESSAGE_IMPORT_WAL_5E42804E = "import-wal";
  public static final String
      MESSAGE_PATH_OF_A_WAL_FILE_OR_A_DIRECTORY_CONTAINING_WAL_FILES_473D0554 =
      "Path of a WAL file or a directory containing WAL files.";
  public static final String MESSAGE_TARGET_IOTDB_HOST_DEFAULT_127_0_0_1_3729156F =
      "Target IoTDB host. Default: 127.0.0.1.";
  public static final String MESSAGE_TARGET_IOTDB_RPC_PORT_DEFAULT_6667_FC0D345D =
      "Target IoTDB RPC port. Default: 6667.";
  public static final String MESSAGE_TARGET_IOTDB_USERNAME_DEFAULT_ROOT_EB91453B =
      "Target IoTDB username. Default: root.";
  public static final String
      MESSAGE_TARGET_IOTDB_PASSWORD_PROMPTED_INTERACTIVELY_IF_OMITTED_29681961 =
      "Target IoTDB password. Prompted interactively if omitted.";
  public static final String MESSAGE_PASSWORD_PROMPT_F2D0E794 = "Password: ";
  public static final String MESSAGE_TARGET_DATABASE_FOR_TABLE_MODEL_WAL_ENTRIES_27BACD1C =
      "Target database for table-model WAL entries.";
  public static final String
      MESSAGE_WHEN_ALL_WAL_FILES_ARE_REPLAYED_SUCCESSFULLY_DO_OPERATION_ON_SOURCE_WAL_FILES_OPTIONAL_PARAMETERS_ARE_NONE_DEFAULT_AND_DELETE_41963A66 =
          "When all WAL files are replayed successfully, do operation on source WAL files. Optional parameters are none (default) and delete.";
  public static final String
      MESSAGE_NUMBER_OF_THREADS_USED_TO_REPLAY_WAL_DIRECTORIES_IN_PARALLEL_DEFAULT_1_6AEF4F50 =
          "Number of threads used to replay WAL directories in parallel. Default: 1.";
  public static final String MESSAGE_PRINT_THIS_HELP_MESSAGE_E800AF7A =
      "Print this help message.";
  public static final String MESSAGE_ARGUMENT_ERROR_ARG_A9767F62 = "Argument error: %s";
  public static final String MESSAGE_WAL_IMPORT_FAILED_ARG_55C014BA = "WAL import failed: %s";
  public static final String EXCEPTION_SOURCE_PATH_DOES_NOT_EXIST_ARG_7C806CA2 =
      "Source path does not exist: %s";
  public static final String EXCEPTION_SOURCE_FILE_IS_NOT_A_WAL_FILE_ARG_14A43F76 =
      "Source file is not a WAL file: %s";
  public static final String EXCEPTION_NO_WAL_FILES_FOUND_UNDER_ARG_45F7FA22 =
      "No WAL files found under: %s";
  public static final String EXCEPTION_INVALID_PORT_ARG_A7CDD5AC = "Invalid port: %s";
  public static final String
      EXCEPTION_INVALID_THREAD_COUNT_ARG_EXPECTED_A_POSITIVE_INTEGER_F3AE2CFD =
          "Invalid thread count: %s. Expected a positive integer.";
  public static final String EXCEPTION_WAL_REPLAY_WAS_INTERRUPTED_770BA8AD =
      "WAL replay was interrupted.";
  public static final String
      MESSAGE_REPLAYED_ARG_OPERATIONS_FROM_ARG_WAL_FILES_SKIPPED_ARG_ENTRIES_F0D37E3A =
          "Replayed %d operations from %d WAL files; skipped %d entries.";
  public static final String
      MESSAGE_PROGRESS_ARG_COMPLETED_FILES_ARG_TOTAL_FILES_ARG_PROCESSED_BYTES_ARG_TOTAL_BYTES_ARG_PERCENT_ARG_ELAPSED_SECONDS_ARG_RATE_ARG_MB_PER_SECOND_F1C1356F =
          "Progress: %d/%d WAL files completed, %d/%d bytes (%.1f%%), elapsed %.1f s, rate %.1f MB/s.";
  public static final String
      MESSAGE_IMPORT_DURATION_ARG_SECONDS_TOTAL_SIZE_ARG_BYTES_AVERAGE_RATE_ARG_MB_PER_SECOND_4B4EA58D =
          "Import duration: %.1f s; total size: %d bytes; average rate: %.1f MB/s.";
  public static final String MESSAGE_DELETED_ARG_SOURCE_WAL_FILES_C7A5AA1B =
      "Deleted %d source WAL files.";
  public static final String EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9 =
      "Failed to replay WAL file %s at offset %d: %s";
  public static final String EXCEPTION_FAILED_TO_DELETE_SOURCE_WAL_FILE_ARG_ARG_236AF580 =
      "Failed to delete source WAL file %s: %s";
  public static final String
      EXCEPTION_UNSUPPORTED_ON_SUCCESS_VALUE_ARG_EXPECTED_NONE_OR_DELETE_F1C8EACE =
          "Unsupported on_success value: %s. Expected none or delete.";
  public static final String EXCEPTION_TABLE_MODEL_WAL_ENTRIES_REQUIRE_DB_DATABASE_F7597726 =
      "Table-model WAL entries require -db/--database.";
  public static final String EXCEPTION_UNSUPPORTED_WAL_OPERATION_ARG_ABD227A0 =
      "Unsupported WAL operation: %s";
  public static final String MESSAGE_UNSUPPORTED_WAL_OPERATION_ARG_SKIP_THIS_ENTRY_Y_N_DAFBE650 =
      "Unsupported WAL operation: %s. Skip this entry? [y/N]: ";
  public static final String EXCEPTION_INSERT_NODE_ARG_CONTAINS_NO_REPLAYABLE_DATA_5DA13453 =
      "Insert node %s contains no replayable data.";
  public static final String EXCEPTION_UNSUPPORTED_SNAPSHOT_DATA_TYPE_ARG_7A32D312 =
      "Unsupported snapshot data type: %s";
  public static final String EXCEPTION_THE_WAL_FILE_IS_TRUNCATED_OR_CORRUPTED_6B0734C5 =
      "The WAL file is truncated or corrupted.";
  public static final String
      EXCEPTION_PASSWORD_WAS_NOT_PROVIDED_AND_INTERACTIVE_INPUT_IS_UNAVAILABLE_40F42BCD =
          "Password was not provided and interactive input is unavailable. Specify -pw/--password.";

  private ImportWALMessages() {}
}
