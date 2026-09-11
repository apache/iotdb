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

package org.apache.iotdb.cli.fs.command;

import org.apache.iotdb.cli.i18n.CliMessages;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Command help follows the usage/result/default/examples layout of TsFile CLI. */
public final class FilesystemCommandHelp {
  private static final Map<String, Entry> COMMANDS = new LinkedHashMap<>();

  static {
    COMMANDS.put(
        "pwd",
        new Entry(
            CliMessages.MESSAGE_PWD_9003D1DF,
            CliMessages.MESSAGE_ABSOLUTE_VIRTUAL_WORKING_DIRECTORY_A179DC18,
            CliMessages.MESSAGE_NO_OPTIONS_2420248A,
            CliMessages.MESSAGE_PWD_9003D1DF));
    COMMANDS.put(
        "ls",
        new Entry(
            CliMessages.MESSAGE_LS_LAR_PATH_B103CAFF,
            CliMessages
                .MESSAGE_ENTRY_NAMES_L_ADDS_MODE_LINK_COUNT_OWNER_GROUP_AND_PLACEHOLDER_SIZE_A_INCLUDES_THE_CURRENT_AND_PARENT_DIRECTORIES_83802D00,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_R_PRINTS_THE_RECURSIVE_TREE_F35CB2DB,
            CliMessages.MESSAGE_LS_LS_LA_DB1_0A5F54F4));
    COMMANDS.put(
        "ll",
        new Entry(
            CliMessages.MESSAGE_LL_LAR_PATH_60CAF30F,
            CliMessages.MESSAGE_LONG_LISTING_AS_WITH_LS_L_1817C31A,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_4B3788F6,
            CliMessages.MESSAGE_LL_A_DB1_13B82162));
    COMMANDS.put(
        "cd",
        new Entry(
            CliMessages.MESSAGE_CD_PATH_3F25118B,
            CliMessages.MESSAGE_CHANGES_THE_VIRTUAL_WORKING_DIRECTORY_NO_OUTPUT_ON_SUCCESS_2F56A52C,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_4B3788F6,
            CliMessages.MESSAGE_CD_DB1_CD_9B32B3E0));
    COMMANDS.put(
        "stat",
        new Entry(
            CliMessages.MESSAGE_STAT_PATH_09D48F35,
            CliMessages.MESSAGE_PATH_VIRTUAL_FILE_TYPE_AND_AVAILABLE_METADATA_7EC74779,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_4B3788F6,
            CliMessages.MESSAGE_STAT_DB1_TABLE1_CSV_411B4588));
    COMMANDS.put(
        "schema",
        new Entry(
            CliMessages.MESSAGE_SCHEMA_PATH_USAGE,
            CliMessages.MESSAGE_SCHEMA_PATH_RESULT,
            CliMessages.MESSAGE_SCHEMA_PATH_DEFAULT,
            CliMessages.MESSAGE_SCHEMA_PATH_EXAMPLE));
    COMMANDS.put(
        "cat",
        new Entry(
            CliMessages.MESSAGE_CAT_PATH_C889DFB2,
            CliMessages
                .MESSAGE_SIDECAR_TEXT_IS_PRINTED_UNCHANGED_DATA_ROWS_ARE_TAB_SEPARATED_WITHOUT_AN_ADDED_HEADER_2E8E51C3,
            CliMessages
                .MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_FROM_EACH_PATH_IN_ORDER_9BD68A9A,
            CliMessages.MESSAGE_CAT_DB1_TABLE1_CSV_CAT_REPORT_CSV_F9359B28));
    COMMANDS.put(
        "head",
        new Entry(
            CliMessages.MESSAGE_HEAD_N_COUNT_COUNT_PATH_2A61E54C,
            CliMessages.MESSAGE_FIRST_COUNT_TEXT_LINES_OR_DATA_ROWS_8ABCCAD5,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_COUNT_IS_10_INCLUDING_ANY_SIDECAR_HEADER_6C7FD37F,
            CliMessages.MESSAGE_HEAD_N_5_DB1_TABLE1_CSV_4DE75834));
    COMMANDS.put(
        "tail",
        new Entry(
            CliMessages.MESSAGE_TAIL_N_COUNT_COUNT_PATH_07ADC736,
            CliMessages.MESSAGE_LAST_COUNT_TEXT_LINES_OR_DATA_ROWS_EF2CC0FC,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_COUNT_IS_10_INCLUDING_ANY_SIDECAR_HEADER_6C7FD37F,
            CliMessages.MESSAGE_TAIL_N_5_DB1_TABLE1_CSV_E75ED46B));
    COMMANDS.put(
        "wc",
        new Entry(
            CliMessages.MESSAGE_WC_L_PATH_062C2793,
            CliMessages.MESSAGE_ROW_COUNT_AND_ABSOLUTE_PATH_49560C8C,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_L_IS_IMPLIED_5191D6B4,
            CliMessages.MESSAGE_WC_L_DB1_TABLE1_CSV_40CA70C4));
    COMMANDS.put(
        "grep",
        new Entry(
            CliMessages.MESSAGE_GREP_PATTERN_PATH_3EF6BB72,
            CliMessages
                .MESSAGE_LINES_CONTAINING_THE_LITERAL_PATTERN_REGULAR_EXPRESSIONS_ARE_NOT_USED_47F2D493,
            CliMessages
                .MESSAGE_BOTH_PATTERN_AND_PATH_ARE_REQUIRED_SEARCHES_AT_MOST_20_ROWS_0B801CE1,
            CliMessages.MESSAGE_GREP_DEVICE_1_DB1_TABLE1_CSV_6B7DEB82));
    COMMANDS.put(
        "find",
        new Entry(
            CliMessages.MESSAGE_FIND_PATH_NAME_PATTERN_D67E4643,
            CliMessages
                .MESSAGE_MATCHING_ABSOLUTE_PATHS_VISITED_RECURSIVELY_NAME_MATCHES_THE_EXACT_ENTRY_NAME_5B1560AA,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_INCLUDES_ALL_NAMES_IF_NAME_IS_OMITTED_B5541942,
            CliMessages.MESSAGE_FIND_DB1_NAME_TABLE1_CSV_9B02F992));
    COMMANDS.put(
        "less",
        new Entry(
            CliMessages.MESSAGE_LESS_PATH_8196C183,
            CliMessages.MESSAGE_TEXT_LINES_OR_DATA_ROWS_PRINTED_WITHOUT_INTERACTIVE_PAGING_6C652AEF,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_29770E37,
            CliMessages.MESSAGE_LESS_DB1_TABLE1_CSV_1C486298));
    COMMANDS.put(
        "more",
        new Entry(
            CliMessages.MESSAGE_MORE_PATH_75C477B2,
            CliMessages.MESSAGE_TEXT_LINES_OR_DATA_ROWS_PRINTED_WITHOUT_INTERACTIVE_PAGING_6C652AEF,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_29770E37,
            CliMessages.MESSAGE_MORE_DB1_TABLE1_CSV_63580724));
    COMMANDS.put(
        "file",
        new Entry(
            CliMessages.MESSAGE_FILE_PATH_4928CBD2,
            CliMessages.MESSAGE_ABSOLUTE_PATH_AND_VIRTUAL_FILE_TYPE_C3A88F3C,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_4B3788F6,
            CliMessages.MESSAGE_FILE_DB1_TABLE1_CSV_A4914994));
    COMMANDS.put(
        "du",
        new Entry(
            CliMessages.MESSAGE_DU_PATH_B7DB6302,
            CliMessages
                .MESSAGE_ROW_COUNT_AND_ABSOLUTE_PATH_SEPARATED_BY_A_TAB_THE_COUNT_IS_NOT_A_BYTE_SIZE_8624847B,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_4B3788F6,
            CliMessages.MESSAGE_DU_DB1_TABLE1_CSV_03098463));
    COMMANDS.put(
        "mkdir",
        new Entry(
            CliMessages.MESSAGE_MKDIR_PATH_76FAFA85,
            CliMessages.MESSAGE_CREATES_A_TABLE_MODEL_DATABASE_NO_OUTPUT_ON_SUCCESS_DAE4AAC6,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_REQUIRES_FS_WRITE_MODE_ENABLED_5A485B47,
            CliMessages.MESSAGE_MKDIR_DB1_FD8E7AF9));
    COMMANDS.put(
        "rmdir",
        new Entry(
            CliMessages.MESSAGE_RMDIR_PATH_A23525AE,
            CliMessages
                .MESSAGE_DROPS_A_TABLE_MODEL_DATABASE_AND_ITS_TABLES_NO_OUTPUT_ON_SUCCESS_1F281CFB,
            CliMessages.MESSAGE_CURRENT_DIRECTORY_REQUIRES_FS_WRITE_MODE_ENABLED_5A485B47,
            CliMessages.MESSAGE_RMDIR_DB1_40BDEEB7));
    COMMANDS.put(
        "rm",
        new Entry(
            CliMessages.MESSAGE_RM_R_PATH_B96CAAA7,
            CliMessages
                .MESSAGE_DROPS_THE_SELECTED_CSV_TABLE_R_DROPS_A_DATABASE_AND_ITS_TABLES_C499AA6A,
            CliMessages.MESSAGE_PATH_IS_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_4429FD1C,
            CliMessages.MESSAGE_RM_DB1_TABLE1_CSV_RM_R_DB1_5F30D7BC));
    COMMANDS.put(
        "mv",
        new Entry(
            CliMessages.MESSAGE_MV_SOURCE_TARGET_A3FDF16A,
            CliMessages
                .MESSAGE_RENAMES_A_CSV_TABLE_WITHIN_THE_SAME_DATABASE_NO_OUTPUT_ON_SUCCESS_214800C4,
            CliMessages.MESSAGE_BOTH_PATHS_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_0CF793C2,
            CliMessages.MESSAGE_MV_DB1_TABLE1_CSV_DB1_TABLE2_CSV_E8D0CD22));
    COMMANDS.put(
        "cp",
        new Entry(
            CliMessages.MESSAGE_CP_SOURCE_TARGET_AF9799A3,
            CliMessages
                .MESSAGE_COPIES_A_SCHEMA_TABLE_DEFINITION_USING_CREATE_TABLE_LIKE_NO_DATA_IS_COPIED_4B219F3F,
            CliMessages.MESSAGE_BOTH_PATHS_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_0CF793C2,
            CliMessages.MESSAGE_CP_DB1_TABLE1_SCHEMA_DB1_TABLE2_SCHEMA_DD111A6B));
    COMMANDS.put(
        "cut",
        new Entry(
            CliMessages.MESSAGE_CUT_D_DELIMITER_F_FIELDS_PATH_9FE10722,
            CliMessages
                .MESSAGE_SELECTED_FIELDS_IN_SOURCE_ORDER_FIELDS_ACCEPT_COMMA_SEPARATED_POSITIVE_NUMBERS_AND_CLOSED_ASCENDING_RANGES_E5FF82DC,
            CliMessages
                .MESSAGE_TAB_DELIMITER_FIELDS_AND_PATH_ARE_REQUIRED_READS_AT_MOST_20_ROWS_46932C89,
            CliMessages.MESSAGE_CUT_D_F1_3_5_DB1_TABLE1_CSV_D096FEBD));
    COMMANDS.put(
        "paste",
        new Entry(
            CliMessages.MESSAGE_PASTE_PATH_PATH_2EBAB7CB,
            CliMessages
                .MESSAGE_CORRESPONDING_LINES_JOINED_WITH_TABS_SHORTER_INPUTS_CONTRIBUTE_EMPTY_COLUMNS_A4A4DA9C,
            CliMessages
                .MESSAGE_AT_LEAST_ONE_PATH_IS_REQUIRED_READS_AT_MOST_20_ROWS_PER_PATH_2BF93D65,
            CliMessages.MESSAGE_PASTE_DB1_TABLE1_CSV_DB1_TABLE2_CSV_35614E37));
    COMMANDS.put(
        "join",
        new Entry(
            CliMessages.MESSAGE_JOIN_T_DELIMITER_1_FIELD_2_FIELD_PATH1_PATH2_2425772D,
            CliMessages
                .MESSAGE_MATCHING_ROWS_JOINED_BY_KEY_FOLLOWED_BY_NON_KEY_FIELDS_FROM_EACH_INPUT_46F574EA,
            CliMessages
                .MESSAGE_WHITESPACE_DELIMITER_FIELD_1_IS_THE_KEY_READS_AT_MOST_20_ROWS_PER_INPUT_7C11E3C7,
            CliMessages.MESSAGE_JOIN_T_1_2_2_1_DB1_TABLE1_CSV_DB1_TABLE2_CSV_7E3608F9));
    COMMANDS.put(
        "tee",
        new Entry(
            CliMessages.MESSAGE_TEE_A_PATH_9071FE69,
            CliMessages
                .MESSAGE_APPENDS_STDIN_LINES_IN_BATCH_MODE_INTERACTIVELY_USE_WQ_TO_WRITE_OR_Q_TO_DISCARD_DD0C826A,
            CliMessages.MESSAGE_A_AND_PATH_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_CA1D5E95,
            CliMessages.MESSAGE_TEE_A_DB1_TABLE1_CSV_DEFFDD8A));
    COMMANDS.put(
        "tree",
        new Entry(
            CliMessages.MESSAGE_TREE_L_DEPTH_PATH_80213473,
            CliMessages.MESSAGE_ENTRY_NAMES_WITH_INDENTATION_FOR_EACH_DIRECTORY_LEVEL_0A2D420E,
            CliMessages
                .MESSAGE_CURRENT_DIRECTORY_UNLIMITED_DEPTH_DEPTH_0_PRINTS_NO_DESCENDANTS_7B9643C9,
            CliMessages.MESSAGE_TREE_L_2_DB1_4857586B));
    COMMANDS.put(
        "sql",
        new Entry(
            CliMessages.MESSAGE_SQL_STATEMENT_635619E5,
            CliMessages
                .MESSAGE_SQL_PASSTHROUGH_IS_NOT_SUPPORTED_IN_FILESYSTEM_MODE_USE_THE_DEFAULT_SQL_ACCESS_MODE_6B83ED75,
            CliMessages.MESSAGE_THE_STATEMENT_IS_REQUIRED_AND_KEEPS_ITS_ORIGINAL_QUOTING_D8A652DF,
            CliMessages.MESSAGE_SQL_SELECT_FROM_ROOT_SG_D1_74E0542D));
    COMMANDS.put(
        "help",
        new Entry(
            CliMessages.MESSAGE_HELP_COMMAND_D620EA8F,
            CliMessages
                .MESSAGE_GENERAL_HELP_OR_HELP_FOR_ONE_KNOWN_COMMAND_COMMAND_HELP_IS_EQUIVALENT_E6ADD9ED,
            CliMessages.MESSAGE_GENERAL_HELP_HELP_MUST_BE_USED_WITHOUT_OTHER_ARGUMENTS_67857584,
            CliMessages.MESSAGE_HELP_HELP_HEAD_HEAD_HELP_77DB2FED));
    COMMANDS.put(
        "exit",
        new Entry(
            CliMessages.MESSAGE_EXIT_F24F62EE,
            CliMessages.MESSAGE_LEAVES_FILESYSTEM_MODE_QUIT_IS_AN_ALIAS_FOR_EXIT_B456121F,
            CliMessages.MESSAGE_NO_OPTIONS_2420248A,
            CliMessages.MESSAGE_EXIT_F24F62EE));
    COMMANDS.put(
        "quit",
        new Entry(
            CliMessages.MESSAGE_QUIT_DBD73C2B,
            CliMessages.MESSAGE_LEAVES_FILESYSTEM_MODE_QUIT_IS_AN_ALIAS_FOR_EXIT_B456121F,
            CliMessages.MESSAGE_NO_OPTIONS_2420248A,
            CliMessages.MESSAGE_QUIT_DBD73C2B));
  }

  private FilesystemCommandHelp() {}

  public static void print(PrintStream out, String command) {
    if (command == null || command.isEmpty()) {
      out.println(CliMessages.MESSAGE_FILESYSTEM_COMMANDS_USE_HELP_COMMAND_FOR_DETAILS_38FE89C6);
      for (Entry entry : COMMANDS.values()) {
        out.println("  " + entry.usage);
      }
      out.println();
      out.println(
          CliMessages
              .MESSAGE_QUOTE_PATHS_AND_PATTERNS_CONTAINING_SPACES_USE_BEFORE_OPERANDS_BEGINNING_WITH_COUNTS_USE_UNSIGNED_DECIMAL_INTEGERS_WITHOUT_LEADING_ZEROS_FIELDS_START_AT_1_OPTIONS_MAY_PRECEDE_OR_FOLLOW_PATHS_SINGLETON_OPTIONS_MUST_NOT_REPEAT_WRITES_REQUIRE_FS_WRITE_MODE_ENABLED_BATCH_OUTPUT_GOES_TO_STDOUT_ERRORS_GO_TO_STDERR_EXIT_STATUS_0_SUCCESS_1_USAGE_ERROR_2_INPUT_ERROR_3_RUNTIME_ERROR_832F0BFC);
      return;
    }
    Entry entry = COMMANDS.get(command.toLowerCase(Locale.ROOT));
    if (entry == null) {
      out.println(String.format(CliMessages.MESSAGE_UNKNOWN_COMMAND_ARG_00157142, command));
      return;
    }
    out.println(
        String.format(
            CliMessages.MESSAGE_USAGE_ARG_RESULT_ARG_DEFAULT_ARG_EXAMPLES_ARG_05BEA07B,
            entry.usage,
            entry.result,
            entry.defaults,
            entry.examples));
  }

  private static final class Entry {
    private final String usage;
    private final String result;
    private final String defaults;
    private final String examples;

    private Entry(String usage, String result, String defaults, String examples) {
      this.usage = usage;
      this.result = result;
      this.defaults = defaults;
      this.examples = examples;
    }
  }
}
