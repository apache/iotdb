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
import org.apache.iotdb.cli.i18n.FsHelpMessages;
import org.apache.iotdb.cli.i18n.FsWindowMessages;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Command help follows the usage/result/default/examples layout of TsFile CLI. */
public final class FilesystemCommandHelp {
  private static final Map<String, Entry> COMMANDS = new LinkedHashMap<>();
  private static final String SCOPE = "[-d device | -t table] [-m column ...]";
  private static final String FORMAT = "[-f table|ndjson|csv]";
  private static final String READ =
      SCOPE
          + " "
          + FORMAT
          + " [--offset n] [--start time] [--end time]"
          + " [--tag-filter tag op [value]] [--tag-match all|any]";

  private static final String TEMPORAL_READ =
      "[-f table|ndjson|csv] [--offset n] [--start ms] [--end ms] [--tag-filter tag op [value]] [--tag-match all|any]";

  static {
    add(
        "windows",
        "windows --start ms --end ms --origin epoch-or-ISO --width n [--step n] [--unit ms|day|month] [--timezone zone] [-n n] [-f ndjson] [--offset n]",
        new String[] {
          FsWindowMessages
              .MESSAGE_GENERATE_AN_EXPLICIT_FIXED_DURATION_OR_CALENDAR_WINDOW_GRID_CC82BDC7,
          FsWindowMessages
              .MESSAGE_GENERATE_EXPLICIT_WINDOWS_WITHOUT_READING_DATA_REQUIRED_START_END_ARE_INCLUSIVE_EPOCH_MS_ORIGIN_IS_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_WIDTH_IS_POSITIVE_STEP_DEFAULTS_TO_WIDTH_UNIT_MS_DAY_MONTH_DEFAULTS_TO_MS_TIMEZONE_DEFAULTS_TO_UTC_SELECT_GRID_STARTS_INSIDE_THE_RANGE_CLIP_EACH_FINAL_END_TO_THE_REQUESTED_END_OUTPUT_NOMINAL_HALF_OPEN_AND_EFFECTIVE_INCLUSIVE_BOUNDS_LOCAL_LABELS_ORIGIN_UNIT_AND_TIMEZONE_CALENDAR_OFFSETS_ARE_ANCHORED_TO_ORIGIN_NO_ITERATIVE_MONTH_DRIFT_AMBIGUOUS_NONEXISTENT_LOCAL_BOUNDARIES_AND_OVERFLOW_ARE_ERRORS_MAXIMUM_256_WINDOWS_PAGING_USES_N_OFFSET_90AB8B84
        },
        "windows --start 0 --end 9999 --origin 0 --width 1000 -f ndjson");
    add(
        "reduce",
        "reduce path --field field [--metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms] [--group-by entity|pooled] [--rank-by metric --order asc|desc --top n] [--windows start:end,... | --start ms --end ms] [--origin epoch-or-ISO --width n --step n --unit ms|day|month --timezone zone] [-n n] [--offset n] [-f table|csv|ndjson] [--tag-filter tag op value] [--tag-match all|any]",
        new String[] {
          FsWindowMessages
              .MESSAGE_REDUCE_OBSERVATIONS_AT_AN_EXPLICIT_ENTITY_GRAIN_AND_RANK_RESULT_ROWS_6D9CCB5B,
          FsWindowMessages
              .MESSAGE_REDUCE_RAW_FINITE_NUMERIC_OBSERVATIONS_LOCALLY_FIELD_REQUIRED_METRICS_DEFAULTS_TO_COUNT_FIRST_LAST_DELTA_GROUP_BY_ENTITY_DEFAULT_KEEPS_TREE_DEVICE_OR_FULL_TABLE_TAG_TUPLE_POOLED_EXPLICITLY_MERGES_OBSERVATIONS_PER_WINDOW_NEVER_AVERAGES_ENTITY_AVERAGES_NULL_TARGET_SKIPPED_EMPTY_GROUPS_OMITTED_DELTA_LAST_FIRST_ELAPSED_MS_LAST_AT_FIRST_AT_RATE_PER_MS_DELTA_ELAPSED_MS_NULL_FOR_ZERO_SPAN_ENDPOINT_TIES_USE_LEXICOGRAPHIC_ENTITY_IDENTITY_NULL_TAGS_FIRST_RETURN_ENDPOINT_IDENTITIES_TIMES_OPTIONAL_RANK_BY_SELECTED_METRIC_ORDER_ASC_DESC_DEFAULT_DESC_TOP_POSITIVE_TIES_USE_WINDOW_INDEX_THEN_ENTITY_IDENTITY_NULL_SCORES_LAST_RANK_IS_ROW_NUMBER_RANKING_PRECEDES_TOP_THEN_OUTPUT_PAGING_USE_EXPLICIT_INCLUSIVE_WINDOWS_OR_START_END_GRID_PARAMETERS_FOLLOW_WINDOWS_COMMAND_AND_EXCLUDE_EXPLICIT_WINDOWS_ALL_RESULTS_RETAIN_GROUP_WINDOW_TIMEZONE_DESCRIPTORS_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_NO_SQL_AGGREGATION_PUSHDOWN_F01EECDA
        },
        "reduce /root/db --field value --metrics avg,delta --rank-by delta --top 5 -f ndjson");
    add("pwd", "pwd", FsHelpMessages.PWD, "pwd");
    add("ls", "ls [-laR] " + FORMAT + " [path]", FsHelpMessages.LS, "ls /\n  ls -la /db1");
    add("ll", "ll [-aR] " + FORMAT + " [path]", FsHelpMessages.LL, "ll -a /db1");
    add("cd", "cd [path | -]", FsHelpMessages.CD, "cd /db1\n  cd -");
    add("stat", "stat [path]", FsHelpMessages.STAT, "stat /db1/table1.csv");
    add("file", "file [path]", FsHelpMessages.FILE, "file /db1/table1.csv");
    add(
        "schema",
        "schema " + SCOPE + " " + FORMAT + " [path]",
        FsHelpMessages.SCHEMA,
        "schema /db1/table1.csv -f csv");
    add("meta", "meta " + FORMAT + " [path]", FsHelpMessages.META, "meta /db1/table1.csv");
    add(
        "stats",
        "stats "
            + SCOPE
            + " "
            + FORMAT
            + " [--start time] [--end time] [--tag-filter tag op [value]]"
            + " [--tag-match all|any] [--aggregates count,min,max,sum,avg,median,first,last,min_time,max_time,null_count] [--windows start:end,...] [path]",
        FsHelpMessages.STATS,
        "stats /db1/table1.csv -m temperature --start 0 --end 1000 --aggregates avg,median");
    add(
        "gaps",
        "gaps path --field field [--windows start:end,...] [-n count] " + TEMPORAL_READ,
        FsHelpMessages.GAPS,
        "gaps /root/db/device --field value --windows 0:3599999,3600000:7199999 -f ndjson");
    add(
        "sessions",
        "sessions path --field field --gap-ms duration [-n count] " + TEMPORAL_READ,
        FsHelpMessages.SESSIONS,
        "sessions /root/db/device --field value --gap-ms 14400000 -f ndjson");
    add(
        "extreme",
        "extreme path --field field --mode min|max [-m field ...] [--windows start:end,...] [-n count] "
            + TEMPORAL_READ,
        FsHelpMessages.EXTREME,
        "extreme /root/db/device --field temperature --mode max -m humidity -f ndjson");
    add(
        "count",
        "count " + SCOPE + " " + FORMAT + " [path]",
        FsHelpMessages.COUNT,
        "count /db1/table1.csv -f ndjson");
    add(
        "cat",
        "cat [-n count] " + READ + " [path ...]",
        FsHelpMessages.CAT,
        "cat /db1/table1.csv -f csv\n  cat -- -report.csv");
    add(
        "head",
        "head [-n count | -count] [path] " + READ,
        FsHelpMessages.HEAD,
        "head -n 5 /db1/table1.csv");
    add(
        "tail",
        "tail [-n [+]count | -c [+]bytes] [-f] [--format table|ndjson|csv] [path]",
        FsHelpMessages.TAIL,
        "tail -n 5 /db1/table1.csv\n  tail -f /db1/table1.csv");
    add("wc", "wc -c [path ...]", FsHelpMessages.WC, "wc -c /db1/table1.csv");
    add(
        "grep",
        "grep [-E | -F] [-i] [-v] [-n] <pattern> [path ...]",
        FsHelpMessages.GREP,
        "grep -E 'device[12]' /db1/table1.csv");
    add(
        "find",
        "find [path] [-name pattern] [-type f|d] [-maxdepth n]",
        FsHelpMessages.FIND,
        "find /db1 -name '*.csv'");
    add("less", "less [path]", FsHelpMessages.PAGING, "less /db1/table1.csv");
    add("more", "more [path]", FsHelpMessages.PAGING, "more /db1/table1.csv");
    add("mkdir", "mkdir [-p] <path ...>", FsHelpMessages.MKDIR, "mkdir /db1");
    add("rmdir", "rmdir <path ...>", FsHelpMessages.RMDIR, "rmdir /db1");
    add(
        "rm",
        "rm [-r] [-f] [-i] <path ...>",
        FsHelpMessages.RM,
        "rm /db1/table1.csv\n  rm -r /db1");
    add(
        "mv",
        "mv [-i | -n | -f] <source ...> <target>",
        FsHelpMessages.MV,
        "mv /db1/table1.csv /db1/table2.csv");
    add(
        "cp",
        "cp [-i | -n | -f] <source ...> <target>",
        FsHelpMessages.CP,
        "cp /db1/table1.csv /db2/table2.csv");
    add(
        "cut",
        "cut (-f fields [-d delimiter] [-s] | -b bytes | -c characters) [path ...]",
        FsHelpMessages.CUT,
        "cut -d, -f1-3,5 /db1/table1.csv");
    add(
        "paste",
        "paste [-s] [-d delimiters] [path ...]",
        FsHelpMessages.PASTE,
        "paste /db1/table1.csv /db1/table2.csv");
    add(
        "join",
        "join [-t delimiter] [-1 field] [-2 field] [-a 1|2] [-v 1|2] [-e empty] [-o list] <path1>"
            + " <path2>",
        FsHelpMessages.JOIN,
        "join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv");
    COMMANDS.put(
        "write",
        new Entry(
            CliMessages
                .MESSAGE_WRITE_TABLE_NAME_TAG_NAME_STRING_FIELD_NAME_TYPE_ENCODING_TYPE_ENCODING_COMPRESSION_TYPE_COMPRESSION_I_INPUT_INPUT_CSV_STDIN_O_OUTPUT_OUT_TSFILE_V_VERBOSE_AE31E0C1,
            CliMessages
                .MESSAGE_CREATE_A_NEW_LOCAL_TABLE_MODEL_TSFILE_FROM_STRICT_CSV_SUCCESS_IS_SILENT_V_PRINTS_DETAILS_TO_STDERR_2ADF8D40,
            CliMessages
                .MESSAGE_REQUIRES_FS_WRITE_MODE_ENABLED_THE_TARGET_MUST_NOT_EXIST_CSV_REQUIRES_TIME_AND_EXACTLY_THE_DECLARED_COLUMNS_UNQUOTED_N_IS_NULL_TIME_MUST_STRICTLY_INCREASE_PER_TAG_DEVICE_LOCAL_PATHS_ARE_RELATIVE_TO_THE_PROCESS_WORKING_DIRECTORY_43E64C5C,
            CliMessages
                .MESSAGE_WRITE_TABLE_SENSORS_TAG_SITE_STRING_FIELD_TEMPERATURE_DOUBLE_I_INPUT_CSV_O_OUTPUT_TSFILE_D9241DEC));
    add("tee", "tee [-a] [path ...]", FsHelpMessages.TEE, "tee -a /db1/table1.csv");
    add(
        "export",
        "export --type table|ndjson|csv (-d device ... | -t table ...) (-o file [--force] |"
            + " --output-dir directory) [path]",
        FsHelpMessages.EXPORT,
        "export --type csv -t table1 -o table1.csv /db1");
    add(
        "sketch",
        "sketch [-o file [--force]] <local.tsfile>",
        FsHelpMessages.SKETCH,
        "sketch ./output.tsfile");
    add("tree", "tree [-L depth] [path]", FsHelpMessages.TREE, "tree -L 2 /db1");
    add("sql", "sql <statement>", FsHelpMessages.SQL, "sql SELECT * FROM db1.table1");
    add("help", "help [command]", FsHelpMessages.HELP, "help\n  head -h\n  head --help");
    add("exit", "exit [status]", FsHelpMessages.EXIT, "exit 0");
    add("quit", "quit [status]", FsHelpMessages.EXIT, "quit");
  }

  private FilesystemCommandHelp() {}

  private static void add(String name, String usage, String[] description, String example) {
    COMMANDS.put(name, new Entry(usage, description[0], description[1], example));
  }

  public static void print(PrintStream out, String command) {
    if (command == null || command.isEmpty()) {
      out.println(CliMessages.MESSAGE_FILESYSTEM_COMMANDS_USE_HELP_COMMAND_FOR_DETAILS_38FE89C6);
      for (Entry entry : COMMANDS.values()) {
        out.println("  " + entry.usage);
      }
      out.println();
      out.println(FsHelpMessages.GENERAL);
      return;
    }
    String name = command.toLowerCase(Locale.ROOT);
    Entry entry = COMMANDS.get(name);
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
    if ("head".equals(name)
        || "cat".equals(name)
        || "stats".equals(name)
        || "export".equals(name)) {
      out.println(FsHelpMessages.READ_OPTIONS);
    }
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
