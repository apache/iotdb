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

  static {
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
        "stats " + SCOPE + " " + FORMAT + " [path]",
        FsHelpMessages.STATS,
        "stats /db1/table1.csv -m temperature");
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
    if ("head".equals(name) || "cat".equals(name) || "export".equals(name)) {
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
