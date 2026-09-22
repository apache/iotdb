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
import org.apache.iotdb.cli.i18n.FsParserMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FilesystemCommandParser {

  private static final String DEFAULT_PATH = ".";
  private static final int DEFAULT_TREE_DEPTH = Integer.MAX_VALUE;
  private static final int DEFAULT_HEAD_LIMIT = 10;

  private FilesystemCommandParser() {}

  public static FilesystemCommand parse(String input) {
    String line = input == null ? "" : input.trim();
    if (line.isEmpty()) {
      return FilesystemCommand.invalid(CliMessages.MESSAGE_EMPTY_COMMAND_943E8DA9);
    }
    // SQL has its own quoting and escaping rules; pass its body through unchanged.
    int commandEnd = 0;
    while (commandEnd < line.length() && !Character.isWhitespace(line.charAt(commandEnd))) {
      commandEnd++;
    }
    if ("sql".equalsIgnoreCase(line.substring(0, commandEnd))) {
      String statement = line.substring(commandEnd).trim();
      if (statement.isEmpty()) {
        return FilesystemCommand.invalid(CliMessages.MESSAGE_SQL_STATEMENT_IS_EMPTY_676FCD59);
      }
      if ("--help".equals(statement) || "-h".equals(statement)) {
        return FilesystemCommand.path(FilesystemCommand.Type.HELP, "sql");
      }
      if (statement.startsWith("--help")
          && Character.isWhitespace(statement.charAt("--help".length()))) {
        return FilesystemCommand.invalid(
            CliMessages.MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
      }
      return FilesystemCommand.sql(statement);
    }

    List<String> tokens;
    List<FsShellWords.Word> words;
    try {
      words = FsShellWords.parse(line);
      tokens = new ArrayList<>();
      for (FsShellWords.Word word : words) tokens.add(word.getValue());
    } catch (IllegalArgumentException e) {
      return FilesystemCommand.invalid(
          CliMessages.MESSAGE_UNCLOSED_QUOTE_OR_ESCAPE_IN_FILESYSTEM_COMMAND_42C74084,
          "grep".equalsIgnoreCase(line.substring(0, commandEnd)) ? 2 : 1);
    }
    String command = tokens.get(0).toLowerCase(Locale.ROOT);
    if ("help".equals(command) || "--help".equals(command) || "-h".equals(command)) {
      if (tokens.size() == 1) {
        return FilesystemCommand.simple(FilesystemCommand.Type.HELP);
      }
      if ("help".equals(command) && tokens.size() == 2 && "--help".equals(tokens.get(1))) {
        return FilesystemCommand.path(FilesystemCommand.Type.HELP, "help");
      }
      if ("help".equals(command) && tokens.size() == 2 && isKnownCommand(tokens.get(1))) {
        return FilesystemCommand.path(
            FilesystemCommand.Type.HELP, tokens.get(1).toLowerCase(Locale.ROOT));
      }
      return FilesystemCommand.invalid(
          CliMessages.MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
    }
    if (!isKnownCommand(command)) {
      return FilesystemCommand.invalid(
          String.format(CliMessages.MESSAGE_UNKNOWN_COMMAND_ARG_00157142, tokens.get(0)));
    }
    if (tokens.size() == 2 && ("--help".equals(tokens.get(1)) || "-h".equals(tokens.get(1)))) {
      return FilesystemCommand.path(FilesystemCommand.Type.HELP, command);
    }
    try {
      if ("write".equals(command)) {
        if (tokens.contains("--help")) {
          return FilesystemCommand.invalid(
              CliMessages
                  .MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
        }
        return FilesystemCommand.write(WriteCommandParser.parse(tokens));
      }
      Arguments args = new Arguments(command, tokens, words);
      FilesystemCommand parsed = parseCommand(command, args).withOptions(args.options);
      return parsed.withPathPatterns(args.pathPatterns(parsed));
    } catch (IllegalArgumentException e) {
      return FilesystemCommand.invalid(e.getMessage(), "grep".equals(command) ? 2 : 1);
    }
  }

  static boolean isKnownCommand(String command) {
    try {
      FilesystemCommand.Type type = commandType(command.toLowerCase(Locale.ROOT));
      return type != FilesystemCommand.Type.INVALID;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static FilesystemCommand.Type commandType(String command) {
    return "quit".equals(command)
        ? FilesystemCommand.Type.EXIT
        : FilesystemCommand.Type.valueOf(command.toUpperCase(Locale.ROOT));
  }

  private static FilesystemCommand parseCommand(String command, Arguments args) {
    FilesystemCommand.Type type = commandType(command);
    switch (type) {
      case SQL:
        return FilesystemCommand.invalid(CliMessages.MESSAGE_SQL_STATEMENT_IS_EMPTY_676FCD59);
      case PWD:
        args.paths(0, 0);
        return FilesystemCommand.simple(type);
      case EXIT:
        args.paths(0, 1);
        return FilesystemCommand.simple(type)
            .withLimit(
                args.operands.isEmpty()
                    ? 0
                    : unsignedInteger(command, "status", args.operands.get(0), false) & 255);
      case CD:
        args.paths(0, 1);
        return FilesystemCommand.path(type, args.operands.isEmpty() ? "/" : args.operands.get(0));
      case SCHEMA:
      case META:
      case STATS:
      case COUNT:
        validateScope(args);
        if (type == FilesystemCommand.Type.STATS && args.has("--aggregates")) {
          validateAggregates(args.value("--aggregates"));
        }
        return withReadOptions(FilesystemCommand.path(type, args.path(false)), args, "-f");
      case SKETCH:
        if (args.has("--force") && !args.has("-o")) {
          throw invalid(CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "-o");
        }
        return FilesystemCommand.path(type, args.path(true));
      case EXPORT:
        validateScope(args);
        if (!args.has("--type") || args.has("-o") == args.has("--output-dir")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB,
              command,
              "--type and -o/--output-dir");
        }
        if (!args.has("-d") && !args.has("-t")) {
          throw invalid(CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "-d/-t");
        }
        if (args.has("--output-dir") && args.has("--force")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, command, "--force");
        }
        if (args.has("-o")
            && (args.options.containsKey("-d.1") || args.options.containsKey("-t.1"))) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D,
              command,
              "multiple objects with -o");
        }
        args.options.put("-f", format(args.value("--type")));
        return withReadOptions(
            FilesystemCommand.path(type, args.path(false))
                .withLimit(
                    args.has("-n") ? unsignedInteger(command, "-n", args.value("-n"), false) : -1),
            args,
            "-f");
      case WC:
        if (args.has("-c")) {
          return FilesystemCommand.option(type, "-c", "-").withPaths(stdinPaths(args));
        }
        throw invalid(FsParserMessages.WC_MODE);
      case LS:
      case LL:
        String listPath = args.path(false);
        return withReadOptions(
            FilesystemCommand.option(
                args.has("-l") ? FilesystemCommand.Type.LL : type,
                args.has("-a") ? "-a" : "",
                listPath),
            args,
            "-f");
      case CAT:
        validateScope(args);
        List<String> catPaths = args.paths(0, Integer.MAX_VALUE);
        FilesystemCommand catCommand =
            catPaths.isEmpty()
                ? FilesystemCommand.path(type, DEFAULT_PATH)
                : FilesystemCommand.paths(type, catPaths);
        return catCommand
            .withLimit(
                args.has("-n") ? unsignedInteger(command, "-n", args.value("-n"), false) : -1)
            .withReadOptions(
                format(args.valueOrDefault("-f", "table")),
                args.valueOrDefault("-d", ""),
                args.valueOrDefault("-t", ""),
                args.values("-m"),
                args.has("--offset")
                    ? parseLong(command, "--offset", args.value("--offset"), false)
                    : 0,
                parseTimestamp(command, "--start", args.valueOrDefault("--start", null)),
                parseTimestamp(command, "--end", args.valueOrDefault("--end", null)),
                args.values("--tag-filter"),
                args.valueOrDefault("--tag-match", "all"));
      case PASTE:
        return FilesystemCommand.paths(type, stdinPaths(args));
      case MV:
      case CP:
        return FilesystemCommand.paths(type, args.paths(2, Integer.MAX_VALUE));
      case HEAD:
      case TAIL:
        validateScope(args);
        if (type == FilesystemCommand.Type.TAIL && args.has("-c") && args.has("-n")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, command, "-n with -c");
        }
        String countOption = args.has("-c") ? "-c" : "-n";
        String countValue = args.valueOrDefault(countOption, "10");
        if (type == FilesystemCommand.Type.TAIL && countValue.startsWith("+")) {
          args.options.put("--from-start", "");
          countValue = countValue.substring(1);
        }
        int limit =
            args.has(countOption)
                ? (type == FilesystemCommand.Type.HEAD
                    ? unsignedInteger(command, "-n", countValue, false)
                    : unsignedInteger(command, countOption, countValue, false))
                : DEFAULT_HEAD_LIMIT;
        String readPath = args.path(false);
        if (type == FilesystemCommand.Type.TAIL && args.operands.isEmpty()) readPath = "-";
        FilesystemCommand readCommand =
            type == FilesystemCommand.Type.HEAD
                ? FilesystemCommand.head(readPath, limit)
                : FilesystemCommand.tail(readPath, limit);
        return readCommand.withReadOptions(
            format(
                args.valueOrDefault(
                    type == FilesystemCommand.Type.TAIL ? "--format" : "-f", "table")),
            args.valueOrDefault("-d", ""),
            args.valueOrDefault("-t", ""),
            args.values("-m"),
            args.has("--offset")
                ? parseLong(command, "--offset", args.value("--offset"), false)
                : 0,
            parseTimestamp(command, "--start", args.valueOrDefault("--start", null)),
            parseTimestamp(command, "--end", args.valueOrDefault("--end", null)),
            args.values("--tag-filter"),
            args.valueOrDefault("--tag-match", "all"));
      case GREP:
        args.paths(1, Integer.MAX_VALUE, true);
        if (args.has("-F") && args.has("-E")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, command, "-F with -E");
        }
        List<String> grepPaths =
            args.operands.size() == 1
                ? Collections.singletonList("-")
                : new ArrayList<>(args.operands.subList(1, args.operands.size()));
        return FilesystemCommand.pattern(type, args.operands.get(0), grepPaths.get(0))
            .withPaths(grepPaths);
      case FIND:
        if (args.has("-type")
            && !"f".equals(args.value("-type"))
            && !"d".equals(args.value("-type"))) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
              command,
              args.value("-type"));
        }
        if (args.has("-maxdepth"))
          unsignedInteger(command, "-maxdepth", args.value("-maxdepth"), false);
        return FilesystemCommand.pattern(type, args.valueOrDefault("-name", ""), args.path(false));
      case TREE:
        int depth = DEFAULT_TREE_DEPTH;
        if (args.has("-L")) {
          try {
            depth = unsignedInteger(command, "-L", args.value("-L"), false);
          } catch (IllegalArgumentException e) {
            throw invalid(CliMessages.EXCEPTION_INVALID_TREE_DEPTH_ARG_EF544DD4, args.value("-L"));
          }
        }
        return FilesystemCommand.tree(args.path(false), depth);
      case RM:
        return FilesystemCommand.option(type, args.has("-r") ? "-r" : "", "")
            .withPaths(args.paths(args.has("-f") ? 0 : 1, Integer.MAX_VALUE));
      case TEE:
        return FilesystemCommand.option(type, args.has("-a") ? "-a" : "", "")
            .withPaths(args.paths(0, Integer.MAX_VALUE));
      case CUT:
        int modes = (args.has("-f") ? 1 : 0) + (args.has("-b") ? 1 : 0) + (args.has("-c") ? 1 : 0);
        if (modes != 1 || (!args.has("-f") && (args.has("-d") || args.has("-s")))) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "-f/-b/-c");
        }
        String cutDelimiter = args.valueOrDefault("-d", "\t");
        validateDelimiter(command, cutDelimiter);
        String fields = args.value(args.has("-f") ? "-f" : args.has("-b") ? "-b" : "-c");
        validateCutFields(fields);
        return FilesystemCommand.cut(cutDelimiter, fields, "-").withPaths(stdinPaths(args));
      case JOIN:
        String joinDelimiter = args.valueOrDefault("-t", "");
        if (args.has("-t")) {
          validateDelimiter(command, joinDelimiter);
        }
        int left = args.has("-1") ? unsignedInteger(command, "-1", args.value("-1"), true) : 1;
        int right = args.has("-2") ? unsignedInteger(command, "-2", args.value("-2"), true) : 1;
        for (String flag : new String[] {"-a", "-v"}) {
          if (args.has(flag) && !"1".equals(args.value(flag)) && !"2".equals(args.value(flag))) {
            throw invalid(
                CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
                command,
                args.value(flag));
          }
        }
        if (args.has("-o")
            && !args.value("-o")
                .matches("(?:0|[12]\\.[1-9][0-9]*)(?:[, ]+(?:0|[12]\\.[1-9][0-9]*))*")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
              command,
              args.value("-o"));
        }
        return FilesystemCommand.join(joinDelimiter, left + "," + right, args.paths(2, 2));
      case MKDIR:
        if (args.has("-m") && !args.value("-m").matches("[0-7]{1,4}")) {
          throw invalid(
              CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
              command,
              args.value("-m"));
        }
        return FilesystemCommand.paths(type, args.paths(1, Integer.MAX_VALUE));
      case RMDIR:
        return FilesystemCommand.paths(type, args.paths(1, Integer.MAX_VALUE));
      default:
        return FilesystemCommand.path(type, args.path(false));
    }
  }

  private static List<String> stdinPaths(Arguments args) {
    List<String> paths = args.paths(0, Integer.MAX_VALUE);
    return paths.isEmpty() ? Collections.singletonList("-") : paths;
  }

  private static FilesystemCommand withReadOptions(
      FilesystemCommand command, Arguments args, String formatFlag) {
    return command.withReadOptions(
        format(args.valueOrDefault(formatFlag, "table")),
        args.valueOrDefault("-d", ""),
        args.valueOrDefault("-t", ""),
        args.values("-m"),
        args.has("--offset")
            ? parseLong(args.command, "--offset", args.value("--offset"), false)
            : 0,
        parseTimestamp(args.command, "--start", args.valueOrDefault("--start", null)),
        parseTimestamp(args.command, "--end", args.valueOrDefault("--end", null)),
        args.values("--tag-filter"),
        args.valueOrDefault("--tag-match", "all"));
  }

  private static void validateDelimiter(String command, String delimiter) {
    if (delimiter.length() != 1) {
      throw invalid(
          CliMessages.EXCEPTION_ARG_DELIMITER_MUST_BE_A_SINGLE_CHARACTER_23C3CA5E, command);
    }
  }

  private static int unsignedInteger(
      String command, String option, String value, boolean positive) {
    if (value.isEmpty() || (value.length() > 1 && value.charAt(0) == '0')) {
      throw invalid(
          CliMessages.EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04,
          command,
          option,
          value);
    }
    for (int i = 0; i < value.length(); i++) {
      if (value.charAt(i) < '0' || value.charAt(i) > '9') {
        throw invalid(
            CliMessages.EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04,
            command,
            option,
            value);
      }
    }
    try {
      int result = Integer.parseInt(value);
      if (positive && result == 0) {
        throw invalid(
            CliMessages.EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04,
            command,
            option,
            value);
      }
      return result;
    } catch (NumberFormatException e) {
      throw invalid(
          CliMessages.EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04,
          command,
          option,
          value);
    }
  }

  private static String format(String value) {
    if (!"table".equals(value) && !"csv".equals(value) && !"ndjson".equals(value)) {
      throw invalid(FsParserMessages.OUTPUT_FORMAT, value);
    }
    return value;
  }

  private static void validateScope(Arguments args) {
    if (args.has("-d") && args.has("-t")) {
      throw invalid(FsParserMessages.EXCLUSIVE_SCOPE);
    }
    if (args.has("--start") && args.has("--end")) {
      String start = parseTimestamp(args.command, "--start", args.value("--start"));
      String end = parseTimestamp(args.command, "--end", args.value("--end"));
      if (Long.parseLong(start) > Long.parseLong(end))
        throw invalid(CliMessages.MESSAGE_FS_INVALID_TIME_RANGE);
    }
    if ("0".equals(args.value("-n"))
        && args.has("--offset")
        && !"0".equals(args.value("--offset"))) {
      throw invalid(CliMessages.MESSAGE_FS_OFFSET_WITH_ZERO_LIMIT);
    }
    if (args.has("--tag-match") && args.values("--tag-filter").isEmpty()) {
      throw invalid(FsParserMessages.TAG_MATCH_FILTER);
    }
    if (args.has("--tag-match")) {
      String match = args.value("--tag-match");
      if (!"all".equals(match) && !"any".equals(match))
        throw invalid(FsParserMessages.TAG_MATCH_VALUE, match);
      if (args.values("--tag-filter").size() < 2) {
        throw invalid(FsParserMessages.TAG_MATCH_COUNT);
      }
    } else if (args.values("--tag-filter").size() >= 2) {
      throw invalid(FsParserMessages.TAG_FILTER_MATCH);
    }
  }

  private static void validateAggregates(String value) {
    List<String> seen = new ArrayList<>();
    for (String aggregate : value.split(",", -1)) {
      if (!("count".equals(aggregate)
              || "min".equals(aggregate)
              || "max".equals(aggregate)
              || "sum".equals(aggregate)
              || "avg".equals(aggregate)
              || "median".equals(aggregate))
          || seen.contains(aggregate)) {
        throw invalid(CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F, "stats", value);
      }
      seen.add(aggregate);
    }
  }

  private static long parseLong(String command, String option, String value, boolean positive) {
    try {
      if (value == null || value.isEmpty() || value.charAt(0) == '-')
        throw new NumberFormatException();
      if (value.length() > 1 && value.charAt(0) == '0') {
        throw new NumberFormatException();
      }
      for (int i = 0; i < value.length(); i++) {
        if (value.charAt(i) < '0' || value.charAt(i) > '9') {
          throw new NumberFormatException();
        }
      }
      long parsed = Long.parseLong(value);
      if (parsed < 0 || (positive && parsed == 0)) throw new NumberFormatException();
      return parsed;
    } catch (NumberFormatException e) {
      throw invalid(FsParserMessages.OPTION_VALUE, option, value);
    }
  }

  private static String parseTimestamp(String command, String option, String value) {
    if (value == null) return null;
    try {
      if (value.isEmpty() || "+".equals(value)) throw new NumberFormatException();
      int start = value.charAt(0) == '-' ? 1 : 0;
      if (start == value.length() || (value.charAt(start) == '0' && start + 1 < value.length())) {
        throw new NumberFormatException();
      }
      for (int i = start; i < value.length(); i++) {
        if (value.charAt(i) < '0' || value.charAt(i) > '9') {
          throw new NumberFormatException();
        }
      }
      if (value.startsWith("-0")) throw new NumberFormatException();
      Long.parseLong(value);
      return value;
    } catch (NumberFormatException e) {
      throw invalid(FsParserMessages.OPTION_VALUE, option, value);
    }
  }

  private static void validateCutFields(String fields) {
    try {
      for (String field : fields.split(",", -1)) {
        int dash = field.indexOf('-');
        if (dash < 0) {
          unsignedInteger("cut", "-f", field, true);
        } else {
          if (field.length() == 1) {
            throw new IllegalArgumentException();
          }
          int start = dash == 0 ? 1 : unsignedInteger("cut", "-f", field.substring(0, dash), true);
          int end =
              dash == field.length() - 1
                  ? Integer.MAX_VALUE
                  : unsignedInteger("cut", "-f", field.substring(dash + 1), true);
          if (start > end) {
            throw invalid(
                CliMessages
                    .EXCEPTION_INVALID_CUT_FIELDS_ARG_USE_POSITIVE_FIELD_NUMBERS_OR_ASCENDING_RANGES_95F4C873,
                fields);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      throw invalid(
          CliMessages
              .EXCEPTION_INVALID_CUT_FIELDS_ARG_USE_POSITIVE_FIELD_NUMBERS_OR_ASCENDING_RANGES_95F4C873,
          fields);
    }
  }

  private static IllegalArgumentException invalid(String template, Object... values) {
    return new IllegalArgumentException(String.format(template, values));
  }

  /** Shared token consumption keeps options, duplicate checks and operand counts consistent. */
  private static class Arguments {
    private final String command;
    private final Map<String, String> options = new HashMap<>();
    private final Map<String, List<String>> repeated = new HashMap<>();
    private final List<String> operands = new ArrayList<>();
    private final List<String> operandPatterns = new ArrayList<>();

    private Arguments(String command, List<String> tokens, List<FsShellWords.Word> words) {
      this.command = command;
      boolean optionsEnded = false;
      for (int i = 1; i < tokens.size(); i++) {
        String token = tokens.get(i);
        if (!optionsEnded && "--".equals(token)) {
          if (i + 1 == tokens.size()) {
            throw invalid(CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "--");
          }
          optionsEnded = true;
        } else if (!optionsEnded && token.startsWith("-") && token.length() > 1) {
          if ("--help".equals(token) || "-h".equals(token)) {
            throw new IllegalArgumentException(
                CliMessages
                    .MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
          }
          if (("ls".equals(command) || "ll".equals(command))
              && !token.startsWith("-f")
              && !token.startsWith("--format")) {
            for (int j = 1; j < token.length(); j++) {
              String flag = "-" + token.charAt(j);
              if (!"-l".equals(flag) && !"-a".equals(flag) && !"-R".equals(flag)) {
                throw invalid(
                    CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, command, flag);
              }
              put(flag, "");
            }
            continue;
          }
          if (("head".equals(command) || "tail".equals(command))
              && token.charAt(1) >= '0'
              && token.charAt(1) <= '9') {
            put("-n", token.substring(1));
            continue;
          }
          String flag = token;
          String attached = null;
          if (token.length() > 2 && !token.startsWith("--") && allShortFlags(token)) {
            for (int j = 1; j < token.length(); j++) {
              put("-" + token.charAt(j), "");
            }
            continue;
          }
          if (token.length() > 2 && !token.startsWith("--") && takesValue(token.substring(0, 2))) {
            flag = token.substring(0, 2);
            attached = token.substring(2);
          }
          if (token.startsWith("--") && token.contains("=")) {
            int equals = token.indexOf('=');
            flag = token.substring(0, equals);
            attached = token.substring(equals + 1);
          }
          if (isFlag(flag)) {
            put(flag, "");
          } else if (takesValue(flag)) {
            if (attached != null || (i + 1 < tokens.size() && !"--".equals(tokens.get(i + 1)))) {
              String value = attached != null ? attached : tokens.get(++i);
              if ("--help".equals(value)) {
                throw new IllegalArgumentException(
                    CliMessages
                        .MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
              }
              if ("--format".equals(flag) && !"tail".equals(command)) flag = "-f";
              if ("--output".equals(flag)) flag = "-o";
              if ("--limit".equals(flag)) flag = "-n";
              if ("--device".equals(flag)) flag = "-d";
              if ("--table".equals(flag)) flag = "-t";
              if ("--measurements".equals(flag)) flag = "-m";
              if ("--tag-filter".equals(flag)) {
                if (i + 1 >= tokens.size() || "--".equals(tokens.get(i + 1))) {
                  throw invalid(
                      CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, flag);
                }
                String op = tokens.get(++i);
                if (!"eq".equals(op)
                    && !"neq".equals(op)
                    && !"regexp".equals(op)
                    && !"is-null".equals(op)
                    && !"not-null".equals(op)) {
                  throw invalid(FsParserMessages.TAG_OPERATOR, op);
                }
                String filter = value + " " + op;
                boolean requiresValue = "eq".equals(op) || "neq".equals(op) || "regexp".equals(op);
                if (requiresValue) {
                  if (i + 1 >= tokens.size() || "--".equals(tokens.get(i + 1))) {
                    throw invalid(
                        CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, flag);
                  }
                  filter += " " + tokens.get(++i);
                }
                repeated.computeIfAbsent(flag, ignored -> new ArrayList<>()).add(filter);
                options.putIfAbsent(flag, filter);
              } else if (("-m".equals(flag) || "--measurements".equals(flag))
                  && !"mkdir".equals(command)) {
                List<String> measurements =
                    repeated.computeIfAbsent(flag, ignored -> new ArrayList<>());
                if (measurements.contains(value)) {
                  throw invalid(FsParserMessages.DUPLICATE_MEASUREMENT, value);
                }
                measurements.add(value);
                options.putIfAbsent(flag, value);
              } else {
                put(flag, value);
              }
            } else {
              throw invalid(
                  CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, flag);
            }
          } else {
            throw invalid(
                CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, command, token);
          }
        } else {
          operands.add(token);
          operandPatterns.add(words.get(i).getGlobPattern());
        }
      }
    }

    private List<String> pathPatterns(FilesystemCommand parsed) {
      int firstPath = parsed.getType() == FilesystemCommand.Type.GREP ? 1 : 0;
      if (operands.size() - firstPath != parsed.getPaths().size()) return Collections.emptyList();
      return new ArrayList<>(operandPatterns.subList(firstPath, operandPatterns.size()));
    }

    private boolean isFlag(String flag) {
      return ("wc".equals(command) && "-c".equals(flag))
          || ("rm".equals(command) && ("-r".equals(flag) || "-f".equals(flag) || "-i".equals(flag)))
          || (("mv".equals(command) || "cp".equals(command))
              && ("-f".equals(flag) || "-i".equals(flag) || "-n".equals(flag)))
          || ("mkdir".equals(command) && "-p".equals(flag))
          || ("tee".equals(command) && "-a".equals(flag))
          || ("tail".equals(command) && "-f".equals(flag))
          || (("cut".equals(command) || "paste".equals(command)) && "-s".equals(flag))
          || ("grep".equals(command)
              && ("-F".equals(flag)
                  || "-E".equals(flag)
                  || "-i".equals(flag)
                  || "-v".equals(flag)
                  || "-n".equals(flag)))
          || (("export".equals(command) || "sketch".equals(command)) && "--force".equals(flag));
    }

    private boolean allShortFlags(String token) {
      for (int j = 1; j < token.length(); j++) {
        if (!isFlag("-" + token.charAt(j))) return false;
      }
      return true;
    }

    private boolean takesValue(String flag) {
      switch (command) {
        case "wc":
          return false;
        case "meta":
        case "ls":
        case "ll":
          return "-f".equals(flag) || "--format".equals(flag);
        case "schema":
        case "count":
          return "-f".equals(flag)
              || "--format".equals(flag)
              || "-d".equals(flag)
              || "--device".equals(flag)
              || "-t".equals(flag)
              || "--table".equals(flag)
              || "-m".equals(flag)
              || "--measurements".equals(flag);
        case "stats":
          return "-f".equals(flag)
              || "--format".equals(flag)
              || "-d".equals(flag)
              || "--device".equals(flag)
              || "-t".equals(flag)
              || "--table".equals(flag)
              || "-m".equals(flag)
              || "--measurements".equals(flag)
              || "--start".equals(flag)
              || "--end".equals(flag)
              || "--tag-filter".equals(flag)
              || "--tag-match".equals(flag)
              || "--aggregates".equals(flag);
        case "sketch":
          return "-o".equals(flag) || "--output".equals(flag);
        case "export":
          if ("-o".equals(flag)
              || "--output".equals(flag)
              || "--output-dir".equals(flag)
              || "--type".equals(flag)) return true;
          if ("-f".equals(flag) || "--format".equals(flag)) return false;
        case "cat":
        case "head":
        case "tail":
          return "-n".equals(flag)
              || "--limit".equals(flag)
              || "-f".equals(flag)
              || "--format".equals(flag)
              || "-d".equals(flag)
              || "--device".equals(flag)
              || "-t".equals(flag)
              || "--table".equals(flag)
              || "-m".equals(flag)
              || "--measurements".equals(flag)
              || "--offset".equals(flag)
              || "--start".equals(flag)
              || "--end".equals(flag)
              || "--tag-filter".equals(flag)
              || "--tag-match".equals(flag)
              || ("tail".equals(command) && "-c".equals(flag));
        case "tree":
          return "-L".equals(flag);
        case "find":
          return "-name".equals(flag) || "-type".equals(flag) || "-maxdepth".equals(flag);
        case "cut":
          return "-d".equals(flag) || "-f".equals(flag) || "-b".equals(flag) || "-c".equals(flag);
        case "paste":
          return "-d".equals(flag);
        case "mkdir":
          return "-m".equals(flag);
        case "join":
          return "-t".equals(flag)
              || "-1".equals(flag)
              || "-2".equals(flag)
              || "-a".equals(flag)
              || "-v".equals(flag)
              || "-e".equals(flag)
              || "-o".equals(flag);
        default:
          return false;
      }
    }

    private void put(String option, String value) {
      if ("export".equals(command) && ("-d".equals(option) || "-t".equals(option))) {
        int index = 0;
        while (options.containsKey(option + "." + index)) {
          if (value.equals(options.get(option + "." + index))) {
            throw invalid(
                CliMessages.EXCEPTION_ARG_OPTION_SPECIFIED_MORE_THAN_ONCE_ARG_CEB275DB,
                command,
                option);
          }
          index++;
        }
        options.put(option + "." + index, value);
        options.putIfAbsent(option, value);
        return;
      }
      if (options.containsKey(option) && !"-m".equals(option) && !"--tag-filter".equals(option)) {
        throw invalid(
            CliMessages.EXCEPTION_ARG_OPTION_SPECIFIED_MORE_THAN_ONCE_ARG_CEB275DB,
            command,
            option);
      }
      options.put(option, value);
    }

    private List<String> values(String option) {
      return repeated.getOrDefault(option, java.util.Collections.emptyList());
    }

    private boolean has(String option) {
      return options.containsKey(option);
    }

    private String value(String option) {
      return options.get(option);
    }

    private String valueOrDefault(String option, String defaultValue) {
      return options.getOrDefault(option, defaultValue);
    }

    private String path(boolean required) {
      paths(required ? 1 : 0, 1);
      return operands.isEmpty() ? DEFAULT_PATH : operands.get(0);
    }

    private List<String> paths(int minimum, int maximum) {
      return paths(minimum, maximum, false);
    }

    private List<String> paths(int minimum, int maximum, boolean patternFirst) {
      if (operands.size() < minimum) {
        throw invalid(
            CliMessages.EXCEPTION_ARG_EXPECTED_AT_LEAST_ARG_PATH_ARGUMENT_S_2040D496,
            command,
            minimum);
      }
      if (operands.size() > maximum) {
        throw invalid(
            CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
            command,
            operands.get(maximum));
      }
      for (int i = patternFirst ? 1 : 0; i < operands.size(); i++) {
        if (operands.get(i).isEmpty()) {
          throw invalid(CliMessages.EXCEPTION_ARG_PATH_MUST_NOT_BE_EMPTY_FEC583BE, command);
        }
      }
      return operands;
    }
  }
}
