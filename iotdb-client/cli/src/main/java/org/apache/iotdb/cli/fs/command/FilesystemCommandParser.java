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

import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;

import java.util.ArrayList;
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
      if ("--help".equals(statement)) {
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
    try {
      tokens =
          new DefaultParser()
              .eofOnUnclosedQuote(true)
              .eofOnEscapedNewLine(true)
              .parse(line, line.length(), Parser.ParseContext.ACCEPT_LINE)
              .words();
    } catch (SyntaxError e) {
      return FilesystemCommand.invalid(
          CliMessages.MESSAGE_UNCLOSED_QUOTE_OR_ESCAPE_IN_FILESYSTEM_COMMAND_42C74084);
    }
    String command = tokens.get(0).toLowerCase(Locale.ROOT);
    if ("help".equals(command) || "--help".equals(command)) {
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
    if (tokens.size() == 2 && "--help".equals(tokens.get(1))) {
      return FilesystemCommand.path(FilesystemCommand.Type.HELP, command);
    }
    try {
      return parseCommand(command, new Arguments(command, tokens));
    } catch (IllegalArgumentException e) {
      return FilesystemCommand.invalid(e.getMessage());
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
      case EXIT:
        args.paths(0, 0);
        return FilesystemCommand.simple(type);
      case SCHEMA:
      case META:
      case STATS:
      case COUNT:
        return FilesystemCommand.path(type, args.path(false));
      case WC:
        if (args.has("-c")) {
          return FilesystemCommand.option(type, "-c", args.path(false));
        }
        throw invalid("wc supports only -c");
      case LS:
      case LL:
        String listPath = args.path(false);
        if (args.has("-R")) {
          return FilesystemCommand.tree(listPath, DEFAULT_TREE_DEPTH);
        }
        return FilesystemCommand.option(
            args.has("-l") ? FilesystemCommand.Type.LL : type,
            args.has("-a") ? "-a" : "",
            listPath);
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
        return FilesystemCommand.paths(type, args.paths(1, Integer.MAX_VALUE));
      case MV:
      case CP:
        return FilesystemCommand.paths(type, args.paths(2, 2));
      case HEAD:
      case TAIL:
        validateScope(args);
        int limit =
            args.has("-n")
                ? unsignedInteger(command, "-n", args.value("-n"), false)
                : DEFAULT_HEAD_LIMIT;
        String readPath = args.path(false);
        FilesystemCommand readCommand =
            type == FilesystemCommand.Type.HEAD
                ? FilesystemCommand.head(readPath, limit)
                : FilesystemCommand.tail(readPath, limit);
        return readCommand.withReadOptions(
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
      case GREP:
        // The first operand is a literal pattern and may be empty.
        args.paths(2, 2, true);
        return FilesystemCommand.pattern(type, args.operands.get(0), args.operands.get(1));
      case FIND:
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
        return FilesystemCommand.option(type, args.has("-r") ? "-r" : "", args.path(true));
      case TEE:
        if (!args.has("-a")) {
          throw invalid(CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "-a");
        }
        return FilesystemCommand.option(type, "-a", args.path(true));
      case CUT:
        if (!args.has("-f")) {
          throw invalid(CliMessages.EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB, command, "-f");
        }
        String cutDelimiter = args.valueOrDefault("-d", "\t");
        validateDelimiter(command, cutDelimiter);
        String fields = args.value("-f");
        validateCutFields(fields);
        return FilesystemCommand.cut(cutDelimiter, fields, args.path(true));
      case JOIN:
        String joinDelimiter = args.valueOrDefault("-t", "");
        if (args.has("-t")) {
          validateDelimiter(command, joinDelimiter);
        }
        int left = args.has("-1") ? unsignedInteger(command, "-1", args.value("-1"), true) : 1;
        int right = args.has("-2") ? unsignedInteger(command, "-2", args.value("-2"), true) : 1;
        return FilesystemCommand.join(joinDelimiter, left + "," + right, args.paths(2, 2));
      default:
        return FilesystemCommand.path(type, args.path(false));
    }
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
      throw invalid("Invalid output format: %s", value);
    }
    return value;
  }

  private static void validateScope(Arguments args) {
    if (args.has("-d") && args.has("-t")) {
      throw invalid("Options -d and -t are mutually exclusive");
    }
    if (args.has("--tag-match") && args.values("--tag-filter").isEmpty()) {
      throw invalid("--tag-match requires --tag-filter");
    }
    if (args.has("--tag-match")) {
      String match = args.value("--tag-match");
      if (!"all".equals(match) && !"any".equals(match))
        throw invalid("Invalid --tag-match: %s", match);
      if (args.values("--tag-filter").size() < 2) {
        throw invalid("--tag-match requires at least two tag filters");
      }
    } else if (args.values("--tag-filter").size() >= 2) {
      throw invalid("two or more tag filters require --tag-match all or any");
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
      throw invalid("Invalid value for %s: %s", option, value);
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
      throw invalid("Invalid value for %s: %s", option, value);
    }
  }

  private static void validateCutFields(String fields) {
    try {
      for (String field : fields.split(",", -1)) {
        int dash = field.indexOf('-');
        if (dash < 0) {
          unsignedInteger("cut", "-f", field, true);
        } else {
          int start = unsignedInteger("cut", "-f", field.substring(0, dash), true);
          int end = unsignedInteger("cut", "-f", field.substring(dash + 1), true);
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

    private Arguments(String command, List<String> tokens) {
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
          if ("--help".equals(token)) {
            throw new IllegalArgumentException(
                CliMessages
                    .MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
          }
          if ("ls".equals(command) || "ll".equals(command)) {
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
          if (("cut".equals(command) && (token.startsWith("-d") || token.startsWith("-f")))
              || ("join".equals(command) && token.startsWith("-t"))) {
            flag = token.substring(0, 2);
            if (token.length() > 2) {
              attached = token.substring(2);
            }
          }
          if (isFlag(flag)) {
            put(flag, "");
          } else if (takesValue(flag)) {
            if (attached != null) {
              put(flag, attached);
            } else if (i + 1 < tokens.size() && !"--".equals(tokens.get(i + 1))) {
              String value = tokens.get(++i);
              if ("--help".equals(value)) {
                throw new IllegalArgumentException(
                    CliMessages
                        .MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5);
              }
              if ("--format".equals(flag)) flag = "-f";
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
                  throw invalid("Invalid --tag-filter operator: %s", op);
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
              } else if ("-m".equals(flag) || "--measurements".equals(flag)) {
                List<String> measurements =
                    repeated.computeIfAbsent(flag, ignored -> new ArrayList<>());
                if (measurements.contains(value)) {
                  throw invalid("measurement '%s' specified more than once", value);
                }
                measurements.add(value);
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
        }
      }
    }

    private boolean isFlag(String flag) {
      return ("wc".equals(command) && "-c".equals(flag))
          || ("rm".equals(command) && "-r".equals(flag))
          || ("tee".equals(command) && "-a".equals(flag));
    }

    private boolean takesValue(String flag) {
      switch (command) {
        case "wc":
          return false;
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
              || "--tag-match".equals(flag);
        case "tree":
          return "-L".equals(flag);
        case "find":
          return "-name".equals(flag);
        case "cut":
          return "-d".equals(flag) || "-f".equals(flag);
        case "join":
          return "-t".equals(flag) || "-1".equals(flag) || "-2".equals(flag);
        default:
          return false;
      }
    }

    private void put(String option, String value) {
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
