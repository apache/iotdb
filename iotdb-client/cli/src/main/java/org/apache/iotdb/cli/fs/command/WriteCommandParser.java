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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Parses the schema declarations and options used by TsFile-Cli write. */
public final class WriteCommandParser {
  private static final Set<String> DATA_TYPES =
      new HashSet<>(
          Arrays.asList(
              "BOOLEAN",
              "INT32",
              "INT64",
              "FLOAT",
              "DOUBLE",
              "TEXT",
              "STRING",
              "BLOB",
              "DATE",
              "TIMESTAMP"));
  private static final Set<String> INTEGER_ENCODINGS =
      new HashSet<>(
          Arrays.asList(
              "PLAIN", "TS_2DIFF", "GORILLA", "ZIGZAG", "RLE", "SPRINTZ", "CHIMP", "RLBE"));
  private static final Set<String> FLOAT_ENCODINGS =
      new HashSet<>(Arrays.asList("PLAIN", "TS_2DIFF", "GORILLA", "SPRINTZ", "CHIMP", "RLBE"));
  private static final Set<String> COMPRESSIONS =
      new HashSet<>(Arrays.asList("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "LZ4", "ZSTD", "LZMA2"));

  private WriteCommandParser() {}

  /** Tokens include the command name and have already undergone shell unquoting. */
  public static WriteOptions parse(List<String> tokens) {
    if (tokens == null || tokens.isEmpty() || !"write".equalsIgnoreCase(tokens.get(0))) {
      throw invalid(CliMessages.EXCEPTION_WRITE_COMMAND_IS_REQUIRED_6DD7F72C);
    }
    String table = null;
    String input = null;
    String output = null;
    boolean verbose = false;
    List<WriteOptions.Column> columns = new ArrayList<>();
    Set<String> columnNames = new HashSet<>();
    List<String[]> overrides = new ArrayList<>();
    for (int i = 1; i < tokens.size(); i++) {
      String option = tokens.get(i);
      switch (option) {
        case "-t":
        case "--table":
          if (table != null)
            throw invalid(CliMessages.EXCEPTION_ARG_SPECIFIED_MORE_THAN_ONCE_255E2870, "--table");
          table = normalizedIdentifier(value(tokens, ++i, option));
          break;
        case "--tag":
        case "--field":
          String name = normalizedIdentifier(value(tokens, ++i, option));
          String type = value(tokens, ++i, option);
          if (!DATA_TYPES.contains(type))
            throw invalid(CliMessages.EXCEPTION_UNKNOWN_TYPE_ARG_0FCF53E3, type);
          if ("--tag".equals(option) && !"STRING".equals(type)) {
            throw invalid(CliMessages.EXCEPTION_TAG_COLUMN_ARG_MUST_USE_STRING_93F86185, name);
          }
          if (!columnNames.add(name))
            throw invalid(CliMessages.EXCEPTION_DUPLICATE_COLUMN_NAME_ARG_AB717F15, name);
          columns.add(
              new WriteOptions.Column(name, type, "--tag".equals(option) ? "TAG" : "FIELD"));
          break;
        case "--encoding":
        case "--compression":
          String overrideType = value(tokens, ++i, option);
          String setting = value(tokens, ++i, option);
          overrides.add(new String[] {option, overrideType, setting});
          break;
        case "-i":
        case "--input":
          if (input != null)
            throw invalid(CliMessages.EXCEPTION_CHOOSE_EXACTLY_ONE_OF_INPUT_OR_STDIN_966F4870);
          input = value(tokens, ++i, option);
          break;
        case "--stdin":
          if (input != null)
            throw invalid(CliMessages.EXCEPTION_CHOOSE_EXACTLY_ONE_OF_INPUT_OR_STDIN_966F4870);
          input = "-";
          break;
        case "-o":
        case "--output":
          if (output != null)
            throw invalid(CliMessages.EXCEPTION_ARG_SPECIFIED_MORE_THAN_ONCE_255E2870, "--output");
          output = value(tokens, ++i, option);
          break;
        case "-v":
        case "--verbose":
          verbose = true;
          break;
        default:
          throw invalid(CliMessages.EXCEPTION_UNKNOWN_WRITE_OPTION_ARG_EAF8B4F9, option);
      }
    }
    if (table == null) throw invalid(CliMessages.EXCEPTION_WRITE_REQUIRES_T_TABLE_4B9990EB);
    if (columns.stream().noneMatch(column -> "FIELD".equals(column.getCategory()))) {
      throw invalid(CliMessages.EXCEPTION_WRITE_REQUIRES_AT_LEAST_ONE_FIELD_COLUMN_E714D04C);
    }
    if (input == null)
      throw invalid(CliMessages.EXCEPTION_CHOOSE_EXACTLY_ONE_OF_INPUT_OR_STDIN_966F4870);
    if (output == null) throw invalid(CliMessages.EXCEPTION_WRITE_REQUIRES_O_OUTPUT_25A5E793);
    Map<String, String> encodings = new LinkedHashMap<>();
    Map<String, String> compressions = new LinkedHashMap<>();
    validateOverrides(columns, overrides, encodings, compressions);
    return new WriteOptions(table, columns, input, output, verbose, encodings, compressions);
  }

  private static String value(List<String> tokens, int index, String option) {
    if (index >= tokens.size() || tokens.get(index).isEmpty()) {
      throw invalid(CliMessages.EXCEPTION_MISSING_VALUE_FOR_ARG_0AF4A1C7, option);
    }
    return tokens.get(index);
  }

  private static String normalizedIdentifier(String name) {
    if (name.isEmpty())
      throw invalid(
          CliMessages
              .EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704,
          name);
    StringBuilder normalized = new StringBuilder(name.length());
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (c < 0x20 || (c >= 0x7F && c <= 0x9F) || c == '\uFEFF') {
        throw invalid(
            CliMessages
                .EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704,
            name);
      }
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= name.length() || !Character.isLowSurrogate(name.charAt(i + 1))) {
          throw invalid(
              CliMessages
                  .EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704,
              name);
        }
        normalized.append(c).append(name.charAt(++i));
      } else if (Character.isLowSurrogate(c)) {
        throw invalid(
            CliMessages
                .EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704,
            name);
      } else {
        normalized.append(c >= 'A' && c <= 'Z' ? (char) (c + 'a' - 'A') : c);
      }
    }
    String result = normalized.toString();
    if ("time".equals(result))
      throw invalid(CliMessages.EXCEPTION_NAME_ARG_IS_RESERVED_59CAD66D, name);
    return result;
  }

  private static void validateOverrides(
      List<WriteOptions.Column> columns,
      List<String[]> overrides,
      Map<String, String> encodings,
      Map<String, String> compressions) {
    Set<String> usedTypes = new HashSet<>();
    for (WriteOptions.Column column : columns) usedTypes.add(column.getType());
    for (String[] override : overrides) {
      String option = override[0];
      String type = override[1];
      String setting = override[2];
      if (!DATA_TYPES.contains(type))
        throw invalid(
            CliMessages
                .EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_MUST_BE_A_USED_CANONICAL_DATA_TYPE_C36A91EC,
            type);
      if (!usedTypes.contains(type))
        throw invalid(
            CliMessages
                .EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_IS_NOT_USED_BY_ANY_DECLARED_TAG_OR_FIELD_E0A808E7,
            type);
      boolean encoding = "--encoding".equals(option);
      Map<String, String> settings = encoding ? encodings : compressions;
      if (settings.containsKey(type)) {
        throw invalid(
            CliMessages.EXCEPTION_ARG_FOR_DATA_TYPE_ARG_SPECIFIED_MORE_THAN_ONCE_6D9687F3,
            option,
            type);
      }
      if (encoding && !encodingSupported(type, setting)) {
        throw invalid(
            CliMessages.EXCEPTION_ENCODING_ARG_IS_NOT_SUPPORTED_FOR_DATA_TYPE_ARG_218D4FB0,
            setting,
            type);
      }
      if (!encoding && !COMPRESSIONS.contains(setting)) {
        throw invalid(CliMessages.EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_18307F13, setting);
      }
      settings.put(type, setting);
    }
  }

  private static boolean encodingSupported(String type, String encoding) {
    switch (type) {
      case "BOOLEAN":
        return "PLAIN".equals(encoding);
      case "INT32":
      case "INT64":
      case "DATE":
      case "TIMESTAMP":
        return INTEGER_ENCODINGS.contains(encoding);
      case "DOUBLE":
        return "CAMEL".equals(encoding) || FLOAT_ENCODINGS.contains(encoding);
      case "FLOAT":
        return FLOAT_ENCODINGS.contains(encoding);
      case "TEXT":
      case "STRING":
      case "BLOB":
        return "PLAIN".equals(encoding) || "DICTIONARY".equals(encoding);
      default:
        return false;
    }
  }

  private static IllegalArgumentException invalid(String template, Object... values) {
    return new IllegalArgumentException(String.format(template, values));
  }
}
