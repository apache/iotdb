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

package org.apache.iotdb.cli.fs;

import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.i18n.FsLocalMessages;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Text transformations shared by virtual files and standard input. */
public final class UnixTextCommands {
  private UnixTextCommands() {}

  public static int execute(FilesystemCommand command, List<List<String>> inputs, PrintStream out) {
    switch (command.getType()) {
      case GREP:
        return grep(command, inputs, out);
      case CUT:
        cut(command, inputs, out);
        return 0;
      case PASTE:
        paste(command, inputs, out);
        return 0;
      case JOIN:
        join(command, inputs, out);
        return 0;
      default:
        throw new IllegalArgumentException(
            String.format(FsLocalMessages.UNSUPPORTED_TEXT_COMMAND, command.getType().name()));
    }
  }

  public static boolean matchesName(String name, String glob) {
    StringBuilder expression = new StringBuilder();
    boolean inClass = false;
    for (int i = 0; i < glob.length(); i++) {
      char c = glob.charAt(i);
      if (c == '\\' && i + 1 < glob.length()) {
        expression.append(Pattern.quote(String.valueOf(glob.charAt(++i))));
      } else if (c == '[') {
        inClass = true;
        expression.append('[');
        if (i + 1 < glob.length() && glob.charAt(i + 1) == '!') {
          expression.append('^');
          i++;
        }
      } else if (c == ']' && inClass) {
        inClass = false;
        expression.append(']');
      } else if (inClass) {
        expression.append(c);
      } else if (c == '*') {
        expression.append(".*");
      } else if (c == '?') {
        expression.append('.');
      } else {
        expression.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return compile(expression.toString(), 0).matcher(name).matches();
  }

  private static int grep(FilesystemCommand command, List<List<String>> inputs, PrintStream out) {
    String expression = command.getPattern();
    if (command.hasOption("-F")) {
      expression = Pattern.quote(expression);
    } else if (!command.hasOption("-E")) {
      expression = basicExpression(expression);
    }
    Pattern pattern =
        compile(
            expression,
            command.hasOption("-i") ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0);
    boolean any = false;
    for (int file = 0; file < inputs.size(); file++) {
      List<String> lines = inputs.get(file);
      for (int row = 0; row < lines.size(); row++) {
        String line = lines.get(row);
        if (pattern.matcher(line).find() != command.hasOption("-v")) {
          any = true;
          if (inputs.size() > 1) out.print(command.getPaths().get(file) + ":");
          if (command.hasOption("-n")) out.print((row + 1) + ":");
          out.println(line);
        }
      }
    }
    return any ? 0 : 1;
  }

  private static Pattern compile(String expression, int flags) {
    try {
      return Pattern.compile(expression, flags);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format(CliMessages.MESSAGE_FS_INVALID_PATTERN, expression), e);
    }
  }

  // BRE uses escaped grouping and repetition; Java's parser uses ERE-style operators.
  private static String basicExpression(String expression) {
    StringBuilder converted = new StringBuilder();
    boolean inClass = false;
    for (int i = 0; i < expression.length(); i++) {
      char c = expression.charAt(i);
      if (c == '\\' && i + 1 < expression.length()) {
        char next = expression.charAt(++i);
        if (!inClass && "()+?|{}".indexOf(next) >= 0) converted.append(next);
        else converted.append('\\').append(next);
      } else {
        if (c == '[') inClass = true;
        if (!inClass && "()+?|{}".indexOf(c) >= 0) converted.append('\\');
        converted.append(c);
        if (c == ']') inClass = false;
      }
    }
    return converted.toString();
  }

  private static void cut(FilesystemCommand command, List<List<String>> inputs, PrintStream out) {
    List<int[]> ranges = ranges(command.getPattern());
    String delimiter = command.getOption();
    for (List<String> lines : inputs) {
      for (String line : lines) {
        if (command.hasOption("-b")) {
          byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
          for (int i = 0; i < bytes.length; i++) if (selected(i + 1, ranges)) out.write(bytes[i]);
          out.println();
        } else if (command.hasOption("-c")) {
          int[] characters = line.codePoints().toArray();
          StringBuilder result = new StringBuilder();
          for (int i = 0; i < characters.length; i++) {
            if (selected(i + 1, ranges)) result.appendCodePoint(characters[i]);
          }
          out.println(result);
        } else {
          if (!line.contains(delimiter)) {
            if (!command.hasOption("-s")) out.println(line);
            continue;
          }
          String[] fields = line.split(Pattern.quote(delimiter), -1);
          List<String> result = new ArrayList<>();
          for (int i = 0; i < fields.length; i++) {
            if (selected(i + 1, ranges)) result.add(fields[i]);
          }
          out.println(String.join(delimiter, result));
        }
      }
    }
  }

  private static List<int[]> ranges(String specification) {
    List<int[]> ranges = new ArrayList<>();
    for (String item : specification.split(",")) {
      int dash = item.indexOf('-');
      if (dash < 0) {
        int index = Integer.parseInt(item);
        ranges.add(new int[] {index, index});
      } else {
        int start = dash == 0 ? 1 : Integer.parseInt(item.substring(0, dash));
        int end =
            dash == item.length() - 1
                ? Integer.MAX_VALUE
                : Integer.parseInt(item.substring(dash + 1));
        ranges.add(new int[] {start, end});
      }
    }
    return ranges;
  }

  private static boolean selected(int index, List<int[]> ranges) {
    for (int[] range : ranges) if (index >= range[0] && index <= range[1]) return true;
    return false;
  }

  private static void paste(FilesystemCommand command, List<List<String>> inputs, PrintStream out) {
    List<String> delimiters = pasteDelimiters(command.optionValue("-d", "\t"));
    if (command.hasOption("-s")) {
      for (List<String> lines : inputs) {
        for (int row = 0; row < lines.size(); row++) {
          if (row > 0) out.print(delimiters.get((row - 1) % delimiters.size()));
          out.print(lines.get(row));
        }
        out.println();
      }
    } else {
      int[] positions = new int[inputs.size()];
      int standardPosition = 0;
      while (true) {
        boolean any = false;
        StringBuilder line = new StringBuilder();
        for (int file = 0; file < inputs.size(); file++) {
          if (file > 0) line.append(delimiters.get((file - 1) % delimiters.size()));
          boolean standard = "-".equals(command.getPaths().get(file));
          int position = standard ? standardPosition : positions[file];
          if (position < inputs.get(file).size()) {
            any = true;
            line.append(inputs.get(file).get(position));
            if (standard) standardPosition++;
            else positions[file]++;
          }
        }
        if (!any) break;
        out.println(line);
      }
    }
  }

  private static List<String> pasteDelimiters(String value) {
    List<String> delimiters = new ArrayList<>();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' && i + 1 < value.length()) {
        c = value.charAt(++i);
        delimiters.add(c == 't' ? "\t" : c == 'n' ? "\n" : c == '0' ? "" : String.valueOf(c));
      } else delimiters.add(String.valueOf(c));
    }
    return delimiters.isEmpty() ? Collections.singletonList("") : delimiters;
  }

  private static void join(FilesystemCommand command, List<List<String>> inputs, PrintStream out) {
    String[] indexes = command.getPattern().split(",");
    int leftKey = Integer.parseInt(indexes[0]) - 1;
    int rightKey = Integer.parseInt(indexes[1]) - 1;
    List<String[]> left = splitJoinInput(inputs.get(0), command.getOption());
    List<String[]> right = splitJoinInput(inputs.get(1), command.getOption());
    checkSorted(left, leftKey, command.getPaths().get(0));
    checkSorted(right, rightKey, command.getPaths().get(1));
    int l = 0;
    int r = 0;
    while (l < left.size() || r < right.size()) {
      int comparison =
          l == left.size()
              ? 1
              : r == right.size()
                  ? -1
                  : field(left.get(l), leftKey).compareTo(field(right.get(r), rightKey));
      if (comparison < 0) {
        if (unpaired(command, "1")) printJoin(command, left.get(l), null, leftKey, rightKey, out);
        l++;
      } else if (comparison > 0) {
        if (unpaired(command, "2")) printJoin(command, null, right.get(r), leftKey, rightKey, out);
        r++;
      } else {
        int lEnd = l + 1;
        int rEnd = r + 1;
        while (lEnd < left.size()
            && field(left.get(lEnd), leftKey).equals(field(left.get(l), leftKey))) lEnd++;
        while (rEnd < right.size()
            && field(right.get(rEnd), rightKey).equals(field(right.get(r), rightKey))) rEnd++;
        if (!command.hasOption("-v")) {
          for (int i = l; i < lEnd; i++)
            for (int j = r; j < rEnd; j++) {
              printJoin(command, left.get(i), right.get(j), leftKey, rightKey, out);
            }
        }
        l = lEnd;
        r = rEnd;
      }
    }
  }

  private static List<String[]> splitJoinInput(List<String> lines, String delimiter) {
    List<String[]> result = new ArrayList<>();
    for (String line : lines) {
      result.add(
          delimiter.isEmpty()
              ? line.trim().split("[ \\t]+", -1)
              : line.split(Pattern.quote(delimiter), -1));
    }
    return result;
  }

  private static void checkSorted(List<String[]> rows, int key, String path) {
    for (int i = 1; i < rows.size(); i++) {
      if (field(rows.get(i - 1), key).compareTo(field(rows.get(i), key)) > 0) {
        throw new IllegalArgumentException(
            String.format(CliMessages.MESSAGE_FS_JOIN_UNSORTED, path));
      }
    }
  }

  private static boolean unpaired(FilesystemCommand command, String side) {
    return side.equals(command.optionValue("-a", "")) || side.equals(command.optionValue("-v", ""));
  }

  private static String field(String[] row, int index) {
    return row == null || index >= row.length ? "" : row[index];
  }

  private static void printJoin(
      FilesystemCommand command,
      String[] left,
      String[] right,
      int leftKey,
      int rightKey,
      PrintStream out) {
    List<String> result = new ArrayList<>();
    String key = left == null ? field(right, rightKey) : field(left, leftKey);
    if (command.hasOption("-o")) {
      for (String selected : command.optionValue("-o", "").split("[, ]+")) {
        result.add(
            "0".equals(selected)
                ? key
                : field(
                    selected.charAt(0) == '1' ? left : right,
                    Integer.parseInt(selected.substring(2)) - 1));
      }
    } else {
      result.add(key);
      if (left != null) for (int i = 0; i < left.length; i++) if (i != leftKey) result.add(left[i]);
      if (right != null)
        for (int i = 0; i < right.length; i++) if (i != rightKey) result.add(right[i]);
    }
    String replacement = command.optionValue("-e", "");
    for (int i = 0; i < result.size(); i++) if (result.get(i).isEmpty()) result.set(i, replacement);
    out.println(String.join(command.getOption().isEmpty() ? " " : command.getOption(), result));
  }
}
