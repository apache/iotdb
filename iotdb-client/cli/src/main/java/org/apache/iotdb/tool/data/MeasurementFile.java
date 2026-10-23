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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.exception.ArgsErrorException;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Validates the entire UTF-8 input before any metadata or data query is issued. */
record MeasurementFile(Path source, List<PatternLine> patterns, int lines, int duplicates) {
  record PatternLine(int line, String original, PartialPath path) {}

  static MeasurementFile read(Path source) throws IOException, ArgsErrorException {
    if (!Files.isRegularFile(source) || !Files.isReadable(source)) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_MEASUREMENT_INPUT_MUST_BE_A_READABLE_REGULAR_FILE_ARG_43D4A272,
              source));
    }
    Map<String, PatternLine> distinct = new LinkedHashMap<>();
    int lineNumber = 0;
    int duplicates = 0;
    String original = "";
    try (BufferedInputStream reader = new BufferedInputStream(Files.newInputStream(source))) {
      byte[] bytes;
      while ((bytes = readLine(reader)) != null) {
        try {
          original =
              StandardCharsets.UTF_8
                  .newDecoder()
                  .onMalformedInput(CodingErrorAction.REPORT)
                  .onUnmappableCharacter(CodingErrorAction.REPORT)
                  .decode(ByteBuffer.wrap(bytes))
                  .toString();
        } catch (IOException e) {
          throw new ArgsErrorException(
              String.format(
                  CliMessages
                      .EXCEPTION_INVALID_MEASUREMENT_INPUT_ARG_AT_LINE_ARG_CONTENT_ARG_ARG_27A0F948,
                  source,
                  lineNumber + 1,
                  new String(bytes, StandardCharsets.UTF_8),
                  e.getMessage()));
        }
        lineNumber++;
        String pattern = original;
        if (lineNumber == 1 && pattern.startsWith("\uFEFF")) {
          pattern = pattern.substring(1);
        }
        pattern = pattern.trim();
        if (pattern.isEmpty() || pattern.startsWith("#")) {
          continue;
        }
        try {
          PartialPath path = new PartialPath(pattern);
          if (!"root".equals(path.getFirstNode()) || path.getNodeLength() < 2) {
            throw new ArgsErrorException(
                CliMessages
                    .EXCEPTION_A_COMPLETE_IOTDB_PATH_PATTERN_STARTING_WITH_ROOT_IS_REQUIRED_7D000A79);
          }
          PatternLine previous =
              distinct.putIfAbsent(path.getFullPath(), new PatternLine(lineNumber, original, path));
          if (previous != null) {
            duplicates++;
          }
        } catch (IllegalPathException | ArgsErrorException e) {
          throw new ArgsErrorException(
              String.format(
                  CliMessages
                      .EXCEPTION_INVALID_MEASUREMENT_INPUT_ARG_AT_LINE_ARG_CONTENT_ARG_ARG_27A0F948,
                  source,
                  lineNumber,
                  original,
                  e.getMessage()));
        }
      }
    } catch (IOException e) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_CANNOT_READ_UTF_8_MEASUREMENT_FILE_ARG_AT_LINE_ARG_ARG_A5739D3F,
              source,
              lineNumber + 1,
              e.getMessage()),
          e);
    }
    if (distinct.isEmpty()) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_MEASUREMENT_INPUT_CONTAINS_NO_PATH_PATTERNS_ARG_96760070,
              source));
    }
    return new MeasurementFile(
        source, List.copyOf(new ArrayList<>(distinct.values())), lineNumber, duplicates);
  }

  private static byte[] readLine(BufferedInputStream input) throws IOException {
    ByteArrayOutputStream line = new ByteArrayOutputStream();
    int next;
    while ((next = input.read()) != -1) {
      if (next == '\n') {
        return line.toByteArray();
      }
      if (next == '\r') {
        input.mark(1);
        if (input.read() != '\n') {
          input.reset();
        }
        return line.toByteArray();
      }
      line.write(next);
    }
    return line.size() == 0 ? null : line.toByteArray();
  }
}
