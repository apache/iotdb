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
import org.apache.iotdb.exception.ArgsErrorException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Retains readable full paths, escaping characters that cannot be used on Windows or Unix. */
final class ExportFileNames {
  private final Map<String, String> names = new HashMap<>();

  Path measurement(ExportConfig config, String measurement) throws ArgsErrorException {
    String name = encode((config.prefix.isEmpty() ? "" : config.prefix + "_") + measurement);
    // Leave room for the split index and extension on filesystems with 255-byte components.
    if (name.getBytes(StandardCharsets.UTF_8).length > 220) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_MEASUREMENT_OUTPUT_FILENAME_EXCEEDS_THE_SUPPORTED_LENGTH_ARG_FF01275D,
              measurement));
    }
    String key = Normalizer.normalize(name, Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
    String previous = names.putIfAbsent(key, measurement);
    if (previous != null && !previous.equals(measurement)) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_MEASUREMENTS_ARG_AND_ARG_MAP_TO_THE_SAME_OUTPUT_FILENAME_ARG_58E1FD4D,
              previous,
              measurement,
              name));
    }
    return config.targetDirectory.resolve(name);
  }

  private static String encode(String value) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char character = value.charAt(i);
      if (character < 32
          || "\\/:*?\"<>|%".indexOf(character) >= 0
          || (i == value.length() - 1 && (character == '.' || character == ' '))) {
        for (byte b : String.valueOf(character).getBytes(StandardCharsets.UTF_8)) {
          result.append('%');
          result.append(Character.toUpperCase(Character.forDigit((b & 0xff) >>> 4, 16)));
          result.append(Character.toUpperCase(Character.forDigit(b & 15, 16)));
        }
      } else {
        result.append(character);
      }
    }
    return result.toString();
  }

  static void checkOutputCollision(Path output, ExportConfig config) throws ArgsErrorException {
    checkLogCollision(output, config.logFile);
    Path input = config.measurementFile;
    if (input != null
        && (output.equals(input)
            || (java.nio.file.Files.exists(output)
                && java.nio.file.Files.exists(input)
                && sameFile(output, input)))) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_MEASUREMENT_INPUT_FILE_ARG_1CBEBB6C,
              output));
    }
  }

  static void checkLogCollision(Path output, Path log) throws ArgsErrorException {
    if (output.equals(log)
        || (java.nio.file.Files.exists(output)
            && java.nio.file.Files.exists(log)
            && sameFile(output, log))) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_LOG_FILE_ARG_F01BE79D,
              output));
    }
  }

  private static boolean sameFile(Path left, Path right) throws ArgsErrorException {
    try {
      return java.nio.file.Files.isSameFile(left, right);
    } catch (java.io.IOException e) {
      throw new ArgsErrorException(e.getMessage());
    }
  }
}
