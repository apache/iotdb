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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Utility methods for encoding and decoding load attributes into directory structures. */
public final class ActiveLoadPathHelper {

  private static final String SEGMENT_SEPARATOR = "-";

  private static final List<String> KEY_ORDER =
      Collections.unmodifiableList(
          Arrays.asList(
              LoadTsFileConfigurator.DATABASE_NAME_KEY,
              LoadTsFileConfigurator.DATABASE_LEVEL_KEY,
              LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY,
              LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY,
              LoadTsFileConfigurator.VERIFY_KEY,
              LoadTsFileConfigurator.DATABASE_KEY,
              LoadTsFileConfigurator.PIPE_GENERATED_KEY));

  private ActiveLoadPathHelper() {
    throw new IllegalStateException("Utility class");
  }

  public static Map<String, String> buildAttributes(
      final String databaseName,
      final Integer databaseLevel,
      final Boolean convertOnTypeMismatch,
      final Boolean verify,
      final Long tabletConversionThresholdBytes,
      final Boolean pipeGenerated) {
    final Map<String, String> attributes = new LinkedHashMap<>();
    if (Objects.nonNull(databaseName) && !databaseName.isEmpty()) {
      attributes.put(LoadTsFileConfigurator.DATABASE_NAME_KEY, databaseName);
    }

    if (Objects.nonNull(databaseLevel)) {
      attributes.put(LoadTsFileConfigurator.DATABASE_LEVEL_KEY, databaseLevel.toString());
    }

    if (Objects.nonNull(convertOnTypeMismatch)) {
      attributes.put(
          LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY,
          Boolean.toString(convertOnTypeMismatch));
    }

    if (Objects.nonNull(tabletConversionThresholdBytes)) {
      attributes.put(
          LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY,
          tabletConversionThresholdBytes.toString());
    }

    if (Objects.nonNull(verify)) {
      attributes.put(LoadTsFileConfigurator.VERIFY_KEY, Boolean.toString(verify));
    }

    if (Objects.nonNull(pipeGenerated) && pipeGenerated) {
      attributes.put(LoadTsFileConfigurator.PIPE_GENERATED_KEY, Boolean.TRUE.toString());
    }
    return attributes;
  }

  public static File resolveTargetDir(final File baseDir, final Map<String, String> attributes) {
    File current = baseDir;
    for (final String key : KEY_ORDER) {
      final String value = attributes.get(key);
      if (value == null) {
        continue;
      }
      current = new File(current, formatSegment(key, value));
    }
    return current;
  }

  public static Map<String, String> parseAttributes(final File file, final File pendingDir) {
    if (file == null) {
      return Collections.emptyMap();
    }

    final Map<String, String> attributes = new HashMap<>();
    File current = file.getParentFile();
    while (current != null) {
      final String dirName = current.getName();
      if (pendingDir != null && current.equals(pendingDir)) {
        break;
      }
      for (final String key : KEY_ORDER) {
        final String prefix = key + SEGMENT_SEPARATOR;
        if (dirName.startsWith(prefix)) {
          extractAndValidateAttributeValue(key, dirName, prefix.length())
              .ifPresent(value -> attributes.putIfAbsent(key, value));
          break;
        }
      }
      current = current.getParentFile();
    }
    return attributes;
  }

  public static File findPendingDirectory(final File file) {
    if (file == null) {
      return null;
    }
    String[] dirs = IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs();
    File current = file;
    while (current != null) {
      for (final String dir : dirs) {
        if (current.isDirectory() && current.getAbsolutePath().equals(dir)) {
          return current;
        }
      }
      current = current.getParentFile();
    }
    return null;
  }

  public static void applyAttributesToStatement(
      final Map<String, String> attributes,
      final LoadTsFileStatement statement,
      final boolean defaultVerify) {

    Optional.ofNullable(attributes.get(LoadTsFileConfigurator.DATABASE_NAME_KEY))
        .filter(name -> !name.isEmpty())
        .ifPresent(statement::setDatabase);

    if (statement.getDatabase() == null || statement.getDatabase().isEmpty()) {
      Optional.ofNullable(attributes.get(LoadTsFileConfigurator.DATABASE_KEY))
          .filter(name -> !name.isEmpty())
          .ifPresent(statement::setDatabase);
    }

    Optional.ofNullable(attributes.get(LoadTsFileConfigurator.DATABASE_LEVEL_KEY))
        .ifPresent(
            level -> {
              try {
                statement.setDatabaseLevel(Integer.parseInt(level));
              } catch (final NumberFormatException ignored) {
                // keep the default when parsing fails
              }
            });

    Optional.ofNullable(attributes.get(LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY))
        .ifPresent(value -> statement.setConvertOnTypeMismatch(Boolean.parseBoolean(value)));

    Optional.ofNullable(attributes.get(LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY))
        .ifPresent(
            threshold -> {
              try {
                statement.setTabletConversionThresholdBytes(Long.parseLong(threshold));
              } catch (final NumberFormatException ignored) {
                // keep the default when parsing fails
              }
            });

    if (attributes.containsKey(LoadTsFileConfigurator.VERIFY_KEY)) {
      statement.setVerifySchema(
          Boolean.parseBoolean(attributes.get(LoadTsFileConfigurator.VERIFY_KEY)));
    } else {
      statement.setVerifySchema(defaultVerify);
    }

    if (attributes.containsKey(LoadTsFileConfigurator.PIPE_GENERATED_KEY)
        && Boolean.parseBoolean(attributes.get(LoadTsFileConfigurator.PIPE_GENERATED_KEY))) {
      statement.markIsGeneratedByPipe();
    }
  }

  public static boolean containsDatabaseName(final Map<String, String> attributes) {
    return attributes.containsKey(LoadTsFileConfigurator.DATABASE_NAME_KEY)
        || attributes.containsKey(LoadTsFileConfigurator.DATABASE_KEY);
  }

  private static String formatSegment(final String key, final String value) {
    return key + SEGMENT_SEPARATOR + encodeValue(value);
  }

  private static String encodeValue(final String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
    } catch (final UnsupportedEncodingException e) {
      // UTF-8 should always be supported; fallback to raw value when unexpected
      return value;
    }
  }

  private static Optional<String> extractAndValidateAttributeValue(
      final String key, final String dirName, final int prefixLength) {
    if (dirName.length() <= prefixLength) {
      return Optional.empty();
    }

    final String encodedValue = dirName.substring(prefixLength);
    final String decodedValue = decodeValue(encodedValue);
    try {
      validateAttributeValue(key, decodedValue);
      return Optional.of(decodedValue);
    } catch (final SemanticException e) {
      return Optional.empty();
    }
  }

  private static void validateAttributeValue(final String key, final String value) {
    switch (key) {
      case LoadTsFileConfigurator.DATABASE_NAME_KEY:
        if (value == null || value.isEmpty()) {
          throw new SemanticException("Database name must not be empty.");
        }
        break;
      case LoadTsFileConfigurator.DATABASE_LEVEL_KEY:
        LoadTsFileConfigurator.validateDatabaseLevelParam(value);
        break;
      case LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY:
        LoadTsFileConfigurator.validateConvertOnTypeMismatchParam(value);
        break;
      case LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY:
        validateTabletConversionThreshold(value);
        break;
      case LoadTsFileConfigurator.VERIFY_KEY:
        LoadTsFileConfigurator.validateVerifyParam(value);
        break;
      default:
        LoadTsFileConfigurator.validateParameters(key, value);
    }
  }

  private static void validateTabletConversionThreshold(final String value) {
    try {
      final long threshold = Long.parseLong(value);
      if (threshold < 0) {
        throw new SemanticException(
            "Tablet conversion threshold must be a non-negative long value.");
      }
    } catch (final NumberFormatException e) {
      throw new SemanticException(
          String.format("Tablet conversion threshold '%s' is not a valid long value.", value));
    }
  }

  private static String decodeValue(final String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8.toString());
    } catch (final UnsupportedEncodingException e) {
      return value;
    }
  }
}
