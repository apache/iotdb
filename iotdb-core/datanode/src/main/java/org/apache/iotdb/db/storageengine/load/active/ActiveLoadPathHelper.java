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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import javax.annotation.Nullable;

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
              LoadTsFileConfigurator.VERIFY_KEY));

  private ActiveLoadPathHelper() {
    throw new IllegalStateException("Utility class");
  }

  public static Map<String, String> buildAttributes(
      @Nullable final String databaseName,
      @Nullable final Integer databaseLevel,
      @Nullable final Boolean convertOnTypeMismatch,
      @Nullable final Boolean verify,
      @Nullable final Long tabletConversionThresholdBytes) {

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
      // compatibility: keep placing tablet conversion and verify under convert folder
      if (LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY.equals(key)) {
        final String threshold = attributes.get(LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY);
        if (threshold != null) {
          current = new File(current, formatSegment(LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY, threshold));
        }
        final String verify = attributes.get(LoadTsFileConfigurator.VERIFY_KEY);
        if (verify != null) {
          current = new File(current, formatSegment(LoadTsFileConfigurator.VERIFY_KEY, verify));
        }
        return current;
      }
    }
    return current;
  }

  public static Map<String, String> parseAttributes(
      @Nullable final File file, @Nullable final File pendingDir) {
    if (file == null) {
      return Collections.emptyMap();
    }

    final Map<String, String> attributes = new HashMap<>();
    File current = file.getParentFile();
    boolean convertFolderVisited = false;
    while (current != null) {
      final String dirName = current.getName();
      if (pendingDir != null && current.equals(pendingDir)) {
        current = current.getParentFile();
        continue;
      }
      for (final String key : KEY_ORDER) {
        final String prefix = key + SEGMENT_SEPARATOR;
        if (dirName.startsWith(prefix)) {
          final String encodedValue = dirName.substring(prefix.length());
          attributes.putIfAbsent(key, decodeValue(encodedValue));
          if (LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY.equals(key)) {
            convertFolderVisited = true;
          }
          break;
        }
      }
      if (convertFolderVisited
          && !attributes.containsKey(LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY)
          && dirName.startsWith(
              LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY + SEGMENT_SEPARATOR)) {
        final String encodedValue =
            dirName.substring(
                (LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY + SEGMENT_SEPARATOR)
                    .length());
        attributes.putIfAbsent(
            LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY, decodeValue(encodedValue));
      }
      if (convertFolderVisited
          && !attributes.containsKey(LoadTsFileConfigurator.VERIFY_KEY)
          && dirName.startsWith(LoadTsFileConfigurator.VERIFY_KEY + SEGMENT_SEPARATOR)) {
        final String encodedValue =
            dirName.substring((LoadTsFileConfigurator.VERIFY_KEY + SEGMENT_SEPARATOR).length());
        attributes.putIfAbsent(LoadTsFileConfigurator.VERIFY_KEY, decodeValue(encodedValue));
      }
      current = current.getParentFile();
    }
    return attributes;
  }

  public static Map<String, String> parseAttributes(@Nullable final File file) {
    return parseAttributes(file, null);
  }

  public static @Nullable File findPendingDirectory(@Nullable final File file) {
    if (file == null) {
      return null;
    }
    File current = file.getParentFile();
    while (current != null) {
      if (IoTDBConstant.LOAD_TSFILE_ACTIVE_LISTENING_PENDING_FOLDER_NAME.equals(current.getName())) {
        return current;
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
  }

  public static boolean containsDatabaseName(final Map<String, String> attributes) {
    return attributes.containsKey(LoadTsFileConfigurator.DATABASE_NAME_KEY);
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

  private static String decodeValue(final String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8.toString());
    } catch (final UnsupportedEncodingException e) {
      return value;
    }
  }
}


