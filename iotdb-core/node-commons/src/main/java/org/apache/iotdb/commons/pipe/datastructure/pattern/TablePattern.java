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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_TABLES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_ORIGINAL_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_NAME_KEY;

public class TablePattern {

  private final boolean isTableModelDataAllowedToBeCaptured;

  private final Pattern databasePattern;
  private final Pattern tablePattern;
  private final Map<String, Set<String>> matchedOriginalDatabaseTables;

  public TablePattern(
      final boolean isTableModelDataAllowedToBeCaptured,
      final String databasePatternString,
      final String tablePatternString) {
    this(
        isTableModelDataAllowedToBeCaptured,
        databasePatternString,
        tablePatternString,
        Collections.emptyMap());
  }

  public TablePattern(
      final boolean isTableModelDataAllowedToBeCaptured,
      final String databasePatternString,
      final String tablePatternString,
      final Map<String, Set<String>> matchedOriginalDatabaseTables) {
    this.isTableModelDataAllowedToBeCaptured = isTableModelDataAllowedToBeCaptured;
    databasePattern =
        databasePatternString == null
                || databasePatternString.trim().equals(EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE)
            ? null
            : Pattern.compile(databasePatternString);
    tablePattern =
        tablePatternString == null
                || tablePatternString.trim().equals(EXTRACTOR_TABLE_NAME_DEFAULT_VALUE)
            ? null
            : Pattern.compile(tablePatternString);
    this.matchedOriginalDatabaseTables =
        deepCopyMatchedOriginalDatabaseTables(matchedOriginalDatabaseTables);
  }

  public boolean isTableModelDataAllowedToBeCaptured() {
    return isTableModelDataAllowedToBeCaptured;
  }

  public boolean hasUserSpecifiedDatabasePatternOrTablePattern() {
    return databasePattern != null
        || tablePattern != null
        || !matchedOriginalDatabaseTables.isEmpty();
  }

  public boolean coversDb(final String database) {
    return !hasUserSpecifiedDatabasePatternOrTablePattern()
        || (databasePattern != null
            && databasePattern.matcher(database).matches()
            && tablePattern == null);
  }

  public boolean matchesDatabase(final String database) {
    return databasePattern == null || databasePattern.matcher(database).matches();
  }

  public boolean mayMatchDatabase(final String database) {
    return matchesDatabase(database) || matchedOriginalDatabaseTables.containsKey(database);
  }

  public boolean matchesTable(final String table) {
    return tablePattern == null || tablePattern.matcher(table).matches();
  }

  public boolean matchesDatabaseAndTable(final String database, final String table) {
    if (matchesDatabase(database) && matchesTable(table)) {
      return true;
    }
    final Set<String> tables = matchedOriginalDatabaseTables.get(database);
    return Objects.nonNull(tables) && tables.contains(table);
  }

  public String getDatabasePattern() {
    return databasePattern == null
        ? EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE
        : databasePattern.pattern();
  }

  public String getTablePattern() {
    return tablePattern == null ? EXTRACTOR_TABLE_NAME_DEFAULT_VALUE : tablePattern.pattern();
  }

  public boolean hasTablePattern() {
    return tablePattern != null || !matchedOriginalDatabaseTables.isEmpty();
  }

  /**
   * Interpret from source parameters and get a pipe pattern.
   *
   * @return The interpreted {@link TablePattern} which is not {@code null}.
   */
  public static TablePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    return parsePipePatternFromSourceParametersInternal(sourceParameters, false);
  }

  public static TablePattern parsePipeDataPatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    return parsePipePatternFromSourceParametersInternal(sourceParameters, true);
  }

  private static TablePattern parsePipePatternFromSourceParametersInternal(
      final PipeParameters sourceParameters, final boolean useOriginalTableForData) {
    final boolean isTableModelDataAllowedToBeCaptured =
        isTableModelDataAllowToBeCaptured(sourceParameters);
    String databaseNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(
                EXTRACTOR_DATABASE_NAME_KEY,
                SOURCE_DATABASE_NAME_KEY,
                EXTRACTOR_DATABASE_KEY,
                SOURCE_DATABASE_KEY),
            EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE);
    String tableNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(
                EXTRACTOR_TABLE_NAME_KEY,
                SOURCE_TABLE_NAME_KEY,
                EXTRACTOR_TABLE_KEY,
                SOURCE_TABLE_KEY),
            EXTRACTOR_TABLE_NAME_DEFAULT_VALUE);
    try {
      if (useOriginalTableForData) {
        final Map<String, Set<String>> matchedOriginalDatabaseTables =
            deserializeDatabaseTablePairs(
                sourceParameters.getString(SOURCE_ORIGINAL_DATABASE_TABLES_KEY));
        if (!matchedOriginalDatabaseTables.isEmpty()) {
          return new TablePattern(
              isTableModelDataAllowedToBeCaptured,
              databaseNamePattern,
              tableNamePattern,
              matchedOriginalDatabaseTables);
        }

        final String originalDatabaseName =
            sourceParameters.getString(SOURCE_ORIGINAL_DATABASE_KEY);
        final String originalTableName = sourceParameters.getString(SOURCE_ORIGINAL_TABLE_KEY);
        if (originalDatabaseName != null && originalTableName != null) {
          databaseNamePattern = Pattern.quote(originalDatabaseName);
          tableNamePattern = Pattern.quote(originalTableName);
        }
      }

      return new TablePattern(
          isTableModelDataAllowedToBeCaptured, databaseNamePattern, tableNamePattern);
    } catch (final Exception e) {
      throw new PipeException(PipeMessages.ILLEGAL_DB_OR_TABLE_PATTERN + e.getMessage(), e);
    }
  }

  public static String serializeDatabaseTablePairs(
      final Map<String, Set<String>> databaseToTableNames) {
    if (databaseToTableNames == null || databaseToTableNames.isEmpty()) {
      return "";
    }
    final Base64.Encoder encoder = Base64.getEncoder();
    final StringJoiner pairJoiner = new StringJoiner(";");
    new TreeMap<>(databaseToTableNames)
        .forEach(
            (database, tables) -> {
              if (Objects.isNull(database) || Objects.isNull(tables)) {
                return;
              }
              new TreeSet<>(tables)
                  .forEach(
                      table -> {
                        if (Objects.nonNull(table)) {
                          pairJoiner.add(encode(encoder, database) + "," + encode(encoder, table));
                        }
                      });
            });
    return pairJoiner.toString();
  }

  public static Map<String, Set<String>> deserializeDatabaseTablePairs(
      final String serializedDatabaseTablePairs) {
    if (serializedDatabaseTablePairs == null || serializedDatabaseTablePairs.isEmpty()) {
      return Collections.emptyMap();
    }

    final Base64.Decoder decoder = Base64.getDecoder();
    final Map<String, Set<String>> databaseToTables = new HashMap<>();
    for (final String pair : serializedDatabaseTablePairs.split(";")) {
      if (pair.isEmpty()) {
        continue;
      }
      final String[] encodedDatabaseAndTable = pair.split(",", -1);
      if (encodedDatabaseAndTable.length != 2) {
        throw new IllegalArgumentException(
            "Illegal database-table pair in " + SOURCE_ORIGINAL_DATABASE_TABLES_KEY);
      }
      databaseToTables
          .computeIfAbsent(decode(decoder, encodedDatabaseAndTable[0]), key -> new HashSet<>())
          .add(decode(decoder, encodedDatabaseAndTable[1]));
    }
    return deepCopyMatchedOriginalDatabaseTables(databaseToTables);
  }

  private static String encode(final Base64.Encoder encoder, final String value) {
    return encoder.encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static String decode(final Base64.Decoder decoder, final String value) {
    return new String(decoder.decode(value), java.nio.charset.StandardCharsets.UTF_8);
  }

  private static Map<String, Set<String>> deepCopyMatchedOriginalDatabaseTables(
      final Map<String, Set<String>> databaseToTableNames) {
    if (databaseToTableNames == null || databaseToTableNames.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<String, Set<String>> copiedDatabaseToTableNames = new HashMap<>();
    databaseToTableNames.forEach(
        (database, tables) -> {
          if (Objects.nonNull(database) && Objects.nonNull(tables) && !tables.isEmpty()) {
            copiedDatabaseToTableNames.put(database, new HashSet<>(tables));
          }
        });
    return copiedDatabaseToTableNames.isEmpty()
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(copiedDatabaseToTableNames);
  }

  public static boolean isTableModelDataAllowToBeCaptured(final PipeParameters sourceParameters) {
    return VisibilityUtils.isTableModelDataAllowToBeCaptured(sourceParameters);
  }

  @Override
  public String toString() {
    return "TablePattern{"
        + "isTableModelDataAllowedToBeCaptured="
        + isTableModelDataAllowedToBeCaptured
        + ", databasePattern="
        + databasePattern
        + ", tablePattern="
        + tablePattern
        + ", matchedOriginalDatabaseTables="
        + matchedOriginalDatabaseTables
        + '}';
  }
}
