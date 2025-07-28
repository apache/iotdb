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

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_NAME_KEY;

public class TablePattern {

  private final boolean isTableModelDataAllowedToBeCaptured;

  private final Pattern databasePattern;
  private final Pattern tablePattern;

  public TablePattern(
      final boolean isTableModelDataAllowedToBeCaptured,
      final String databasePatternString,
      final String tablePatternString) {
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
  }

  public boolean isTableModelDataAllowedToBeCaptured() {
    return isTableModelDataAllowedToBeCaptured;
  }

  public boolean hasUserSpecifiedDatabasePatternOrTablePattern() {
    return databasePattern != null || tablePattern != null;
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

  public boolean matchesTable(final String table) {
    return tablePattern == null || tablePattern.matcher(table).matches();
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
    return tablePattern != null;
  }

  /**
   * Interpret from source parameters and get a pipe pattern.
   *
   * @return The interpreted {@link TablePattern} which is not {@code null}.
   */
  public static TablePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final boolean isTableModelDataAllowedToBeCaptured =
        isTableModelDataAllowToBeCaptured(sourceParameters);
    final String databaseNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(
                EXTRACTOR_DATABASE_NAME_KEY,
                SOURCE_DATABASE_NAME_KEY,
                EXTRACTOR_DATABASE_KEY,
                SOURCE_DATABASE_KEY),
            EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE);
    final String tableNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(
                EXTRACTOR_TABLE_NAME_KEY,
                SOURCE_TABLE_NAME_KEY,
                EXTRACTOR_TABLE_KEY,
                SOURCE_TABLE_KEY),
            EXTRACTOR_TABLE_NAME_DEFAULT_VALUE);
    try {
      return new TablePattern(
          isTableModelDataAllowedToBeCaptured, databaseNamePattern, tableNamePattern);
    } catch (final Exception e) {
      throw new PipeException("Illegal database or table pattern. Detail: " + e.getMessage(), e);
    }
  }

  public static boolean isTableModelDataAllowToBeCaptured(final PipeParameters sourceParameters) {
    return sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE)
        || sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
                PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY),
            !sourceParameters
                .getStringOrDefault(
                    SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
                .equals(SystemConstant.SQL_DIALECT_TREE_VALUE));
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
        + '}';
  }
}
