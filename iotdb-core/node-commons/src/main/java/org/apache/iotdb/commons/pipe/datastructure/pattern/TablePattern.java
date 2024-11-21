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

import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_TABLE_NAME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_TABLE_NAME_KEY;

public class TablePattern {

  private final boolean isTableModelDataAllowedToBeCaptured;

  private final Pattern databasePattern;
  private final Pattern tablePattern;

  protected TablePattern(
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

  /**
   * Interpret from source parameters and get a pipe pattern.
   *
   * @return The interpreted {@link TablePattern} which is not {@code null}.
   */
  public static TablePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final boolean isTableModelDataAllowedToBeCaptured =
        sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
                PipeExtractorConstant.SOURCE_CAPTURE_TABLE_KEY),
            !sourceParameters
                .getStringOrDefault(
                    SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
                .equals(SystemConstant.SQL_DIALECT_TREE_VALUE));
    final String databaseNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_DATABASE_NAME_KEY, SOURCE_DATABASE_NAME_KEY),
            EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE);
    final String tableNamePattern =
        sourceParameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_TABLE_NAME_KEY, SOURCE_TABLE_NAME_KEY),
            EXTRACTOR_TABLE_NAME_DEFAULT_VALUE);
    try {
      return new TablePattern(
          isTableModelDataAllowedToBeCaptured, databaseNamePattern, tableNamePattern);
    } catch (final Exception e) {
      throw new PipeException("Illegal database or table pattern. Detail: " + e.getMessage(), e);
    }
  }

  @Override
  public String toString() {
    return "TablePattern{"
        + "databasePattern="
        + databasePattern
        + ", tablePattern="
        + tablePattern
        + '}';
  }
}
