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

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_TABLE_NAME_KEY;

public class TablePattern {

  private static final Logger LOGGER = LoggerFactory.getLogger(TablePattern.class);

  private final Pattern databasePattern;
  private final Pattern tablePattern;

  protected TablePattern(final String databasePatternString, final String tablePatternString) {
    databasePattern = databasePatternString == null ? null : Pattern.compile(databasePatternString);
    tablePattern = tablePatternString == null ? null : Pattern.compile(tablePatternString);
  }

  public boolean hasUserSpecifiedDatabasePatternOrTablePattern() {
    return databasePattern != null || tablePattern != null;
  }

  public boolean matchesDatabase(final String database) {
    return databasePattern == null || databasePattern.matcher(database).matches();
  }

  public boolean matchesTable(final String table) {
    return tablePattern == null || tablePattern.matcher(table).matches();
  }

  public String getDatabasePattern() {
    return databasePattern == null ? ".*" : databasePattern.pattern();
  }

  public String getTablePattern() {
    return tablePattern == null ? ".*" : tablePattern.pattern();
  }

  /**
   * Interpret from source parameters and get a pipe pattern.
   *
   * @return The interpreted {@link TablePattern} which is not null.
   */
  public static TablePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final String databaseNamePattern =
        sourceParameters.getStringByKeys(EXTRACTOR_DATABASE_NAME_KEY, SOURCE_DATABASE_NAME_KEY);
    final String tableNamePattern =
        sourceParameters.getStringByKeys(EXTRACTOR_TABLE_NAME_KEY, SOURCE_TABLE_NAME_KEY);
    try {
      return new TablePattern(databaseNamePattern, tableNamePattern);
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
