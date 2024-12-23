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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;

public abstract class TreePattern {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreePattern.class);

  protected final boolean isTreeModelDataAllowedToBeCaptured;
  protected final String pattern;

  protected TreePattern(final boolean isTreeModelDataAllowedToBeCaptured, final String pattern) {
    this.isTreeModelDataAllowedToBeCaptured = isTreeModelDataAllowedToBeCaptured;
    this.pattern = pattern != null ? pattern : getDefaultPattern();
  }

  public boolean isTreeModelDataAllowedToBeCaptured() {
    return isTreeModelDataAllowedToBeCaptured;
  }

  public String getPattern() {
    return pattern;
  }

  public boolean isRoot() {
    return Objects.isNull(pattern) || this.pattern.equals(this.getDefaultPattern());
  }

  /**
   * Interpret from source parameters and get a {@link TreePattern}.
   *
   * @return The interpreted {@link TreePattern} which is not {@code null}.
   */
  public static TreePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final boolean isTreeModelDataAllowedToBeCaptured =
        sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_CAPTURE_TREE_KEY,
                PipeExtractorConstant.SOURCE_CAPTURE_TREE_KEY),
            sourceParameters
                .getStringOrDefault(
                    SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
                .equals(SystemConstant.SQL_DIALECT_TREE_VALUE));

    final String path = sourceParameters.getStringByKeys(EXTRACTOR_PATH_KEY, SOURCE_PATH_KEY);

    // 1. If "source.path" is specified, it will be interpreted as an IoTDB-style path,
    // ignoring the other 2 parameters.
    if (path != null) {
      return new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, path);
    }

    final String pattern =
        sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY);

    // 2. Otherwise, If "source.pattern" is specified, it will be interpreted
    // according to "source.pattern.format".
    if (pattern != null) {
      final String patternFormat =
          sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY);

      // If "source.pattern.format" is not specified, use prefix format by default.
      if (patternFormat == null) {
        return new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, pattern);
      }

      switch (patternFormat.toLowerCase()) {
        case EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE:
          return new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, pattern);
        case EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE:
          return new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, pattern);
        default:
          LOGGER.info(
              "Unknown pattern format: {}, use prefix matching format by default.", patternFormat);
          return new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, pattern);
      }
    }

    // 3. If neither "source.path" nor "source.pattern" is specified,
    // this pipe source will match all data.
    return new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, null);
  }

  public abstract String getDefaultPattern();

  /** Check if this pattern is legal. Different pattern type may have different rules. */
  public abstract boolean isLegal();

  /** Check if this pattern matches all time-series under a database. */
  public abstract boolean coversDb(final String db);

  /** Check if a device's all measurements are covered by this pattern. */
  public abstract boolean coversDevice(final IDeviceID device);

  /**
   * Check if a database may have some measurements matched by the pattern.
   *
   * @return {@code true} if the pattern may overlap with the database, {@code false} otherwise.
   */
  public abstract boolean mayOverlapWithDb(final String db);

  /**
   * Check if a device may have some measurements matched by the pattern.
   *
   * <p>NOTE1: this is only called when {@link TreePattern#coversDevice} is {@code false}.
   *
   * <p>NOTE2: this is just a loose check and may have false positives. To further check if a
   * measurement matches the pattern, please use {@link TreePattern#matchesMeasurement} after this.
   */
  public abstract boolean mayOverlapWithDevice(final IDeviceID device);

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link TreePattern#mayOverlapWithDevice} is {@code true}.
   */
  public abstract boolean matchesMeasurement(final IDeviceID device, final String measurement);

  @Override
  public String toString() {
    return "{pattern='"
        + pattern
        + "', isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
