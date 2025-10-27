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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_KEY;

public abstract class TreePattern {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreePattern.class);

  protected final boolean isTreeModelDataAllowedToBeCaptured;

  protected TreePattern(final boolean isTreeModelDataAllowedToBeCaptured) {
    this.isTreeModelDataAllowedToBeCaptured = isTreeModelDataAllowedToBeCaptured;
  }

  public boolean isTreeModelDataAllowedToBeCaptured() {
    return isTreeModelDataAllowedToBeCaptured;
  }

  public static <T> List<T> applyIndexesOnList(
      final int[] filteredIndexes, final List<T> originalList) {
    return Objects.nonNull(originalList)
        ? Arrays.stream(filteredIndexes).mapToObj(originalList::get).collect(Collectors.toList())
        : null;
  }

  /**
   * Interpret from source parameters and get a {@link TreePattern}.
   *
   * @return The interpreted {@link TreePattern} which is not {@code null}.
   */
  public static TreePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final boolean isTreeModelDataAllowedToBeCaptured =
        isTreeModelDataAllowToBeCaptured(sourceParameters);

    final String path = sourceParameters.getStringByKeys(EXTRACTOR_PATH_KEY, SOURCE_PATH_KEY);
    final String pattern =
        sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY);

    // 1. If both "source.path" and "source.pattern" are specified, their union will be used.
    if (path != null && pattern != null) {
      final List<TreePattern> result = new ArrayList<>();
      // Parse "source.path" as IoTDB-style path.
      result.addAll(
          parseMultiplePatterns(
              path, p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p)));
      // Parse "source.pattern" using the helper method.
      result.addAll(
          parsePatternsFromPatternParameter(
              pattern, sourceParameters, isTreeModelDataAllowedToBeCaptured));
      return buildUnionPattern(isTreeModelDataAllowedToBeCaptured, result);
    }

    // 2. If only "source.path" is specified, it will be interpreted as an IoTDB-style path.
    if (path != null) {
      return buildUnionPattern(
          isTreeModelDataAllowedToBeCaptured,
          parseMultiplePatterns(
              path, p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p)));
    }

    // 3. If only "source.pattern" is specified, parse it using the helper method.
    if (pattern != null) {
      return buildUnionPattern(
          isTreeModelDataAllowedToBeCaptured,
          parsePatternsFromPatternParameter(
              pattern, sourceParameters, isTreeModelDataAllowedToBeCaptured));
    }

    // 4. If neither "source.path" nor "source.pattern" is specified,
    // this pipe source will match all data.
    return buildUnionPattern(
        isTreeModelDataAllowedToBeCaptured,
        Collections.singletonList(new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, null)));
  }

  /**
   * A private helper method to parse a list of {@link TreePattern}s from the "pattern" parameter,
   * considering its "format".
   *
   * @param pattern The pattern string to parse.
   * @param sourceParameters The source parameters to read the format from.
   * @param isTreeModelDataAllowedToBeCaptured A boolean flag passed to the TreePattern constructor.
   * @return A list of parsed {@link TreePattern}s.
   */
  private static List<TreePattern> parsePatternsFromPatternParameter(
      final String pattern,
      final PipeParameters sourceParameters,
      final boolean isTreeModelDataAllowedToBeCaptured) {
    final String patternFormat =
        sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY);

    // If "source.pattern.format" is not specified, use prefix format by default.
    if (patternFormat == null) {
      return parseMultiplePatterns(
          pattern, p -> new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, p));
    }

    switch (patternFormat.toLowerCase()) {
      case EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE:
        return parseMultiplePatterns(
            pattern, p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p));
      case EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE:
        return parseMultiplePatterns(
            pattern, p -> new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, p));
      default:
        LOGGER.info(
            "Unknown pattern format: {}, use prefix matching format by default.", patternFormat);
        return parseMultiplePatterns(
            pattern, p -> new PrefixTreePattern(isTreeModelDataAllowedToBeCaptured, p));
    }
  }

  public static List<TreePattern> parseMultiplePatterns(
      final String pattern, final Function<String, TreePattern> patternSupplier) {
    if (pattern.isEmpty()) {
      return Collections.singletonList(patternSupplier.apply(pattern));
    }

    final List<TreePattern> patterns = new ArrayList<>();
    final StringBuilder currentPattern = new StringBuilder();
    boolean inBackticks = false;

    for (final char c : pattern.toCharArray()) {
      if (c == '`') {
        inBackticks = !inBackticks;
        currentPattern.append(c);
      } else if (c == ',' && !inBackticks) {
        final String singlePattern = currentPattern.toString().trim();
        if (!singlePattern.isEmpty()) {
          patterns.add(patternSupplier.apply(singlePattern));
        }
        currentPattern.setLength(0);
      } else {
        currentPattern.append(c);
      }
    }

    final String lastPattern = currentPattern.toString().trim();
    if (!lastPattern.isEmpty()) {
      patterns.add(patternSupplier.apply(lastPattern));
    }

    return patterns;
  }

  /**
   * A private helper method to build the most specific UnionTreePattern possible. If all patterns
   * are IoTDBTreePattern, it returns an IoTDBUnionTreePattern. Otherwise, it returns a general
   * UnionTreePattern.
   */
  public static TreePattern buildUnionPattern(
      final boolean isTreeModelDataAllowedToBeCaptured, final List<TreePattern> patterns) {
    // Check if all instances in the list are of type IoTDBTreePattern
    boolean allIoTDB = true;
    for (final TreePattern p : patterns) {
      if (!(p instanceof IoTDBTreePattern)) {
        allIoTDB = false;
        break;
      }
    }

    if (allIoTDB) {
      final List<IoTDBTreePattern> iotdbPatterns = new ArrayList<>(patterns.size());
      for (final TreePattern p : patterns) {
        iotdbPatterns.add((IoTDBTreePattern) p);
      }
      return new UnionIoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, iotdbPatterns);
    } else {
      // If there's a mix of pattern types, use the general UnionTreePattern
      return new UnionTreePattern(isTreeModelDataAllowedToBeCaptured, patterns);
    }
  }

  public static boolean isTreeModelDataAllowToBeCaptured(final PipeParameters sourceParameters) {
    return sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE)
        || sourceParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
                PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY),
            sourceParameters
                .getStringOrDefault(
                    SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
                .equals(SystemConstant.SQL_DIALECT_TREE_VALUE));
  }

  public abstract boolean isSingle();

  public abstract String getPattern();

  public abstract boolean isRoot();

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
}
