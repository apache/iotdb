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

public abstract class PipePattern {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePattern.class);

  public static <T> List<T> applyIndexesOnList(
      final int[] filteredIndexes, final List<T> originalList) {
    return Objects.nonNull(originalList)
        ? Arrays.stream(filteredIndexes).mapToObj(originalList::get).collect(Collectors.toList())
        : null;
  }

  /**
   * Interpret from source parameters and get a {@link PipePattern}.
   *
   * @return The interpreted {@link PipePattern} which is not {@code null}.
   */
  public static PipePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final String path = sourceParameters.getStringByKeys(EXTRACTOR_PATH_KEY, SOURCE_PATH_KEY);
    final String pattern =
        sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY);

    // 1. If both "source.path" and "source.pattern" are specified, their union will be used.
    if (path != null && pattern != null) {
      final List<PipePattern> result = new ArrayList<>();
      // Parse "source.path" as IoTDB-style path.
      result.addAll(parseMultiplePatterns(path, IoTDBPipePattern::new));
      // Parse "source.pattern" using the helper method.
      result.addAll(parsePatternsFromPatternParameter(pattern, sourceParameters));
      return buildUnionPattern(result);
    }

    // 2. If only "source.path" is specified, it will be interpreted as an IoTDB-style path.
    if (path != null) {
      return buildUnionPattern(parseMultiplePatterns(path, IoTDBPipePattern::new));
    }

    // 3. If only "source.pattern" is specified, parse it using the helper method.
    if (pattern != null) {
      return buildUnionPattern(parsePatternsFromPatternParameter(pattern, sourceParameters));
    }

    // 4. If neither "source.path" nor "source.pattern" is specified,
    // this pipe source will match all data.
    return buildUnionPattern(Collections.singletonList(new IoTDBPipePattern(null)));
  }

  /**
   * A private helper method to parse a list of {@link PipePattern}s from the "pattern" parameter,
   * considering its "format".
   *
   * @param pattern The pattern string to parse.
   * @param sourceParameters The source parameters to read the format from.
   * @return A list of parsed {@link PipePattern}s.
   */
  private static List<PipePattern> parsePatternsFromPatternParameter(
      final String pattern, final PipeParameters sourceParameters) {
    final String patternFormat =
        sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY);

    // If "source.pattern.format" is not specified, use prefix format by default.
    if (patternFormat == null) {
      return parseMultiplePatterns(pattern, PrefixPipePattern::new);
    }

    switch (patternFormat.toLowerCase()) {
      case EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE:
        return parseMultiplePatterns(pattern, IoTDBPipePattern::new);
      case EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE:
        return parseMultiplePatterns(pattern, PrefixPipePattern::new);
      default:
        LOGGER.info(
            "Unknown pattern format: {}, use prefix matching format by default.", patternFormat);
        return parseMultiplePatterns(pattern, PrefixPipePattern::new);
    }
  }

  public static List<PipePattern> parseMultiplePatterns(
      final String pattern, final Function<String, PipePattern> patternSupplier) {
    if (pattern.isEmpty()) {
      return Collections.singletonList(patternSupplier.apply(pattern));
    }

    final List<PipePattern> patterns = new ArrayList<>();
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
   * A private helper method to build the most specific UnionPipePattern possible. If all patterns
   * are IoTDBPipePattern, it returns an IoTDBUnionPipePattern. Otherwise, it returns a general
   * UnionPipePattern.
   */
  public static PipePattern buildUnionPattern(final List<PipePattern> patterns) {
    // Check if all instances in the list are of type IoTDBPipePattern
    boolean allIoTDB = true;
    for (final PipePattern p : patterns) {
      if (!(p instanceof IoTDBPipePattern)) {
        allIoTDB = false;
        break;
      }
    }

    if (allIoTDB) {
      final List<IoTDBPipePattern> iotdbPatterns = new ArrayList<>(patterns.size());
      for (final PipePattern p : patterns) {
        iotdbPatterns.add((IoTDBPipePattern) p);
      }
      return new UnionIoTDBPipePattern(iotdbPatterns);
    } else {
      // If there's a mix of pattern types, use the general UnionPipePattern
      return new UnionPipePattern(patterns);
    }
  }

  public abstract boolean isSingle();

  public abstract String getPattern();

  public abstract boolean isRoot();

  /** Check if this pattern is legal. Different pattern type may have different rules. */
  public abstract boolean isLegal();

  /** Check if this pattern matches all time-series under a database. */
  public abstract boolean coversDb(final String db);

  /** Check if a device's all measurements are covered by this pattern. */
  public abstract boolean coversDevice(final String device);

  /**
   * Check if a database may have some measurements matched by the pattern.
   *
   * @return {@code true} if the pattern may overlap with the database, {@code false} otherwise.
   */
  public abstract boolean mayOverlapWithDb(final String db);

  /**
   * Check if a device may have some measurements matched by the pattern.
   *
   * <p>NOTE1: this is only called when {@link PipePattern#coversDevice} is false.
   *
   * <p>NOTE2: this is just a loose check and may have false positives. To further check if a
   * measurement matches the pattern, please use {@link PipePattern#matchesMeasurement} after this.
   */
  public abstract boolean mayOverlapWithDevice(final String device);

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link PipePattern#mayOverlapWithDevice(String)} is true.
   */
  public abstract boolean matchesMeasurement(final String device, final String measurement);
}
