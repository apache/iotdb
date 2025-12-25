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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

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

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_EXCLUSION_KEY;
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

  //////////////////////////// Interface ////////////////////////////

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

  /**
   * Get all 'base' PartialPath patterns that this pattern represents, for the purpose of checking
   * pattern coverage.
   *
   * <p>For IoTDB patterns, it's their direct PartialPath.
   *
   * <p>For Prefix patterns (e.g., "root.d1"), it's approximated as a union of PartialPaths to model
   * its string-based matching: "root.d1", "root.d1*", "root.d1.**", and "root.d1*.**".
   *
   * <p>For Union patterns, it's a list from all sub-patterns.
   *
   * <p>For Exclusion patterns, it's the *effective* set of paths.
   *
   * @return A list of PartialPaths representing the inclusion paths.
   */
  public abstract List<PartialPath> getBaseInclusionPaths();

  //////////////////////////// Utilities ////////////////////////////

  public static <T> List<T> applyIndexesOnList(
      final int[] filteredIndexes, final List<T> originalList) {
    return Objects.nonNull(originalList)
        ? Arrays.stream(filteredIndexes).mapToObj(originalList::get).collect(Collectors.toList())
        : null;
  }

  /**
   * Interpret from source parameters and get a {@link TreePattern}. This method parses both
   * inclusion and exclusion patterns.
   *
   * @return The interpreted {@link TreePattern} which is not {@code null}.
   */
  public static TreePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final boolean isTreeModelDataAllowedToBeCaptured =
        isTreeModelDataAllowToBeCaptured(sourceParameters);

    // 1. Define the default inclusion pattern (matches all, "root.**")
    // This is used if no inclusion patterns are specified.
    final TreePattern defaultInclusionPattern =
        buildUnionPattern(
            isTreeModelDataAllowedToBeCaptured,
            Collections.singletonList(
                new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, null)));

    // 2. Parse INCLUSION patterns using the helper
    final TreePattern inclusionPattern =
        parsePatternUnion(
            sourceParameters,
            isTreeModelDataAllowedToBeCaptured,
            // 'path' keys (IoTDB wildcard)
            EXTRACTOR_PATH_KEY,
            SOURCE_PATH_KEY,
            // 'pattern' keys (Prefix or IoTDB via format)
            EXTRACTOR_PATTERN_KEY,
            SOURCE_PATTERN_KEY,
            // Default pattern if no keys are found
            defaultInclusionPattern);

    // 3. Parse EXCLUSION patterns using the helper
    final TreePattern exclusionPattern =
        parsePatternUnion(
            sourceParameters,
            isTreeModelDataAllowedToBeCaptured,
            // 'path.exclusion' keys (IoTDB wildcard)
            EXTRACTOR_PATH_EXCLUSION_KEY,
            SOURCE_PATH_EXCLUSION_KEY,
            // 'pattern.exclusion' keys (Prefix)
            EXTRACTOR_PATTERN_EXCLUSION_KEY,
            SOURCE_PATTERN_EXCLUSION_KEY,
            // Default for exclusion is "match nothing" (null)
            null);

    // 4. Combine inclusion and exclusion
    if (exclusionPattern == null) {
      // No exclusion defined, return the inclusion pattern directly
      return inclusionPattern;
    } else {
      // If both inclusion and exclusion patterns support IoTDB operations,
      // use the specialized ExclusionIoTDBTreePattern
      if (inclusionPattern instanceof IoTDBTreePatternOperations
          && exclusionPattern instanceof IoTDBTreePatternOperations) {
        return new WithExclusionIoTDBTreePattern(
            isTreeModelDataAllowedToBeCaptured,
            (IoTDBTreePatternOperations) inclusionPattern,
            (IoTDBTreePatternOperations) exclusionPattern);
      }
      // Both are defined, wrap them in an ExclusionTreePattern
      return new WithExclusionTreePattern(
          isTreeModelDataAllowedToBeCaptured, inclusionPattern, exclusionPattern);
    }
  }

  /**
   * The main entry point for parsing a pattern string. This method can handle simple patterns
   * ("root.a"), union patterns ("root.a,root.b"), and exclusion patterns ("INCLUSION(root.a),
   * EXCLUSION(root.b)").
   */
  public static TreePattern parsePatternFromString(
      final String patternString,
      final boolean isTreeModelDataAllowedToBeCaptured,
      final Function<String, TreePattern> basePatternSupplier) {
    final String trimmedPattern = (patternString == null) ? "" : patternString.trim();
    if (trimmedPattern.isEmpty()) {
      return basePatternSupplier.apply("");
    }

    // 1. Check if it's an Exclusion pattern
    if (trimmedPattern.startsWith("INCLUSION(") && trimmedPattern.endsWith(")")) {
      // Find the closing parenthesis for "INCLUSION(...)"
      final int inclusionEndIndex =
          findMatchingParenthesis(trimmedPattern, "INCLUSION(".length() - 1);
      if (inclusionEndIndex == -1) {
        // Malformed, treat as a normal pattern
        return buildUnionPattern(
            isTreeModelDataAllowedToBeCaptured,
            parseMultiplePatterns(trimmedPattern, basePatternSupplier));
      }

      // Look for the ", EXCLUSION(" part
      final String remaining = trimmedPattern.substring(inclusionEndIndex + 1).trim();
      if (!remaining.startsWith(", EXCLUSION(")) {
        // Malformed, treat as a normal pattern
        return buildUnionPattern(
            isTreeModelDataAllowedToBeCaptured,
            parseMultiplePatterns(trimmedPattern, basePatternSupplier));
      }

      try {
        // Extract the string inside INCLUSION(...)
        final String inclusionSubstring =
            trimmedPattern.substring("INCLUSION(".length(), inclusionEndIndex);

        // Extract the string inside EXCLUSION(...)
        final String exclusionSubstring =
            trimmedPattern.substring(
                inclusionEndIndex + ", EXCLUSION(".length() + 1, trimmedPattern.length() - 1);

        // 2. Parse recursively
        final TreePattern inclusionPattern =
            parsePatternFromString(
                inclusionSubstring, isTreeModelDataAllowedToBeCaptured, basePatternSupplier);
        final TreePattern exclusionPattern =
            parsePatternFromString(
                exclusionSubstring, isTreeModelDataAllowedToBeCaptured, basePatternSupplier);

        // 3. Build ExclusionTreePattern
        if (inclusionPattern instanceof IoTDBTreePatternOperations
            && exclusionPattern instanceof IoTDBTreePatternOperations) {
          return new WithExclusionIoTDBTreePattern(
              isTreeModelDataAllowedToBeCaptured,
              (IoTDBTreePatternOperations) inclusionPattern,
              (IoTDBTreePatternOperations) exclusionPattern);
        }
        return new WithExclusionTreePattern(
            isTreeModelDataAllowedToBeCaptured, inclusionPattern, exclusionPattern);
      } catch (final Exception e) {
        // Error during parsing (e.g., index out of bounds), treat as a normal pattern
        return buildUnionPattern(
            isTreeModelDataAllowedToBeCaptured,
            parseMultiplePatterns(trimmedPattern, basePatternSupplier));
      }
    }

    // 4. Not an Exclusion pattern, treat as a normal pattern
    return buildUnionPattern(
        isTreeModelDataAllowedToBeCaptured,
        parseMultiplePatterns(trimmedPattern, basePatternSupplier));
  }

  /**
   * A private helper method to parse a set of 'path' and 'pattern' keys into a single union
   * TreePattern. This contains the original logic of parsePipePatternFromSourceParameters.
   *
   * @param sourceParameters The source parameters.
   * @param isTreeModelDataAllowedToBeCaptured Flag for TreePattern constructor.
   * @param extractorPathKey Key for extractor path (e.g., "extractor.path").
   * @param sourcePathKey Key for source path (e.g., "source.path").
   * @param extractorPatternKey Key for extractor pattern (e.g., "extractor.pattern").
   * @param sourcePatternKey Key for source pattern (e.g., "source.pattern").
   * @param defaultPattern The pattern to return if both path and pattern are null. If this
   *     parameter is null, this method returns null.
   * @return The parsed TreePattern, or defaultPattern, or null if defaultPattern is null and no
   *     patterns are specified.
   */
  private static TreePattern parsePatternUnion(
      final PipeParameters sourceParameters,
      final boolean isTreeModelDataAllowedToBeCaptured,
      final String extractorPathKey,
      final String sourcePathKey,
      final String extractorPatternKey,
      final String sourcePatternKey,
      final TreePattern defaultPattern) {

    final String path = sourceParameters.getStringByKeys(extractorPathKey, sourcePathKey);
    final String pattern = sourceParameters.getStringByKeys(extractorPatternKey, sourcePatternKey);

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
    // return the provided default pattern (which may be null).
    return defaultPattern;
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

  private static List<TreePattern> parseMultiplePatterns(
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

  /** Helper method to find the matching closing parenthesis, respecting backticks. */
  private static int findMatchingParenthesis(final String text, final int openParenIndex) {
    int depth = 1;
    boolean inBackticks = false;

    for (int i = openParenIndex + 1; i < text.length(); i++) {
      final char c = text.charAt(i);

      if (c == '`') {
        inBackticks = !inBackticks;
      } else if (c == '(' && !inBackticks) {
        depth++;
      } else if (c == ')' && !inBackticks) {
        depth--;
        if (depth == 0) {
          return i; // Found the matching closing parenthesis
        }
      }
    }
    return -1; // Not found
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

  /**
   * Checks if the exclusion pattern fully or partially covers the inclusion pattern and logs a
   * warning or throws an exception.
   *
   * <p>This check uses the 'base' inclusion paths from both patterns. It is intended to catch
   * configuration errors where an exclusion rule negates all or part of an inclusion rule.
   *
   * <p>If the exclusion pattern *fully* covers the inclusion pattern (and the inclusion pattern is
   * not empty), this method will throw a {@link PipeException} to prevent the creation of a pipe
   * that matches nothing.
   *
   * <p>If the exclusion pattern *partially* covers the inclusion pattern, a WARN log will be
   * generated.
   *
   * @param inclusion The inclusion pattern.
   * @param exclusion The exclusion pattern.
   * @return An int array `[coveredCount, totalInclusionPaths]` for testing non-failing scenarios.
   * @throws PipeException If the inclusion pattern is fully covered by the exclusion pattern.
   */
  public static int[] checkAndLogPatternCoverage(
      final TreePattern inclusion, final TreePattern exclusion) throws PipeException {
    if (inclusion == null || exclusion == null) {
      return new int[] {0, 0};
    }

    final List<PartialPath> inclusionPaths;
    final List<PartialPath> exclusionPaths;
    int coveredCount = 0;

    try {
      // Get the list of individual paths from both patterns
      inclusionPaths = inclusion.getBaseInclusionPaths();
      exclusionPaths = exclusion.getBaseInclusionPaths();

      if (inclusionPaths.isEmpty() || exclusionPaths.isEmpty()) {
        // Nothing to check
        return new int[] {0, inclusionPaths.size()};
      }

      for (final PartialPath incPath : inclusionPaths) {
        // Check if *any* exclusion path includes this inclusion path
        final boolean isCovered =
            exclusionPaths.stream().anyMatch(excPath -> excPath.include(incPath));
        if (isCovered) {
          coveredCount++;
        }
      }
    } catch (final Exception e) {
      // This check is best-effort. Do not fail construction.
      LOGGER.warn(
          "Pipe: Failed to perform pattern coverage check for inclusion [{}] and exclusion [{}].",
          inclusion.getPattern(),
          exclusion.getPattern(),
          e);
      // Return -1 to indicate failure in tests
      return new int[] {-1, -1};
    }

    if (coveredCount == inclusionPaths.size() && !inclusionPaths.isEmpty()) {
      // All inclusion paths are covered by the exclusion
      final String msg =
          String.format(
              "Pipe: The provided exclusion pattern fully covers the inclusion pattern. "
                  + "This pipe pattern will match nothing. "
                  + "Inclusion: [%s], Exclusion: [%s]",
              inclusion.getPattern(), exclusion.getPattern());
      LOGGER.warn(msg);
      throw new PipeException(msg);
    } else if (coveredCount > 0) {
      // Some inclusion paths are covered
      LOGGER.warn(
          "Pipe: The provided exclusion pattern covers {} out of {} inclusion paths. "
              + "These paths will be excluded. "
              + "Inclusion: [{}], Exclusion: [{}]",
          coveredCount,
          inclusionPaths.size(),
          inclusion.getPattern(),
          exclusion.getPattern());
    }

    return new int[] {coveredCount, inclusionPaths.size()};
  }
}
