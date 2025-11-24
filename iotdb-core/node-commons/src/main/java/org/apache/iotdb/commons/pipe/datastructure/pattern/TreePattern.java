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

    // 1. Parse INCLUSION patterns into a list
    List<TreePattern> inclusionPatterns =
        parsePatternList(
            sourceParameters,
            isTreeModelDataAllowedToBeCaptured,
            EXTRACTOR_PATH_KEY,
            SOURCE_PATH_KEY,
            EXTRACTOR_PATTERN_KEY,
            SOURCE_PATTERN_KEY);

    // If no inclusion patterns are specified, use default "root.**"
    if (inclusionPatterns.isEmpty()) {
      inclusionPatterns =
          new ArrayList<>(
              Collections.singletonList(
                  new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, null)));
    }

    // 2. Parse EXCLUSION patterns into a list
    List<TreePattern> exclusionPatterns =
        parsePatternList(
            sourceParameters,
            isTreeModelDataAllowedToBeCaptured,
            EXTRACTOR_PATH_EXCLUSION_KEY,
            SOURCE_PATH_EXCLUSION_KEY,
            EXTRACTOR_PATTERN_EXCLUSION_KEY,
            SOURCE_PATTERN_EXCLUSION_KEY);

    // 3. Optimize the lists: remove redundant patterns (e.g., if "root.**" exists, "root.db" is
    // redundant)
    inclusionPatterns = optimizePatterns(inclusionPatterns);
    exclusionPatterns = optimizePatterns(exclusionPatterns);

    // 4. Prune inclusion patterns: if an inclusion pattern is fully covered by an exclusion
    // pattern, remove it
    inclusionPatterns = pruneInclusionPatterns(inclusionPatterns, exclusionPatterns);

    // 5. Check if the resulting inclusion pattern is empty
    if (inclusionPatterns.isEmpty()) {
      throw new PipeException(
          "Pipe: The inclusion pattern is empty after pruning by the exclusion pattern. "
              + "This pipe pattern will match nothing.");
    }

    // 6. Build final patterns
    final TreePattern finalInclusionPattern =
        buildUnionPattern(isTreeModelDataAllowedToBeCaptured, inclusionPatterns);

    if (exclusionPatterns.isEmpty()) {
      return finalInclusionPattern;
    }

    final TreePattern finalExclusionPattern =
        buildUnionPattern(isTreeModelDataAllowedToBeCaptured, exclusionPatterns);

    // 7. Combine inclusion and exclusion
    if (finalInclusionPattern instanceof IoTDBTreePatternOperations
        && finalExclusionPattern instanceof IoTDBTreePatternOperations) {
      return new WithExclusionIoTDBTreePattern(
          isTreeModelDataAllowedToBeCaptured,
          (IoTDBTreePatternOperations) finalInclusionPattern,
          (IoTDBTreePatternOperations) finalExclusionPattern);
    }

    return new WithExclusionTreePattern(
        isTreeModelDataAllowedToBeCaptured, finalInclusionPattern, finalExclusionPattern);
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
   * Helper method to parse pattern parameters into a list of patterns without creating the Union
   * object immediately.
   */
  private static List<TreePattern> parsePatternList(
      final PipeParameters sourceParameters,
      final boolean isTreeModelDataAllowedToBeCaptured,
      final String extractorPathKey,
      final String sourcePathKey,
      final String extractorPatternKey,
      final String sourcePatternKey) {

    final String path = sourceParameters.getStringByKeys(extractorPathKey, sourcePathKey);
    final String pattern = sourceParameters.getStringByKeys(extractorPatternKey, sourcePatternKey);

    final List<TreePattern> result = new ArrayList<>();

    if (path != null) {
      result.addAll(
          parseMultiplePatterns(
              path, p -> new IoTDBTreePattern(isTreeModelDataAllowedToBeCaptured, p)));
    }

    if (pattern != null) {
      result.addAll(
          parsePatternsFromPatternParameter(
              pattern, sourceParameters, isTreeModelDataAllowedToBeCaptured));
    }

    return result;
  }

  /**
   * Removes patterns from the list that are covered by other patterns in the same list. For
   * example, if "root.**" and "root.db.**" are present, "root.db.**" is removed.
   */
  private static List<TreePattern> optimizePatterns(final List<TreePattern> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return new ArrayList<>();
    }
    if (patterns.size() == 1) {
      return patterns;
    }

    final List<TreePattern> optimized = new ArrayList<>();
    // Determine coverage using base paths
    for (int i = 0; i < patterns.size(); i++) {
      final TreePattern current = patterns.get(i);
      boolean isCoveredByOther = false;

      for (int j = 0; j < patterns.size(); j++) {
        if (i == j) {
          continue;
        }
        final TreePattern other = patterns.get(j);

        // If 'other' covers 'current', 'current' is redundant.
        // Note: if they mutually cover each other (duplicates), we must ensure we keep one.
        // We use index comparison to break ties for exact duplicates.
        if (covers(other, current)) {
          if (covers(current, other)) {
            // Both cover each other (likely identical). Keep the one with smaller index.
            if (j < i) {
              isCoveredByOther = true;
              break;
            }
          } else {
            // Strict coverage
            isCoveredByOther = true;
            break;
          }
        }
      }

      if (!isCoveredByOther) {
        optimized.add(current);
      }
    }
    return optimized;
  }

  /**
   * Prunes patterns from the inclusion list that are fully covered by ANY pattern in the exclusion
   * list.
   */
  private static List<TreePattern> pruneInclusionPatterns(
      final List<TreePattern> inclusion, final List<TreePattern> exclusion) {
    if (inclusion == null || inclusion.isEmpty()) {
      return new ArrayList<>();
    }
    if (exclusion == null || exclusion.isEmpty()) {
      return inclusion;
    }

    final List<TreePattern> prunedInclusion = new ArrayList<>();
    for (final TreePattern inc : inclusion) {
      boolean isFullyExcluded = false;
      for (final TreePattern exc : exclusion) {
        if (covers(exc, inc)) {
          isFullyExcluded = true;
          break;
        }
      }
      if (!isFullyExcluded) {
        prunedInclusion.add(inc);
      }
    }
    return prunedInclusion;
  }

  /** Checks if 'coverer' pattern fully covers 'coveree' pattern. */
  private static boolean covers(final TreePattern coverer, final TreePattern coveree) {
    try {
      final List<PartialPath> covererPaths = coverer.getBaseInclusionPaths();
      final List<PartialPath> covereePaths = coveree.getBaseInclusionPaths();

      if (covererPaths.isEmpty() || covereePaths.isEmpty()) {
        return false;
      }

      // Logic: For 'coverer' to cover 'coveree', ALL paths in 'coveree' must be included
      // by at least one path in 'coverer'.
      for (final PartialPath sub : covereePaths) {
        boolean isSubCovered = false;
        for (final PartialPath sup : covererPaths) {
          if (sup.include(sub)) {
            isSubCovered = true;
            break;
          }
        }
        if (!isSubCovered) {
          return false;
        }
      }
      return true;
    } catch (final Exception e) {
      // In case of path parsing errors or unsupported operations, assume no coverage
      // to be safe and avoid aggressive pruning.
      return false;
    }
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
  private static TreePattern buildUnionPattern(
      final boolean isTreeModelDataAllowedToBeCaptured, final List<TreePattern> patterns) {
    if (patterns.size() == 1) {
      return patterns.get(0);
    }

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
