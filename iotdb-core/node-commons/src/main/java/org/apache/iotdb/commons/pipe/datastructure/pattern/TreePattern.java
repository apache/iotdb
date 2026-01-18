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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      final String msg =
          String.format(
              "Pipe: The provided exclusion pattern fully covers the inclusion pattern. "
                  + "This pipe pattern will match nothing. "
                  + "Inclusion: %s, Exclusion: %s",
              sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY),
              sourceParameters.getStringByKeys(
                  EXTRACTOR_PATTERN_EXCLUSION_KEY, SOURCE_PATTERN_EXCLUSION_KEY));
      LOGGER.warn(msg);
      throw new PipeException(msg);
    }

    // 6. Prune exclusion patterns: if an exclusion pattern does not overlap with
    // ANY of the remaining inclusion patterns, it is useless and should be removed.
    exclusionPatterns = pruneIrrelevantExclusions(inclusionPatterns, exclusionPatterns);

    // 7. Build final patterns
    final TreePattern finalInclusionPattern =
        buildUnionPattern(isTreeModelDataAllowedToBeCaptured, inclusionPatterns);

    if (exclusionPatterns.isEmpty()) {
      return finalInclusionPattern;
    }

    final TreePattern finalExclusionPattern =
        buildUnionPattern(isTreeModelDataAllowedToBeCaptured, exclusionPatterns);

    // 8. Combine inclusion and exclusion
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
   *
   * <p><b>Optimization Strategy:</b>
   *
   * <ol>
   *   <li><b>Sort First:</b> Patterns are sorted by "broadness" (shortest length and most wildcards
   *       first). This ensures that dominating patterns (like {@code root.**}) are processed first.
   *   <li><b>Filter with Trie:</b> Instead of comparing every pattern against every other pattern
   *       (O(N^2)), we build a Trie to check for coverage. For each pattern, we check if it is
   *       already "covered" by the Trie. If it is, we discard it; if not, we add it to the Trie.
   * </ol>
   *
   * <p><b>Time Complexity:</b> O(N * L), where N is the number of patterns and L is the average
   * path length.
   */
  private static List<TreePattern> optimizePatterns(final List<TreePattern> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return new ArrayList<>();
    }
    if (patterns.size() == 1) {
      return patterns;
    }

    // 1. Sort patterns by "Broadness"
    // Heuristic: Shorter paths and paths with wildcards should come first.
    // This allows us to insert 'root.**' first, so we can quickly skip 'root.sg.d1' later.
    final List<TreePattern> sortedPatterns = new ArrayList<>(patterns);
    sortedPatterns.sort(
        (o1, o2) -> {
          // We can only approximate comparison here since TreePattern represents multiple paths.
          // We use the first inclusion path as a representative.
          final PartialPath p1 = o1.getBaseInclusionPaths().get(0);
          final PartialPath p2 = o2.getBaseInclusionPaths().get(0);

          // 1. Length: Shorter is generally broader (e.g., root.** vs root.sg.d1)
          final int lenCompare = Integer.compare(p1.getNodeLength(), p2.getNodeLength());
          if (lenCompare != 0) {
            return lenCompare;
          }

          // 2. Wildcards: Pattern with wildcards is broader (e.g., root.sg.* vs root.sg.d1)
          final boolean w1 = p1.hasWildcard();
          final boolean w2 = p2.hasWildcard();
          if (w1 && !w2) {
            return -1;
          }
          if (!w1 && w2) {
            return 1;
          }

          // 3. Deterministic tie-breaker
          return p1.compareTo(p2);
        });

    // 2. Filter using Trie
    final PatternTrie trie = new PatternTrie();
    final List<TreePattern> optimized = new ArrayList<>();

    for (final TreePattern pattern : sortedPatterns) {
      boolean isCovered = true;
      // A pattern is redundant only if ALL its base paths are covered by the Trie
      for (final PartialPath path : pattern.getBaseInclusionPaths()) {
        if (!trie.isCovered(path)) {
          isCovered = false;
          break;
        }
      }

      if (!isCovered) {
        optimized.add(pattern);
        // Add all its paths to the Trie to cover future patterns
        for (final PartialPath path : pattern.getBaseInclusionPaths()) {
          trie.add(path);
        }
      }
    }

    return optimized;
  }

  /**
   * Prunes patterns from the inclusion list that are fully covered by ANY pattern in the exclusion
   * list.
   *
   * <p><b>Optimization Strategy:</b>
   *
   * <ol>
   *   <li><b>Build Exclusion Trie:</b> Construct a Trie containing all paths from the exclusion
   *       list. This aggregates the coverage of all exclusion patterns into a single structure.
   *   <li><b>Check Coverage:</b> Iterate through the inclusion list. For each inclusion pattern,
   *       check if ALL of its represented paths are covered by the Exclusion Trie. If so, the
   *       pattern is redundant and removed.
   * </ol>
   *
   * <p><b>Time Complexity:</b> O((N + M) * L), where N is the number of inclusion patterns, M is
   * the number of exclusion patterns, and L is the average path length.
   */
  private static List<TreePattern> pruneInclusionPatterns(
      final List<TreePattern> inclusion, final List<TreePattern> exclusion) {
    if (inclusion == null || inclusion.isEmpty()) {
      return new ArrayList<>();
    }
    if (exclusion == null || exclusion.isEmpty()) {
      return inclusion;
    }

    // 1. Build Trie with all exclusion paths
    // The Trie represents the union of all excluded areas.
    final PatternTrie exclusionTrie = new PatternTrie();
    for (final TreePattern exc : exclusion) {
      for (final PartialPath path : exc.getBaseInclusionPaths()) {
        exclusionTrie.add(path);
      }
    }

    final List<TreePattern> prunedInclusion = new ArrayList<>();
    // 2. Filter inclusion patterns
    for (final TreePattern inc : inclusion) {
      boolean isFullyExcluded = true;
      // An inclusion pattern is fully excluded only if ALL its constituent base paths
      // are covered by the exclusion Trie.
      for (final PartialPath path : inc.getBaseInclusionPaths()) {
        if (!exclusionTrie.isCovered(path)) {
          isFullyExcluded = false;
          break;
        }
      }

      // If not fully excluded (i.e., at least one path survives), keep it.
      if (!isFullyExcluded) {
        prunedInclusion.add(inc);
      }
    }
    return prunedInclusion;
  }

  /**
   * Prunes patterns from the exclusion list that do NOT overlap with any of the remaining inclusion
   * patterns.
   *
   * <p><b>Optimization Strategy:</b>
   *
   * <ol>
   *   <li><b>Build Inclusion Trie:</b> Construct a Trie containing all paths from the inclusion
   *       list. This aggregates the search space of inclusion patterns.
   *   <li><b>Filter Exclusions:</b> Iterate through the exclusion list. For each exclusion pattern,
   *       check if it overlaps with the Inclusion Trie. Only exclusions that overlap with at least
   *       one inclusion pattern are kept.
   * </ol>
   *
   * <p><b>Time Complexity:</b> O((N + M) * L), where N is the number of inclusion patterns, M is
   * the number of exclusion patterns, and L is the average path length.
   */
  private static List<TreePattern> pruneIrrelevantExclusions(
      final List<TreePattern> inclusion, final List<TreePattern> exclusion) {
    if (exclusion == null || exclusion.isEmpty()) {
      return new ArrayList<>();
    }
    if (inclusion == null || inclusion.isEmpty()) {
      // If inclusion is empty, exclusion is irrelevant anyway.
      return new ArrayList<>();
    }

    // 1. Build Trie from Inclusion Patterns
    final PatternTrie inclusionTrie = new PatternTrie();
    for (final TreePattern inc : inclusion) {
      for (final PartialPath path : inc.getBaseInclusionPaths()) {
        inclusionTrie.add(path);
      }
    }

    // 2. Filter Exclusion Patterns using the Trie
    final List<TreePattern> relevantExclusion = new ArrayList<>();
    for (final TreePattern exc : exclusion) {
      boolean overlapsWithAnyInclusion = false;
      // An exclusion pattern is relevant if ANY of its base paths overlap with the inclusion Trie
      for (final PartialPath path : exc.getBaseInclusionPaths()) {
        if (inclusionTrie.overlaps(path)) {
          overlapsWithAnyInclusion = true;
          break;
        }
      }

      if (overlapsWithAnyInclusion) {
        relevantExclusion.add(exc);
      }
    }
    return relevantExclusion;
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
  @TestOnly
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

  /** A specialized Trie to efficiently check path coverage. */
  private static class PatternTrie {
    private final TrieNode root = new TrieNode();

    private static class TrieNode {
      // Children nodes mapped by specific path segments (excluding *)
      Map<String, TrieNode> children = new HashMap<>();
      // Optimized field for One Level Wildcard (*) child to reduce map lookups
      TrieNode wildcardNode = null;

      // Marks if a pattern ends here (e.g., "root.sg" is a set path)
      boolean isLeaf = false;
      // Special flags for optimization
      boolean isMultiLevelWildcard = false; // Ends with **
    }

    /** Adds a path to the Trie. */
    public void add(final PartialPath path) {
      TrieNode node = root;
      final String[] nodes = path.getNodes();

      for (final String segment : nodes) {
        // If we are at a node that is already a MultiLevelWildcard (**),
        // everything below is already covered. We can stop adding.
        if (node.isMultiLevelWildcard) {
          return;
        }

        // Check for Multi-Level Wildcard (**)
        if (segment.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
          node.isMultiLevelWildcard = true;
          // Optimization: clear children as ** covers everything
          node.children = Collections.emptyMap();
          node.wildcardNode = null;
          node.isLeaf = true;
          return;
        }

        // Check for One-Level Wildcard (*)
        if (segment.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
          if (node.wildcardNode == null) {
            node.wildcardNode = new TrieNode();
          }
          node = node.wildcardNode;
        } else {
          // Regular specific node
          node = node.children.computeIfAbsent(segment, k -> new TrieNode());
        }
      }
      node.isLeaf = true;
    }

    /** Checks if the given path is covered by any existing pattern in the Trie. */
    public boolean isCovered(final PartialPath path) {
      return checkCoverage(root, path.getNodes(), 0);
    }

    private boolean checkCoverage(final TrieNode node, final String[] pathNodes, final int index) {
      // 1. If the Trie node is a Multi-Level Wildcard (**), it covers everything remainder
      if (node.isMultiLevelWildcard) {
        return true;
      }

      // 2. If we reached the end of the query path
      if (index >= pathNodes.length) {
        // The path is covered if the Trie also ends here (isLeaf)
        return node.isLeaf;
      }

      final String currentSegment = pathNodes[index];

      // 3. Direct Match in Trie
      final TrieNode child = node.children.get(currentSegment);
      if (child != null && checkCoverage(child, pathNodes, index + 1)) {
        return true;
      }

      // 4. Single Level Wildcard (*) in Trie
      if (node.wildcardNode != null) {
        return checkCoverage(node.wildcardNode, pathNodes, index + 1);
      }

      return false;
    }

    /** Checks if the given path overlaps with any pattern in the Trie. */
    public boolean overlaps(final PartialPath path) {
      return checkOverlap(root, path.getNodes(), 0);
    }

    private boolean checkOverlap(final TrieNode node, final String[] pathNodes, final int index) {
      // 1. If Trie has '**', it overlaps everything.
      if (node.isMultiLevelWildcard) {
        return true;
      }

      // 2. If Query Path has '**', it overlaps everything remaining in this valid branch.
      if (index < pathNodes.length
          && pathNodes[index].equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        return true;
      }

      // 3. End of Query Path: Overlap exists if Trie also ends here.
      if (index >= pathNodes.length) {
        return node.isLeaf;
      }

      final String pNode = pathNodes[index];

      // 4. Case: Query Node is '*' (matches any child in Trie, both specific and wildcard)
      if (pNode.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
        // Check all specific children
        for (final TrieNode child : node.children.values()) {
          if (checkOverlap(child, pathNodes, index + 1)) {
            return true;
          }
        }
        // Check wildcard child
        if (node.wildcardNode != null) {
          return checkOverlap(node.wildcardNode, pathNodes, index + 1);
        }
        return false;
      }

      // 5. Case: Query Node is specific (e.g., "d1")
      // 5a. Check exact match in Trie
      final TrieNode exactChild = node.children.get(pNode);
      if (exactChild != null && checkOverlap(exactChild, pathNodes, index + 1)) {
        return true;
      }

      // 5b. Check '*' in Trie (matches specific query node)
      return node.wildcardNode != null && checkOverlap(node.wildcardNode, pathNodes, index + 1);
    }
  }
}
