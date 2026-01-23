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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

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

public abstract class PipePattern {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePattern.class);

  //////////////////////////// Interface ////////////////////////////

  public abstract String getPattern();

  public abstract boolean isRoot();

  public abstract boolean isSingle();

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
   * Interpret from source parameters and get a {@link PipePattern}. This method parses both
   * inclusion and exclusion patterns.
   *
   * @return The interpreted {@link PipePattern} which is not {@code null}.
   */
  public static PipePattern parsePipePatternFromSourceParameters(
      final PipeParameters sourceParameters) {
    final PipePattern pipePattern = parsePipePatternFromSourceParametersInternal(sourceParameters);
    if (!pipePattern.isSingle()) {
      final String msg =
          String.format(
              "Pipe: The provided pattern should be single now. Inclusion: %s, Exclusion: %s",
              sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY),
              sourceParameters.getStringByKeys(
                  EXTRACTOR_PATTERN_EXCLUSION_KEY, SOURCE_PATTERN_EXCLUSION_KEY));
      LOGGER.warn(msg);
      throw new PipeException(msg);
    }
    return pipePattern;
  }

  public static PipePattern parsePipePatternFromSourceParametersInternal(
      final PipeParameters sourceParameters) {
    // 1. Parse INCLUSION patterns into a list
    List<PipePattern> inclusionPatterns =
        parsePatternList(
            sourceParameters,
            EXTRACTOR_PATH_KEY,
            SOURCE_PATH_KEY,
            EXTRACTOR_PATTERN_KEY,
            SOURCE_PATTERN_KEY);

    // If no inclusion patterns are specified, use default "root.**"
    if (inclusionPatterns.isEmpty()) {
      inclusionPatterns = new ArrayList<>(Collections.singletonList(new IoTDBPipePattern(null)));
    }

    // 2. Parse EXCLUSION patterns into a list
    List<PipePattern> exclusionPatterns =
        parsePatternList(
            sourceParameters,
            EXTRACTOR_PATH_EXCLUSION_KEY,
            SOURCE_PATH_EXCLUSION_KEY,
            EXTRACTOR_PATTERN_EXCLUSION_KEY,
            SOURCE_PATTERN_EXCLUSION_KEY);

    // 3. Optimize the lists: remove redundant patterns
    inclusionPatterns = optimizePatterns(inclusionPatterns);
    exclusionPatterns = optimizePatterns(exclusionPatterns);

    // 4. Prune inclusion patterns covered by exclusions
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

    // 6. Prune exclusion patterns that do not overlap with any inclusion
    exclusionPatterns = pruneIrrelevantExclusions(inclusionPatterns, exclusionPatterns);

    // 7. Build final patterns
    final PipePattern finalInclusionPattern = buildUnionPattern(inclusionPatterns);

    if (exclusionPatterns.isEmpty()) {
      return finalInclusionPattern;
    }

    final PipePattern finalExclusionPattern = buildUnionPattern(exclusionPatterns);

    // 8. Combine inclusion and exclusion
    if (finalInclusionPattern instanceof IoTDBPipePatternOperations
        && finalExclusionPattern instanceof IoTDBPipePatternOperations) {
      return new WithExclusionIoTDBPipePattern(
          (IoTDBPipePatternOperations) finalInclusionPattern,
          (IoTDBPipePatternOperations) finalExclusionPattern);
    }

    return new WithExclusionPipePattern(finalInclusionPattern, finalExclusionPattern);
  }

  /**
   * The main entry point for parsing a pattern string. This method can handle simple patterns
   * ("root.a"), union patterns ("root.a,root.b"), and exclusion patterns ("INCLUSION(root.a),
   * EXCLUSION(root.b)").
   */
  public static PipePattern parsePatternFromString(
      final String patternString, final Function<String, PipePattern> basePatternSupplier) {
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
        return buildUnionPattern(parseMultiplePatterns(trimmedPattern, basePatternSupplier));
      }

      // Look for the ", EXCLUSION(" part
      final String remaining = trimmedPattern.substring(inclusionEndIndex + 1).trim();
      if (!remaining.startsWith(", EXCLUSION(")) {
        // Malformed, treat as a normal pattern
        return buildUnionPattern(parseMultiplePatterns(trimmedPattern, basePatternSupplier));
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
        final PipePattern inclusionPattern =
            parsePatternFromString(inclusionSubstring, basePatternSupplier);
        final PipePattern exclusionPattern =
            parsePatternFromString(exclusionSubstring, basePatternSupplier);

        // 3. Build ExclusionPipePattern
        if (inclusionPattern instanceof IoTDBPipePatternOperations
            && exclusionPattern instanceof IoTDBPipePatternOperations) {
          return new WithExclusionIoTDBPipePattern(
              (IoTDBPipePatternOperations) inclusionPattern,
              (IoTDBPipePatternOperations) exclusionPattern);
        }
        return new WithExclusionPipePattern(inclusionPattern, exclusionPattern);
      } catch (final Exception e) {
        // Error during parsing (e.g., index out of bounds), treat as a normal pattern
        return buildUnionPattern(parseMultiplePatterns(trimmedPattern, basePatternSupplier));
      }
    }

    // 4. Not an Exclusion pattern, treat as a normal pattern
    return buildUnionPattern(parseMultiplePatterns(trimmedPattern, basePatternSupplier));
  }

  /**
   * Helper method to parse pattern parameters into a list of patterns without creating the Union
   * object immediately.
   */
  private static List<PipePattern> parsePatternList(
      final PipeParameters sourceParameters,
      final String extractorPathKey,
      final String sourcePathKey,
      final String extractorPatternKey,
      final String sourcePatternKey) {

    final String path = sourceParameters.getStringByKeys(extractorPathKey, sourcePathKey);
    final String pattern = sourceParameters.getStringByKeys(extractorPatternKey, sourcePatternKey);

    final List<PipePattern> result = new ArrayList<>();

    if (path != null) {
      result.addAll(parseMultiplePatterns(path, IoTDBPipePattern::new));
    }

    if (pattern != null) {
      result.addAll(parsePatternsFromPatternParameter(pattern, sourceParameters));
    }

    return result;
  }

  /**
   * Removes patterns from the list that are covered by other patterns in the same list. For
   * example, if "root.**" and "root.db.**" are present, "root.db.**" is removed.
   */
  private static List<PipePattern> optimizePatterns(final List<PipePattern> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return new ArrayList<>();
    }
    if (patterns.size() == 1) {
      return patterns;
    }

    // 1. Sort patterns by "Broadness"
    final List<PipePattern> sortedPatterns = new ArrayList<>(patterns);
    sortedPatterns.sort(
        (o1, o2) -> {
          final PartialPath p1 = o1.getBaseInclusionPaths().get(0);
          final PartialPath p2 = o2.getBaseInclusionPaths().get(0);

          final int lenCompare = Integer.compare(p1.getNodeLength(), p2.getNodeLength());
          if (lenCompare != 0) {
            return lenCompare;
          }

          final boolean w1 = p1.hasWildcard();
          final boolean w2 = p2.hasWildcard();
          if (w1 && !w2) {
            return -1;
          }
          if (!w1 && w2) {
            return 1;
          }

          return p1.compareTo(p2);
        });

    // 2. Filter using Trie
    final PatternTrie trie = new PatternTrie();
    final List<PipePattern> optimized = new ArrayList<>();

    for (final PipePattern pattern : sortedPatterns) {
      boolean isCovered = true;
      for (final PartialPath path : pattern.getBaseInclusionPaths()) {
        if (!trie.isCovered(path)) {
          isCovered = false;
          break;
        }
      }

      if (!isCovered) {
        optimized.add(pattern);
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
   */
  private static List<PipePattern> pruneInclusionPatterns(
      final List<PipePattern> inclusion, final List<PipePattern> exclusion) {
    if (inclusion == null || inclusion.isEmpty()) {
      return new ArrayList<>();
    }
    if (exclusion == null || exclusion.isEmpty()) {
      return inclusion;
    }

    final PatternTrie exclusionTrie = new PatternTrie();
    for (final PipePattern exc : exclusion) {
      for (final PartialPath path : exc.getBaseInclusionPaths()) {
        exclusionTrie.add(path);
      }
    }

    final List<PipePattern> prunedInclusion = new ArrayList<>();
    for (final PipePattern inc : inclusion) {
      boolean isFullyExcluded = true;
      for (final PartialPath path : inc.getBaseInclusionPaths()) {
        if (!exclusionTrie.isCovered(path)) {
          isFullyExcluded = false;
          break;
        }
      }

      if (!isFullyExcluded) {
        prunedInclusion.add(inc);
      }
    }
    return prunedInclusion;
  }

  /**
   * Prunes patterns from the exclusion list that do NOT overlap with any of the remaining inclusion
   * patterns.
   */
  private static List<PipePattern> pruneIrrelevantExclusions(
      final List<PipePattern> inclusion, final List<PipePattern> exclusion) {
    if (exclusion == null || exclusion.isEmpty()) {
      return new ArrayList<>();
    }
    if (inclusion == null || inclusion.isEmpty()) {
      return new ArrayList<>();
    }

    final PatternTrie inclusionTrie = new PatternTrie();
    for (final PipePattern inc : inclusion) {
      for (final PartialPath path : inc.getBaseInclusionPaths()) {
        inclusionTrie.add(path);
      }
    }

    final List<PipePattern> relevantExclusion = new ArrayList<>();
    for (final PipePattern exc : exclusion) {
      boolean overlapsWithAnyInclusion = false;
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

  private static List<PipePattern> parseMultiplePatterns(
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
  private static PipePattern buildUnionPattern(final List<PipePattern> patterns) {
    if (patterns.size() == 1) {
      return patterns.get(0);
    }

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
      final PipePattern inclusion, final PipePattern exclusion) throws PipeException {
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
      Map<String, TrieNode> children = new HashMap<>();
      TrieNode wildcardNode = null;

      boolean isLeaf = false;
      boolean isMultiLevelWildcard = false;
    }

    public void add(final PartialPath path) {
      TrieNode node = root;
      final String[] nodes = path.getNodes();

      for (final String segment : nodes) {
        if (node.isMultiLevelWildcard) {
          return;
        }

        if (segment.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
          node.isMultiLevelWildcard = true;
          node.children = Collections.emptyMap();
          node.wildcardNode = null;
          node.isLeaf = true;
          return;
        }

        if (segment.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
          if (node.wildcardNode == null) {
            node.wildcardNode = new TrieNode();
          }
          node = node.wildcardNode;
        } else {
          node = node.children.computeIfAbsent(segment, k -> new TrieNode());
        }
      }
      node.isLeaf = true;
    }

    public boolean isCovered(final PartialPath path) {
      return checkCoverage(root, path.getNodes(), 0);
    }

    private boolean checkCoverage(final TrieNode node, final String[] pathNodes, final int index) {
      if (node.isMultiLevelWildcard) {
        return true;
      }

      if (index >= pathNodes.length) {
        return node.isLeaf;
      }

      final String currentSegment = pathNodes[index];

      final TrieNode child = node.children.get(currentSegment);
      if (child != null && checkCoverage(child, pathNodes, index + 1)) {
        return true;
      }

      if (node.wildcardNode != null) {
        return checkCoverage(node.wildcardNode, pathNodes, index + 1);
      }

      return false;
    }

    public boolean overlaps(final PartialPath path) {
      return checkOverlap(root, path.getNodes(), 0);
    }

    private boolean checkOverlap(final TrieNode node, final String[] pathNodes, final int index) {
      if (node.isMultiLevelWildcard) {
        return true;
      }

      if (index < pathNodes.length
          && pathNodes[index].equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        return true;
      }

      if (index >= pathNodes.length) {
        return node.isLeaf;
      }

      final String pNode = pathNodes[index];

      if (pNode.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
        for (final TrieNode child : node.children.values()) {
          if (checkOverlap(child, pathNodes, index + 1)) {
            return true;
          }
        }
        if (node.wildcardNode != null) {
          return checkOverlap(node.wildcardNode, pathNodes, index + 1);
        }
        return false;
      }

      final TrieNode exactChild = node.children.get(pNode);
      if (exactChild != null && checkOverlap(exactChild, pathNodes, index + 1)) {
        return true;
      }

      return node.wildcardNode != null && checkOverlap(node.wildcardNode, pathNodes, index + 1);
    }
  }
}
