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

package org.apache.iotdb.db.queryengine.plan.execution.config.executor;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.config.PipeSourceTreePatternUtils;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.cartesian.BindSchemaForExpressionVisitor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_KEY;

/**
 * Resolves user-visible tree source patterns into CREATE/ALTER-time internal physical path
 * snapshots for renamed series. The internal paths are only used by DataRegion data matching and
 * are intentionally not a dynamic alias resolver.
 */
final class PipeRenamedSeriesSourceParameterResolver {

  private static final String PATTERN_LIST_SEPARATOR = ",";
  private static final char BACK_QUOTE = '`';
  private static final String FULL_TREE_PATTERN =
      IoTDBConstant.PATH_ROOT
          + IoTDBConstant.PATH_SEPARATOR
          + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

  private static final List<String> PATH_INCLUSION_KEYS =
      Arrays.asList(SOURCE_PATH_KEY, EXTRACTOR_PATH_KEY);

  private static final List<String> PATH_EXCLUSION_KEYS =
      Arrays.asList(SOURCE_PATH_EXCLUSION_KEY, EXTRACTOR_PATH_EXCLUSION_KEY);

  private static final List<String> PATTERN_INCLUSION_KEYS =
      Arrays.asList(
          SOURCE_PATTERN_INCLUSION_KEY,
          EXTRACTOR_PATTERN_INCLUSION_KEY,
          SOURCE_PATTERN_KEY,
          EXTRACTOR_PATTERN_KEY);

  private static final List<String> PATTERN_EXCLUSION_KEYS =
      Arrays.asList(SOURCE_PATTERN_EXCLUSION_KEY, EXTRACTOR_PATTERN_EXCLUSION_KEY);

  private static final List<SourceAttributeGroup> SOURCE_ATTRIBUTE_GROUPS =
      Arrays.asList(
          SourceAttributeGroup.pathInclusion(PATH_INCLUSION_KEYS),
          SourceAttributeGroup.pathExclusion(PATH_EXCLUSION_KEYS),
          SourceAttributeGroup.patternInclusion(PATTERN_INCLUSION_KEYS),
          SourceAttributeGroup.patternExclusion(PATTERN_EXCLUSION_KEYS));

  private PipeRenamedSeriesSourceParameterResolver() {}

  static PipeParameters resolve(final PipeParameters parameters) {
    PipeSourceConstant.validateUserProvidedInternalSourcePatternAttributes(
        parameters.getAttribute(), null, false);
    final Map<String, String> attributes = new HashMap<>(parameters.getAttribute());
    PipeSourceConstant.stripInternalSourceAttributes(attributes);

    final ResolutionState state = new ResolutionState();
    for (final SourceAttributeGroup group : SOURCE_ATTRIBUTE_GROUPS) {
      resolveAttributeGroup(attributes, state, group);
    }

    applyInternalAttributes(attributes, state);
    return new PipeParameters(attributes);
  }

  private static void resolveAttributeGroup(
      final Map<String, String> attributes,
      final ResolutionState state,
      final SourceAttributeGroup group) {
    for (final String key : group.keys) {
      if (group.isPath) {
        resolvePathAttribute(attributes, key, state, group.isInclusion);
      } else {
        resolvePatternListAttribute(
            attributes, key, state, group.isInclusion, isIoTDBFormat(attributes));
      }
    }
  }

  /**
   * Path parameters only support a single pattern. User-visible values are kept as-is; renamed or
   * invalid alias paths are stored as physical-path snapshots in internal keys.
   *
   * @throws PipeException if an inclusion path only matches an invalid physical series.
   */
  private static void resolvePathAttribute(
      final Map<String, String> attributes,
      final String key,
      final ResolutionState state,
      final boolean isInclusion) {
    final String value = attributes.get(key);
    if (value == null || value.trim().isEmpty()) {
      return;
    }

    if (isInclusion && isFullTreePattern(value.trim())) {
      return;
    }

    if (hasWildcard(value)) {
      snapshotPattern(key, value.trim(), state, isInclusion, true);
      return;
    }

    final ResolveResult result = resolveExactPattern(key, value.trim(), state, isInclusion);
    if (isInclusion && result.onlyInvalidPhysicalDirectMatch) {
      throw new PipeException(
          String.format(
              DataNodePipeMessages.PIPE_SOURCE_ONLY_MATCHES_INVALID_RENAMED_PHYSICAL_SERIES,
              key,
              value));
    }
    applyExactResolveResult(result, state, isInclusion);
  }

  private static void resolvePatternListAttribute(
      final Map<String, String> attributes,
      final String key,
      final ResolutionState state,
      final boolean isInclusion,
      final boolean isIoTDBFormat) {
    final String value = attributes.get(key);
    if (value == null || value.trim().isEmpty()) {
      return;
    }

    final List<String> patterns = PipeSourceTreePatternUtils.splitPatternList(value);
    if (isInclusion && containsFullTreePattern(patterns)) {
      return;
    }

    final PatternListResolveState patternListResolveState = new PatternListResolveState();

    for (final String pattern : patterns) {
      resolvePatternListSegment(
          key, pattern, state, patternListResolveState, isInclusion, isIoTDBFormat);
    }

    if (isInclusion
        && patternListResolveState.hasOnlyInvalidPhysicalDirectMatch
        && !patternListResolveState.hasResolvableInclusion
        && state.internalInclusions.isEmpty()) {
      throw new PipeException(
          String.format(
              DataNodePipeMessages.PIPE_SOURCE_ONLY_MATCHES_INVALID_RENAMED_PHYSICAL_SERIES,
              key,
              value));
    }
  }

  private static void resolvePatternListSegment(
      final String key,
      final String pattern,
      final ResolutionState state,
      final PatternListResolveState patternListResolveState,
      final boolean isInclusion,
      final boolean isIoTDBFormat) {
    final String trimmedPattern = pattern.trim();
    if (trimmedPattern.isEmpty()) {
      return;
    }

    if (!isIoTDBFormat || hasWildcard(trimmedPattern)) {
      snapshotPattern(key, trimmedPattern, state, isInclusion, isIoTDBFormat);
      patternListResolveState.hasResolvableInclusion |= isInclusion;
      return;
    }

    final ResolveResult result = resolveExactPattern(key, trimmedPattern, state, isInclusion);
    patternListResolveState.record(result, isInclusion);
    applyExactResolveResult(result, state, isInclusion);
  }

  private static void applyExactResolveResult(
      final ResolveResult result, final ResolutionState state, final boolean isInclusion) {
    if (isInclusion) {
      state.internalInclusions.addAll(result.physicalInclusions);
      state.internalInvalidExclusions.addAll(result.invalidPhysicalExclusions);
    } else {
      state.internalExclusions.addAll(result.physicalExclusions);
    }
  }

  /**
   * Snapshot alias-related physical paths matched by a wildcard pattern at CREATE/ALTER time. User
   * source attributes are kept unchanged; internal attributes record path-only runtime decisions.
   */
  private static void snapshotPattern(
      final String key,
      final String pattern,
      final ResolutionState state,
      final boolean isInclusion,
      final boolean isIoTDBFormat) {
    final List<MeasurementPath> measurementPaths =
        fetchMeasurementPaths(key, getSchemaFetchPatterns(pattern, isIoTDBFormat));
    for (final MeasurementPath measurementPath : measurementPaths) {
      if (isInclusion) {
        collectSnapshotInclusion(measurementPath, pattern, state, isIoTDBFormat);
      } else {
        collectSnapshotExclusion(measurementPath, pattern, state.internalExclusions, isIoTDBFormat);
      }
    }
  }

  private static void collectSnapshotInclusion(
      final MeasurementPath measurementPath,
      final String pattern,
      final ResolutionState state,
      final boolean isIoTDBFormat) {
    final Map<String, String> props = getProps(measurementPath);
    if (MeasurementPropsUtils.isRenamed(props)) {
      final PartialPath originalPath = MeasurementPropsUtils.getOriginalPath(props);
      if (originalPath != null) {
        state.internalInclusions.add(originalPath.getFullPath());
      }
      return;
    }
    if (MeasurementPropsUtils.isInvalid(props)) {
      final PartialPath aliasPath = MeasurementPropsUtils.getAliasPath(props);
      if (aliasPath != null && matchesPattern(pattern, aliasPath, isIoTDBFormat)) {
        state.internalInclusions.add(measurementPath.getFullPath());
        state.aliasPathToPhysicalPath.put(aliasPath.getFullPath(), measurementPath.getFullPath());
      } else {
        state.internalInvalidExclusions.add(measurementPath.getFullPath());
      }
    }
  }

  private static void collectSnapshotExclusion(
      final MeasurementPath measurementPath,
      final String pattern,
      final Set<String> internalExclusions,
      final boolean isIoTDBFormat) {
    final Map<String, String> props = getProps(measurementPath);
    if (MeasurementPropsUtils.isRenamed(props)) {
      final PartialPath originalPath = MeasurementPropsUtils.getOriginalPath(props);
      if (originalPath != null) {
        internalExclusions.add(originalPath.getFullPath());
      }
      return;
    }
    if (MeasurementPropsUtils.isInvalid(props)) {
      final PartialPath aliasPath = MeasurementPropsUtils.getAliasPath(props);
      if (aliasPath != null && matchesPattern(pattern, aliasPath, isIoTDBFormat)) {
        internalExclusions.add(measurementPath.getFullPath());
      }
    }
  }

  private static ResolveResult resolveExactPattern(
      final String key,
      final String pattern,
      final ResolutionState state,
      final boolean isInclusion) {
    final ResolveResult result = new ResolveResult();
    final List<MeasurementPath> measurementPaths = fetchMeasurementPaths(key, pattern);
    if (measurementPaths.isEmpty()) {
      collectExactSnapshotAliasMatch(pattern, state, result, isInclusion);
      return result;
    }

    for (final MeasurementPath measurementPath : measurementPaths) {
      if (BindSchemaForExpressionVisitor.isAliasSeries(measurementPath)) {
        collectExactAliasMatch(measurementPath, result, isInclusion);
      } else {
        collectExactInvalidMatch(pattern, measurementPath, result, isInclusion);
      }
    }
    return result;
  }

  private static void collectExactAliasMatch(
      final MeasurementPath measurementPath,
      final ResolveResult result,
      final boolean isInclusion) {
    final PartialPath originalPath =
        BindSchemaForExpressionVisitor.getOriginalPathFromAliasSeries(measurementPath);
    if (originalPath == null) {
      return;
    }
    if (isInclusion) {
      result.physicalInclusions.add(originalPath.getFullPath());
    } else {
      result.physicalExclusions.add(originalPath.getFullPath());
    }
  }

  private static void collectExactSnapshotAliasMatch(
      final String pattern,
      final ResolutionState state,
      final ResolveResult result,
      final boolean isInclusion) {
    final String physicalPath = state.aliasPathToPhysicalPath.get(pattern);
    if (physicalPath == null) {
      return;
    }
    if (isInclusion) {
      result.physicalInclusions.add(physicalPath);
    } else {
      result.physicalExclusions.add(physicalPath);
    }
  }

  private static void collectExactInvalidMatch(
      final String pattern,
      final MeasurementPath measurementPath,
      final ResolveResult result,
      final boolean isInclusion) {
    final Map<String, String> props = getProps(measurementPath);
    if (!MeasurementPropsUtils.isInvalid(props)) {
      return;
    }
    if (matchesAliasPath(pattern, props)) {
      if (isInclusion) {
        result.physicalInclusions.add(measurementPath.getFullPath());
      } else {
        result.physicalExclusions.add(measurementPath.getFullPath());
      }
      return;
    }
    if (!isInclusion) {
      return;
    }
    result.invalidPhysicalExclusions.add(measurementPath.getFullPath());
    if (measurementPath.getFullPath().equals(pattern)) {
      result.onlyInvalidPhysicalDirectMatch = true;
    }
  }

  private static boolean matchesAliasPath(final String pattern, final Map<String, String> props) {
    final PartialPath aliasPath = MeasurementPropsUtils.getAliasPath(props);
    return aliasPath != null && aliasPath.getFullPath().equals(pattern);
  }

  private static void applyInternalAttributes(
      final Map<String, String> attributes, final ResolutionState state) {
    final Set<String> effectiveInternalInclusions = new LinkedHashSet<>(state.internalInclusions);
    effectiveInternalInclusions.removeAll(state.internalExclusions);
    if (!effectiveInternalInclusions.isEmpty()) {
      attributes.put(
          PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY,
          joinPatternList(effectiveInternalInclusions));
    }

    final Set<String> effectiveInternalExclusions = new LinkedHashSet<>(state.internalExclusions);
    effectiveInternalExclusions.addAll(state.internalInvalidExclusions);
    effectiveInternalExclusions.removeAll(effectiveInternalInclusions);
    if (!effectiveInternalExclusions.isEmpty()) {
      attributes.put(
          PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY,
          joinPatternList(effectiveInternalExclusions));
    }
  }

  private static boolean matchesPattern(
      final String pattern, final PartialPath path, final boolean isIoTDBFormat) {
    try {
      for (final String schemaFetchPattern : getSchemaFetchPatterns(pattern, isIoTDBFormat)) {
        if (new MeasurementPath(schemaFetchPattern).overlapWith(path)) {
          return true;
        }
      }
      return false;
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  private static boolean isIoTDBFormat(final Map<String, String> attributes) {
    final String patternFormat =
        new PipeParameters(attributes)
            .getStringByKeys(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY);
    return patternFormat != null
        && EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE.equalsIgnoreCase(patternFormat);
  }

  private static List<String> getSchemaFetchPatterns(
      final String pattern, final boolean isIoTDBFormat) {
    final List<String> patterns = new ArrayList<>();
    patterns.add(pattern);
    if (!isIoTDBFormat && !hasWildcard(pattern)) {
      patterns.add(
          pattern + IoTDBConstant.PATH_SEPARATOR + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD);
    }
    return patterns;
  }

  private static boolean hasWildcard(final String pattern) {
    if (pattern == null) {
      return false;
    }
    boolean inBackticks = false;
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == BACK_QUOTE) {
        if (inBackticks && i + 1 < pattern.length() && pattern.charAt(i + 1) == BACK_QUOTE) {
          i++;
          continue;
        }
        inBackticks = !inBackticks;
      } else if (!inBackticks && c == IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.charAt(0)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isFullTreePattern(final String pattern) {
    return FULL_TREE_PATTERN.equals(pattern);
  }

  private static boolean containsFullTreePattern(final List<String> patterns) {
    for (final String pattern : patterns) {
      if (isFullTreePattern(pattern.trim())) {
        return true;
      }
    }
    return false;
  }

  private static List<MeasurementPath> fetchMeasurementPaths(
      final String key, final String pattern) {
    final MeasurementPath patternPath;
    try {
      patternPath = new MeasurementPath(pattern);
    } catch (final IllegalPathException e) {
      throw new PipeException(
          String.format(DataNodePipeMessages.ILLEGAL_TREE_PATTERN_FMT, pattern), e);
    }

    try {
      final PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(patternPath);
      patternTree.constructTree();
      final ISchemaTree schemaTree =
          ClusterSchemaFetcher.getInstance().fetchSchema(patternTree, true, null, true);
      return schemaTree.searchMeasurementPaths(patternPath).left;
    } catch (final Exception e) {
      throw new PipeException(
          String.format(DataNodePipeMessages.FAILED_TO_FETCH_PIPE_SOURCE_PATTERN, key, pattern), e);
    }
  }

  private static List<MeasurementPath> fetchMeasurementPaths(
      final String key, final List<String> patterns) {
    final Map<String, MeasurementPath> measurementPaths = new HashMap<>();
    for (final String pattern : patterns) {
      for (final MeasurementPath measurementPath : fetchMeasurementPaths(key, pattern)) {
        measurementPaths.put(measurementPath.getFullPath(), measurementPath);
      }
    }
    return new ArrayList<>(measurementPaths.values());
  }

  private static Map<String, String> getProps(final MeasurementPath measurementPath) {
    return measurementPath.getMeasurementSchema() instanceof MeasurementSchema
        ? measurementPath.getMeasurementSchema().getProps()
        : null;
  }

  private static String joinPatternList(final Set<String> patterns) {
    return patterns.stream()
        .map(PipeSourceTreePatternUtils::quotePathIfNecessary)
        .collect(Collectors.joining(PATTERN_LIST_SEPARATOR));
  }

  private static final class ResolutionState {
    private final Set<String> internalInclusions = new LinkedHashSet<>();
    private final Set<String> internalExclusions = new LinkedHashSet<>();
    private final Set<String> internalInvalidExclusions = new LinkedHashSet<>();
    private final Map<String, String> aliasPathToPhysicalPath = new HashMap<>();
  }

  private static final class SourceAttributeGroup {
    private final List<String> keys;
    private final boolean isPath;
    private final boolean isInclusion;

    private SourceAttributeGroup(
        final List<String> keys, final boolean isPath, final boolean isInclusion) {
      this.keys = keys;
      this.isPath = isPath;
      this.isInclusion = isInclusion;
    }

    private static SourceAttributeGroup pathInclusion(final List<String> keys) {
      return new SourceAttributeGroup(keys, true, true);
    }

    private static SourceAttributeGroup pathExclusion(final List<String> keys) {
      return new SourceAttributeGroup(keys, true, false);
    }

    private static SourceAttributeGroup patternInclusion(final List<String> keys) {
      return new SourceAttributeGroup(keys, false, true);
    }

    private static SourceAttributeGroup patternExclusion(final List<String> keys) {
      return new SourceAttributeGroup(keys, false, false);
    }
  }

  private static final class ResolveResult {
    private boolean onlyInvalidPhysicalDirectMatch;
    private final Set<String> physicalInclusions = new LinkedHashSet<>();
    private final Set<String> physicalExclusions = new LinkedHashSet<>();
    private final Set<String> invalidPhysicalExclusions = new LinkedHashSet<>();
  }

  private static final class PatternListResolveState {
    private boolean hasResolvableInclusion;
    private boolean hasOnlyInvalidPhysicalDirectMatch;

    private void record(final ResolveResult result, final boolean isInclusion) {
      if (!isInclusion) {
        return;
      }
      if (result.onlyInvalidPhysicalDirectMatch) {
        hasOnlyInvalidPhysicalDirectMatch = true;
      }
      if (!result.physicalInclusions.isEmpty()) {
        hasResolvableInclusion = true;
      }
    }
  }
}
