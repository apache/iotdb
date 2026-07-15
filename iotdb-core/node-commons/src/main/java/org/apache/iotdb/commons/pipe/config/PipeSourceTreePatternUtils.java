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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_INTERNAL_PATTERN_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_KEY;

public final class PipeSourceTreePatternUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSourceTreePatternUtils.class);

  private static final char PATTERN_LIST_SEPARATOR = ',';
  private static final char BACK_QUOTE = '`';

  private PipeSourceTreePatternUtils() {
    // Utility class.
  }

  /** Whether the source has any tree-model path/pattern configuration, including internal keys. */
  public static boolean hasTreePatternSourceAttributes(final PipeParameters sourceParameters) {
    return sourceParameters.hasAnyAttributes(
        EXTRACTOR_PATH_KEY,
        SOURCE_PATH_KEY,
        EXTRACTOR_PATTERN_KEY,
        SOURCE_PATTERN_KEY,
        EXTRACTOR_PATTERN_INCLUSION_KEY,
        SOURCE_PATTERN_INCLUSION_KEY,
        EXTRACTOR_PATTERN_EXCLUSION_KEY,
        SOURCE_PATTERN_EXCLUSION_KEY,
        EXTRACTOR_PATH_EXCLUSION_KEY,
        SOURCE_PATH_EXCLUSION_KEY,
        SOURCE_INTERNAL_PATTERN_INCLUSION_KEY,
        SOURCE_INTERNAL_PATTERN_EXCLUSION_KEY);
  }

  /** Check whether a database may contain series listed in the internal inclusion source key. */
  public static boolean mayDatabaseOverlapInternalInclusion(
      final String databaseRawName, final PipeParameters sourceParameters) {
    return mayDatabaseOverlapInternalInclusion(
        databaseRawName, parseInternalInclusionPathPatternTree(sourceParameters));
  }

  public static boolean mayDatabaseOverlapInternalInclusion(
      final String databaseRawName, final PathPatternTree inclusionPathPatternTree) {
    if (inclusionPathPatternTree == null) {
      return false;
    }

    final PartialPath databasePattern;
    try {
      databasePattern =
          PartialPath.getQualifiedDatabasePartialPath(databaseRawName)
              .concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD);
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          PipeMessages.FAILED_TO_CHECK_INTERNAL_PIPE_SOURCE_PATTERN_OVERLAP_FOR_DATABASE,
          databaseRawName,
          e);
      return false;
    }

    return !inclusionPathPatternTree.getOverlappedPathPatterns(databasePattern).isEmpty();
  }

  public static PathPatternTree parseInternalInclusionPathPatternTree(
      final PipeParameters sourceParameters) {
    final List<String> inclusionPaths = getInternalInclusionPathList(sourceParameters);
    if (inclusionPaths.isEmpty()) {
      return null;
    }

    final PathPatternTree inclusionPathPatternTree = new PathPatternTree();
    for (final String inclusionPath : inclusionPaths) {
      try {
        inclusionPathPatternTree.appendPathPattern(new PartialPath(inclusionPath));
      } catch (final IllegalPathException e) {
        LOGGER.warn(
            PipeMessages.FAILED_TO_PARSE_INTERNAL_PIPE_SOURCE_PATTERN_INCLUSION_PATH,
            inclusionPath,
            e);
      }
    }
    inclusionPathPatternTree.constructTree();
    return inclusionPathPatternTree;
  }

  public static List<String> getInternalInclusionPathList(final PipeParameters sourceParameters) {
    if (!sourceParameters.hasAnyAttributes(SOURCE_INTERNAL_PATTERN_INCLUSION_KEY)) {
      return Collections.emptyList();
    }
    return splitPatternList(sourceParameters.getString(SOURCE_INTERNAL_PATTERN_INCLUSION_KEY));
  }

  public static List<String> splitPatternList(final String patternList) {
    if (patternList == null || patternList.trim().isEmpty()) {
      return Collections.emptyList();
    }
    final List<String> segments = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    boolean inBackticks = false;
    for (int i = 0; i < patternList.length(); i++) {
      final char c = patternList.charAt(i);
      if (c == BACK_QUOTE) {
        if (inBackticks
            && i + 1 < patternList.length()
            && patternList.charAt(i + 1) == BACK_QUOTE) {
          current.append(c).append(patternList.charAt(++i));
          continue;
        }
        inBackticks = !inBackticks;
        current.append(c);
      } else if (c == PATTERN_LIST_SEPARATOR && !inBackticks) {
        addTrimmedSegment(patternList, segments, current);
      } else {
        current.append(c);
      }
    }
    if (inBackticks) {
      throw new PipeException(
          String.format(PipeMessages.PATTERN_LIST_HAS_UNCLOSED_BACKQUOTE, patternList));
    }
    addTrimmedSegment(patternList, segments, current);
    return segments;
  }

  public static String quotePathIfNecessary(final String path) {
    try {
      return quotePathIfNecessary(new PartialPath(path));
    } catch (final IllegalPathException e) {
      LOGGER.debug("Path {} is not a valid PartialPath, skip quoting.", path, e);
      return path;
    }
  }

  public static String quotePathIfNecessary(final PartialPath path) {
    return Arrays.stream(path.getNodes())
        .map(PipeSourceTreePatternUtils::quoteNodeIfNecessary)
        .collect(Collectors.joining(String.valueOf(IoTDBConstant.PATH_SEPARATOR)));
  }

  private static void addTrimmedSegment(
      final String patternList, final List<String> segments, final StringBuilder segment) {
    final String trimmedSegment = segment.toString().trim();
    if (trimmedSegment.isEmpty()) {
      throw new PipeException(
          String.format(PipeMessages.PATTERN_LIST_HAS_EMPTY_SEGMENT, patternList));
    }
    segments.add(trimmedSegment);
    segment.setLength(0);
  }

  private static String quoteNodeIfNecessary(final String node) {
    if (IoTDBConstant.PATH_ROOT.equals(node)
        || IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)
        || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)
        || (!IoTDBConstant.reservedWords.contains(node.toUpperCase())
            && !PathUtils.isRealNumber(node)
            && node.indexOf(PATTERN_LIST_SEPARATOR) < 0
            && node.indexOf(BACK_QUOTE) < 0
            && TsFileConstant.NODE_NAME_PATTERN.matcher(node).matches())) {
      return node;
    }
    return TsFileConstant.BACK_QUOTE_STRING
        + node.replace(TsFileConstant.BACK_QUOTE_STRING, TsFileConstant.DOUBLE_BACK_QUOTE_STRING)
        + TsFileConstant.BACK_QUOTE_STRING;
  }
}
