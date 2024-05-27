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

package org.apache.iotdb.commons.pipe.pattern;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class IoTDBPipePattern extends PipePattern {

  private final PartialPath patternPartialPath;

  public IoTDBPipePattern(final String pattern) {
    super(pattern);

    try {
      patternPartialPath = new PartialPath(getPattern());
    } catch (final IllegalPathException e) {
      throw new PipeException("Illegal IoTDBPipePattern: " + getPattern(), e);
    }
  }

  public static <T> List<T> applyIndexesOnList(
      final int[] filteredIndexes, final List<T> originalList) {
    return Arrays.stream(filteredIndexes).mapToObj(originalList::get).collect(Collectors.toList());
  }

  @Override
  public String getDefaultPattern() {
    return PipeExtractorConstant.EXTRACTOR_PATTERN_IOTDB_DEFAULT_VALUE;
  }

  @Override
  public boolean isLegal() {
    if (!pattern.startsWith("root")) {
      return false;
    }

    try {
      PathUtils.isLegalPath(pattern);
      return true;
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean coversDb(final String db) {
    try {
      return patternPartialPath.include(
          new PartialPath(db, IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean coversDevice(final String device) {
    try {
      return patternPartialPath.include(
          new PartialPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean mayOverlapWithDevice(final String device) {
    try {
      // Another way is to use patternPath.overlapWith("device.*"),
      // there will be no false positives but time cost may be higher.
      return patternPartialPath.matchPrefixPath(new PartialPath(device));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean matchesMeasurement(final String device, final String measurement) {
    // For aligned timeseries, empty measurement is an alias of the time column.
    if (Objects.isNull(measurement) || measurement.isEmpty()) {
      return false;
    }

    try {
      return patternPartialPath.matchFullPath(new PartialPath(device, measurement));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  /**
   * Check if the {@link PipePattern} matches the given prefix path.
   *
   * <p>NOTE: In schema transmission, {@link #mayOverlapWithDevice(String)} can be used to detect
   * whether the given path can act as a parent path of the {@link PipePattern}, and to transmit
   * possibly used schemas like database creation and template setting.
   */
  public boolean matchPrefixPath(final String path) {
    return mayOverlapWithDevice(path);
  }

  /**
   * This is the precise form of the device overlap and is used only be device template transfer.
   */
  public boolean matchDevice(final String devicePath) {
    try {
      return patternPartialPath.overlapWith(new PartialPath(devicePath, "*"));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  /**
   * Return if the given tail node matches the pattern's tail node. Caller shall ensure that it is a
   * prefix or full path pattern.
   */
  public boolean matchTailNode(final String tailNode) {
    return !isFullPath() || patternPartialPath.getTailNode().equals(tailNode);
  }

  /**
   * Get the intersection of the given {@link PartialPath} and the {@link PipePattern}, Only used by
   * schema transmission. Caller shall ensure that it is a prefix or full path pattern.
   */
  public List<PartialPath> getIntersection(final PartialPath partialPath) {
    if (isFullPath()) {
      return partialPath.matchFullPath(patternPartialPath)
          ? Collections.singletonList(patternPartialPath)
          : Collections.emptyList();
    }
    return partialPath.intersectWithPrefixPattern(patternPartialPath);
  }

  /**
   * Get the intersection of the given {@link PathPatternTree} and the {@link PipePattern}. Only
   * used by schema transmission. Caller shall ensure that it is a prefix or full path pattern.
   */
  public PathPatternTree getIntersection(final PathPatternTree patternTree) {
    final PathPatternTree thisPatternTree = new PathPatternTree();
    thisPatternTree.appendPathPattern(patternPartialPath);
    thisPatternTree.constructTree();
    return patternTree.intersectWithFullPathPrefixTree(thisPatternTree);
  }

  public boolean isPrefix() {
    return PathPatternUtil.isMultiLevelMatchWildcard(patternPartialPath.getTailNode())
        && !new PartialPath(
                Arrays.copyOfRange(
                    patternPartialPath.getNodes(), 0, patternPartialPath.getNodeLength() - 1))
            .hasWildcard();
  }

  public boolean isFullPath() {
    return !patternPartialPath.hasWildcard();
  }

  @Override
  public String toString() {
    return "IoTDBPipePattern" + super.toString();
  }
}
