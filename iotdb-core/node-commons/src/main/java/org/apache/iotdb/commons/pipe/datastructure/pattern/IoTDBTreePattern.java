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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IoTDBTreePattern extends IoTDBTreePatternOperations {

  private final String pattern;
  private final PartialPath patternPartialPath;

  private static volatile DevicePathGetter devicePathGetter = PartialPath::new;
  private static volatile MeasurementPathGetter measurementPathGetter = MeasurementPath::new;

  public IoTDBTreePattern(final boolean isTreeModelDataAllowedToBeCaptured, final String pattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.pattern = pattern != null ? pattern : getDefaultPattern();

    try {
      patternPartialPath = new PartialPath(getPattern());
    } catch (final IllegalPathException e) {
      throw new PipeException("Illegal IoTDBPipePattern: " + getPattern(), e);
    }
  }

  public IoTDBTreePattern(final String pattern) {
    this(true, pattern);
  }

  private String getDefaultPattern() {
    return PipeSourceConstant.EXTRACTOR_PATTERN_IOTDB_DEFAULT_VALUE;
  }

  //////////////////////////// Tree Pattern Operations ////////////////////////////

  public static <T> List<T> applyReversedIndexesOnList(
      final List<Integer> filteredIndexes, final @Nullable List<T> originalList) {
    if (Objects.isNull(originalList)) {
      return null;
    }
    // No need to sort, the caller guarantees that the filtered sequence == original sequence
    final List<T> filteredList = new ArrayList<>(originalList.size() - filteredIndexes.size());
    int filteredIndexPos = 0;
    int processingIndex = 0;
    for (; processingIndex < originalList.size(); processingIndex++) {
      if (filteredIndexPos >= filteredIndexes.size()) {
        // all filteredIndexes processed, add remaining to the filteredList
        filteredList.addAll(originalList.subList(processingIndex, originalList.size()));
        break;
      } else {
        int filteredIndex = filteredIndexes.get(filteredIndexPos);
        if (filteredIndex == processingIndex) {
          // the index is filtered, move to the next filtered pos
          filteredIndexPos++;
        } else {
          // the index is not filtered, add to the filteredList
          filteredList.add(originalList.get(processingIndex));
        }
      }
    }
    return filteredList;
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public boolean isRoot() {
    return Objects.isNull(pattern) || this.pattern.equals(this.getDefaultPattern());
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
          new MeasurementPath(db, IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    try {
      return patternPartialPath.include(
          new MeasurementPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    try {
      return patternPartialPath.overlapWith(new PartialPath(db + ".**"));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    try {
      // Another way is to use patternPath.overlapWith("device.*"),
      // there will be no false positives but time cost may be higher.
      return patternPartialPath.matchPrefixPath(devicePathGetter.apply(device));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    // For aligned timeseries, empty measurement is an alias of the time column.
    if (Objects.isNull(measurement) || measurement.isEmpty()) {
      return false;
    }

    try {
      return patternPartialPath.matchFullPath(measurementPathGetter.apply(device, measurement));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  @Override
  public List<PartialPath> getBaseInclusionPaths() {
    return Collections.singletonList(patternPartialPath);
  }

  //////////////////////////// IoTDB Tree Pattern Operations ////////////////////////////

  /**
   * Check if the {@link TreePattern} matches the given prefix path. In schema transmission, this
   * can be used to detect whether the given path can act as a parent path of the {@link
   * TreePattern}, and to transmit possibly used schemas like database creation and template
   * setting.
   */
  @Override
  public boolean matchPrefixPath(final String path) {
    try {
      return patternPartialPath.matchPrefixPath(new PartialPath(path));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  /**
   * This is the precise form of the device overlap and is used only be device template transfer.
   */
  @Override
  public boolean matchDevice(final String devicePath) {
    try {
      return patternPartialPath.overlapWith(new MeasurementPath(devicePath, "*"));
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  /**
   * Return if the given tail node matches the pattern's tail node. Caller shall ensure that it is a
   * prefix or full path pattern.
   */
  @Override
  public boolean matchTailNode(final String tailNode) {
    return !isFullPath() || patternPartialPath.getTailNode().equals(tailNode);
  }

  /**
   * Get the intersection of the given {@link PartialPath} and the {@link TreePattern}, Only used by
   * schema transmission. Caller shall ensure that it is a prefix or full path pattern.
   */
  @Override
  public List<PartialPath> getIntersection(final PartialPath partialPath) {
    if (isFullPath()) {
      return partialPath.matchFullPath(patternPartialPath)
          ? Collections.singletonList(partialPath)
          : Collections.emptyList();
    }
    return partialPath.intersectWithPrefixPattern(patternPartialPath);
  }

  /**
   * Get the intersection of the given {@link PathPatternTree} and the {@link TreePattern}. Only
   * used by schema transmission. Caller shall ensure that it is a prefix or full path pattern.
   */
  @Override
  public PathPatternTree getIntersection(final PathPatternTree patternTree) {
    final PathPatternTree thisPatternTree = new PathPatternTree();
    thisPatternTree.appendPathPattern(patternPartialPath);
    thisPatternTree.constructTree();
    return patternTree.intersectWithFullPathPrefixTree(thisPatternTree);
  }

  @Override
  public boolean isPrefixOrFullPath() {
    return isPrefix() || isFullPath();
  }

  @Override
  public boolean mayMatchMultipleTimeSeriesInOneDevice() {
    return PathPatternUtil.hasWildcard(patternPartialPath.getTailNode());
  }

  private boolean isPrefix() {
    return PathPatternUtil.isMultiLevelMatchWildcard(patternPartialPath.getTailNode())
        && !new PartialPath(
                Arrays.copyOfRange(
                    patternPartialPath.getNodes(), 0, patternPartialPath.getNodeLength() - 1))
            .hasWildcard();
  }

  private boolean isFullPath() {
    return !patternPartialPath.hasWildcard();
  }

  //////////////////////////// Getter ////////////////////////////

  public static void setDevicePathGetter(final DevicePathGetter devicePathGetter) {
    IoTDBTreePattern.devicePathGetter = devicePathGetter;
  }

  public static void setMeasurementPathGetter(final MeasurementPathGetter measurementPathGetter) {
    IoTDBTreePattern.measurementPathGetter = measurementPathGetter;
  }

  public interface DevicePathGetter {
    PartialPath apply(final IDeviceID deviceId) throws IllegalPathException;
  }

  public interface MeasurementPathGetter {
    MeasurementPath apply(final IDeviceID deviceId, final String measurement)
        throws IllegalPathException;
  }

  //////////////////////////// Object ////////////////////////////

  @Override
  public String toString() {
    return "IoTDBTreePattern{pattern='"
        + pattern
        + "', isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
