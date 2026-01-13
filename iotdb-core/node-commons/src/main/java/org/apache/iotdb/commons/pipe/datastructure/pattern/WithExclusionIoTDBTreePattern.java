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
import org.apache.iotdb.commons.path.PathPatternTree;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an exclusion pattern specifically for IoTDB-operation-aware patterns. It holds an
 * inclusion and exclusion pattern, both implementing {@link IoTDBTreePatternOperations}.
 *
 * <p>The logic is: "Matches inclusion AND NOT exclusion" for all methods.
 */
public class WithExclusionIoTDBTreePattern extends IoTDBTreePatternOperations {

  private final IoTDBTreePatternOperations inclusionPattern;
  private final IoTDBTreePatternOperations exclusionPattern;

  public WithExclusionIoTDBTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured,
      final IoTDBTreePatternOperations inclusionPattern,
      final IoTDBTreePatternOperations exclusionPattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.inclusionPattern = inclusionPattern;
    this.exclusionPattern = exclusionPattern;
  }

  public WithExclusionIoTDBTreePattern(
      final IoTDBTreePatternOperations inclusionPattern,
      final IoTDBTreePatternOperations exclusionPattern) {
    this(true, inclusionPattern, exclusionPattern);
  }

  //////////////////////////// Tree Pattern Operations ////////////////////////////

  @Override
  public String getPattern() {
    return "INCLUSION("
        + inclusionPattern.getPattern()
        + "), EXCLUSION("
        + exclusionPattern.getPattern()
        + ")";
  }

  @Override
  public boolean isRoot() {
    // Since the exclusion is not empty, the whole pattern is always not root because it may more or
    // less filter some data.
    return false;
  }

  @Override
  public boolean isLegal() {
    return inclusionPattern.isLegal() && exclusionPattern.isLegal();
  }

  @Override
  public boolean coversDb(final String db) {
    return inclusionPattern.coversDb(db) && !exclusionPattern.mayOverlapWithDb(db);
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    return inclusionPattern.coversDevice(device) && !exclusionPattern.mayOverlapWithDevice(device);
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    return inclusionPattern.mayOverlapWithDb(db) && !exclusionPattern.coversDb(db);
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    return inclusionPattern.mayOverlapWithDevice(device) && !exclusionPattern.coversDevice(device);
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    return inclusionPattern.matchesMeasurement(device, measurement)
        && !exclusionPattern.matchesMeasurement(device, measurement);
  }

  @Override
  public List<PartialPath> getBaseInclusionPaths() {
    throw new UnsupportedOperationException();
  }

  //////////////////////////// IoTDB Tree Pattern Operations ////////////////////////////

  @Override
  public boolean matchPrefixPath(final String path) {
    return inclusionPattern.matchPrefixPath(path) && !exclusionPattern.matchPrefixPath(path);
  }

  @Override
  public boolean matchDevice(final String devicePath) {
    return inclusionPattern.matchDevice(devicePath) && !exclusionPattern.matchDevice(devicePath);
  }

  @Override
  public boolean matchTailNode(final String tailNode) {
    return inclusionPattern.matchTailNode(tailNode) && !exclusionPattern.matchTailNode(tailNode);
  }

  @Override
  public List<PartialPath> getIntersection(final PartialPath partialPath) {
    // 1. Calculate Intersection(Input, Inclusion)
    final List<PartialPath> inclusionIntersections = inclusionPattern.getIntersection(partialPath);
    if (inclusionIntersections.isEmpty()) {
      return Collections.emptyList();
    }

    // 2. Calculate Intersection(Input, Exclusion)
    final List<PartialPath> exclusionIntersections = exclusionPattern.getIntersection(partialPath);
    if (exclusionIntersections.isEmpty()) {
      // Optimization: No exclusion intersection, return inclusion intersection directly
      return inclusionIntersections;
    }

    // 3. Perform the "Subtraction"
    // Filter out paths from inclusionIntersections that are fully covered by any path
    // in exclusionIntersections.
    return inclusionIntersections.stream()
        .filter(
            incPath ->
                exclusionIntersections.stream().noneMatch(excPath -> excPath.include(incPath)))
        .collect(Collectors.toList());
  }

  @Override
  public PathPatternTree getIntersection(final PathPatternTree patternTree) {
    // 1. Calculate Intersection(Input, Inclusion)
    final PathPatternTree inclusionIntersectionTree = inclusionPattern.getIntersection(patternTree);
    if (inclusionIntersectionTree.isEmpty()) {
      return inclusionIntersectionTree; // Return empty tree
    }

    // 2. Calculate Intersection(Input, Exclusion)
    final PathPatternTree exclusionIntersectionTree = exclusionPattern.getIntersection(patternTree);
    if (exclusionIntersectionTree.isEmpty()) {
      // Optimization: No exclusion intersection, return inclusion intersection directly
      return inclusionIntersectionTree;
    }

    // 3. Perform the "Subtraction"
    final List<PartialPath> inclusionPaths = inclusionIntersectionTree.getAllPathPatterns();
    final List<PartialPath> exclusionPaths = exclusionIntersectionTree.getAllPathPatterns();

    final PathPatternTree finalResultTree = new PathPatternTree();
    for (final PartialPath incPath : inclusionPaths) {
      // Check if the current inclusion path is covered by *any* exclusion path pattern
      boolean excluded = exclusionPaths.stream().anyMatch(excPath -> excPath.include(incPath));

      if (!excluded) {
        finalResultTree.appendPathPattern(incPath); // Add non-excluded path to the result tree
      }
    }

    finalResultTree.constructTree();
    return finalResultTree;
  }

  @Override
  public boolean isPrefixOrFullPath() {
    // Both must be prefix/full-path for the exclusion logic to be sound
    return inclusionPattern.isPrefixOrFullPath() && exclusionPattern.isPrefixOrFullPath();
  }

  @Override
  public boolean mayMatchMultipleTimeSeriesInOneDevice() {
    // If inclusion might, the result might (even if exclusion trims it)
    return inclusionPattern.mayMatchMultipleTimeSeriesInOneDevice();
  }

  //////////////////////////// Object ////////////////////////////

  @Override
  public String toString() {
    return "ExclusionIoTDBTreePattern{"
        + "inclusionPattern="
        + inclusionPattern
        + ", exclusionPattern="
        + exclusionPattern
        + ", isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
