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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an exclusion pattern specifically for IoTDB-operation-aware patterns. It holds an
 * inclusion and exclusion pattern, both implementing {@link IoTDBPatternOperations}.
 *
 * <p>The logic is: "Matches inclusion AND NOT exclusion" for all methods.
 */
public class ExclusionIoTDBTreePattern extends TreePattern implements IoTDBPatternOperations {

  private final IoTDBPatternOperations inclusionPattern;
  private final IoTDBPatternOperations exclusionPattern;

  public ExclusionIoTDBTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured,
      final IoTDBPatternOperations inclusionPattern,
      final IoTDBPatternOperations exclusionPattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.inclusionPattern = inclusionPattern;
    this.exclusionPattern = exclusionPattern;
  }

  // **********************************************************************
  // Implementation of abstract methods from TreePattern
  // **********************************************************************

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
    return inclusionPattern.isRoot() && !exclusionPattern.isRoot();
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

  // **********************************************************************
  // Implementation of abstract methods from IoTDBPatternOperations
  // **********************************************************************

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
    // NOTE: This is a simple set-difference, which is semantically correct
    // ONLY IF partialPath does NOT contain wildcards.
    // A true intersection of (A AND NOT B) with C (where C has wildcards)
    // is far more complex and may not be representable as a List<PartialPath>.
    final List<PartialPath> inclusionPaths = inclusionPattern.getIntersection(partialPath);
    if (inclusionPaths.isEmpty()) {
      return inclusionPaths;
    }
    final List<PartialPath> exclusionPaths = exclusionPattern.getIntersection(partialPath);
    if (exclusionPaths.isEmpty()) {
      return inclusionPaths;
    }

    // Return (inclusionPaths - exclusionPaths)
    return inclusionPaths.stream()
        .filter(p -> !exclusionPaths.contains(p))
        .collect(Collectors.toList());
  }

  @Override
  public PathPatternTree getIntersection(final PathPatternTree patternTree) {
    // A true set difference (A.intersect(B) - C.intersect(B))
    // would require a PathPatternTree.subtract() method, which does not exist.
    // This operation is unsupported.
    // TODO
    throw new UnsupportedOperationException(
        "getIntersection(PathPatternTree) is not supported for ExclusionIoTDBTreePattern.");
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
