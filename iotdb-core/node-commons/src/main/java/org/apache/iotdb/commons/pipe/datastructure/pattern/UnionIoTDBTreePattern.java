/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a union of multiple {@link IoTDBTreePattern}s. This specialized class ensures type
 * safety and provides access to methods specific to IoTDBTreePattern, such as getIntersection.
 */
public class UnionIoTDBTreePattern extends TreePattern {

  private final List<IoTDBTreePattern> patterns;

  public UnionIoTDBTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured, final List<IoTDBTreePattern> patterns) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.patterns = patterns;
  }

  public UnionIoTDBTreePattern(final List<IoTDBTreePattern> patterns) {
    super(true);
    this.patterns = patterns;
  }

  public UnionIoTDBTreePattern(final IoTDBTreePattern pattern) {
    super(true);
    this.patterns = Collections.singletonList(pattern);
  }

  // **********************************************************************
  // IoTDBTreePattern-specific aggregated methods
  // **********************************************************************

  public boolean matchPrefixPath(final String path) {
    return patterns.stream().anyMatch(p -> p.matchPrefixPath(path));
  }

  public boolean matchDevice(final String devicePath) {
    return patterns.stream().anyMatch(p -> p.matchDevice(devicePath));
  }

  public boolean matchTailNode(final String tailNode) {
    return patterns.stream().anyMatch(p -> p.matchTailNode(tailNode));
  }

  public List<PartialPath> getIntersection(final PartialPath partialPath) {
    final Set<PartialPath> uniqueIntersections = new LinkedHashSet<>();
    for (final IoTDBTreePattern pattern : patterns) {
      uniqueIntersections.addAll(pattern.getIntersection(partialPath));
    }
    return new ArrayList<>(uniqueIntersections);
  }

  public PathPatternTree getIntersection(final PathPatternTree patternTree) {
    final PathPatternTree resultTree = new PathPatternTree();
    for (final IoTDBTreePattern pattern : patterns) {
      final PathPatternTree intersection = pattern.getIntersection(patternTree);
      if (intersection.isEmpty()) {
        continue;
      }
      intersection.getAllPathPatterns().forEach(resultTree::appendPathPattern);
    }
    resultTree.constructTree();
    return resultTree;
  }

  public boolean isPrefixOrFullPath() {
    return patterns.stream().allMatch(p -> p.isPrefix() || p.isFullPath());
  }

  public boolean mayMatchMultipleTimeSeriesInOneDevice() {
    return patterns.stream().anyMatch(IoTDBTreePattern::mayMatchMultipleTimeSeriesInOneDevice);
  }

  // **********************************************************************
  // Implementation of abstract methods from TreePattern
  // **********************************************************************

  @Override
  public boolean isSingle() {
    return patterns.size() == 1;
  }

  @Override
  public String getPattern() {
    return patterns.stream().map(TreePattern::getPattern).collect(Collectors.joining(","));
  }

  @Override
  public boolean isRoot() {
    return patterns.stream().anyMatch(TreePattern::isRoot);
  }

  @Override
  public boolean isLegal() {
    return patterns.stream().allMatch(TreePattern::isLegal);
  }

  @Override
  public boolean coversDb(final String db) {
    return patterns.stream().anyMatch(p -> p.coversDb(db));
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    return patterns.stream().anyMatch(p -> p.coversDevice(device));
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    return patterns.stream().anyMatch(p -> p.mayOverlapWithDb(db));
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    return patterns.stream().anyMatch(p -> p.mayOverlapWithDevice(device));
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    return patterns.stream().anyMatch(p -> p.matchesMeasurement(device, measurement));
  }

  @Override
  public String toString() {
    return "UnionIoTDBTreePattern{"
        + "patterns="
        + patterns
        + ", isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
