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

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a union of multiple {@link TreePattern}s. A path is considered to match if it matches
 * any of the patterns in the collection.
 */
public class UnionTreePattern extends TreePattern {

  private final List<TreePattern> patterns;

  public UnionTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured, final List<TreePattern> patterns) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.patterns = patterns;
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
  public List<PartialPath> getBaseInclusionPaths() {
    final List<PartialPath> paths = new ArrayList<>();
    for (final TreePattern p : patterns) {
      paths.addAll(p.getBaseInclusionPaths());
    }
    return paths;
  }

  @Override
  public String toString() {
    return "UnionTreePattern{"
        + "patterns="
        + patterns
        + ", isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
