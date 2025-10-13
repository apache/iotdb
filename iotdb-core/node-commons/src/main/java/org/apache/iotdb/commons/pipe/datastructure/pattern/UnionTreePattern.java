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

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a union of multiple {@link TreePattern}s. A path is considered to match if it matches
 * any of the patterns in the collection.
 */
public class UnionTreePattern extends TreePattern {

  private final List<TreePattern> patterns;

  /**
   * Constructs a {@link UnionTreePattern} with a list of {@link TreePattern}s.
   *
   * @param patterns A list of {@link TreePattern}s. Assumes all patterns share the same value for
   *     isTreeModelDataAllowedToBeCaptured.
   */
  public UnionTreePattern(final List<TreePattern> patterns) {
    super(patterns.stream().anyMatch(pattern -> pattern.isTreeModelDataAllowedToBeCaptured));
    this.patterns = patterns;
  }

  public TreePattern getFirstPattern() {
    return patterns.get(0);
  }

  @Override
  public String getPattern() {
    // TODO
    return patterns.stream().map(TreePattern::getPattern).collect(Collectors.joining(","));
  }

  @Override
  public boolean isRoot() {
    for (final TreePattern pattern : patterns) {
      if (pattern.isRoot()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isLegal() {
    for (final TreePattern pattern : patterns) {
      if (!pattern.isLegal()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean coversDb(final String db) {
    for (final TreePattern pattern : patterns) {
      if (pattern.coversDb(db)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    for (final TreePattern pattern : patterns) {
      if (pattern.coversDevice(device)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    for (final TreePattern pattern : patterns) {
      if (pattern.mayOverlapWithDb(db)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    for (final TreePattern pattern : patterns) {
      if (pattern.mayOverlapWithDevice(device)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    for (final TreePattern pattern : patterns) {
      if (pattern.matchesMeasurement(device, measurement)) {
        return true;
      }
    }
    return false;
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
