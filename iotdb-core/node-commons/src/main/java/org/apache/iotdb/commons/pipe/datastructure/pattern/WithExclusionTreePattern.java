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

import java.util.List;

/**
 * Represents a pattern that includes data matched by an inclusion pattern, except for data matched
 * by an exclusion pattern.
 *
 * <p>The logic implemented in the methods is: "Matches the inclusion pattern AND NOT the exclusion
 * pattern."
 */
public class WithExclusionTreePattern extends TreePattern {

  private final TreePattern inclusionPattern;
  private final TreePattern exclusionPattern;

  public WithExclusionTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured,
      final TreePattern inclusionPattern,
      final TreePattern exclusionPattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.inclusionPattern = inclusionPattern;
    this.exclusionPattern = exclusionPattern;
  }

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
    // Covers DB if inclusion covers it AND exclusion doesn't overlap at all.
    return inclusionPattern.coversDb(db) && !exclusionPattern.mayOverlapWithDb(db);
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    // Covers device if inclusion covers it AND exclusion doesn't overlap at all.
    return inclusionPattern.coversDevice(device) && !exclusionPattern.mayOverlapWithDevice(device);
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    // May overlap if inclusion overlaps AND exclusion doesn't fully cover it.
    return inclusionPattern.mayOverlapWithDb(db) && !exclusionPattern.coversDb(db);
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    // May overlap if inclusion overlaps AND exclusion doesn't fully cover it.
    return inclusionPattern.mayOverlapWithDevice(device) && !exclusionPattern.coversDevice(device);
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    // The core logic: Must match inclusion AND NOT match exclusion.
    return inclusionPattern.matchesMeasurement(device, measurement)
        && !exclusionPattern.matchesMeasurement(device, measurement);
  }

  @Override
  public List<PartialPath> getBaseInclusionPaths() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "ExclusionTreePattern{"
        + "inclusionPattern="
        + inclusionPattern
        + ", exclusionPattern="
        + exclusionPattern
        + ", isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
