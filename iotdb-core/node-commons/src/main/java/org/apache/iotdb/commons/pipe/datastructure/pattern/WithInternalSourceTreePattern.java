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
import java.util.Objects;

/**
 * Wraps a user source pattern with system-generated physical path snapshots.
 *
 * <p>The runtime matching is path-only: first reject physical paths that are known not to be
 * captured, then accept physical paths that are known to be captured, and finally fall back to the
 * user-visible source pattern for normal series.
 */
public class WithInternalSourceTreePattern extends TreePattern {

  private final TreePattern userPattern;
  private final TreePattern internalInclusionPattern;
  private final TreePattern internalExclusionPattern;

  public WithInternalSourceTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured,
      final TreePattern userPattern,
      final TreePattern internalInclusionPattern,
      final TreePattern internalExclusionPattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.userPattern = userPattern;
    this.internalInclusionPattern = internalInclusionPattern;
    this.internalExclusionPattern = internalExclusionPattern;
  }

  @Override
  public String getPattern() {
    return userPattern.getPattern();
  }

  @Override
  public boolean isRoot() {
    return Objects.isNull(internalInclusionPattern)
        && Objects.isNull(internalExclusionPattern)
        && userPattern.isRoot();
  }

  @Override
  public boolean isSingle() {
    return false;
  }

  @Override
  public boolean isLegal() {
    return userPattern.isLegal()
        && (Objects.isNull(internalInclusionPattern) || internalInclusionPattern.isLegal())
        && (Objects.isNull(internalExclusionPattern) || internalExclusionPattern.isLegal());
  }

  @Override
  public boolean coversDb(final String db) {
    return Objects.isNull(internalExclusionPattern) && userPattern.coversDb(db);
  }

  @Override
  public boolean coversDevice(final IDeviceID device) {
    return Objects.isNull(internalExclusionPattern) && userPattern.coversDevice(device);
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    return !isDatabaseCoveredByInternalExclusion(db)
        && (userPattern.mayOverlapWithDb(db)
            || (Objects.nonNull(internalInclusionPattern)
                && internalInclusionPattern.mayOverlapWithDb(db)));
  }

  @Override
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    return !isDeviceCoveredByInternalExclusion(device)
        && (userPattern.mayOverlapWithDevice(device)
            || (Objects.nonNull(internalInclusionPattern)
                && internalInclusionPattern.mayOverlapWithDevice(device)));
  }

  @Override
  public boolean overlapWithDevice(final IDeviceID device) {
    return !isDeviceCoveredByInternalExclusion(device)
        && (userPattern.overlapWithDevice(device)
            || (Objects.nonNull(internalInclusionPattern)
                && internalInclusionPattern.overlapWithDevice(device)));
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, final String measurement) {
    if (Objects.nonNull(internalExclusionPattern)
        && internalExclusionPattern.matchesMeasurement(device, measurement)) {
      return false;
    }
    if (Objects.nonNull(internalInclusionPattern)
        && internalInclusionPattern.matchesMeasurement(device, measurement)) {
      return true;
    }
    return userPattern.matchesMeasurement(device, measurement);
  }

  @Override
  public List<PartialPath> getBaseInclusionPaths() {
    final List<PartialPath> result = new ArrayList<>(userPattern.getBaseInclusionPaths());
    if (Objects.nonNull(internalInclusionPattern)) {
      result.addAll(internalInclusionPattern.getBaseInclusionPaths());
    }
    return result;
  }

  public TreePattern getUserPattern() {
    return userPattern;
  }

  public TreePattern getInternalInclusionPattern() {
    return internalInclusionPattern;
  }

  public TreePattern getInternalExclusionPattern() {
    return internalExclusionPattern;
  }

  @Override
  public boolean isPrefixOrFullPath() {
    return userPattern.isPrefixOrFullPath()
        && (Objects.isNull(internalInclusionPattern)
            || internalInclusionPattern.isPrefixOrFullPath())
        && (Objects.isNull(internalExclusionPattern)
            || internalExclusionPattern.isPrefixOrFullPath());
  }

  private boolean isDatabaseCoveredByInternalExclusion(final String db) {
    return Objects.nonNull(internalExclusionPattern) && internalExclusionPattern.coversDb(db);
  }

  private boolean isDeviceCoveredByInternalExclusion(final IDeviceID device) {
    return Objects.nonNull(internalExclusionPattern)
        && internalExclusionPattern.coversDevice(device);
  }

  @Override
  public String toString() {
    return "WithInternalSourceTreePattern{"
        + "userPattern="
        + userPattern
        + ", internalInclusionPattern="
        + internalInclusionPattern
        + ", internalExclusionPattern="
        + internalExclusionPattern
        + ", isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
