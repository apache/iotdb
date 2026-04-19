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

package org.apache.iotdb.db.subscription.broker.consensus;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Runtime control state for consensus subscription delivery on a single region replica. */
public class ConsensusRegionRuntimeState {

  private final long runtimeVersion;
  private final int preferredWriterNodeId;
  private final boolean active;
  private final Set<Integer> activeWriterNodeIds;

  public ConsensusRegionRuntimeState(
      final long runtimeVersion,
      final int preferredWriterNodeId,
      final boolean active,
      final Set<Integer> activeWriterNodeIds) {
    this.runtimeVersion = runtimeVersion;
    this.preferredWriterNodeId = preferredWriterNodeId;
    this.active = active;
    this.activeWriterNodeIds =
        Collections.unmodifiableSet(
            new LinkedHashSet<>(Objects.requireNonNull(activeWriterNodeIds)));
  }

  public long getRuntimeVersion() {
    return runtimeVersion;
  }

  public int getPreferredWriterNodeId() {
    return preferredWriterNodeId;
  }

  public boolean isActive() {
    return active;
  }

  public Set<Integer> getActiveWriterNodeIds() {
    return activeWriterNodeIds;
  }

  public static ConsensusRegionRuntimeState leaderOnly(
      final long runtimeVersion, final int preferredWriterNodeId, final boolean active) {
    return new ConsensusRegionRuntimeState(
        runtimeVersion,
        preferredWriterNodeId,
        active,
        Collections.singleton(preferredWriterNodeId));
  }

  @Override
  public String toString() {
    return "ConsensusRegionRuntimeState{"
        + "runtimeVersion="
        + runtimeVersion
        + ", preferredWriterNodeId="
        + preferredWriterNodeId
        + ", active="
        + active
        + ", activeWriterNodeIds="
        + activeWriterNodeIds
        + '}';
  }
}
