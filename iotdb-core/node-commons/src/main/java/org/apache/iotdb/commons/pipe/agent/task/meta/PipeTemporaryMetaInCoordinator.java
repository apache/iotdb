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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTemporaryMetaInCoordinator implements PipeTemporaryMeta {

  // ConfigNode statistics
  private final Set<Integer> completedDataNodeIds =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ConcurrentMap<Integer, Long> nodeId2RemainingEventMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Double> nodeId2RemainingTimeMap = new ConcurrentHashMap<>();

  public void markDataNodeCompleted(final int dataNodeId) {
    completedDataNodeIds.add(dataNodeId);
  }

  public void markDataNodeUncompleted(final int dataNodeId) {
    completedDataNodeIds.remove(dataNodeId);
  }

  public void setRemainingEvent(final int dataNodeId, final long remainingEventCount) {
    nodeId2RemainingEventMap.put(dataNodeId, remainingEventCount);
  }

  public void setRemainingTime(final int dataNodeId, final double remainingTime) {
    nodeId2RemainingTimeMap.put(dataNodeId, remainingTime);
  }

  public Set<Integer> getCompletedDataNodeIds() {
    return completedDataNodeIds;
  }

  public long getGlobalRemainingEvents() {
    return nodeId2RemainingEventMap.values().stream().reduce(Long::sum).orElse(0L);
  }

  public double getGlobalRemainingTime() {
    return nodeId2RemainingTimeMap.values().stream().reduce(Math::max).orElse(0d);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeTemporaryMetaInCoordinator that = (PipeTemporaryMetaInCoordinator) o;
    return Objects.equals(this.completedDataNodeIds, that.completedDataNodeIds)
        && Objects.equals(this.nodeId2RemainingEventMap, that.nodeId2RemainingEventMap)
        && Objects.equals(this.nodeId2RemainingTimeMap, that.nodeId2RemainingTimeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(completedDataNodeIds, nodeId2RemainingEventMap, nodeId2RemainingTimeMap);
  }

  @Override
  public String toString() {
    return "PipeTemporaryMeta{"
        + "completedDataNodeIds="
        + completedDataNodeIds
        + ", nodeId2RemainingEventMap="
        + nodeId2RemainingEventMap
        + ", nodeId2RemainingTimeMap"
        + nodeId2RemainingTimeMap
        + '}';
  }
}
