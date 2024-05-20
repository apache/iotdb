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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.load.cache.AbstractStatistics;

import java.util.Objects;

/** NodeStatistics indicates the statistics of a Node. */
public class NodeStatistics extends AbstractStatistics {

  private final NodeStatus status;
  // The reason why lead to the current NodeStatus (for showing cluster)
  // Notice: Default is null
  private final String statusReason;
  // For guiding queries, the higher the score the higher the load
  private final long loadScore;

  public NodeStatistics(
      long statisticsNanoTimestamp, NodeStatus status, String statusReason, long loadScore) {
    super(statisticsNanoTimestamp);
    this.status = status;
    this.statusReason = statusReason;
    this.loadScore = loadScore;
  }

  @TestOnly
  public NodeStatistics(NodeStatus status) {
    super(System.nanoTime());
    this.status = status;
    this.statusReason = null;
    this.loadScore = Long.MAX_VALUE;
  }

  public static NodeStatistics generateDefaultNodeStatistics() {
    return new NodeStatistics(Long.MIN_VALUE, NodeStatus.Unknown, null, Long.MAX_VALUE);
  }

  public NodeStatus getStatus() {
    return status;
  }

  public String getStatusReason() {
    return statusReason;
  }

  public long getLoadScore() {
    return loadScore;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeStatistics that = (NodeStatistics) o;
    return status == that.status && Objects.equals(statusReason, that.statusReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, statusReason, loadScore);
  }

  @Override
  public String toString() {
    return "NodeStatistics{" + "status=" + status + ", statusReason='" + statusReason + '\'' + '}';
  }
}
