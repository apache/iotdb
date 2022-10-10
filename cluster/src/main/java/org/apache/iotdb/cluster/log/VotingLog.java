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

package org.apache.iotdb.cluster.log;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterDescriptor;

public class VotingLog {
  protected Log log;
  // for NB-Raft
  protected Set<Integer> weaklyAcceptedNodeIds;
  protected Set<Integer> failedNodeIds;
  // for VGRaft
  protected Set<byte[]> signatures;
  private boolean hasFailed;

  public VotingLog(Log log, int groupSize) {
    this.log = log;
    failedNodeIds = new HashSet<>(groupSize);
    if (ClusterDescriptor.getInstance().getConfig().isUseFollowerSlidingWindow()) {
      weaklyAcceptedNodeIds = new HashSet<>(groupSize);
    }
    if (ClusterDescriptor.getInstance().getConfig().isUseVGRaft()) {
      signatures = new HashSet<>(groupSize);
    }
  }

  public VotingLog(VotingLog another) {
    this.log = another.log;
    this.weaklyAcceptedNodeIds = another.weaklyAcceptedNodeIds;
    this.failedNodeIds = another.failedNodeIds;
    this.signatures = another.signatures;
  }

  public Log getLog() {
    return log;
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public Set<Integer> getWeaklyAcceptedNodeIds() {
    return weaklyAcceptedNodeIds;
  }

  @Override
  public String toString() {
    return log.toString();
  }

  public Set<Integer> getFailedNodeIds() {
    return failedNodeIds;
  }

  public Set<byte[]> getSignatures() {
    return signatures;
  }

  public boolean isHasFailed() {
    return hasFailed;
  }

  public void setHasFailed(boolean hasFailed) {
    this.hasFailed = hasFailed;
  }
}
