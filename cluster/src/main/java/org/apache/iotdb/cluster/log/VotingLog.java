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

public class VotingLog {
  protected Log log;
  protected Set<Integer> stronglyAcceptedNodeIds;
  protected Set<Integer> weaklyAcceptedNodeIds;
  protected Set<Integer> failedNodeIds;
  public AtomicLong acceptedTime;

  public VotingLog(Log log, int groupSize) {
    this.log = log;
    stronglyAcceptedNodeIds = new HashSet<>(groupSize);
    weaklyAcceptedNodeIds = new HashSet<>(groupSize);
    acceptedTime = new AtomicLong();
    failedNodeIds = new HashSet<>(groupSize);
  }

  public VotingLog(VotingLog another) {
    this.log = another.log;
    this.stronglyAcceptedNodeIds = another.stronglyAcceptedNodeIds;
    this.weaklyAcceptedNodeIds = another.weaklyAcceptedNodeIds;
    this.acceptedTime = another.acceptedTime;
    this.failedNodeIds = another.failedNodeIds;
  }

  public Log getLog() {
    return log;
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public Set<Integer> getStronglyAcceptedNodeIds() {
    return stronglyAcceptedNodeIds;
  }

  public void setStronglyAcceptedNodeIds(Set<Integer> stronglyAcceptedNodeIds) {
    this.stronglyAcceptedNodeIds = stronglyAcceptedNodeIds;
  }

  public Set<Integer> getWeaklyAcceptedNodeIds() {
    return weaklyAcceptedNodeIds;
  }

  public void setWeaklyAcceptedNodeIds(Set<Integer> weaklyAcceptedNodeIds) {
    this.weaklyAcceptedNodeIds = weaklyAcceptedNodeIds;
  }

  @Override
  public String toString() {
    return log.toString();
  }

  public Set<Integer> getFailedNodeIds() {
    return failedNodeIds;
  }
}
