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

package org.apache.iotdb.cluster.expr;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.VotingLog;

import java.util.HashSet;
import java.util.Set;

public class ExprVotingLog extends VotingLog {
  private Set<Integer> weaklyAcceptedNodeIds;

  public ExprVotingLog(Log log, int groupSize) {
    super(log, groupSize);
    this.weaklyAcceptedNodeIds = new HashSet<>(groupSize);
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
}
