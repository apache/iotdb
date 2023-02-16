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

package org.apache.iotdb.consensus.natraft.protocol.log;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryRequest;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

public class VotingLog {

  protected Entry entry;
  // for NB-Raft
  protected Set<TEndPoint> weaklyAcceptedNodes;
  protected Set<TEndPoint> failedNodes;
  private boolean hasFailed;
  private AppendEntryRequest appendEntryRequest;
  private Future<ByteBuffer> serializedLogFuture;
  private int quorumSize;

  public VotingLog(
      Entry entry,
      int groupSize,
      AppendEntryRequest appendEntryRequest,
      int quorumSize,
      RaftConfig config) {
    this.entry = entry;
    failedNodes = new HashSet<>(groupSize);
    if (config.isUseFollowerSlidingWindow()) {
      weaklyAcceptedNodes = new HashSet<>(groupSize);
    }
    this.setAppendEntryRequest(appendEntryRequest);
    this.setQuorumSize(quorumSize);
  }

  public VotingLog(VotingLog another) {
    this.entry = another.entry;
    this.weaklyAcceptedNodes = another.weaklyAcceptedNodes;
    this.failedNodes = another.failedNodes;
    this.setAppendEntryRequest(another.appendEntryRequest);
    this.setQuorumSize(another.quorumSize);
    this.setSerializedLogFuture(another.getSerializedLogFuture());
  }

  public Entry getEntry() {
    return entry;
  }

  public void setEntry(Entry entry) {
    this.entry = entry;
  }

  public Set<TEndPoint> getWeaklyAcceptedNodes() {
    return weaklyAcceptedNodes != null ? weaklyAcceptedNodes : Collections.emptySet();
  }

  @Override
  public String toString() {
    return entry.toString();
  }

  public Set<TEndPoint> getFailedNodes() {
    return failedNodes;
  }

  public boolean isHasFailed() {
    return hasFailed;
  }

  public void setHasFailed(boolean hasFailed) {
    this.hasFailed = hasFailed;
  }

  public AppendEntryRequest getAppendEntryRequest() {
    return appendEntryRequest;
  }

  public void setAppendEntryRequest(AppendEntryRequest appendEntryRequest) {
    this.appendEntryRequest = appendEntryRequest;
  }

  public Future<ByteBuffer> getSerializedLogFuture() {
    return serializedLogFuture;
  }

  public void setSerializedLogFuture(Future<ByteBuffer> serializedLogFuture) {
    this.serializedLogFuture = serializedLogFuture;
  }

  public int getQuorumSize() {
    return quorumSize;
  }

  public void setQuorumSize(int quorumSize) {
    this.quorumSize = quorumSize;
  }
}
