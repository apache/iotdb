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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.expr.vgraft.TrustValueHolder;
import org.apache.iotdb.cluster.rpc.thrift.Node;

public class VotingLog {
  protected Log log;
  protected Set<Integer> stronglyAcceptedNodeIds;
  protected Set<Integer> weaklyAcceptedNodeIds;
  protected Set<Integer> failedNodeIds;
  protected Set<byte[]> signatures;
  public AtomicLong acceptedTime;

  public VotingLog(Log log, int groupSize) {
    this.log = log;
    stronglyAcceptedNodeIds = new HashSet<>(groupSize);
    weaklyAcceptedNodeIds = new HashSet<>(groupSize);
    acceptedTime = new AtomicLong();
    failedNodeIds = new HashSet<>(groupSize);
    signatures = new HashSet<>(groupSize);
  }

  public VotingLog(VotingLog another) {
    this.log = another.log;
    this.stronglyAcceptedNodeIds = another.stronglyAcceptedNodeIds;
    this.weaklyAcceptedNodeIds = another.weaklyAcceptedNodeIds;
    this.acceptedTime = another.acceptedTime;
    this.failedNodeIds = another.failedNodeIds;
    this.signatures = another.signatures;
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

  public boolean onStronglyAccept(long index, long term, Node acceptingNode, int quorumSize,
      ByteBuffer signature, int nodeNum) {
    if (getLog().getCurrLogIndex() <= index
        && getLog().getCurrLogTerm() == term) {
      synchronized (this) {
        getStronglyAcceptedNodeIds().add(acceptingNode.nodeIdentifier);
        if (signature != null) {
          signatures.add(Arrays.copyOfRange(signature.array(), signature.arrayOffset(),
              signature.arrayOffset() + signature.remaining()));
        }
      }

      if (getStronglyAcceptedNodeIds().size()
          + getWeaklyAcceptedNodeIds().size()
          >= quorumSize) {
        acceptedTime.set(System.nanoTime());
      }
      if (ClusterDescriptor.getInstance().getConfig().isUseVGRaft()) {
        return signatures.size() > TrustValueHolder.verifierGroupSize(nodeNum) / 2;
      } else {
        return getStronglyAcceptedNodeIds().size() >= quorumSize;
      }
    }
    return false;
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
}
