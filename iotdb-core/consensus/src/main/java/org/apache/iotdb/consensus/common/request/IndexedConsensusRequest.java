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

package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.commons.request.IConsensusRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** only used for iot consensus. */
public class IndexedConsensusRequest implements IConsensusRequest {

  /** we do not need to serialize these two fields as they are useless in other nodes. */
  private final long searchIndex;

  private final long syncIndex;

  /** routing epoch from ConfigNode broadcast for ordered consensus subscription */
  private long routingEpoch = 0;

  /** Millisecond physical time used as the first ordering key in the new subscription progress. */
  private long physicalTime = 0;

  /** Writer node id used as the second ordering key across multiple writers. */
  private int nodeId = -1;

  private final List<IConsensusRequest> requests;
  private final List<ByteBuffer> serializedRequests;
  private long memorySize = 0;
  private long retainedMemorySize = 0;
  private boolean serializedRequestsBuilt = false;
  private final AtomicLong referenceCnt = new AtomicLong();

  public IndexedConsensusRequest(long searchIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = new ArrayList<>(requests);
    this.syncIndex = -1L;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public IndexedConsensusRequest(
      long searchIndex, long syncIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = new ArrayList<>(requests);
    this.syncIndex = syncIndex;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public synchronized void buildSerializedRequests() {
    if (serializedRequestsBuilt) {
      return;
    }
    this.requests.forEach(
        r -> {
          ByteBuffer buffer = r.serializeToByteBuffer();
          this.serializedRequests.add(buffer);
          this.memorySize += Long.max(buffer.capacity(), r.getMemorySize());
          this.retainedMemorySize += buffer.capacity() + r.getMemorySize();
        });
    serializedRequestsBuilt = true;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    throw new UnsupportedOperationException();
  }

  public List<IConsensusRequest> getRequests() {
    return requests;
  }

  public List<ByteBuffer> getSerializedRequests() {
    return serializedRequests;
  }

  public long getMemorySize() {
    return memorySize;
  }

  /**
   * Returns the memory retained while this request object is alive.
   *
   * <p>Before replication serialization, the request only retains its original request objects.
   * Afterwards it retains both the originals and the serialized buffers. Batch memory continues to
   * use {@link #getMemorySize()} because a batch no longer owns the original requests.
   */
  public synchronized long getRetainedMemorySize() {
    if (serializedRequestsBuilt) {
      return retainedMemorySize;
    }
    return requests.stream().mapToLong(IConsensusRequest::getMemorySize).sum();
  }

  /**
   * Releases the original request objects after their serialized buffers have been materialized.
   * Replication only needs the serialized buffers, while subscription delivery still needs the
   * original objects and therefore must not call this method.
   */
  public synchronized void clearRequests() {
    if (requests.isEmpty()) {
      return;
    }
    if (serializedRequestsBuilt) {
      retainedMemorySize -= requests.stream().mapToLong(IConsensusRequest::getMemorySize).sum();
    }
    requests.clear();
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  public long getSyncIndex() {
    return syncIndex;
  }

  /**
   * Returns the writer-local sequence used by the new subscription progress model.
   *
   * <p>For locally generated requests this is the request searchIndex. For replicated requests this
   * is the source leader's propagated localSeq carried in syncIndex.
   */
  public long getProgressLocalSeq() {
    return syncIndex >= 0 ? syncIndex : searchIndex;
  }

  public long getRoutingEpoch() {
    return routingEpoch;
  }

  public IndexedConsensusRequest setRoutingEpoch(long routingEpoch) {
    this.routingEpoch = routingEpoch;
    return this;
  }

  public long getPhysicalTime() {
    return physicalTime;
  }

  public IndexedConsensusRequest setPhysicalTime(long physicalTime) {
    this.physicalTime = physicalTime;
    return this;
  }

  public int getNodeId() {
    return nodeId;
  }

  public IndexedConsensusRequest setNodeId(int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public long getLocalSeq() {
    return searchIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexedConsensusRequest that = (IndexedConsensusRequest) o;
    return searchIndex == that.searchIndex && requests.equals(that.requests);
  }

  @Override
  public int hashCode() {
    return Objects.hash(searchIndex, requests);
  }

  public long incRef() {
    return referenceCnt.getAndIncrement();
  }

  public long decRef() {
    return referenceCnt.getAndDecrement();
  }
}
