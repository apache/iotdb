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
  private long epoch = 0;

  /** Millisecond physical time used as the first ordering key in the new subscription progress. */
  private long physicalTime = 0;

  /** Writer node id used as the second ordering key across multiple writers. */
  private int nodeId = -1;

  /** Writer-local lifecycle id. */
  private long writerEpoch = 0;

  private final List<IConsensusRequest> requests;
  private final List<ByteBuffer> serializedRequests;
  private long memorySize = 0;
  private AtomicLong referenceCnt = new AtomicLong();

  public IndexedConsensusRequest(long searchIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = requests;
    this.syncIndex = -1L;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public IndexedConsensusRequest(
      long searchIndex, long syncIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = requests;
    this.syncIndex = syncIndex;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public void buildSerializedRequests() {
    this.requests.forEach(
        r -> {
          ByteBuffer buffer = r.serializeToByteBuffer();
          this.serializedRequests.add(buffer);
          this.memorySize += Long.max(buffer.capacity(), r.getMemorySize());
        });
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

  public long getEpoch() {
    return epoch;
  }

  public IndexedConsensusRequest setEpoch(long epoch) {
    this.epoch = epoch;
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

  public long getWriterEpoch() {
    return writerEpoch;
  }

  public IndexedConsensusRequest setWriterEpoch(long writerEpoch) {
    this.writerEpoch = writerEpoch;
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
