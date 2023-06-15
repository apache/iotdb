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

import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

/**
 * Log records operations that are made on this cluster. Each log records 2 longs: currLogIndex,
 * currLogTerm, so that the logs in a cluster will form a log chain and abnormal operations can thus
 * be distinguished and removed.
 */
public abstract class Entry implements Comparable<Entry> {

  private static final Comparator<Entry> COMPARATOR =
      Comparator.comparingLong(Entry::getCurrLogIndex).thenComparing(Entry::getCurrLogTerm);

  // make this configurable or adaptive
  public static int DEFAULT_SERIALIZATION_BUFFER_SIZE = 16 * 1024;
  private volatile long currLogIndex = Long.MIN_VALUE;
  private long currLogTerm = -1;
  private long prevTerm = -1;

  // for async application
  private volatile boolean applied;

  @SuppressWarnings("java:S3077")
  private volatile Exception exception;

  private long byteSize = -1;
  private boolean fromThisNode = false;

  public long receiveTime;
  public long createTime;
  public long acceptedTime;
  public long committedTime;
  public long applyTime;
  public long waitEndTime;

  public EntrySerialization serialization;

  public int getDefaultSerializationBufferSize() {
    return DEFAULT_SERIALIZATION_BUFFER_SIZE;
  }

  protected abstract ByteBuffer serializeInternal(byte[] buffer);

  protected volatile VotingEntry votingEntry;

  /**
   * Perform serialization before indexing to avoid serialization under locked environment. It
   * should be noticed that at this time point, the index is not set yet, so when the final
   * serialization is called, it must set the correct index, term, and prevTerm (starting from the
   * second byte in the ByteBuffer).
   */
  public void preSerialize() {
    serialization.preSerialize(this);
  }

  public ByteBuffer serialize() {
    return serialization.serialize(this);
  }

  public abstract void deserialize(ByteBuffer buffer);

  public enum Types {
    // DO CHECK LogParser when you add a new type of log
    CLIENT_REQUEST,
    EMPTY,
    CONFIG_CHANGE
  }

  public long getCurrLogIndex() {
    return currLogIndex;
  }

  public void setCurrLogIndex(long currLogIndex) {
    this.currLogIndex = currLogIndex;
  }

  public long getCurrLogTerm() {
    return currLogTerm;
  }

  public void setCurrLogTerm(long currLogTerm) {
    this.currLogTerm = currLogTerm;
  }

  @SuppressWarnings("java:S2886") // synchronized outside
  public boolean isApplied() {
    return applied;
  }

  public void setApplied(boolean applied) {
    this.applied = applied;

    if (createTime != 0) {
      Statistic.LOG_DISPATCHER_FROM_CREATE_TO_APPLIED.calOperationCostTimeFromStart(createTime);
    }
    if (fromThisNode) {
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entry e = (Entry) o;
    return currLogIndex == e.currLogIndex && currLogTerm == e.currLogTerm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(currLogIndex, currLogTerm);
  }

  @Override
  public int compareTo(Entry o) {
    return COMPARATOR.compare(this, o);
  }

  public long estimateSize() {
    return cachedSize();
  }

  public long cachedSize() {
    long serializedSize = serialization.serializedSize();
    return serializedSize > 0 ? serializedSize : byteSize;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public long getPrevTerm() {
    return prevTerm;
  }

  public void setPrevTerm(long prevTerm) {
    this.prevTerm = prevTerm;
  }

  public boolean isFromThisNode() {
    return fromThisNode;
  }

  public void setFromThisNode(boolean fromThisNode) {
    this.fromThisNode = fromThisNode;
  }

  public void setSerializationCache(ByteBuffer serializationCache) {
    serialization.setSerializationCache(serializationCache);
  }

  public void recycle() {
    currLogTerm = -1;
    prevTerm = -1;
    applied = false;
    exception = null;
    byteSize = -1;
    fromThisNode = false;
    receiveTime = 0;
    createTime = 0;
    acceptedTime = 0;
    committedTime = 0;
    applyTime = 0;
    waitEndTime = 0;
    serialization.clear();
    votingEntry = null;
  }

  public VotingEntry getVotingEntry() {
    return votingEntry;
  }

  public void setVotingEntry(VotingEntry votingEntry) {
    this.votingEntry = votingEntry;
  }
}
