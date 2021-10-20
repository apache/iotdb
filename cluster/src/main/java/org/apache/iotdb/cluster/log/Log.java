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
import java.util.Comparator;
import java.util.Objects;

/**
 * Log records operations that are made on this cluster. Each log records 2 longs: currLogIndex,
 * currLogTerm, so that the logs in a cluster will form a log chain and abnormal operations can thus
 * be distinguished and removed.
 */
public abstract class Log implements Comparable<Log> {

  private static final Comparator<Log> COMPARATOR =
      Comparator.comparingLong(Log::getCurrLogIndex).thenComparing(Log::getCurrLogTerm);

  protected static final int DEFAULT_BUFFER_SIZE = 4096;
  private long currLogIndex;
  private long currLogTerm;

  // for async application
  private volatile boolean applied;

  @SuppressWarnings("java:S3077")
  private volatile Exception exception;

  private long createTime;
  private long enqueueTime;

  private int byteSize = 0;

  public abstract ByteBuffer serialize();

  public abstract void deserialize(ByteBuffer buffer);

  public enum Types {
    // DO CHECK LogParser when you add a new type of log
    ADD_NODE,
    PHYSICAL_PLAN,
    CLOSE_FILE,
    REMOVE_NODE,
    EMPTY_CONTENT,
    TEST_LARGE_CONTENT
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
    synchronized (this) {
      this.applied = applied;
      this.notifyAll();
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
    Log log = (Log) o;
    return currLogIndex == log.currLogIndex && currLogTerm == log.currLogTerm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(currLogIndex, currLogTerm);
  }

  @Override
  public int compareTo(Log o) {
    return COMPARATOR.compare(this, o);
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getEnqueueTime() {
    return enqueueTime;
  }

  public void setEnqueueTime(long enqueueTime) {
    this.enqueueTime = enqueueTime;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(int byteSize) {
    this.byteSize = byteSize;
  }
}
