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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleProgressIndex extends ProgressIndex {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SimpleProgressIndex.class) + ProgressIndex.LOCK_SIZE;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final int rebootTimes;
  private final long memTableFlushOrderId;

  public SimpleProgressIndex(int rebootTimes, long memtableFlushOrderId) {
    this.rebootTimes = rebootTimes;
    this.memTableFlushOrderId = memtableFlushOrderId;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.SIMPLE_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(rebootTimes, byteBuffer);
      ReadWriteIOUtils.write(memTableFlushOrderId, byteBuffer);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.SIMPLE_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(rebootTimes, stream);
      ReadWriteIOUtils.write(memTableFlushOrderId, stream);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return true;
      }

      if (progressIndex instanceof HybridProgressIndex) {
        return ((HybridProgressIndex) progressIndex).isGivenProgressIndexAfterSelf(this);
      }

      if (!(progressIndex instanceof SimpleProgressIndex)) {
        return false;
      }

      final SimpleProgressIndex thisSimpleProgressIndex = this;
      final SimpleProgressIndex thatSimpleProgressIndex = (SimpleProgressIndex) progressIndex;
      if (thisSimpleProgressIndex.rebootTimes > thatSimpleProgressIndex.rebootTimes) {
        return true;
      }
      if (thisSimpleProgressIndex.rebootTimes < thatSimpleProgressIndex.rebootTimes) {
        return false;
      }
      // thisSimpleProgressIndex.rebootTimes == thatSimpleProgressIndex.rebootTimes
      return thisSimpleProgressIndex.memTableFlushOrderId
          > thatSimpleProgressIndex.memTableFlushOrderId;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof SimpleProgressIndex)) {
        return false;
      }

      final SimpleProgressIndex thisSimpleProgressIndex = this;
      final SimpleProgressIndex thatSimpleProgressIndex = (SimpleProgressIndex) progressIndex;
      return thisSimpleProgressIndex.rebootTimes == thatSimpleProgressIndex.rebootTimes
          && thisSimpleProgressIndex.memTableFlushOrderId
              == thatSimpleProgressIndex.memTableFlushOrderId;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SimpleProgressIndex)) {
      return false;
    }
    return this.equals((SimpleProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rebootTimes, memTableFlushOrderId);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof SimpleProgressIndex)) {
        return ProgressIndex.blendProgressIndex(this, progressIndex);
      }

      final SimpleProgressIndex thisSimpleProgressIndex = this;
      final SimpleProgressIndex thatSimpleProgressIndex = (SimpleProgressIndex) progressIndex;
      if (thisSimpleProgressIndex.rebootTimes > thatSimpleProgressIndex.rebootTimes) {
        return this;
      }
      if (thisSimpleProgressIndex.rebootTimes < thatSimpleProgressIndex.rebootTimes) {
        return progressIndex;
      }
      // thisSimpleProgressIndex.rebootTimes == thatSimpleProgressIndex.rebootTimes
      if (thisSimpleProgressIndex.memTableFlushOrderId
          > thatSimpleProgressIndex.memTableFlushOrderId) {
        return this;
      }
      if (thisSimpleProgressIndex.memTableFlushOrderId
          < thatSimpleProgressIndex.memTableFlushOrderId) {
        return progressIndex;
      }
      // thisSimpleProgressIndex.memtableFlushOrderId ==
      // thatSimpleProgressIndex.memtableFlushOrderId
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ProgressIndexType getType() {
    return ProgressIndexType.SIMPLE_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    return new TotalOrderSumTuple(memTableFlushOrderId, (long) rebootTimes);
  }

  public static SimpleProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final int rebootTimes = ReadWriteIOUtils.readInt(byteBuffer);
    final long memtableFlushOrderId = ReadWriteIOUtils.readLong(byteBuffer);
    return new SimpleProgressIndex(rebootTimes, memtableFlushOrderId);
  }

  public static SimpleProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final int rebootTimes = ReadWriteIOUtils.readInt(stream);
    final long memtableFlushOrderId = ReadWriteIOUtils.readLong(stream);
    return new SimpleProgressIndex(rebootTimes, memtableFlushOrderId);
  }

  public int getRebootTimes() {
    return rebootTimes;
  }

  public long getMemTableFlushOrderId() {
    return memTableFlushOrderId;
  }

  @Override
  public String toString() {
    return "SimpleProgressIndex{"
        + "rebootTimes="
        + rebootTimes
        + ", memtableFlushOrderId="
        + memTableFlushOrderId
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
