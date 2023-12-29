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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link MetaProgressIndex} is used only for meta transfer progress recording. It shall not be
 * blended or compared to {@link ProgressIndex}es other than {@link MetaProgressIndex} or {@link
 * MinimumProgressIndex}.
 */
public class MetaProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private long index;

  public MetaProgressIndex() {
    // Empty constructor
  }

  public MetaProgressIndex(long index) {
    this.index = index;
  }

  // Meta extractors need to set index to iterate from the listening queue
  public long getIndex() {
    return index;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.SCHEMA_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(index, byteBuffer);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.SCHEMA_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(index, stream);
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

      if (!(progressIndex instanceof MetaProgressIndex)) {
        return false;
      }

      final MetaProgressIndex thisMetaProgressIndex = this;
      final MetaProgressIndex thatMetaProgressIndex = (MetaProgressIndex) progressIndex;
      return thatMetaProgressIndex.index < thisMetaProgressIndex.index;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof MetaProgressIndex)) {
        return false;
      }

      final MetaProgressIndex thisMetaProgressIndex = this;
      final MetaProgressIndex thatMetaProgressIndex = (MetaProgressIndex) progressIndex;
      return thisMetaProgressIndex.index == thatMetaProgressIndex.index;
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
    if (!(obj instanceof MetaProgressIndex)) {
      return false;
    }
    return this.equals((MetaProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof MetaProgressIndex)) {
        return this;
      }

      this.index = Math.max(this.index, ((MetaProgressIndex) progressIndex).index);
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ProgressIndexType getType() {
    return ProgressIndexType.SCHEMA_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    return null;
  }

  public static MetaProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final MetaProgressIndex metaProgressIndex = new MetaProgressIndex();
    metaProgressIndex.index = ReadWriteIOUtils.readInt(byteBuffer);
    return metaProgressIndex;
  }

  public static MetaProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final MetaProgressIndex metaProgressIndex = new MetaProgressIndex();
    metaProgressIndex.index = ReadWriteIOUtils.readInt(stream);
    return metaProgressIndex;
  }

  @Override
  public String toString() {
    return "SchemaProgressIndex{" + "index=" + index + '}';
  }
}
