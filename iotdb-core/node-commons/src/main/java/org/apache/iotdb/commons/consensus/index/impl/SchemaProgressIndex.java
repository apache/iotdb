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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link SchemaProgressIndex} is used only for schema progress recording. It shall not be blended
 * or compared to {@link ProgressIndex}es other than {@link SchemaProgressIndex} or {@link
 * MinimumProgressIndex}.
 */
public class SchemaProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private int index;

  public SchemaProgressIndex() {
    // Empty constructor
  }

  public SchemaProgressIndex(int index) {
    this.index = index;
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
  public boolean isAfter(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return true;
      }

      if (!(progressIndex instanceof SchemaProgressIndex)) {
        return false;
      }

      final SchemaProgressIndex thisSchemaProgressIndex = this;
      final SchemaProgressIndex thatSchemaProgressIndex = (SchemaProgressIndex) progressIndex;
      return thatSchemaProgressIndex.index < thisSchemaProgressIndex.index;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof SchemaProgressIndex)) {
        return false;
      }

      final SchemaProgressIndex thisSchemaProgressIndex = this;
      final SchemaProgressIndex thatSchemaProgressIndex = (SchemaProgressIndex) progressIndex;
      return thisSchemaProgressIndex.index == thatSchemaProgressIndex.index;
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
    if (!(obj instanceof SchemaProgressIndex)) {
      return false;
    }
    return this.equals(obj);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof SchemaProgressIndex)) {
        return this;
      }

      this.index = Math.max(this.index, ((SchemaProgressIndex) progressIndex).index);
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ProgressIndexType getType() {
    return ProgressIndexType.RECOVER_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    return null;
  }

  public static SchemaProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final SchemaProgressIndex schemaProgressIndex = new SchemaProgressIndex();
    schemaProgressIndex.index = ReadWriteIOUtils.readInt(byteBuffer);
    return schemaProgressIndex;
  }

  public static SchemaProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final SchemaProgressIndex schemaProgressIndex = new SchemaProgressIndex();
    schemaProgressIndex.index = ReadWriteIOUtils.readInt(stream);
    return schemaProgressIndex;
  }

  @Override
  public String toString() {
    return "SchemaProgressIndex{" + "index=" + index + '}';
  }
}
