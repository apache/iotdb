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

import java.nio.ByteBuffer;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;

public class EntrySerialization {
  private volatile byte[] recycledBuffer;
  private volatile ByteBuffer preSerializationCache;
  private volatile ByteBuffer serializationCache;

  public void preSerialize(Entry entry) {
    if (preSerializationCache != null || serializationCache != null) {
      return;
    }
    long startTime = Statistic.SERIALIZE_ENTRY.getOperationStartTime();
    ByteBuffer byteBuffer = entry.serializeInternal(recycledBuffer);
    Statistic.SERIALIZE_ENTRY.calOperationCostTimeFromStart(startTime);
    preSerializationCache = byteBuffer;
  }

  public ByteBuffer serialize(Entry entry) {
    ByteBuffer cache = serializationCache;
    if (cache != null) {
      return cache.slice();
    }
    if (preSerializationCache != null) {
      ByteBuffer slice = preSerializationCache.slice();
      slice.position(1);
      slice.putLong(entry.getCurrLogIndex());
      slice.putLong(entry.getCurrLogTerm());
      slice.putLong(entry.getPrevTerm());
      slice.position(0);
      serializationCache = slice;
      preSerializationCache = null;
    } else {
      long startTime = Statistic.SERIALIZE_ENTRY.getOperationStartTime();
      ByteBuffer byteBuffer = entry.serializeInternal(recycledBuffer);
      Statistic.SERIALIZE_ENTRY.calOperationCostTimeFromStart(startTime);
      serializationCache = byteBuffer;
    }
    entry.setByteSize(serializationCache.remaining());
    return serializationCache.slice();
  }

  public long serializedSize() {
    ByteBuffer cache;
    if ((cache = serializationCache) != null) {
      return cache.remaining();
    } else if ((cache = preSerializationCache) != null) {
      return cache.remaining();
    }
    return 0;
  }

  public void clear() {
    if (preSerializationCache != null) {
      recycledBuffer = preSerializationCache.array();
      preSerializationCache = null;
    }
    if (serializationCache != null) {
      recycledBuffer = serializationCache.array();
      serializationCache = null;
    }
  }

  public byte[] getRecycledBuffer() {
    return recycledBuffer;
  }

  public void setRecycledBuffer(byte[] recycledBuffer) {
    this.recycledBuffer = recycledBuffer;
  }

  public ByteBuffer getPreSerializationCache() {
    return preSerializationCache;
  }

  public void setPreSerializationCache(ByteBuffer preSerializationCache) {
    this.preSerializationCache = preSerializationCache;
  }

  public ByteBuffer getSerializationCache() {
    return serializationCache;
  }

  public void setSerializationCache(ByteBuffer serializationCache) {
    this.serializationCache = serializationCache;
  }
}
