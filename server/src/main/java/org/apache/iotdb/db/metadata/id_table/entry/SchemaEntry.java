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

package org.apache.iotdb.db.metadata.id_table.entry;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class SchemaEntry implements ILastCacheContainer {

  /* 39 bits of disk pointer */
  /* 1 bits indicate whether there is a trigger */
  /*  1 byte of compressor  */
  /*   1 byte of encoding   */
  /*    1 byte of type      */
  private long schema;

  private long lastTime;

  private TsPrimitiveType lastValue;

  private long flushTime;

  public SchemaEntry(TSDataType dataType) {
    TSEncoding encoding = getDefaultEncoding(dataType);
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();

    schema |= dataType.serialize();
    schema |= (((long) encoding.serialize()) << 8);
    schema |= (((long) compressionType.serialize()) << 16);

    lastTime = Long.MIN_VALUE;
    flushTime = Long.MIN_VALUE;
  }

  /**
   * get ts data type from long value of schema
   *
   * @return ts data type
   */
  public TSDataType getTSDataType() {
    return TSDataType.deserialize((byte) schema);
  }

  /**
   * get ts encoding from long value of schema
   *
   * @return ts encoding
   */
  public TSEncoding getTSEncoding() {
    return TSEncoding.deserialize((byte) (schema >> 8));
  }

  /**
   * get compression type from long value of schema
   *
   * @return compression type
   */
  public CompressionType getCompressionType() {
    return CompressionType.deserialize((byte) (schema >> 16));
  }

  public void updateLastedFlushTime(long lastFlushTime) {
    flushTime = Math.max(flushTime, lastFlushTime);
  }

  public long getLastTime() {
    return lastTime;
  }

  public Object getLastValue() {
    return lastValue;
  }

  public long getFlushTime() {
    return flushTime;
  }

  // region last cache
  @Override
  public TimeValuePair getCachedLast() {
    return lastValue == null ? null : new TimeValuePair(lastTime, lastValue);
  }

  @Override
  public void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (lastValue == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastTime = timeValuePair.getTimestamp();
        lastValue = timeValuePair.getValue();
      }
    } else if (timeValuePair.getTimestamp() > lastTime
        || (timeValuePair.getTimestamp() == lastTime && highPriorityUpdate)) {
      lastTime = timeValuePair.getTimestamp();
      lastValue = timeValuePair.getValue();
    }
  }

  @Override
  public void resetLastCache() {
    lastValue = null;
  }

  @Override
  public boolean isEmpty() {
    return lastValue == null;
  }
  // endregion
}
