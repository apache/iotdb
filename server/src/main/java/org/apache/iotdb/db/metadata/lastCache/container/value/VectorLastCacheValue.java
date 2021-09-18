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

package org.apache.iotdb.db.metadata.lastCache.container.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

// this class defines the storage of vector lastCache data
public class VectorLastCacheValue implements ILastCacheValue {

  // the last point data of different subSensors may vary from each other on timestamp
  private long[] timestamps;

  private TsPrimitiveType[] values;

  public VectorLastCacheValue(int size) {
    timestamps = new long[size];
    values = new TsPrimitiveType[size];
  }

  @Override
  public int getSize() {
    return values.length;
  }

  @Override
  public long getTimestamp(int index) {
    return timestamps[index];
  }

  @Override
  public void setTimestamp(int index, long timestamp) {
    timestamps[index] = timestamp;
  }

  @Override
  public TsPrimitiveType getValue(int index) {
    return values == null ? null : values[index];
  }

  @Override
  public void setValue(int index, TsPrimitiveType value) {
    values[index] = value;
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    if (values == null || index < 0 || index >= values.length || values[index] == null) {
      return null;
    }
    return new TimeValuePair(timestamps[index], values[index]);
  }

  @Override
  public long getTimestamp() {
    return 0;
  }

  @Override
  public void setTimestamp(long timestamp) {}

  @Override
  public void setValue(TsPrimitiveType value) {}

  @Override
  public TimeValuePair getTimeValuePair() {
    return null;
  }
}
