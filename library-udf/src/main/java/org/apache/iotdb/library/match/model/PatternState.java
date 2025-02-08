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

package org.apache.iotdb.library.match.model;

import org.apache.iotdb.udf.api.State;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PatternState implements State {
  private List<Double> valueBuffer;
  private List<Long> timeBuffer;

  @Override
  public void reset() {
    timeBuffer = new ArrayList<>();
    valueBuffer = new ArrayList<>();
  }

  @Override
  public byte[] serialize() {
    int capacity = Integer.BYTES * 2 + valueBuffer.size() * (Double.BYTES + Long.BYTES);
    ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);

    byteBuffer.putInt(valueBuffer.size());
    Object[] times = timeBuffer.toArray();
    Object[] values = valueBuffer.toArray();
    for (int i = 0; i < timeBuffer.size(); i++) {
      byteBuffer.putLong((long) times[i]);
      byteBuffer.putDouble((double) values[i]);
    }
    return byteBuffer.array();
  }

  @Override
  public void deserialize(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      updateBuffer(byteBuffer.getLong(), byteBuffer.getDouble());
    }
  }

  @Override
  public void destroyState() {
    timeBuffer.clear();
    valueBuffer.clear();
    State.super.destroyState();
  }

  public long getFirstTime() {
    return timeBuffer.get(0);
  }

  public List<Double> getValueBuffer() {
    return valueBuffer;
  }

  public List<Long> getTimeBuffer() {
    return timeBuffer;
  }

  public void updateBuffer(long time, double dataPoint) {
    if (timeBuffer == null) {
      timeBuffer = new ArrayList<>();
    }
    if (valueBuffer == null) {
      valueBuffer = new ArrayList<>();
    }
    timeBuffer.add(time);
    valueBuffer.add(dataPoint);
  }
}
