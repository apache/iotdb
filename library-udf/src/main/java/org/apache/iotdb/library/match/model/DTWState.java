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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class DTWState implements State {
  private List<DTWMatchResult> matchResults;
  private Deque<Double> valueBuffer;
  private Deque<Long> timeBuffer;
  private Integer size;

  public DTWState() {}

  public DTWState(int size) {
    this.size = size;
  }

  @Override
  public void reset() {
    matchResults = new ArrayList<>();
    valueBuffer = new ArrayDeque<>(size);
    timeBuffer = new ArrayDeque<>(size);
  }

  @Override
  public byte[] serialize() {
    int capacity =
        Integer.BYTES * 2
            + valueBuffer.size() * (Double.BYTES + Long.BYTES)
            + matchResults.size() * DTWMatchResult.BYTES;
    ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);

    byteBuffer.putInt(valueBuffer.size());
    Object[] times = timeBuffer.toArray();
    Object[] values = valueBuffer.toArray();
    for (int i = 0; i < timeBuffer.size(); i++) {
      byteBuffer.putLong((long) times[i]);
      byteBuffer.putDouble((double) values[i]);
    }

    byteBuffer.putInt(matchResults.size());
    for (DTWMatchResult matchResult : matchResults) {
      byteBuffer.put(matchResult.toByteArray());
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

    int resultSize = byteBuffer.getInt();
    for (int i = 0; i < resultSize; i++) {
      byte[] dst = new byte[DTWMatchResult.BYTES];
      byteBuffer.get(dst);
      matchResults.add(DTWMatchResult.fromByteArray(dst));
    }
  }

  public void setSize(int size) {
    this.size = size;
  }

  public Integer getSize() {
    return size;
  }

  public void updateBuffer(long time, double dataPoint) {
    if (valueBuffer.size() == size) {
      valueBuffer.poll();
      timeBuffer.poll();
    }
    timeBuffer.offer(time);
    valueBuffer.offer(dataPoint);
  }

  public Double[] getValueBuffer() {
    return valueBuffer.toArray(new Double[0]);
  }

  public Long[] getTimeBuffer() {
    return timeBuffer.toArray(new Long[0]);
  }

  public long getFirstTime() {
    return timeBuffer.getFirst();
  }

  public long getLastTime() {
    return timeBuffer.getLast();
  }

  @Override
  public void destroyState() {
    valueBuffer.clear();
    State.super.destroyState();
  }

  public void addMatchResult(DTWMatchResult matchResult) {
    matchResults.add(matchResult);
  }

  public List<DTWMatchResult> getMatchResults() {
    return matchResults;
  }

  public static void main(String[] args) {
    DTWState state = new DTWState();
    state.setSize(5);
    state.reset();
    for (int i = 0; i < 4; i++) {
      state.updateBuffer(i, i);
      state.addMatchResult(new DTWMatchResult(i, i, i));
    }
    for (int i = 0; i < state.getTimeBuffer().length; i++) {
      System.out.println(state.getTimeBuffer()[i] + " " + state.getValueBuffer()[i]);
    }
    for (DTWMatchResult matchResult : state.matchResults) {
      System.out.println(matchResult);
    }

    DTWState newState = new DTWState();
    newState.setSize(5);
    newState.reset();
    newState.deserialize(state.serialize());
    for (int i = 0; i < state.getTimeBuffer().length; i++) {
      System.out.println(state.getTimeBuffer()[i] + " " + state.getValueBuffer()[i]);
    }
    System.out.println(newState.getMatchResults());
  }
}
