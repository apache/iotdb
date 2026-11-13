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
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DTWState implements State {
  private List<DTWMatchResult> matchResults;
  private Deque<Double> valueBuffer;
  private Deque<Long> timeBuffer;
  // Keep effective input because partial states may arrive in any temporal order or interleave.
  private Map<Long, Double> input;
  private Integer size;
  private boolean needsRebuild;

  public DTWState() {}

  public DTWState(int size) {
    this.size = size;
  }

  @Override
  public void reset() {
    matchResults = new ArrayList<>();
    valueBuffer = new ArrayDeque<>(size);
    timeBuffer = new ArrayDeque<>(size);
    input = new TreeMap<>();
    needsRebuild = false;
  }

  @Override
  public byte[] serialize() {
    int capacity =
        Integer.BYTES * 2
            + Byte.BYTES
            + input.size() * (Double.BYTES + Long.BYTES)
            + matchResults.size() * DTWMatchResult.BYTES;
    ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);

    byteBuffer.putInt(input.size());
    for (Map.Entry<Long, Double> point : input.entrySet()) {
      byteBuffer.putLong(point.getKey());
      byteBuffer.putDouble(point.getValue());
    }

    byteBuffer.putInt(matchResults.size());
    for (DTWMatchResult matchResult : matchResults) {
      byteBuffer.put(matchResult.toByteArray());
    }
    byteBuffer.put((byte) (needsRebuild ? 1 : 0));

    return byteBuffer.array();
  }

  @Override
  public void deserialize(byte[] bytes) {
    reset();
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
    needsRebuild = byteBuffer.get() != 0;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public Integer getSize() {
    return size;
  }

  public void updateBuffer(long time, double dataPoint) {
    if (!timeBuffer.isEmpty() && time <= timeBuffer.getLast()) {
      // Late or replaced points invalidate previously calculated windows, just like a merge.
      needsRebuild = true;
    }
    input.put(time, dataPoint);
    updateWindow(time, dataPoint);
  }

  public void updateWindow(long time, double dataPoint) {
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

  public Map<Long, Double> getInput() {
    return Collections.unmodifiableMap(input);
  }

  public void mergeInput(DTWState other) {
    if (other.input.isEmpty()) {
      return;
    }
    input.putAll(other.input);
    clearWindowAndMatches();
    needsRebuild = true;
  }

  public boolean needsRebuild() {
    return needsRebuild;
  }

  public void clearWindowAndMatches() {
    valueBuffer.clear();
    timeBuffer.clear();
    matchResults.clear();
  }

  public void finishRebuild() {
    needsRebuild = false;
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
    timeBuffer.clear();
    input.clear();
    matchResults.clear();
    State.super.destroyState();
  }

  public void addMatchResult(DTWMatchResult matchResult) {
    matchResults.add(matchResult);
  }

  public List<DTWMatchResult> getMatchResults() {
    return matchResults;
  }
}
