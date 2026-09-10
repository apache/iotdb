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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.ObjectBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;

import org.apache.tsfile.utils.RamUsageEstimator;

final class TimeValueBufferBigArray {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeValueBufferBigArray.class);

  private final ObjectBigArray<TimeValueBuffer> buffers = new ObjectBigArray<>();
  private long buffersRetainedBytes;

  public void ensureCapacity(long groupCount) {
    buffers.ensureCapacity(groupCount);
  }

  public TimeValueBuffer get(int groupId) {
    return buffers.get(groupId);
  }

  public void add(int groupId, long time, double value) {
    TimeValueBuffer buffer = getOrCreate(groupId);
    long previousSize = buffer.getEstimatedSize();
    buffer.add(time, value);
    buffersRetainedBytes += buffer.getEstimatedSize() - previousSize;
  }

  public void merge(int groupId, TimeValueBuffer other) {
    TimeValueBuffer buffer = getOrCreate(groupId);
    long previousSize = buffer.getEstimatedSize();
    buffer.merge(other);
    buffersRetainedBytes += buffer.getEstimatedSize() - previousSize;
  }

  public long sizeOf() {
    return INSTANCE_SIZE + buffers.sizeOf() + buffersRetainedBytes;
  }

  public void reset() {
    buffers.reset();
    buffersRetainedBytes = 0;
  }

  private TimeValueBuffer getOrCreate(int groupId) {
    TimeValueBuffer buffer = buffers.get(groupId);
    if (buffer == null) {
      buffer = new TimeValueBuffer();
      buffers.set(groupId, buffer);
      buffersRetainedBytes += buffer.getEstimatedSize();
    }
    return buffer;
  }
}
