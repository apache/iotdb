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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.LogicalIndexNavigation;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LogicalIndexPointer {
  // a set of labels to navigate over:
  // LAST(A.price, 3) => this is a navigation over rows with label A, so labels = {A}
  // LAST(Union.price, 3) => this is a navigation over rows matching a union variable Union, so for
  // SUBSET Union = (A, B, C), we have labels = {A, B, C}
  // LAST(price, 3) => this is a navigation over "universal pattern variable", which is effectively
  // over all rows, no matter the assigned labels. In such case labels = {}
  private final Set<IrLabel> labels;

  // logical position is a position among rows tagged with certain label (or label from a certain
  // set)
  // it has the following semantics:
  // start from FIRST or LAST row tagged with the label (with RUNNING or FINAL semantics), and go
  // logicalOffset steps forward (for FIRST) or backward (for LAST),
  // skipping to consecutive rows with matching label
  // Default: RUNNING LAST offset = 0
  private final boolean last;
  private final boolean running;
  private final int logicalOffset;

  // physical offset is the offset in physical rows, starting from the logical position. negative
  // for PREV, positive for NEXT. The default is -1 for PREV and 1 for NEXT.
  // Unspecified physical offset defaults to 0.
  private final int physicalOffset;

  public LogicalIndexPointer(
      Set<IrLabel> labels, boolean last, boolean running, int logicalOffset, int physicalOffset) {
    this.labels = requireNonNull(labels, "labels is null");
    this.last = last;
    this.running = running;
    checkArgument(logicalOffset >= 0, "logical offset must be >= 0, actual: %s", logicalOffset);
    this.logicalOffset = logicalOffset;
    this.physicalOffset = physicalOffset;
  }

  public Set<IrLabel> getLabels() {
    return labels;
  }

  public boolean isLast() {
    return last;
  }

  public boolean isRunning() {
    return running;
  }

  public int getLogicalOffset() {
    return logicalOffset;
  }

  public int getPhysicalOffset() {
    return physicalOffset;
  }

  public LogicalIndexNavigation toLogicalIndexNavigation(Map<IrLabel, Integer> mapping) {
    return new LogicalIndexNavigation(
        labels.stream().map(mapping::get).collect(toImmutableSet()),
        last,
        running,
        logicalOffset,
        physicalOffset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogicalIndexPointer that = (LogicalIndexPointer) o;
    return last == that.last
        && running == that.running
        && logicalOffset == that.logicalOffset
        && physicalOffset == that.physicalOffset
        && labels.equals(that.labels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(labels, last, running, logicalOffset, physicalOffset);
  }

  public static void serialize(LogicalIndexPointer pointer, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(pointer.labels.size(), byteBuffer);
    for (IrLabel label : pointer.labels) {
      IrLabel.serialize(label, byteBuffer);
    }

    ReadWriteIOUtils.write(pointer.last, byteBuffer);
    ReadWriteIOUtils.write(pointer.running, byteBuffer);

    ReadWriteIOUtils.write(pointer.logicalOffset, byteBuffer);
    ReadWriteIOUtils.write(pointer.physicalOffset, byteBuffer);
  }

  public static void serialize(LogicalIndexPointer pointer, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(pointer.labels.size(), stream);
    for (IrLabel label : pointer.labels) {
      IrLabel.serialize(label, stream);
    }

    ReadWriteIOUtils.write(pointer.last, stream);
    ReadWriteIOUtils.write(pointer.running, stream);

    ReadWriteIOUtils.write(pointer.logicalOffset, stream);
    ReadWriteIOUtils.write(pointer.physicalOffset, stream);
  }

  public static LogicalIndexPointer deserialize(ByteBuffer byteBuffer) {
    int labelCount = ReadWriteIOUtils.readInt(byteBuffer);
    Set<IrLabel> labels = new HashSet<>();
    for (int i = 0; i < labelCount; i++) {
      labels.add(IrLabel.deserialize(byteBuffer));
    }

    boolean last = ReadWriteIOUtils.readBoolean(byteBuffer);
    boolean running = ReadWriteIOUtils.readBoolean(byteBuffer);

    int logicalOffset = ReadWriteIOUtils.readInt(byteBuffer);
    int physicalOffset = ReadWriteIOUtils.readInt(byteBuffer);

    return new LogicalIndexPointer(labels, last, running, logicalOffset, physicalOffset);
  }
}
