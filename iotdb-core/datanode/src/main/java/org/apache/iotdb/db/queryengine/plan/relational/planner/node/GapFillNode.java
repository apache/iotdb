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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GapFillNode extends SingleChildProcessNode {

  // start time(inclusive) of gapfill, already adjust according to the third parameter(origin) of
  // date_bin_gapfill
  private final long startTime;
  // end time(inclusive) of gapfill, already adjust according to the third parameter(origin) of
  // date_bin_gapfill
  private final long endTime;
  // used when the first parameter(timeInterval) of date_bin_gapfill is like 1y, 1mo, 1y1mo which
  // only containing time interval unit larger than month
  private final int monthDuration;
  // used when the first parameter(timeInterval) of date_bin_gapfill is like 1ns, 1us, 1ms, 1s, 1m,
  // 1h, 1d, 1w and so on which only containing time interval unit less than month
  private final long nonMonthDuration;

  private final Symbol gapFillColumn;

  private final List<Symbol> gapFillGroupingKeys;

  public GapFillNode(
      PlanNodeId id,
      PlanNode child,
      long startTime,
      long endTime,
      int monthDuration,
      long nonMonthDuration,
      Symbol gapFillColumn,
      @Nonnull List<Symbol> gapFillGroupingKeys) {
    super(id, child);
    this.startTime = startTime;
    this.endTime = endTime;
    this.monthDuration = monthDuration;
    this.nonMonthDuration = nonMonthDuration;
    this.gapFillColumn = gapFillColumn;
    this.gapFillGroupingKeys = gapFillGroupingKeys;
  }

  public Symbol getGapFillColumn() {
    return gapFillColumn;
  }

  public List<Symbol> getGapFillGroupingKeys() {
    return gapFillGroupingKeys;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public int getMonthDuration() {
    return monthDuration;
  }

  public long getNonMonthDuration() {
    return nonMonthDuration;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitGapFill(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return child.getOutputSymbols();
  }

  @Override
  public PlanNode clone() {
    return new GapFillNode(
        id,
        null,
        startTime,
        endTime,
        monthDuration,
        nonMonthDuration,
        gapFillColumn,
        gapFillGroupingKeys);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_GAP_FILL_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(startTime, byteBuffer);
    ReadWriteIOUtils.write(endTime, byteBuffer);
    ReadWriteIOUtils.write(monthDuration, byteBuffer);
    ReadWriteIOUtils.write(nonMonthDuration, byteBuffer);
    Symbol.serialize(gapFillColumn, byteBuffer);
    ReadWriteIOUtils.write(gapFillGroupingKeys.size(), byteBuffer);
    for (Symbol symbol : gapFillGroupingKeys) {
      Symbol.serialize(symbol, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_GAP_FILL_NODE.serialize(stream);
    ReadWriteIOUtils.write(startTime, stream);
    ReadWriteIOUtils.write(endTime, stream);
    ReadWriteIOUtils.write(monthDuration, stream);
    ReadWriteIOUtils.write(nonMonthDuration, stream);
    Symbol.serialize(gapFillColumn, stream);
    ReadWriteIOUtils.write(gapFillGroupingKeys.size(), stream);
    for (Symbol symbol : gapFillGroupingKeys) {
      Symbol.serialize(symbol, stream);
    }
  }

  public static GapFillNode deserialize(ByteBuffer byteBuffer) {
    long startTime = ReadWriteIOUtils.readLong(byteBuffer);
    long endTime = ReadWriteIOUtils.readLong(byteBuffer);
    int monthDuration = ReadWriteIOUtils.readInt(byteBuffer);
    long nonMonthDuration = ReadWriteIOUtils.readLong(byteBuffer);
    Symbol gapFillColumn = Symbol.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> gapFillGroupingKeys = new ArrayList<>(size);
    while (size-- > 0) {
      gapFillGroupingKeys.add(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GapFillNode(
        planNodeId,
        null,
        startTime,
        endTime,
        monthDuration,
        nonMonthDuration,
        gapFillColumn,
        gapFillGroupingKeys);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new GapFillNode(
        id,
        Iterables.getOnlyElement(newChildren),
        startTime,
        endTime,
        monthDuration,
        nonMonthDuration,
        gapFillColumn,
        gapFillGroupingKeys);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GapFillNode that = (GapFillNode) o;
    return startTime == that.startTime
        && endTime == that.endTime
        && monthDuration == that.monthDuration
        && nonMonthDuration == that.nonMonthDuration
        && Objects.equals(gapFillColumn, that.gapFillColumn)
        && Objects.equals(gapFillGroupingKeys, that.gapFillGroupingKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        startTime,
        endTime,
        monthDuration,
        nonMonthDuration,
        gapFillColumn,
        gapFillGroupingKeys);
  }

  @Override
  public String toString() {
    return "GapFillNode-" + this.getPlanNodeId();
  }
}
