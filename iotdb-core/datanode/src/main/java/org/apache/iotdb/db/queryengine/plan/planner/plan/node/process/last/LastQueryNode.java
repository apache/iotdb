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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryNode extends MultiChildProcessNode {

  private final Filter timeFilter;

  // the ordering of timeseries in the result of last query
  // which is set to null if there is no need to sort
  private Ordering timeseriesOrdering;

  public LastQueryNode(PlanNodeId id, Filter timeFilter, @Nullable Ordering timeseriesOrdering) {
    super(id);
    this.timeFilter = timeFilter;
    this.timeseriesOrdering = timeseriesOrdering;
  }

  public LastQueryNode(
      PlanNodeId id,
      List<PlanNode> children,
      Filter timeFilter,
      @Nullable Ordering timeseriesOrdering) {
    super(id, children);
    this.timeFilter = timeFilter;
    this.timeseriesOrdering = timeseriesOrdering;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public PlanNode clone() {
    return new LastQueryNode(getPlanNodeId(), timeFilter, timeseriesOrdering);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  public String toString() {
    return String.format("LastQueryNode-%s:[TimeFilter: %s]", this.getPlanNodeId(), timeFilter);
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
    LastQueryNode that = (LastQueryNode) o;
    return Objects.equals(timeFilter, that.timeFilter)
        && timeseriesOrdering.equals(that.timeseriesOrdering);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeFilter, timeseriesOrdering);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQuery(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY.serialize(byteBuffer);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY.serialize(stream);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      timeFilter.serialize(stream);
    }
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), stream);
    }
  }

  public static LastQueryNode deserialize(ByteBuffer byteBuffer) {
    Filter timeFilter = null;
    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      timeFilter = FilterFactory.deserialize(byteBuffer);
    }
    byte needOrderByTimeseries = ReadWriteIOUtils.readByte(byteBuffer);
    Ordering timeseriesOrdering = null;
    if (needOrderByTimeseries == 1) {
      timeseriesOrdering = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryNode(planNodeId, timeFilter, timeseriesOrdering);
  }

  @Override
  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  public Ordering getTimeseriesOrdering() {
    return timeseriesOrdering;
  }

  public void setTimeseriesOrdering(Ordering timeseriesOrdering) {
    this.timeseriesOrdering = timeseriesOrdering;
  }

  public boolean needOrderByTimeseries() {
    return timeseriesOrdering != null;
  }
}
