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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public abstract class SeriesScanSourceNode extends SeriesSourceNode {

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  // push down predicate for current series, could be null if doesn't exist
  @Nullable protected Expression pushDownPredicate;

  // push down limit for result set. The default value is -1, which means no limit
  protected long pushDownLimit;

  // push down offset for result set. The default value is 0
  protected long pushDownOffset;

  // The id of DataRegion where the node will run
  protected TRegionReplicaSet regionReplicaSet;

  protected SeriesScanSourceNode(PlanNodeId id) {
    super(id);
  }

  protected SeriesScanSourceNode(PlanNodeId id, Ordering scanOrder) {
    super(id);
    this.scanOrder = scanOrder;
  }

  protected SeriesScanSourceNode(
      PlanNodeId id,
      Ordering scanOrder,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    super(id);
    this.scanOrder = scanOrder;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  protected SeriesScanSourceNode(
      PlanNodeId id,
      Ordering scanOrder,
      @Nullable Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    super(id);
    this.scanOrder = scanOrder;
    this.pushDownPredicate = pushDownPredicate;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public long getPushDownLimit() {
    return pushDownLimit;
  }

  public long getPushDownOffset() {
    return pushDownOffset;
  }

  public void setPushDownLimit(long pushDownLimit) {
    this.pushDownLimit = pushDownLimit;
  }

  public void setPushDownOffset(long pushDownOffset) {
    this.pushDownOffset = pushDownOffset;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  @Nullable
  @Override
  public Expression getPushDownPredicate() {
    return pushDownPredicate;
  }

  public void setPushDownPredicate(@Nullable Expression pushDownPredicate) {
    this.pushDownPredicate = pushDownPredicate;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet dataRegion) {
    this.regionReplicaSet = dataRegion;
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public void open() throws Exception {
    // do nothing
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for SeriesScanSourceNode");
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesScanSource(this, context);
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
    SeriesScanSourceNode that = (SeriesScanSourceNode) o;
    return pushDownLimit == that.pushDownLimit
        && pushDownOffset == that.pushDownOffset
        && scanOrder == that.scanOrder
        && Objects.equals(pushDownPredicate, that.pushDownPredicate)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        scanOrder,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet);
  }
}
