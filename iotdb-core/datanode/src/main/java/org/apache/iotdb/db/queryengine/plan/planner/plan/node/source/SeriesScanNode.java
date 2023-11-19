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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * SeriesScanNode is responsible for read data a specific series. When reading data, the
 * SeriesScanNode can read the raw data batch by batch. And also, it can leverage the filter and
 * other info to decrease the result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SeriesSourceNode {

  // The path of the target series which will be scanned.
  private final MeasurementPath seriesPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private Ordering scanOrder = Ordering.ASC;

  // push down predicate for current series, could be null if doesn't exist
  @Nullable private Expression pushDownPredicate;

  // push down limit for result set. The default value is -1, which means no limit
  private long pushDownLimit;

  // push down offset for result set. The default value is 0
  private long pushDownOffset;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath, Ordering scanOrder) {
    this(id, seriesPath);
    this.scanOrder = scanOrder;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Ordering scanOrder,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, scanOrder);
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Ordering scanOrder,
      @Nullable Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, scanOrder);
    this.pushDownPredicate = pushDownPredicate;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet dataRegion) {
    this.regionReplicaSet = dataRegion;
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

  public MeasurementPath getSeriesPath() {
    return seriesPath;
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
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for SeriesScanNode");
  }

  @Override
  public PlanNode clone() {
    return new SeriesScanNode(
        getPlanNodeId(),
        getSeriesPath(),
        getScanOrder(),
        getPushDownPredicate(),
        getPushDownLimit(),
        getPushDownOffset(),
        this.regionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of(seriesPath.getFullPath());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesScan(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    }
    ReadWriteIOUtils.write(pushDownLimit, byteBuffer);
    ReadWriteIOUtils.write(pushDownOffset, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SERIES_SCAN.serialize(stream);
    seriesPath.serialize(stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(pushDownPredicate, stream);
    }
    ReadWriteIOUtils.write(pushDownLimit, stream);
    ReadWriteIOUtils.write(pushDownOffset, stream);
  }

  public static SeriesScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Expression pushDownPredicate = null;
    if (isNull == 1) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SeriesScanNode(
        planNodeId, partialPath, scanOrder, pushDownPredicate, limit, offset, null);
  }

  @Override
  public String toString() {
    return String.format(
        "SeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getSeriesPath(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
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
    SeriesScanNode that = (SeriesScanNode) o;
    return pushDownLimit == that.pushDownLimit
        && pushDownOffset == that.pushDownOffset
        && seriesPath.equals(that.seriesPath)
        && scanOrder == that.scanOrder
        && Objects.equals(pushDownPredicate, that.pushDownPredicate)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        seriesPath,
        scanOrder,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet);
  }

  @Override
  public PartialPath getPartitionPath() {
    return getSeriesPath();
  }
}
