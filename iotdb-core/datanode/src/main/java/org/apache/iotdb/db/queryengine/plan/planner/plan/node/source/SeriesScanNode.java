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
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

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
public class SeriesScanNode extends SeriesScanSourceNode {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SeriesScanNode.class);

  // The path of the target series which will be scanned.
  private final MeasurementPath seriesPath;

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath, Ordering scanOrder) {
    super(id, scanOrder);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Ordering scanOrder,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    super(id, scanOrder, pushDownLimit, pushDownOffset, dataRegionReplicaSet);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Ordering scanOrder,
      @Nullable Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet) {
    super(id, scanOrder, pushDownPredicate, pushDownLimit, pushDownOffset, dataRegionReplicaSet);
    this.seriesPath = seriesPath;
  }

  public MeasurementPath getSeriesPath() {
    return seriesPath;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.SERIES_SCAN;
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
        getRegionReplicaSet());
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
    return seriesPath.equals(that.seriesPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), seriesPath);
  }

  @Override
  public PartialPath getPartitionPath() {
    return getSeriesPath();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(id)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(seriesPath);
  }
}
