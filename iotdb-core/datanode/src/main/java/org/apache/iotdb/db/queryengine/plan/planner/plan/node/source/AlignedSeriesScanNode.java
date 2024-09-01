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
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AlignedSeriesScanNode extends SeriesScanSourceNode {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedSeriesScanNode.class);

  // The paths of the target series which will be scanned.
  private final AlignedPath alignedPath;

  // used for limit and offset push down optimizer, if we select all columns from aligned device, we
  // can use statistics to skip
  private boolean queryAllSensors = false;

  public AlignedSeriesScanNode(PlanNodeId id, AlignedPath alignedPath) {
    super(id);
    this.alignedPath = alignedPath;
  }

  public AlignedSeriesScanNode(
      PlanNodeId id, AlignedPath alignedPath, Ordering scanOrder, boolean lastLevelUseWildcard) {
    super(id, scanOrder);
    this.alignedPath = alignedPath;
    this.queryAllSensors = lastLevelUseWildcard;
  }

  public AlignedSeriesScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      Ordering scanOrder,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet,
      boolean lastLevelUseWildcard) {
    super(id, scanOrder, pushDownLimit, pushDownOffset, dataRegionReplicaSet);
    this.alignedPath = alignedPath;
    this.queryAllSensors = lastLevelUseWildcard;
  }

  public AlignedSeriesScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      Ordering scanOrder,
      @Nullable Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet dataRegionReplicaSet,
      boolean lastLevelUseWildcard) {
    super(id, scanOrder, pushDownPredicate, pushDownLimit, pushDownOffset, dataRegionReplicaSet);
    this.alignedPath = alignedPath;
    this.queryAllSensors = lastLevelUseWildcard;
  }

  public AlignedPath getAlignedPath() {
    return alignedPath;
  }

  public boolean isQueryAllSensors() {
    return queryAllSensors;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for AlignedSeriesScanNode");
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.ALIGNED_SERIES_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new AlignedSeriesScanNode(
        getPlanNodeId(),
        getAlignedPath(),
        getScanOrder(),
        getPushDownPredicate(),
        getPushDownLimit(),
        getPushDownOffset(),
        getRegionReplicaSet(),
        isQueryAllSensors());
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    String deviceName = alignedPath.getIDeviceID().toString();
    for (String measurement : alignedPath.getMeasurementList()) {
      outputColumnNames.add(deviceName.concat(TsFileConstant.PATH_SEPARATOR + measurement));
    }
    return outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAlignedSeriesScan(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALIGNED_SERIES_SCAN.serialize(byteBuffer);
    alignedPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    }
    ReadWriteIOUtils.write(pushDownLimit, byteBuffer);
    ReadWriteIOUtils.write(pushDownOffset, byteBuffer);
    ReadWriteIOUtils.write(queryAllSensors, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ALIGNED_SERIES_SCAN.serialize(stream);
    alignedPath.serialize(stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(pushDownPredicate, stream);
    }
    ReadWriteIOUtils.write(pushDownLimit, stream);
    ReadWriteIOUtils.write(pushDownOffset, stream);
    ReadWriteIOUtils.write(queryAllSensors, stream);
  }

  public static AlignedSeriesScanNode deserialize(ByteBuffer byteBuffer) {
    AlignedPath alignedPath = (AlignedPath) PathDeserializeUtil.deserialize(byteBuffer);
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Expression pushDownPredicate = null;
    if (isNull == 1) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean queryAllSensors = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlignedSeriesScanNode(
        planNodeId,
        alignedPath,
        scanOrder,
        pushDownPredicate,
        limit,
        offset,
        null,
        queryAllSensors);
  }

  @Override
  public void serializeUseTemplate(DataOutputStream stream, TypeProvider typeProvider)
      throws IOException {
    PlanNodeType.ALIGNED_SERIES_SCAN.serialize(stream);
    id.serialize(stream);
    ReadWriteIOUtils.write(alignedPath.getNodes().length, stream);
    for (String node : alignedPath.getNodes()) {
      ReadWriteIOUtils.write(node, stream);
    }
  }

  public static AlignedSeriesScanNode deserializeUseTemplate(
      ByteBuffer byteBuffer, TypeProvider typeProvider) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    int nodeSize = ReadWriteIOUtils.readInt(byteBuffer);
    String[] nodes = new String[nodeSize];
    for (int i = 0; i < nodeSize; i++) {
      nodes[i] = ReadWriteIOUtils.readString(byteBuffer);
    }
    AlignedPath alignedPath = new AlignedPath(new PartialPath(nodes));
    alignedPath.setMeasurementList(typeProvider.getTemplatedInfo().getMeasurementList());
    alignedPath.addSchemas(typeProvider.getTemplatedInfo().getSchemaList());

    return new AlignedSeriesScanNode(
        planNodeId,
        alignedPath,
        typeProvider.getTemplatedInfo().getScanOrder(),
        typeProvider.getTemplatedInfo().getPushDownPredicate(),
        typeProvider.getTemplatedInfo().getLimitValue(),
        typeProvider.getTemplatedInfo().getOffsetValue(),
        null,
        typeProvider.getTemplatedInfo().isQueryAllSensors());
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
    AlignedSeriesScanNode that = (AlignedSeriesScanNode) o;
    return queryAllSensors == that.queryAllSensors && alignedPath.equals(that.alignedPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), alignedPath, queryAllSensors);
  }

  public String toString() {
    return String.format(
        "AlignedSeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getAlignedPath().getFormattedString(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  public PartialPath getPartitionPath() {
    return getAlignedPath();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(id)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(alignedPath);
  }
}
