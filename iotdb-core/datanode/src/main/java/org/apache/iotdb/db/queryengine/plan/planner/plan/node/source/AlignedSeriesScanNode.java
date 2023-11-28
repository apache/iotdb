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
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AlignedSeriesScanNode extends SeriesSourceNode {

  // The paths of the target series which will be scanned.
  private final AlignedPath alignedPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private Ordering scanOrder = Ordering.ASC;

  // push down predicate for current series, could be null if it doesn't exist
  @Nullable private Expression pushDownPredicate;

  // push down limit for result set. The default value is -1, which means no limit
  private long pushDownLimit;

  // push down offset for result set. The default value is 0
  private long pushDownOffset;

  // used for limit and offset push down optimizer, if we select all columns from aligned device, we
  // can use statistics to skip
  private boolean queryAllSensors = false;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public AlignedSeriesScanNode(PlanNodeId id, AlignedPath alignedPath) {
    super(id);
    this.alignedPath = alignedPath;
  }

  public AlignedSeriesScanNode(
      PlanNodeId id, AlignedPath alignedPath, Ordering scanOrder, boolean lastLevelUseWildcard) {
    this(id, alignedPath);
    this.scanOrder = scanOrder;
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
    this(id, alignedPath, scanOrder, lastLevelUseWildcard);
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
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
    this(id, alignedPath, scanOrder, lastLevelUseWildcard);
    this.pushDownPredicate = pushDownPredicate;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public AlignedPath getAlignedPath() {
    return alignedPath;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Nullable
  @Override
  public Expression getPushDownPredicate() {
    return pushDownPredicate;
  }

  public void setPushDownPredicate(@Nullable Expression pushDownPredicate) {
    this.pushDownPredicate = pushDownPredicate;
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

  @Override
  public void open() throws Exception {
    // Do nothing
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  public boolean isQueryAllSensors() {
    return queryAllSensors;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for AlignedSeriesScanNode");
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
        this.regionReplicaSet,
        this.queryAllSensors);
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    String deviceName = alignedPath.getDevice();
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
    return pushDownLimit == that.pushDownLimit
        && pushDownOffset == that.pushDownOffset
        && alignedPath.equals(that.alignedPath)
        && scanOrder == that.scanOrder
        && Objects.equals(pushDownPredicate, that.pushDownPredicate)
        && Objects.equals(queryAllSensors, that.queryAllSensors)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        alignedPath,
        scanOrder,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet,
        queryAllSensors);
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
}
