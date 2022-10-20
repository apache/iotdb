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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InternalCreateTimeSeriesNode extends WritePlanNode {

  private PartialPath devicePath;
  private MeasurementGroup measurementGroup;
  private boolean isAligned;

  private TRegionReplicaSet regionReplicaSet;

  public InternalCreateTimeSeriesNode(
      PlanNodeId id, PartialPath devicePath, MeasurementGroup measurementGroup, boolean isAligned) {
    super(id);
    this.devicePath = devicePath;
    this.measurementGroup = measurementGroup;
    this.isAligned = isAligned;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public MeasurementGroup getMeasurementGroup() {
    return measurementGroup;
  }

  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of InternalCreateTimeSeriesNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInternalCreateTimeSeries(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INTERNAL_CREATE_TIMESERIES.serialize(byteBuffer);
    devicePath.serialize(byteBuffer);
    measurementGroup.serialize(byteBuffer);
    ReadWriteIOUtils.write(isAligned, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INTERNAL_CREATE_TIMESERIES.serialize(stream);
    devicePath.serialize(stream);
    measurementGroup.serialize(stream);
    ReadWriteIOUtils.write(isAligned, stream);
  }

  public static InternalCreateTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    PartialPath devicePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.deserialize(byteBuffer);
    boolean isAligned = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new InternalCreateTimeSeriesNode(planNodeId, devicePath, measurementGroup, isAligned);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    TRegionReplicaSet regionReplicaSet =
        analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(devicePath.getFullPath());
    setRegionReplicaSet(regionReplicaSet);
    return ImmutableList.of(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InternalCreateTimeSeriesNode that = (InternalCreateTimeSeriesNode) o;
    return Objects.equals(devicePath, that.devicePath)
        && Objects.equals(measurementGroup, that.measurementGroup);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), devicePath, measurementGroup);
  }
}
