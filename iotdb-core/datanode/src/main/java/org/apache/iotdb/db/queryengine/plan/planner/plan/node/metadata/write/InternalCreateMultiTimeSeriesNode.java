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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalCreateMultiTimeSeriesNode extends WritePlanNode {

  // <DevicePath, <IsAligned, MeasurementGroup>>
  private Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap;

  private TRegionReplicaSet regionReplicaSet;

  public InternalCreateMultiTimeSeriesNode(
      PlanNodeId id, Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap) {
    super(id);
    this.deviceMap = deviceMap;
  }

  private InternalCreateMultiTimeSeriesNode(
      PlanNodeId id,
      Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap,
      TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.deviceMap = deviceMap;
    this.regionReplicaSet = regionReplicaSet;
  }

  public Map<PartialPath, Pair<Boolean, MeasurementGroup>> getDeviceMap() {
    return deviceMap;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES;
  }

  @Override
  public PlanNode clone() {
    return new InternalCreateMultiTimeSeriesNode(getPlanNodeId(), deviceMap, regionReplicaSet);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES.serialize(byteBuffer);

    ReadWriteIOUtils.write(deviceMap.size(), byteBuffer);
    for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry : deviceMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().left, byteBuffer);
      entry.getValue().right.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES.serialize(stream);

    ReadWriteIOUtils.write(deviceMap.size(), stream);
    for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry : deviceMap.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().left, stream);
      entry.getValue().right.serialize(stream);
    }
  }

  public static InternalCreateMultiTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap = new HashMap<>(size);
    PartialPath devicePath;
    boolean isAligned;
    MeasurementGroup measurementGroup;
    for (int i = 0; i < size; i++) {
      devicePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      isAligned = ReadWriteIOUtils.readBool(byteBuffer);
      measurementGroup = new MeasurementGroup();
      measurementGroup.deserialize(byteBuffer);
      deviceMap.put(devicePath, new Pair<>(isAligned, measurementGroup));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new InternalCreateMultiTimeSeriesNode(planNodeId, deviceMap);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    // gather devices to same target region
    Map<TRegionReplicaSet, Map<PartialPath, Pair<Boolean, MeasurementGroup>>> splitMap =
        new HashMap<>();
    for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry : deviceMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet =
          analysis
              .getSchemaPartitionInfo()
              .getSchemaRegionReplicaSet(entry.getKey().getIDeviceIDAsFullDevice());
      splitMap
          .computeIfAbsent(regionReplicaSet, k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }

    List<WritePlanNode> result = new ArrayList<>();
    for (Map.Entry<TRegionReplicaSet, Map<PartialPath, Pair<Boolean, MeasurementGroup>>> entry :
        splitMap.entrySet()) {
      result.add(
          new InternalCreateMultiTimeSeriesNode(getPlanNodeId(), entry.getValue(), entry.getKey()));
    }

    return result;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInternalCreateMultiTimeSeries(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InternalCreateMultiTimeSeriesNode that = (InternalCreateMultiTimeSeriesNode) o;
    return Objects.equals(deviceMap, that.deviceMap)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), deviceMap, regionReplicaSet);
  }
}
