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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateMultiTimeSeriesNode extends WritePlanNode {

  private static final int SPLIT_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getMaxMeasurementNumOfInternalRequest();

  private final Map<PartialPath, MeasurementGroup> measurementGroupMap;

  private TRegionReplicaSet regionReplicaSet;

  public CreateMultiTimeSeriesNode(PlanNodeId id) {
    super(id);
    measurementGroupMap = new HashMap<>();
  }

  public CreateMultiTimeSeriesNode(
      PlanNodeId id,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<String> aliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    super(id);
    measurementGroupMap = new HashMap<>();

    // gather measurements of same device
    int size = paths.size();
    PartialPath devicePath;
    MeasurementGroup measurementGroup;
    for (int i = 0; i < size; i++) {
      devicePath = paths.get(i).getDevicePath();
      measurementGroup = measurementGroupMap.get(devicePath);
      if (measurementGroup == null) {
        measurementGroup = new MeasurementGroup();
        measurementGroupMap.put(devicePath, measurementGroup);
      }

      measurementGroup.addMeasurement(
          paths.get(i).getMeasurement(), dataTypes.get(i), encodings.get(i), compressors.get(i));

      if (propsList != null) {
        measurementGroup.addProps(propsList.get(i));
      }

      if (aliasList != null) {
        measurementGroup.addAlias(aliasList.get(i));
      }

      if (tagsList != null) {
        measurementGroup.addTags(tagsList.get(i));
      }

      if (attributesList != null) {
        measurementGroup.addAttributes(attributesList.get(i));
      }
    }
  }

  public CreateMultiTimeSeriesNode(
      PlanNodeId planNodeId, Map<PartialPath, MeasurementGroup> measurementGroupMap) {
    super(planNodeId);
    this.measurementGroupMap = measurementGroupMap;
  }

  private CreateMultiTimeSeriesNode(
      PlanNodeId planNodeId,
      Map<PartialPath, MeasurementGroup> measurementGroupMap,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.measurementGroupMap = measurementGroupMap;
    this.regionReplicaSet = regionReplicaSet;
  }

  private void addMeasurementGroup(PartialPath devicePath, MeasurementGroup measurementGroup) {
    measurementGroupMap.put(devicePath, measurementGroup);
  }

  public Map<PartialPath, MeasurementGroup> getMeasurementGroupMap() {
    return measurementGroupMap;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of CreateMultiTimeSeriesNode is not implemented");
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C schemaRegion) {
    return visitor.visitCreateMultiTimeSeries(this, schemaRegion);
  }

  public static CreateMultiTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    Map<PartialPath, MeasurementGroup> measurementGroupMap = new HashMap<>();
    int size = byteBuffer.getInt();
    PartialPath devicePath;
    MeasurementGroup measurementGroup;
    for (int i = 0; i < size; i++) {
      devicePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      measurementGroup = new MeasurementGroup();
      measurementGroup.deserialize(byteBuffer);
      measurementGroupMap.put(devicePath, measurementGroup);
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new CreateMultiTimeSeriesNode(planNodeId, measurementGroupMap);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateMultiTimeSeriesNode that = (CreateMultiTimeSeriesNode) o;
    return this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(measurementGroupMap, that.measurementGroupMap);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CREATE_MULTI_TIME_SERIES.serialize(byteBuffer);

    ReadWriteIOUtils.write(measurementGroupMap.size(), byteBuffer);
    for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      entry.getValue().serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CREATE_MULTI_TIME_SERIES.serialize(stream);

    ReadWriteIOUtils.write(measurementGroupMap.size(), stream);
    for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      entry.getKey().serialize(stream);
      entry.getValue().serialize(stream);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getPlanNodeId(), measurementGroupMap);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    // gather devices to same target region
    Map<TRegionReplicaSet, Map<PartialPath, MeasurementGroup>> splitMap = new HashMap<>();
    for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet =
          analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(entry.getKey().getFullPath());
      splitMap
          .computeIfAbsent(regionReplicaSet, k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }

    // split each region's requests to ensure each request is not that huge, which makes the system
    // more stable and may enhance the concurrency
    List<WritePlanNode> result = new ArrayList<>();
    for (Map.Entry<TRegionReplicaSet, Map<PartialPath, MeasurementGroup>> entry :
        splitMap.entrySet()) {
      for (Map<PartialPath, MeasurementGroup> measurementGroupMap :
          splitAndRegroupMeasurements(entry.getValue())) {
        result.add(
            new CreateMultiTimeSeriesNode(getPlanNodeId(), measurementGroupMap, entry.getKey()));
      }
    }

    return result;
  }

  private static List<Map<PartialPath, MeasurementGroup>> splitAndRegroupMeasurements(
      Map<PartialPath, MeasurementGroup> measurementGroupMap) {
    List<Map<PartialPath, MeasurementGroup>> result = new ArrayList<>();
    // keep measurements of same device as much as possible
    int tmpSum = 0;
    Map<PartialPath, MeasurementGroup> tmpMap = new HashMap<>();
    for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      if (entry.getValue().size() > SPLIT_SIZE) {
        for (MeasurementGroup splitMeasurementGroup : entry.getValue().split(SPLIT_SIZE)) {
          if (splitMeasurementGroup.size() == SPLIT_SIZE) {
            Map<PartialPath, MeasurementGroup> singletonMap = new HashMap<>();
            singletonMap.put(entry.getKey(), splitMeasurementGroup);
            result.add(singletonMap);
          } else {
            // each device has at most one splitMeasurementGroup with size < splitSize
            if (tmpSum + splitMeasurementGroup.size() > SPLIT_SIZE) {
              result.add(tmpMap);
              tmpMap = new HashMap<>();
              tmpSum = 0;
            }
            tmpSum += splitMeasurementGroup.size();
            tmpMap.put(entry.getKey(), splitMeasurementGroup);
          }
        }
      } else {
        if (tmpSum + entry.getValue().size() > SPLIT_SIZE) {
          result.add(tmpMap);
          tmpMap = new HashMap<>();
          tmpSum = 0;
        }
        tmpSum += entry.getValue().size();
        tmpMap.put(entry.getKey(), entry.getValue());
      }
    }

    if (!tmpMap.isEmpty()) {
      result.add(tmpMap);
    }

    return result;
  }
}
