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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AggregationMergeSortNode extends MultiChildProcessNode {

  private final OrderByParameter mergeOrderParameter;

  private final List<String> outputColumns;

  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  // TODO if this variable can be simplified?
  private final Map<String, List<Integer>> deviceToMeasurementIndexesMap;

  public AggregationMergeSortNode(
      PlanNodeId id,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns,
      Map<String, List<Integer>> deviceToMeasurementIndexesMap) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
    this.deviceToMeasurementIndexesMap = deviceToMeasurementIndexesMap;
  }

  public AggregationMergeSortNode(
      PlanNodeId id,
      List<PlanNode> children,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns,
      Map<String, List<Integer>> deviceToMeasurementIndexesMap) {
    super(id, children);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
    this.deviceToMeasurementIndexesMap = deviceToMeasurementIndexesMap;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  public Map<String, List<Integer>> getDeviceToMeasurementIndexesMap() {
    return deviceToMeasurementIndexesMap;
  }

  @Override
  public PlanNode clone() {
    return new AggregationMergeSortNode(
        getPlanNodeId(),
        getMergeOrderParameter(),
        outputColumns,
        getDeviceToMeasurementIndexesMap());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new AggregationMergeSortNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)),
        getMergeOrderParameter(),
        outputColumns,
        getDeviceToMeasurementIndexesMap());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumns;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregationMergeSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGG_MERGE_SORT.serialize(byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
    ReadWriteIOUtils.write(deviceToMeasurementIndexesMap.size(), byteBuffer);
    for (Map.Entry<String, List<Integer>> entry : deviceToMeasurementIndexesMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
      for (Integer index : entry.getValue()) {
        ReadWriteIOUtils.write(index, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.AGG_MERGE_SORT.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(outputColumns.size(), stream);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, stream);
    }
    ReadWriteIOUtils.write(deviceToMeasurementIndexesMap.size(), stream);
    for (Map.Entry<String, List<Integer>> entry : deviceToMeasurementIndexesMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Integer index : entry.getValue()) {
        ReadWriteIOUtils.write(index, stream);
      }
    }
  }

  public static AggregationMergeSortNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumns = new ArrayList<>();
    while (columnSize > 0) {
      outputColumns.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>(mapSize);
    while (mapSize > 0) {
      String deviceName = ReadWriteIOUtils.readString(byteBuffer);
      int listSize = ReadWriteIOUtils.readInt(byteBuffer);
      List<Integer> indexes = new ArrayList<>(listSize);
      while (listSize > 0) {
        indexes.add(ReadWriteIOUtils.readInt(byteBuffer));
        listSize--;
      }
      deviceToMeasurementIndexesMap.put(deviceName, indexes);
      mapSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AggregationMergeSortNode(
        planNodeId, orderByParameter, outputColumns, deviceToMeasurementIndexesMap);
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
    AggregationMergeSortNode that = (AggregationMergeSortNode) o;
    return Objects.equals(mergeOrderParameter, that.getMergeOrderParameter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrderParameter);
  }

  @Override
  public String toString() {
    return "AggregationMergeSort-" + this.getPlanNodeId();
  }
}
