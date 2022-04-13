/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.sql.planner.plan.IOutputPlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DeviceMergeOperator is responsible for constructing a device-based view of a set of series. And
 * output the result with specific order. The order could be 'order by device' or 'order by
 * timestamp'
 *
 * <p>Each output from its children should have the same schema. That means, the columns should be
 * same between these TsBlocks. If the input TsBlock contains n columns, the device-based view will
 * contain n+1 columns where the new column is Device column.
 */
public class DeviceMergeNode extends ProcessNode implements IOutputPlanNode {

  // The result output order that this operator
  private OrderBy mergeOrder;

  // The policy to decide whether a row should be discarded
  // The without policy is able to be push down to the DeviceMergeNode because we can know whether a
  // row contains
  // null or not.
  private FilterNullComponent filterNullComponent;

  // The map from deviceName to corresponding query result node responsible for that device.
  // DeviceNode means the node whose output TsBlock contains the data belonged to one device.
  private Map<String, PlanNode> childDeviceNodeMap = new HashMap<>();

  private List<PlanNode> children;

  private final List<ColumnHeader> columnHeaders = new ArrayList<>();;

  public DeviceMergeNode(PlanNodeId id) {
    super(id);
    this.children = new ArrayList<>();
  }

  public DeviceMergeNode(PlanNodeId id, OrderBy mergeOrder) {
    this(id);
    this.mergeOrder = mergeOrder;
    this.children = new ArrayList<>();
  }

  public void setFilterNullComponent(FilterNullComponent filterNullComponent) {
    this.filterNullComponent = filterNullComponent;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  @Override
  public PlanNode clone() {
    return new DeviceMergeNode(getPlanNodeId(), mergeOrder);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  public void addChildDeviceNode(String deviceName, PlanNode childNode) {
    this.childDeviceNodeMap.put(deviceName, childNode);
    this.children.add(childNode);
    updateColumnHeaders(childNode);
  }

  private void updateColumnHeaders(PlanNode childNode) {
    List<ColumnHeader> childColumnHeaders = ((IOutputPlanNode) childNode).getOutputColumnHeaders();
    for (ColumnHeader columnHeader : childColumnHeaders) {
      ColumnHeader tmpColumnHeader = columnHeader.replacePathWithMeasurement();
      if (!columnHeaders.contains(tmpColumnHeader)) {
        columnHeaders.add(tmpColumnHeader);
      }
    }
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return columnHeaders;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnHeaders.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
  }

  public OrderBy getMergeOrder() {
    return mergeOrder;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceMerge(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_MERGE.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), byteBuffer);
    filterNullComponent.serialize(byteBuffer);
    ReadWriteIOUtils.write(childDeviceNodeMap.size(), byteBuffer);
    for (Map.Entry<String, PlanNode> e : childDeviceNodeMap.entrySet()) {
      ReadWriteIOUtils.write(e.getKey(), byteBuffer);
      e.getValue().serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(columnHeaders.size(), byteBuffer);
    for (ColumnHeader columnHeader : columnHeaders) {
      columnHeader.serialize(byteBuffer);
    }
  }

  public static DeviceMergeNode deserialize(ByteBuffer byteBuffer) throws IllegalPathException {
    int orderByIndex = ReadWriteIOUtils.readInt(byteBuffer);
    OrderBy orderBy = OrderBy.values()[orderByIndex];
    FilterNullComponent filterNullComponent = FilterNullComponent.deserialize(byteBuffer);
    Map<String, PlanNode> childDeviceNodeMap = new HashMap<>();
    int childDeviceNodeMapSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < childDeviceNodeMapSize; i++) {
      childDeviceNodeMap.put(
          ReadWriteIOUtils.readString(byteBuffer), PlanFragment.deserializeHelper(byteBuffer));
    }

    List<ColumnHeader> columnHeaders = new ArrayList<>();
    int columnHeaderSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < columnHeaderSize; i++) {
      columnHeaders.add(ColumnHeader.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    DeviceMergeNode deviceMergeNode = new DeviceMergeNode(planNodeId, orderBy);
    deviceMergeNode.filterNullComponent = filterNullComponent;
    deviceMergeNode.childDeviceNodeMap = childDeviceNodeMap;
    deviceMergeNode.columnHeaders.addAll(columnHeaders);
    return deviceMergeNode;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[DeviceMergeNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("MergeOrder: " + (this.getMergeOrder() == null ? "null" : this.getMergeOrder()));
    return new Pair<>(title, attributes);
  }

  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeviceMergeNode that = (DeviceMergeNode) o;
    return mergeOrder == that.mergeOrder
        && Objects.equals(filterNullComponent, that.filterNullComponent)
        && Objects.equals(childDeviceNodeMap, that.childDeviceNodeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mergeOrder, filterNullComponent, childDeviceNodeMap);
  }
}
