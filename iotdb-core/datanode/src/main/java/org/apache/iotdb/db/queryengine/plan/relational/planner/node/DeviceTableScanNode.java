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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeviceTableScanNode extends TableScanNode {

  protected List<DeviceEntry> deviceEntries;

  // Indicates the respective index order of ID and Attribute columns in DeviceEntry.
  // For example, for DeviceEntry `table1.tag1.tag2.attribute1.attribute2.s1.s2`, the content of
  // `tagAndAttributeIndexMap` will
  // be `tag1: 0, tag2: 1, attribute1: 0, attribute2: 1`.
  protected Map<Symbol, Integer> tagAndAttributeIndexMap;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  // extracted time filter expression in where clause
  // case 1: where s1 > 1 and time >= 0 and time <= 10, time predicate will be time >= 0 and time <=
  // 10, pushDownPredicate will be s1 > 1
  // case 2: where s1 > 1 or time < 10, time predicate will be null, pushDownPredicate will be s1 >
  // 1 or time < 10
  @Nullable protected Expression timePredicate;

  protected transient Filter timeFilter;

  // pushLimitToEachDevice == true means that each device in DeviceTableScanNode need to return
  // `pushDownLimit` row number
  // pushLimitToEachDevice == false means that all devices in DeviceTableScanNode totally need to
  // return
  // `pushDownLimit` row number
  protected boolean pushLimitToEachDevice = false;

  protected transient boolean containsNonAlignedDevice;

  protected DeviceTableScanNode() {}

  public DeviceTableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Map<Symbol, Integer> tagAndAttributeIndexMap) {
    super(id, qualifiedObjectName, outputSymbols, assignments);
    this.tagAndAttributeIndexMap = tagAndAttributeIndexMap;
  }

  public DeviceTableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> tagAndAttributeIndexMap,
      Ordering scanOrder,
      Expression timePredicate,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice,
      boolean containsNonAlignedDevice) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset);
    this.deviceEntries = deviceEntries;
    this.tagAndAttributeIndexMap = tagAndAttributeIndexMap;
    this.scanOrder = scanOrder;
    this.timePredicate = timePredicate;
    this.pushDownPredicate = pushDownPredicate;
    this.pushLimitToEachDevice = pushLimitToEachDevice;
    this.containsNonAlignedDevice = containsNonAlignedDevice;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceTableScan(this, context);
  }

  @Override
  public DeviceTableScanNode clone() {
    return new DeviceTableScanNode(
        getPlanNodeId(),
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        tagAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        containsNonAlignedDevice);
  }

  protected static void serializeMemberVariables(
      DeviceTableScanNode node, ByteBuffer byteBuffer, boolean serializeOutputSymbols) {
    TableScanNode.serializeMemberVariables(node, byteBuffer, serializeOutputSymbols);

    ReadWriteIOUtils.write(node.deviceEntries.size(), byteBuffer);
    for (DeviceEntry entry : node.deviceEntries) {
      entry.serialize(byteBuffer);
    }

    ReadWriteIOUtils.write(node.tagAndAttributeIndexMap.size(), byteBuffer);
    for (Map.Entry<Symbol, Integer> entry : node.tagAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(node.scanOrder.ordinal(), byteBuffer);

    if (node.timePredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(node.timePredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(node.pushLimitToEachDevice, byteBuffer);
  }

  protected static void serializeMemberVariables(
      DeviceTableScanNode node, DataOutputStream stream, boolean serializeOutputSymbols)
      throws IOException {
    TableScanNode.serializeMemberVariables(node, stream, serializeOutputSymbols);

    ReadWriteIOUtils.write(node.deviceEntries.size(), stream);
    for (DeviceEntry entry : node.deviceEntries) {
      entry.serialize(stream);
    }

    ReadWriteIOUtils.write(node.tagAndAttributeIndexMap.size(), stream);
    for (Map.Entry<Symbol, Integer> entry : node.tagAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(node.scanOrder.ordinal(), stream);

    if (node.timePredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(node.timePredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(node.pushLimitToEachDevice, stream);
  }

  protected static void deserializeMemberVariables(
      ByteBuffer byteBuffer, DeviceTableScanNode node, boolean deserializeOutputSymbols) {
    TableScanNode.deserializeMemberVariables(byteBuffer, node, deserializeOutputSymbols);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DeviceEntry> deviceEntries = new ArrayList<>(size);
    while (size-- > 0) {
      deviceEntries.add(AlignedDeviceEntry.deserialize(byteBuffer));
    }
    node.deviceEntries = deviceEntries;

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, Integer> tagAndAttributeIndexMap = new HashMap<>(size);
    while (size-- > 0) {
      tagAndAttributeIndexMap.put(
          Symbol.deserialize(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    }
    node.tagAndAttributeIndexMap = tagAndAttributeIndexMap;

    node.scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    boolean hasTimePredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasTimePredicate) {
      node.timePredicate = Expression.deserialize(byteBuffer);
    }

    node.pushLimitToEachDevice = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_TABLE_SCAN_NODE.serialize(byteBuffer);

    DeviceTableScanNode.serializeMemberVariables(this, byteBuffer, true);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_TABLE_SCAN_NODE.serialize(stream);

    DeviceTableScanNode.serializeMemberVariables(this, stream, true);
  }

  public static DeviceTableScanNode deserialize(ByteBuffer byteBuffer) {
    DeviceTableScanNode node = new DeviceTableScanNode();
    DeviceTableScanNode.deserializeMemberVariables(byteBuffer, node, true);

    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  public void setDeviceEntries(List<DeviceEntry> deviceEntries) {
    this.deviceEntries = deviceEntries;
  }

  public Map<Symbol, Integer> getTagAndAttributeIndexMap() {
    return this.tagAndAttributeIndexMap;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public Ordering getScanOrder() {
    return this.scanOrder;
  }

  public List<DeviceEntry> getDeviceEntries() {
    return deviceEntries;
  }

  public void appendDeviceEntry(DeviceEntry deviceEntry) {
    this.deviceEntries.add(deviceEntry);
  }

  public void setPushLimitToEachDevice(boolean pushLimitToEachDevice) {
    this.pushLimitToEachDevice = pushLimitToEachDevice;
  }

  public boolean isPushLimitToEachDevice() {
    return pushLimitToEachDevice;
  }

  public Optional<Expression> getTimePredicate() {
    return Optional.ofNullable(timePredicate);
  }

  public void setTimePredicate(@Nullable Expression timePredicate) {
    this.timePredicate = timePredicate;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public boolean containsNonAlignedDevice() {
    return containsNonAlignedDevice;
  }

  public void setContainsNonAlignedDevice() {
    this.containsNonAlignedDevice = true;
  }

  public String toString() {
    return "DeviceTableScanNode-" + this.getPlanNodeId();
  }
}
