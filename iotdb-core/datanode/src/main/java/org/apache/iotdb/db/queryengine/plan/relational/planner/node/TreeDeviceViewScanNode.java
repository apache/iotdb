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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TreeDeviceViewScanNode extends DeviceTableScanNode {
  protected String treeDBName;
  protected Map<String, String> measurementColumnNameMap;

  public TreeDeviceViewScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Map<Symbol, Integer> tagAndAttributeIndexMap,
      String treeDBName,
      Map<String, String> measurementColumnNameMap) {
    super(id, qualifiedObjectName, outputSymbols, assignments, tagAndAttributeIndexMap);
    this.treeDBName = treeDBName;
    this.measurementColumnNameMap = measurementColumnNameMap;
  }

  public TreeDeviceViewScanNode(
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
      boolean containsNonAlignedDevice,
      String treeDBName,
      Map<String, String> measurementColumnNameMap) {
    super(
        id,
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
    this.treeDBName = treeDBName;
    this.measurementColumnNameMap = measurementColumnNameMap;
  }

  public TreeDeviceViewScanNode() {}

  public void setTreeDBName(String treeDBName) {
    this.treeDBName = treeDBName;
  }

  public String getTreeDBName() {
    return treeDBName;
  }

  public Map<String, String> getMeasurementColumnNameMap() {
    return measurementColumnNameMap;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTreeDeviceViewScan(this, context);
  }

  @Override
  public TreeDeviceViewScanNode clone() {
    return new TreeDeviceViewScanNode(
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
        containsNonAlignedDevice,
        treeDBName,
        measurementColumnNameMap);
  }

  protected static void serializeMemberVariables(
      TreeDeviceViewScanNode node, ByteBuffer byteBuffer) {
    DeviceTableScanNode.serializeMemberVariables(node, byteBuffer, true);

    ReadWriteIOUtils.write(node.treeDBName, byteBuffer);
    ReadWriteIOUtils.write(node.measurementColumnNameMap.size(), byteBuffer);
    for (Map.Entry<String, String> entry : node.measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  protected static void serializeMemberVariables(
      TreeDeviceViewScanNode node, DataOutputStream stream) throws IOException {
    DeviceTableScanNode.serializeMemberVariables(node, stream, true);

    ReadWriteIOUtils.write(node.treeDBName, stream);
    ReadWriteIOUtils.write(node.measurementColumnNameMap.size(), stream);
    for (Map.Entry<String, String> entry : node.measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  protected static void deserializeMemberVariables(
      ByteBuffer byteBuffer, TreeDeviceViewScanNode node) {
    DeviceTableScanNode.deserializeMemberVariables(byteBuffer, node, true);

    node.treeDBName = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, String> measurementColumnNameMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      measurementColumnNameMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    node.measurementColumnNameMap = measurementColumnNameMap;
  }

  // We will transform this node into its sub-Class when generate DistributionPlan, so it will never
  // should be serialized or deserialized.
  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "Unsupported to serialize: " + TreeNonAlignedDeviceViewScanNode.class.getSimpleName());
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        "Unsupported to serialize: " + TreeNonAlignedDeviceViewScanNode.class.getSimpleName());
  }

  public static DeviceTableScanNode deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "Unsupported to deserialize: " + TreeNonAlignedDeviceViewScanNode.class.getSimpleName());
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
    TreeDeviceViewScanNode that = (TreeDeviceViewScanNode) o;
    return Objects.equals(treeDBName, that.treeDBName)
        && Objects.equals(measurementColumnNameMap, that.measurementColumnNameMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), treeDBName, measurementColumnNameMap);
  }

  public String toString() {
    return "TreeDeviceViewScanNode-" + this.getPlanNodeId();
  }
}
