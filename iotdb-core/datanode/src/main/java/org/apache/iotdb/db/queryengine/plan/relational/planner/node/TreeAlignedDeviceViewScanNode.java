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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreeAlignedDeviceViewScanNode extends TreeDeviceViewScanNode {

  public TreeAlignedDeviceViewScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> idAndAttributeIndexMap,
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
        idAndAttributeIndexMap,
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

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTreeAlignedDeviceViewScan(this, context);
  }

  @Override
  public TreeAlignedDeviceViewScanNode clone() {
    return new TreeAlignedDeviceViewScanNode(
        getPlanNodeId(),
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
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

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TREE_ALIGNED_DEVICE_VIEW_SCAN_NODE.serialize(byteBuffer);

    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), byteBuffer);

    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    outputSymbols.forEach(symbol -> ReadWriteIOUtils.write(symbol.getName(), byteBuffer));

    ReadWriteIOUtils.write(assignments.size(), byteBuffer);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ColumnSchema.serialize(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), byteBuffer);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(
          byteBuffer,
          entry instanceof AlignedDeviceEntry
              ? DeviceEntry.DeviceEntryType.ALIGNED.ordinal()
              : DeviceEntry.DeviceEntryType.NON_ALIGNED.ordinal());
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), byteBuffer);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(timePredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(pushDownLimit, byteBuffer);
    ReadWriteIOUtils.write(pushDownOffset, byteBuffer);
    ReadWriteIOUtils.write(pushLimitToEachDevice, byteBuffer);

    ReadWriteIOUtils.write(treeDBName, byteBuffer);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), byteBuffer);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TREE_ALIGNED_DEVICE_VIEW_SCAN_NODE.serialize(stream);
    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), stream);

    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      ReadWriteIOUtils.write(symbol.getName(), stream);
    }

    ReadWriteIOUtils.write(assignments.size(), stream);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ColumnSchema.serialize(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), stream);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(
          stream,
          entry instanceof AlignedDeviceEntry
              ? DeviceEntry.DeviceEntryType.ALIGNED.ordinal()
              : DeviceEntry.DeviceEntryType.NON_ALIGNED.ordinal());
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), stream);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(timePredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(pushDownPredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(pushDownLimit, stream);
    ReadWriteIOUtils.write(pushDownOffset, stream);
    ReadWriteIOUtils.write(pushLimitToEachDevice, stream);

    ReadWriteIOUtils.write(treeDBName, stream);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), stream);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  public static TreeAlignedDeviceViewScanNode deserialize(ByteBuffer byteBuffer) {
    boolean hasDatabaseName = ReadWriteIOUtils.readBool(byteBuffer);
    String databaseName = null;
    if (hasDatabaseName) {
      databaseName = ReadWriteIOUtils.readString(byteBuffer);
    }
    String tableName = ReadWriteIOUtils.readString(byteBuffer);
    QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(databaseName, tableName);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputSymbols = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      outputSymbols.add(Symbol.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, ColumnSchema> assignments = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      assignments.put(Symbol.deserialize(byteBuffer), ColumnSchema.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DeviceEntry> deviceEntries = new ArrayList<>(size);
    while (size-- > 0) {
      deviceEntries.add(DeviceEntry.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>(size);
    while (size-- > 0) {
      idAndAttributeIndexMap.put(
          Symbol.deserialize(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    Expression timePredicate = null;
    boolean hasTimePredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasTimePredicate) {
      timePredicate = Expression.deserialize(byteBuffer);
    }

    Expression pushDownPredicate = null;
    boolean hasPushDownPredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasPushDownPredicate) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }

    long pushDownLimit = ReadWriteIOUtils.readLong(byteBuffer);
    long pushDownOffset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean pushLimitToEachDevice = ReadWriteIOUtils.readBool(byteBuffer);

    String treeDBName = ReadWriteIOUtils.readString(byteBuffer);
    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, String> measurementColumnNameMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      measurementColumnNameMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new TreeAlignedDeviceViewScanNode(
        planNodeId,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        false,
        treeDBName,
        measurementColumnNameMap);
  }
}
