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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.ReadTsFileTableFunction.ExternalTsFileDeviceOffset;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;

public class ExternalTsFileScanNode extends DeviceTableScanNode {
  private List<String> tsFilePaths;
  private List<List<ExternalTsFileDeviceOffset>> deviceOffsets = Collections.emptyList();

  protected ExternalTsFileScanNode() {}

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<String> tsFilePaths) {
    this(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        tsFilePaths,
        Collections.emptyList(),
        Collections.emptyList());
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<String> tsFilePaths,
      List<DeviceEntry> deviceEntries,
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    super(id, qualifiedObjectName, outputSymbols, assignments, buildTagIndexMap(assignments));
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
    this.deviceEntries = new ArrayList<>(deviceEntries);
    this.deviceOffsets = copyDeviceOffsets(deviceOffsets);
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      Ordering scanOrder,
      List<String> tsFilePaths) {
    this(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        scanOrder,
        tsFilePaths,
        Collections.emptyList());
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      Ordering scanOrder,
      List<String> tsFilePaths,
      List<DeviceEntry> deviceEntries) {
    this(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        null,
        scanOrder,
        tsFilePaths,
        deviceEntries,
        Collections.emptyList());
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      Expression timePredicate,
      Ordering scanOrder,
      List<String> tsFilePaths) {
    this(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        timePredicate,
        scanOrder,
        tsFilePaths,
        Collections.emptyList(),
        Collections.emptyList());
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      Expression timePredicate,
      Ordering scanOrder,
      List<String> tsFilePaths,
      List<DeviceEntry> deviceEntries,
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    this(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        timePredicate,
        scanOrder,
        false,
        tsFilePaths,
        deviceEntries,
        deviceOffsets);
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      Expression timePredicate,
      Ordering scanOrder,
      boolean pushLimitToEachDevice,
      List<String> tsFilePaths,
      List<DeviceEntry> deviceEntries,
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        new ArrayList<>(deviceEntries),
        buildTagIndexMap(assignments),
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        false);
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
    this.deviceOffsets = copyDeviceOffsets(deviceOffsets);
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitExternalTsFileScan(this, context);
  }

  @Override
  public ExternalTsFileScanNode clone() {
    return new ExternalTsFileScanNode(
        getPlanNodeId(),
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        timePredicate,
        scanOrder,
        pushLimitToEachDevice,
        tsFilePaths,
        deviceEntries,
        deviceOffsets);
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public List<List<ExternalTsFileDeviceOffset>> getDeviceOffsets() {
    return deviceOffsets;
  }

  public void setDeviceOffsets(List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    this.deviceOffsets = copyDeviceOffsets(deviceOffsets);
  }

  private static List<List<ExternalTsFileDeviceOffset>> copyDeviceOffsets(
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    List<List<ExternalTsFileDeviceOffset>> copiedDeviceOffsets =
        new ArrayList<>(deviceOffsets.size());
    for (List<ExternalTsFileDeviceOffset> offsets : deviceOffsets) {
      copiedDeviceOffsets.add(new ArrayList<>(offsets));
    }
    return copiedDeviceOffsets;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "ExternalTsFileScanNode cannot be serialized because it reads local external TsFiles");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        "ExternalTsFileScanNode cannot be serialized because it reads local external TsFiles");
  }

  public static ExternalTsFileScanNode deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "ExternalTsFileScanNode cannot be deserialized because it reads local external TsFiles");
  }

  @Override
  public String toString() {
    return "ExternalTsFileScanNode-" + this.getPlanNodeId();
  }

  private static Map<Symbol, Integer> buildTagIndexMap(Map<Symbol, ColumnSchema> assignments) {
    Map<Symbol, Integer> tagIndexMap = new java.util.HashMap<>();
    int tagIndex = 0;
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      if (TAG.equals(entry.getValue().getColumnCategory())) {
        tagIndexMap.put(entry.getKey(), tagIndex++);
      }
    }
    return tagIndexMap;
  }
}
