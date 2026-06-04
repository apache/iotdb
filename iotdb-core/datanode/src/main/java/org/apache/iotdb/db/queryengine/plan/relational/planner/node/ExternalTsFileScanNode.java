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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.TableScanNode;
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
import java.util.Optional;

public class ExternalTsFileScanNode extends TableScanNode {
  private List<String> tsFilePaths;
  private Expression timePredicate;
  private Ordering scanOrder = Ordering.ASC;
  private List<DeviceEntry> deviceEntries = Collections.emptyList();
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
    super(id, qualifiedObjectName, outputSymbols, assignments);
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
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset);
    this.timePredicate = timePredicate;
    this.scanOrder = scanOrder;
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
    this.deviceEntries = new ArrayList<>(deviceEntries);
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
        tsFilePaths,
        deviceEntries,
        deviceOffsets);
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public List<DeviceEntry> getDeviceEntries() {
    return deviceEntries;
  }

  public void setDeviceEntries(List<DeviceEntry> deviceEntries) {
    this.deviceEntries = new ArrayList<>(deviceEntries);
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

  public Optional<Expression> getTimePredicate() {
    return Optional.ofNullable(timePredicate);
  }

  public void setTimePredicate(Expression timePredicate) {
    this.timePredicate = timePredicate;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
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
}
