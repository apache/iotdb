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
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.Lists;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExternalTsFileScanNode extends DeviceTableScanNode {
  private ExternalTsFileQueryResource externalTsFileQueryResource;
  private List<Integer> deviceEntryIndexes = Collections.emptyList();
  private int deviceTaskPartitionIndex = -1;
  private SchemaFilter schemaFilter;

  protected ExternalTsFileScanNode() {}

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Map<Symbol, Integer> tagAndAttributeIndexMap,
      ExternalTsFileQueryResource externalTsFileQueryResource) {
    super(id, qualifiedObjectName, outputSymbols, assignments, tagAndAttributeIndexMap);
    this.externalTsFileQueryResource = externalTsFileQueryResource;
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
      Map<Symbol, Integer> tagAndAttributeIndexMap,
      ExternalTsFileQueryResource externalTsFileQueryResource,
      List<Integer> deviceEntryIndexes,
      int deviceTaskPartitionIndex,
      SchemaFilter schemaFilter) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        Lists.transform(
            deviceEntryIndexes, externalTsFileQueryResource.getSharedDeviceEntries()::get),
        tagAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        false);
    this.externalTsFileQueryResource = externalTsFileQueryResource;
    this.deviceEntryIndexes = deviceEntryIndexes;
    this.deviceTaskPartitionIndex = deviceTaskPartitionIndex;
    this.schemaFilter = schemaFilter;
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
        tagAndAttributeIndexMap,
        externalTsFileQueryResource,
        deviceEntryIndexes,
        deviceTaskPartitionIndex,
        schemaFilter);
  }

  public List<String> getTsFilePaths() {
    return externalTsFileQueryResource.getTsFilePaths();
  }

  public ExternalTsFileQueryResource getExternalTsFileQueryResource() {
    return externalTsFileQueryResource;
  }

  public List<Integer> getDeviceEntryIndexes() {
    return deviceEntryIndexes;
  }

  public int getDeviceTaskPartitionIndex() {
    return deviceTaskPartitionIndex;
  }

  @Override
  public void setDeviceEntries(List<DeviceEntry> deviceEntries) {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages
            .EXTERNAL_TSFILE_SCAN_NODE_DEVICE_ENTRIES_MUST_BE_SET_BY_DEVICE_ENTRY_INDEXES);
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.EXTERNAL_TSFILE_SCAN_NODE_CANNOT_BE_SERIALIZED);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.EXTERNAL_TSFILE_SCAN_NODE_CANNOT_BE_SERIALIZED);
  }

  @Override
  public String toString() {
    return "ExternalTsFileScanNode-" + this.getPlanNodeId();
  }
}
