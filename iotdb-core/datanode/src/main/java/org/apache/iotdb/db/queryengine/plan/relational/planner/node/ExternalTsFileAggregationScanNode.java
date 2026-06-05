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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

public class ExternalTsFileAggregationScanNode extends AggregationTableScanNode {
  private List<String> tsFilePaths;
  private List<List<ExternalTsFileDeviceOffset>> deviceOffsets = Collections.emptyList();

  public ExternalTsFileAggregationScanNode(
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
      Assignments projection,
      Map<Symbol, AggregationNode.Aggregation> aggregations,
      AggregationNode.GroupingSetDescriptor groupingSets,
      List<Symbol> preGroupedSymbols,
      AggregationNode.Step step,
      Optional<Symbol> groupIdSymbol,
      List<String> tsFilePaths,
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
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
        containsNonAlignedDevice,
        projection,
        aggregations,
        groupingSets,
        preGroupedSymbols,
        step,
        groupIdSymbol);
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
    this.deviceOffsets = copyDeviceOffsets(deviceOffsets);
  }

  protected ExternalTsFileAggregationScanNode() {}

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitExternalTsFileAggregationScan(this, context);
  }

  @Override
  public ExternalTsFileAggregationScanNode clone() {
    return new ExternalTsFileAggregationScanNode(
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
        containsNonAlignedDevice,
        projection,
        aggregations,
        groupingSets,
        preGroupedSymbols,
        step,
        groupIdSymbol,
        tsFilePaths,
        deviceOffsets);
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public List<List<ExternalTsFileDeviceOffset>> getDeviceOffsets() {
    return deviceOffsets;
  }

  @Override
  public void sortDeviceEntries(Comparator<DeviceEntry> comparator) {
    int[] indexes =
        IntStream.range(0, deviceEntries.size())
            .boxed()
            .sorted(
                (left, right) ->
                    comparator.compare(deviceEntries.get(left), deviceEntries.get(right)))
            .mapToInt(Integer::intValue)
            .toArray();
    List<DeviceEntry> sortedDeviceEntries = new ArrayList<>(deviceEntries.size());
    List<List<ExternalTsFileDeviceOffset>> sortedDeviceOffsets =
        new ArrayList<>(deviceOffsets.size());
    for (int index : indexes) {
      sortedDeviceEntries.add(deviceEntries.get(index));
      sortedDeviceOffsets.add(deviceOffsets.get(index));
    }
    this.deviceEntries = sortedDeviceEntries;
    this.deviceOffsets = sortedDeviceOffsets;
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
        "ExternalTsFileAggregationScanNode cannot be serialized because it reads local external TsFiles");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        "ExternalTsFileAggregationScanNode cannot be serialized because it reads local external TsFiles");
  }

  public static ExternalTsFileAggregationScanNode deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "ExternalTsFileAggregationScanNode cannot be deserialized because it reads local external TsFiles");
  }

  @Override
  public String toString() {
    return "ExternalTsFileAggregationScanNode-" + this.getPlanNodeId();
  }
}
