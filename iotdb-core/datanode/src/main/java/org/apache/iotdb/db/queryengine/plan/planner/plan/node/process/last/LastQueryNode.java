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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class LastQueryNode extends MultiChildProcessNode {

  // the ordering of timeseries in the result of last query
  // which is set to null if there is no need to sort
  private Ordering timeseriesOrdering;

  // if children contains LastTransformNode, this variable is only used in distribute plan
  private boolean containsLastTransformNode;

  // After Logical planning is completed, this map is no longer needed and it will be set to null
  private Map<IMeasurementSchema, Integer> measurementSchema2IdxMap;
  // All LastSeriesSourceNode share this structure
  private final List<IMeasurementSchema> globalMeasurementSchemaList;

  public LastQueryNode(
      PlanNodeId id, @Nullable Ordering timeseriesOrdering, boolean containsLastTransformNode) {
    super(id);
    this.timeseriesOrdering = timeseriesOrdering;
    this.containsLastTransformNode = containsLastTransformNode;
    this.measurementSchema2IdxMap = new HashMap<>();
    this.globalMeasurementSchemaList = new ArrayList<>();
  }

  public LastQueryNode(
      PlanNodeId id,
      @Nullable Ordering timeseriesOrdering,
      boolean containsLastTransformNode,
      List<IMeasurementSchema> globalMeasurementSchemaList) {
    super(id);
    this.timeseriesOrdering = timeseriesOrdering;
    this.containsLastTransformNode = containsLastTransformNode;
    this.globalMeasurementSchemaList = globalMeasurementSchemaList;
  }

  public long addDeviceLastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<IMeasurementSchema> measurementSchemas,
      String outputViewPath,
      TSDataType outputViewPathType) {
    List<Integer> idxList = new ArrayList<>(measurementSchemas.size());
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      int idx =
          measurementSchema2IdxMap.computeIfAbsent(
              measurementSchema,
              key -> {
                this.globalMeasurementSchemaList.add(key);
                return globalMeasurementSchemaList.size() - 1;
              });
      idxList.add(idx);
    }
    LastQueryScanNode scanNode =
        new LastQueryScanNode(
            id,
            devicePath,
            aligned,
            idxList,
            outputViewPath,
            outputViewPathType,
            globalMeasurementSchemaList);
    children.add(scanNode);
    return scanNode.ramBytesUsed();
  }

  public void sort() {
    if (timeseriesOrdering == null) {
      return;
    }
    children.sort(
        Comparator.comparing(
            child -> {
              String sortKey = "";
              if (child instanceof LastQueryScanNode) {
                sortKey = ((LastQueryScanNode) child).getOutputSymbolForSort();
              } else if (child instanceof LastQueryTransformNode) {
                sortKey = ((LastQueryTransformNode) child).getOutputSymbolForSort();
              }
              return sortKey;
            }));
    if (timeseriesOrdering.equals(Ordering.DESC)) {
      Collections.reverse(children);
    }
  }

  public void clearMeasurementSchema2IdxMap() {
    this.measurementSchema2IdxMap = null;
  }

  public long getMemorySizeOfSharedStructures() {
    // MeasurementSchema comes from path, memory has been calculated before
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.shallowSizeOf(globalMeasurementSchemaList)
            + RamUsageEstimator.sizeOfObjectArray(globalMeasurementSchemaList.size()));
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LAST_QUERY;
  }

  @Override
  public PlanNode clone() {
    return new LastQueryNode(
        getPlanNodeId(),
        timeseriesOrdering,
        containsLastTransformNode,
        globalMeasurementSchemaList);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  public String toString() {
    return String.format("LastQueryNode-%s", this.getPlanNodeId());
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
    LastQueryNode that = (LastQueryNode) o;
    if (timeseriesOrdering == null) {
      return that.timeseriesOrdering == null;
    }
    return timeseriesOrdering.equals(that.timeseriesOrdering);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeseriesOrdering);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLastQuery(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY.serialize(byteBuffer);
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), byteBuffer);
    }
    ReadWriteIOUtils.write(globalMeasurementSchemaList.size(), byteBuffer);
    for (IMeasurementSchema measurementSchema : globalMeasurementSchemaList) {
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, byteBuffer);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, byteBuffer);
      }
      measurementSchema.serializeTo(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY.serialize(stream);
    if (timeseriesOrdering == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(timeseriesOrdering.ordinal(), stream);
    }
    ReadWriteIOUtils.write(globalMeasurementSchemaList.size(), stream);
    for (IMeasurementSchema measurementSchema : globalMeasurementSchemaList) {
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, stream);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, stream);
      }
      measurementSchema.serializeTo(stream);
    }
  }

  public static LastQueryNode deserialize(ByteBuffer byteBuffer) {
    byte needOrderByTimeseries = ReadWriteIOUtils.readByte(byteBuffer);
    Ordering timeseriesOrdering = null;
    if (needOrderByTimeseries == 1) {
      timeseriesOrdering = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    }
    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(measurementSize);
    for (int i = 0; i < measurementSize; i++) {
      byte type = ReadWriteIOUtils.readByte(byteBuffer);
      if (type == 0) {
        measurementSchemas.add(MeasurementSchema.deserializeFrom(byteBuffer));
      } else if (type == 1) {
        measurementSchemas.add(VectorMeasurementSchema.deserializeFrom(byteBuffer));
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryNode(planNodeId, timeseriesOrdering, false, measurementSchemas);
  }

  @Override
  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Override
  public void addChild(PlanNode child) {
    if (child instanceof LastQueryScanNode) {
      LastQueryScanNode childNode = (LastQueryScanNode) child;
      childNode.setGlobalMeasurementSchemaList(globalMeasurementSchemaList);
    }
    super.addChild(child);
  }

  public Ordering getTimeseriesOrdering() {
    return timeseriesOrdering;
  }

  public void setTimeseriesOrdering(Ordering timeseriesOrdering) {
    this.timeseriesOrdering = timeseriesOrdering;
  }

  public boolean isContainsLastTransformNode() {
    return this.containsLastTransformNode;
  }

  public void setContainsLastTransformNode() {
    this.containsLastTransformNode = true;
  }

  public boolean needOrderByTimeseries() {
    return timeseriesOrdering != null;
  }

  // Before calling this method, you need to ensure that the current LastQueryNode
  // has been divided according to RegionReplicaSet.
  public TRegionReplicaSet getRegionReplicaSetByFirstChild() {
    SourceNode planNode = (SourceNode) children.get(0);
    return planNode.getRegionReplicaSet();
  }
}
