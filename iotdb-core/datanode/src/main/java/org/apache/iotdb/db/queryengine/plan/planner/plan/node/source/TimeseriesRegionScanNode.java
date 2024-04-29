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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.TimeseriesSchemaInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TimeseriesRegionScanNode extends RegionScanNode {
  private Map<PartialPath, TimeseriesSchemaInfo> timeseriesToSchemaInfo;

  public TimeseriesRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, TimeseriesSchemaInfo> timeseriesToSchemaInfo,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.timeseriesToSchemaInfo = timeseriesToSchemaInfo;
    this.regionReplicaSet = regionReplicaSet;
  }

  public Map<PartialPath, TimeseriesSchemaInfo> getTimeseriesToSchemaInfo() {
    return timeseriesToSchemaInfo;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("TimeseriesRegionScanNode does not support addChild");
  }

  @Override
  public PlanNode clone() {
    return new TimeseriesRegionScanNode(
        getPlanNodeId(), getTimeseriesToSchemaInfo(), getRegionReplicaSet());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputCount
        ? ColumnHeaderConstant.countTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList())
        : ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTimeSeriesRegionScan(this, context);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    Map<PartialPath, TimeseriesSchemaInfo> timeseriesToSchemaInfo = new HashMap<>();
    for (int i = 0; i < size; i++) {
      PartialPath path = PartialPath.deserialize(buffer);
      TimeseriesSchemaInfo timeseriesSchemaInfo = TimeseriesSchemaInfo.deserialize(buffer);
      timeseriesToSchemaInfo.put(path, timeseriesSchemaInfo);
    }
    return new TimeseriesRegionScanNode(null, timeseriesToSchemaInfo, null);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(timeseriesToSchemaInfo.size(), byteBuffer);
    for (Map.Entry<PartialPath, TimeseriesSchemaInfo> entry : timeseriesToSchemaInfo.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      entry.getValue().serializeAttributes(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(stream);
    ReadWriteIOUtils.write(timeseriesToSchemaInfo.size(), stream);
    for (Map.Entry<PartialPath, TimeseriesSchemaInfo> entry : timeseriesToSchemaInfo.entrySet()) {
      entry.getKey().serialize(stream);
      entry.getValue().serializeAttributes(stream);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "TimeseriesRegionScanNode-%s:[DataRegion: %s OutputCount: %s]",
        PlanNodeType.TIMESERIES_REGION_SCAN, timeseriesToSchemaInfo, outputCount);
  }

  @Override
  public List<PartialPath> getDevicePaths() {
    return timeseriesToSchemaInfo.keySet().stream()
        .map(PartialPath::getDevicePath)
        .collect(Collectors.toList());
  }

  @Override
  public void setDevicePaths(Set<PartialPath> paths) {
    Map<PartialPath, TimeseriesSchemaInfo> curTimeseriesToSchemaInfo = new HashMap<>();
    for (Map.Entry<PartialPath, TimeseriesSchemaInfo> entry : timeseriesToSchemaInfo.entrySet()) {
      if (paths.contains(entry.getKey().getDevicePath())) {
        curTimeseriesToSchemaInfo.put(entry.getKey(), entry.getValue());
      }
    }
    this.timeseriesToSchemaInfo = curTimeseriesToSchemaInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeseriesRegionScanNode)) {
      return false;
    }
    TimeseriesRegionScanNode that = (TimeseriesRegionScanNode) o;
    return timeseriesToSchemaInfo.equals(that.timeseriesToSchemaInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeseriesToSchemaInfo);
  }
}
