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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class AlignedLastQueryScanNode extends SeriesSourceNode {
  // The path of the target series which will be scanned.
  private final AlignedPath seriesPath;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public AlignedLastQueryScanNode(PlanNodeId id, AlignedPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public AlignedLastQueryScanNode(
      PlanNodeId id, AlignedPath seriesPath, TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.seriesPath = seriesPath;
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for SeriesScanNode");
  }

  @Override
  public PlanNode clone() {
    return new AlignedLastQueryScanNode(getPlanNodeId(), seriesPath, regionReplicaSet);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAlignedLastQueryScan(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    AlignedLastQueryScanNode that = (AlignedLastQueryScanNode) o;
    return Objects.equals(seriesPath, that.seriesPath)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), seriesPath, regionReplicaSet);
  }

  @Override
  public String toString() {
    return String.format(
        "AlignedLastQueryScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getSeriesPath().getFormattedString(),
        PlanNodeUtil.printRegionReplicaSet(this.getRegionReplicaSet()));
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALIGNED_LAST_QUERY_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ALIGNED_LAST_QUERY_SCAN.serialize(stream);
    seriesPath.serialize(stream);
  }

  public static AlignedLastQueryScanNode deserialize(ByteBuffer byteBuffer) {
    AlignedPath partialPath = (AlignedPath) PathDeserializeUtil.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlignedLastQueryScanNode(planNodeId, partialPath);
  }

  public AlignedPath getSeriesPath() {
    return seriesPath;
  }

  @Override
  public PartialPath getPartitionPath() {
    return seriesPath;
  }

  @Override
  public Filter getPartitionTimeFilter() {
    return null;
  }
}
