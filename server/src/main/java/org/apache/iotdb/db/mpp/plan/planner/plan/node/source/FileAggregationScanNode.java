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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode.LAST_QUERY_HEADER_COLUMNS;

public class FileAggregationScanNode extends SeriesSourceNode {

  private final PartialPath pathPattern;

  private AggregationDescriptor aggregationDescriptor;

  private final int[] levels;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public FileAggregationScanNode(
      PlanNodeId id,
      PartialPath pathPattern,
      AggregationDescriptor aggregationDescriptor,
      int[] levels) {
    super(id);
    this.pathPattern = pathPattern;
    this.aggregationDescriptor = aggregationDescriptor;
    this.levels = levels;
  }

  public FileAggregationScanNode(
      PlanNodeId id,
      PartialPath pathPattern,
      AggregationDescriptor aggregationDescriptor,
      int[] levels,
      TRegionReplicaSet regionReplicaSet) {
    this(id, pathPattern, aggregationDescriptor, levels);
    this.regionReplicaSet = regionReplicaSet;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public AggregationDescriptor getAggregationDescriptor() {
    return aggregationDescriptor;
  }

  public void setAggregationDescriptor(AggregationDescriptor aggregationDescriptor) {
    this.aggregationDescriptor = aggregationDescriptor;
  }

  public int[] getLevels() {
    return levels;
  }

  @Override
  public PartialPath getPartitionPath() {
    return null;
  }

  @Override
  public Filter getPartitionTimeFilter() {
    return null;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public void close() throws Exception {}

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for FileAggregationScanNode");
  }

  @Override
  public PlanNode clone() {
    return new FileAggregationScanNode(
        getPlanNodeId(),
        getPathPattern(),
        getAggregationDescriptor(),
        getLevels(),
        getRegionReplicaSet());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  protected void serializeAttributes(ByteBuffer buffer) {
    PlanNodeType.FILE_AGGREGATION.serialize(buffer);
    pathPattern.serialize(buffer);
    aggregationDescriptor.serialize(buffer);
    ReadWriteIOUtils.write(levels.length, buffer);
    for (int level : levels) {
      ReadWriteIOUtils.write(level, buffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FILE_AGGREGATION.serialize(stream);
    pathPattern.serialize(stream);
    aggregationDescriptor.serialize(stream);
    ReadWriteIOUtils.write(levels.length, stream);
    for (int level : levels) {
      ReadWriteIOUtils.write(level, stream);
    }
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    PartialPath pathPattern = PartialPath.deserialize(buffer);
    AggregationDescriptor aggregationDescriptor = AggregationDescriptor.deserialize(buffer);
    int levelsSize = ReadWriteIOUtils.readInt(buffer);
    int[] levels = new int[levelsSize];
    for (int i = 0; i < levelsSize; i++) {
      levels[i] = ReadWriteIOUtils.readInt(buffer);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new FileAggregationScanNode(planNodeId, pathPattern, aggregationDescriptor, levels);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFileAggregationScan(this, context);
  }
}
