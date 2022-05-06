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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/** This class defines the scan task of schema fetcher. */
public class SchemaFetchScanNode extends SourceNode {

  private final PartialPath storageGroup;
  private final PathPatternTree patternTree;

  private TRegionReplicaSet schemaRegionReplicaSet;

  public SchemaFetchScanNode(PlanNodeId id, PartialPath storageGroup, PathPatternTree patternTree) {
    super(id);
    this.storageGroup = storageGroup;
    this.patternTree = patternTree;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new SchemaFetchScanNode(getPlanNodeId(), storageGroup, patternTree);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SCHEMA_FETCH_SCAN.serialize(byteBuffer);
    storageGroup.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
  }

  public static SchemaFetchScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath storageGroup = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    PathPatternTree patternTree = PathPatternTree.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SchemaFetchScanNode(planNodeId, storageGroup, patternTree);
  }

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return schemaRegionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet schemaRegionReplicaSet) {
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaFetchScan(this, context);
  }
}
