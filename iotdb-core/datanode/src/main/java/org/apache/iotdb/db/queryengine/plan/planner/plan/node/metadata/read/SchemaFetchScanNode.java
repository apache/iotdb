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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public abstract class SchemaFetchScanNode extends SourceNode {
  protected final PartialPath storageGroup;
  protected final PathPatternTree patternTree;
  protected TRegionReplicaSet schemaRegionReplicaSet;

  protected SchemaFetchScanNode(
      PlanNodeId id, PartialPath storageGroup, PathPatternTree patternTree) {
    super(id);
    this.storageGroup = storageGroup;
    this.patternTree = patternTree;
    this.patternTree.constructTree();
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
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of();
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
}
