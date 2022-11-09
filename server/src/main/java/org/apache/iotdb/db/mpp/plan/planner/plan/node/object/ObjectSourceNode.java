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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.object;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;

import java.util.Collections;
import java.util.List;

public abstract class ObjectSourceNode extends SourceNode {

  private TRegionReplicaSet regionReplicaSet;

  public ObjectSourceNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public final List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public final void addChild(PlanNode child) {}

  @Override
  public final int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public final List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public final void open() throws Exception {}

  @Override
  public final void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public final void close() throws Exception {}

  @Override
  public final TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }
}
