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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.NotImplementedException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LoadTsFileNode extends WritePlanNode {

  private final List<TsFileResource> resources;

  public LoadTsFileNode(PlanNodeId id, List<TsFileResource> resources) {
    super(id);
    this.resources = resources;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of load TsFile is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // Do nothing
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // Do nothing
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    List<WritePlanNode> res = new ArrayList<>();
    LoadTsFileStatement statement =
        ((Analysis) analysis).getTreeStatement() instanceof PipeEnrichedStatement
            ? (LoadTsFileStatement)
                ((PipeEnrichedStatement) ((Analysis) analysis).getTreeStatement())
                    .getInnerStatement()
            : (LoadTsFileStatement) ((Analysis) analysis).getTreeStatement();

    for (int i = 0; i < resources.size(); i++) {
      res.add(
          new LoadSingleTsFileNode(
              getPlanNodeId(),
              resources.get(i),
              statement.isDeleteAfterLoad(),
              statement.getWritePointCount(i)));
    }
    return res;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LoadTsFileNode loadTsFileNode = (LoadTsFileNode) o;
    return Objects.equals(resources, loadTsFileNode.resources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resources);
  }
}
