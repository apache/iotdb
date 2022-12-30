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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class ShowQueriesNode extends VirtualSourceNode {

  public static final List<String> SHOW_QUERIES_HEADER_COLUMNS =
      ImmutableList.of(
          ColumnHeaderConstant.QUERY_ID,
          ColumnHeaderConstant.DATA_NODE_ID,
          ColumnHeaderConstant.ELAPSED_TIME,
          ColumnHeaderConstant.STATEMENT);

  public ShowQueriesNode(PlanNodeId id, TDataNodeLocation dataNodeLocation) {
    super(id, dataNodeLocation);
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for ShowQueriesNode");
  }

  @Override
  public PlanNode clone() {
    return new ShowQueriesNode(getPlanNodeId(), getDataNodeLocation());
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return SHOW_QUERIES_HEADER_COLUMNS;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitShowQueries(this, context);
  }

  // We only use DataNodeLocation when do distributionPlan, so DataNodeLocation is no need to
  // serialize
  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SHOW_QUERIES.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SHOW_QUERIES.serialize(stream);
  }

  public static ShowQueriesNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ShowQueriesNode(planNodeId, null);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  @Override
  public String toString() {
    return "ShowQueriesNode-" + this.getPlanNodeId();
  }
}
