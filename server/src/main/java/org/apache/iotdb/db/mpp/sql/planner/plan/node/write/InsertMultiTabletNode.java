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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.List;

public class InsertMultiTabletNode extends InsertNode {

  protected InsertMultiTabletNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static InsertMultiTabletNode deserialize(ByteBuffer byteBuffer) {
    InsertMultiTabletNode tempNode = new InsertMultiTabletNode(null);
    deserializeAttributes(tempNode, byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    InsertMultiTabletNode ansNode = new InsertMultiTabletNode(planNodeId);
    copyAttributes(ansNode, tempNode);
    tempNode = null;
    return ansNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_MULTI_TABLET.serialize(byteBuffer);
    super.serializeAttributes(byteBuffer);
  }
}
