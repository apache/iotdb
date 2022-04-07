/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FillPolicy;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** FillNode is used to fill the empty field in one row. */
public class FillNode extends ProcessNode {

  private PlanNode child;

  // The policy to discard the result from upstream node
  private FillPolicy fillPolicy;

  public FillNode(PlanNodeId id) {
    super(id);
  }

  public FillNode(PlanNodeId id, FillPolicy policy) {
    this(id);
    this.fillPolicy = policy;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNode clone() {
    return new FillNode(getId(), fillPolicy);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  public FillPolicy getFillPolicy() {
    return fillPolicy;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILL.serialize(byteBuffer);
    ReadWriteIOUtils.write(fillPolicy.ordinal(), byteBuffer);
  }

  public static FillNode deserialize(ByteBuffer byteBuffer) {
    int fillIndex = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FillNode(planNodeId, FillPolicy.values()[fillIndex]);
  }

  public FillNode(PlanNodeId id, PlanNode child, FillPolicy fillPolicy) {
    this(id);
    this.child = child;
    this.fillPolicy = fillPolicy;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[FillNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("FillPolicy: " + this.getFillPolicy());
    return new Pair<>(title, attributes);
  }
}
