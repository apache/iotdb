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

import org.apache.iotdb.db.mpp.common.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** WithoutNode is used to discard specific rows from upstream node. */
public class FilterNullNode extends ProcessNode {

  // The policy to discard the result from upstream operator
  private FilterNullPolicy discardPolicy;

  private PlanNode child;

  private List<String> filterNullColumnNames;

  public FilterNullNode(PlanNodeId id, PlanNode child) {
    super(id);
    this.child = child;
  }

  public FilterNullNode(PlanNodeId id, PlanNode child, List<String> filterNullColumnNames) {
    super(id);
    this.child = child;
    this.filterNullColumnNames = filterNullColumnNames;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilterNull(this, context);
  }

  public void setFilterNullColumnNames(List<String> filterNullColumnNames) {
    this.filterNullColumnNames = filterNullColumnNames;
  }
}
