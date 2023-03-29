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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.sink;

import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class MultiChildrenSinkNode extends SinkNode {

  protected List<PlanNode> children;

  protected final List<DownStreamChannelLocation> downStreamChannelLocationList;

  public MultiChildrenSinkNode(PlanNodeId id) {
    super(id);
    this.children = new ArrayList<>();
    this.downStreamChannelLocationList = new ArrayList<>();
  }

  protected MultiChildrenSinkNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id);
    this.children = children;
    this.downStreamChannelLocationList = downStreamChannelLocationList;
  }

  protected MultiChildrenSinkNode(
      PlanNodeId id, List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id);
    this.children = new ArrayList<>();
    this.downStreamChannelLocationList = downStreamChannelLocationList;
  }

  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Override
  public abstract PlanNode clone();

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  public void addChildren(List<PlanNode> children) {
    this.children.addAll(children);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MultiChildrenSinkNode that = (MultiChildrenSinkNode) o;
    return children.equals(that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), children);
  }

  @Override
  public void send() {}

  @Override
  public void close() throws Exception {}

  public List<DownStreamChannelLocation> getDownStreamChannelLocationList() {
    return downStreamChannelLocationList;
  }

  public void addDownStreamChannelLocation(DownStreamChannelLocation downStreamChannelLocation) {
    downStreamChannelLocationList.add(downStreamChannelLocation);
  }

  public int getCurrentLastIndex() {
    return children.size() - 1;
  }
}
