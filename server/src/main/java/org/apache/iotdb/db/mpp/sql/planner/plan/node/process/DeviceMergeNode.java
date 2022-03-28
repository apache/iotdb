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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.utils.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DeviceMergeOperator is responsible for constructing a device-based view of a set of series. And
 * output the result with specific order. The order could be 'order by device' or 'order by
 * timestamp'
 *
 * <p>Each output from its children should have the same schema. That means, the columns should be
 * same between these TsBlocks. If the input TsBlock contains n columns, the device-based view will
 * contain n+1 columns where the new column is Device column.
 */
public class DeviceMergeNode extends ProcessNode {
  // The result output order that this operator
  private OrderBy mergeOrder;

  // The policy to decide whether a row should be discarded
  // The without policy is able to be push down to the DeviceMergeNode because we can know whether a
  // row contains
  // null or not.
  private FilterNullPolicy filterNullPolicy;

  // The map from deviceName to corresponding query result node responsible for that device.
  // DeviceNode means the node whose output TsBlock contains the data belonged to one device.
  private Map<String, PlanNode> childDeviceNodeMap;

  private List<PlanNode> children;

  private List<String> columnNames;

  public DeviceMergeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChildren(PlanNode child) {}

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
    return columnNames;
  }

  public OrderBy getMergeOrder() {
    return mergeOrder;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceMerge(this, context);
  }

  public static DeviceMergeNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public DeviceMergeNode(PlanNodeId id, Map<String, PlanNode> deviceNodeMap) {
    this(id);
    this.childDeviceNodeMap = deviceNodeMap;
    this.children.addAll(deviceNodeMap.values());
  }

  public void addChildDeviceNode(String deviceName, PlanNode childNode) {
    this.childDeviceNodeMap.put(deviceName, childNode);
    this.children.add(childNode);
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[DeviceMergeNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add(
        "MergeOrder: " + (this.getMergeOrder() == null ? "null" : this.getMergeOrder().toString()));
    return new Pair<>(title, attributes);
  }
}
