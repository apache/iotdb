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
package org.apache.iotdb.db.mpp.plan.node.process;

import org.apache.iotdb.db.mpp.common.OrderBy;
import org.apache.iotdb.db.mpp.common.TsBlock;
import org.apache.iotdb.db.mpp.common.WithoutPolicy;
import org.apache.iotdb.db.mpp.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.node.PlanNodeId;

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
  private WithoutPolicy withoutPolicy;

  // The map from deviceName to corresponding query result node responsible for that device.
  // DeviceNode means the node whose output TsBlock contains the data belonged to one device.
  private Map<String, PlanNode<TsBlock>> childDeviceNodeMap;

  public DeviceMergeNode(PlanNodeId id) {
    super(id);
  }

  public DeviceMergeNode(PlanNodeId id, Map<String, PlanNode<TsBlock>> deviceNodeMap) {
    this(id);
    this.childDeviceNodeMap = deviceNodeMap;
    this.children.addAll(deviceNodeMap.values());
  }

  public void addChildDeviceNode(String deviceName, PlanNode<TsBlock> childNode) {
    this.childDeviceNodeMap.put(deviceName, childNode);
    this.children.add(childNode);
  }
}
