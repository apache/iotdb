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

import java.util.Arrays;

/**
 * TimeJoinOperator is responsible for join two or more TsBlock. The join algorithm is like outer
 * join by timestamp column. It will join two or more TsBlock by Timestamp column. The output result
 * of TimeJoinOperator is sorted by timestamp
 */
// TODO: define the TimeJoinMergeNode for distributed plan
public class TimeJoinNode extends ProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private OrderBy mergeOrder;

  // The policy to decide whether a row should be discarded
  // The without policy is able to be push down to the TimeJoinOperator because we can know whether
  // a row contains
  // null or not.
  private WithoutPolicy withoutPolicy;

  public TimeJoinNode(PlanNodeId id) {
    super(id);
    this.mergeOrder = OrderBy.TIMESTAMP_ASC;
  }

  public TimeJoinNode(PlanNodeId id, PlanNode<TsBlock>... children) {
    super(id);
    this.children.addAll(Arrays.asList(children));
  }

  public void addChild(PlanNode<TsBlock> child) {
    this.children.add(child);
  }

  public void setMergeOrder(OrderBy mergeOrder) {
    this.mergeOrder = mergeOrder;
  }

  public void setWithoutPolicy(WithoutPolicy withoutPolicy) {
    this.withoutPolicy = withoutPolicy;
  }
}
