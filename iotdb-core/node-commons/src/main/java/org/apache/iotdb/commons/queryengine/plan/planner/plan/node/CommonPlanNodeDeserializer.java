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

package org.apache.iotdb.commons.queryengine.plan.planner.plan.node;

import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CommonPlanNodeDeserializer implements IPlanNodeDeserializer {
  public static final CommonPlanNodeDeserializer INSTANCE = new CommonPlanNodeDeserializer();

  @Override
  public PlanNode deserializeFromWAL(DataInputStream stream) throws IOException {
    throw new UnsupportedOperationException("Not supported for CommonPlanNodeDeserializer");
  }

  @Override
  public PlanNode deserializeFromWAL(ByteBuffer buffer) {
    throw new UnsupportedOperationException("Not supported for CommonPlanNodeDeserializer");
  }

  @Override
  public PlanNode deserialize(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
    return deserialize(buffer, nodeType);
  }

  @Override
  public PlanNode deserialize(ByteBuffer buffer, short nodeType) {
    switch (nodeType) {
      case 1001:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode
            .deserialize(buffer);
      case 1002:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode
            .deserialize(buffer);
      case 1003:
        return OutputNode.deserialize(buffer);
      case 1004:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode
            .deserialize(buffer);
      case 1005:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode
            .deserialize(buffer);
      case 1006:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode
            .deserialize(buffer);
      case 1007:
        return MergeSortNode.deserialize(buffer);
      case 1008:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode
            .deserialize(buffer);
      case 1009:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode
            .deserialize(buffer);
      case 1010:
        return StreamSortNode.deserialize(buffer);
      case 1011:
        return JoinNode.deserialize(buffer);
      case 1012:
        return PreviousFillNode.deserialize(buffer);
      case 1013:
        return LinearFillNode.deserialize(buffer);
      case 1014:
        return ValueFillNode.deserialize(buffer);
      case 1015:
        return org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode
            .deserialize(buffer);
      case 1017:
        return GapFillNode.deserialize(buffer);
      case 1020:
        return EnforceSingleRowNode.deserialize(buffer);
      case 1025:
        return SemiJoinNode.deserialize(buffer);
      case 1026:
        return MarkDistinctNode.deserialize(buffer);
      case 1027:
        return AssignUniqueId.deserialize(buffer);
      case 1028:
        return TableFunctionNode.deserialize(buffer);
      case 1029:
        return TableFunctionProcessorNode.deserialize(buffer);
      case 1030:
        return GroupNode.deserialize(buffer);
      case 1031:
        return PatternRecognitionNode.deserialize(buffer);
      case 1032:
        return WindowNode.deserialize(buffer);
      case 1034:
        return UnionNode.deserialize(buffer);
      case 1035:
        return IntersectNode.deserialize(buffer);
      case 1036:
        return ExceptNode.deserialize(buffer);
      case 1037:
        return TopKRankingNode.deserialize(buffer);
      case 1038:
        return RowNumberNode.deserialize(buffer);
      case 1039:
        return ValuesNode.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }
}
