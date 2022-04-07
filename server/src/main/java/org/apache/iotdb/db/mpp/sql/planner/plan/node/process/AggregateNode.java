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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This node is used to aggregate required series from multiple sources. The source data will be
 * input as a TsBlock, it may be raw data or partial aggregation result. This node will output the
 * final series aggregated result represented by TsBlock.
 */
public class AggregateNode extends ProcessNode {
  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  private GroupByTimeParameter groupByTimeParameter;

  // The list of aggregation functions, each FunctionExpression will be output as one column of
  // result TsBlock
  // (Currently we only support one series in the aggregation function)
  // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
  private Map<String, FunctionExpression> aggregateFuncMap;

  private final List<PlanNode> children;
  private final List<String> columnNames;

  public AggregateNode(
      PlanNodeId id,
      Map<String, FunctionExpression> aggregateFuncMap,
      List<PlanNode> children,
      List<String> columnNames) {
    super(id);
    this.aggregateFuncMap = aggregateFuncMap;
    this.children = children;
    this.columnNames = columnNames;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of AggregateNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRowBasedSeriesAggregate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGGREGATE.serialize(byteBuffer);
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByTimeParameter.serialize(byteBuffer);
    }
    // TODO serialize aggregateFuncMap，because it is unsure
    ReadWriteIOUtils.write(columnNames.size(), byteBuffer);
    for (String columnName : columnNames) {
      ReadWriteIOUtils.write(columnName, byteBuffer);
    }
  }

  public static AggregateNode deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (type == 1) {
      groupByTimeParameter = new GroupByTimeParameter();
      try {
        groupByTimeParameter.deserialize(byteBuffer);
      } catch (IllegalPathException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // TODO deserialize aggregateFuncMap， because it is unsure
    // Map<String, FunctionExpression> aggregateFuncMap = new HashMap<>();
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> columnNames = new ArrayList<>(columnSize);
    for (int i = 0; i < columnSize; i++) {
      columnNames.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    AggregateNode aggregateNode = new AggregateNode(planNodeId, new HashMap<>(), new ArrayList<>(), columnNames);
    aggregateNode.groupByTimeParameter = groupByTimeParameter;
    return aggregateNode;
  }
}
