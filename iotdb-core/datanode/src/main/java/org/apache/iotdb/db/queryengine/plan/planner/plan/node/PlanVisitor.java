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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.db.node_commons.plan.planner.plan.node.IQueryPlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;

@SuppressWarnings("java:S6539") // suppress "Monster class" warning
public abstract class PlanVisitor<R, C> implements IQueryPlanVisitor<R, C> {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Write Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitInsertRow(InsertRowNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRelationalInsertRow(RelationalInsertRowNode node, C context) {
    return visitInsertRow(node, context);
  }

  public R visitRelationalInsertRows(RelationalInsertRowsNode node, C context) {
    return visitInsertRows(node, context);
  }

  public R visitInsertTablet(InsertTabletNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRelationalInsertTablet(RelationalInsertTabletNode node, C context) {
    return visitInsertTablet(node, context);
  }

  public R visitInsertRows(InsertRowsNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertMultiTablets(InsertMultiTabletsNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitInsertRowsOfOneDevice(InsertRowsOfOneDeviceNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteData(DeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeleteData(RelationalDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitWriteObjectFile(ObjectNode node, C context) {
    return visitPlan(node, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Pipe Related Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitPipeEnrichedInsertNode(PipeEnrichedInsertNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedDeleteDataNode(PipeEnrichedDeleteDataNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedWritePlanNode(PipeEnrichedWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeEnrichedNonWritePlanNode(PipeEnrichedNonWritePlanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitPipeOperateSchemaQueueNode(PipeOperateSchemaQueueNode node, C context) {
    return visitPlan(node, context);
  }
}
