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

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

public class PipeTableStatementToPlanVisitor extends AstVisitor<PlanNode, Void> {
  @Override
  public PlanNode visitNode(final Node node, final Void context) {
    throw new UnsupportedOperationException(
        String.format(
            "PipeStatementToPlanVisitor does not support visiting general statement, Statement: %s",
            node));
  }

  @Override
  public PlanNode visitCreateOrUpdateDevice(final CreateOrUpdateDevice node, final Void context) {
    return new CreateOrUpdateTableDeviceNode(
        new PlanNodeId(""),
        node.getDatabase(),
        node.getTable(),
        node.getDeviceIdList(),
        node.getAttributeNameList(),
        node.getAttributeValueList());
  }
}
