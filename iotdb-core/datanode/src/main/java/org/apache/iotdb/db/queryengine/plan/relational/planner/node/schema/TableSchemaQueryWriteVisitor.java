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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;

public class TableSchemaQueryWriteVisitor extends PlanVisitor<TSStatus, Void> {

  @Override
  public TSStatus visitPlan(final PlanNode node, final Void context) {
    return StatusUtils.OK;
  }

  @Override
  public TSStatus visitFilter(final FilterNode node, final Void context) {
    return node.getChild().accept(this, context);
  }

  @Override
  public TSStatus visitTableDeviceFetch(final TableDeviceFetchNode node, final Void context) {
    return visitTableDeviceSourceNode(node);
  }

  @Override
  public TSStatus visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final Void context) {
    return visitTableDeviceSourceNode(node);
  }

  private TSStatus visitTableDeviceSourceNode(final TableDeviceSourceNode node) {
    return StatusUtils.OK;
  }
}
