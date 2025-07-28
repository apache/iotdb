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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;

import javax.annotation.Nonnull;

public abstract class AbstractTableSchemaQueryAttributeSecurityVisitor<R>
    extends PlanVisitor<R, ConsensusGroupId> {

  public R processFragment(
      final @Nonnull FragmentInstance instance, final @Nonnull ConsensusGroupId groupId) {
    return instance.getFragment().getPlanNodeTree().accept(this, groupId);
  }

  @Override
  public R visitPlan(final PlanNode node, final ConsensusGroupId context) {
    return null;
  }

  @Override
  public R visitIdentitySink(final IdentitySinkNode node, final ConsensusGroupId context) {
    // TODO: apply multi regions?
    return node.getChildren().get(0).accept(this, context);
  }

  @Override
  public R visitFilter(final FilterNode node, final ConsensusGroupId context) {
    return node.getChild().accept(this, context);
  }

  @Override
  public R visitTableDeviceFetch(final TableDeviceFetchNode node, final ConsensusGroupId context) {
    return visitTableDeviceSourceNode(node, context);
  }

  @Override
  public R visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final ConsensusGroupId context) {
    return visitTableDeviceSourceNode(node, context);
  }

  protected abstract R visitTableDeviceSourceNode(
      final TableDeviceSourceNode node, final ConsensusGroupId context);
}
