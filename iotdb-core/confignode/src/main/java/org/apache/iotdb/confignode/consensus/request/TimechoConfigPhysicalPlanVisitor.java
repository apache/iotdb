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

package org.apache.iotdb.confignode.consensus.request;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreAlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RollbackCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;

public class TimechoConfigPhysicalPlanVisitor<R, C> {
  private final ConfigPhysicalPlanVisitor<R, C> apacheVisitor;

  public TimechoConfigPhysicalPlanVisitor(final ConfigPhysicalPlanVisitor<R, C> apacheVisitor) {
    this.apacheVisitor = apacheVisitor;
  }

  public R process(final ConfigPhysicalPlan plan, final C context) {
    switch (plan.getType()) {
      case RollbackCreateWritableView:
        return visitRollbackCreateWritableView((RollbackCreateWritableViewPlan) plan, context);
      case CommitCreateWritableView:
        return visitCommitCreateWritableView((CommitCreateWritableViewPlan) plan, context);
      case AddWritableViewColumn:
        return visitAddWritableViewColumn((AddWritableViewColumnPlan) plan, context);
      case CommitDeleteWritableViewColumn:
        return visitCommitDeleteWritableViewColumn(
            (CommitDeleteWritableViewColumnPlan) plan, context);
      case CommitDeleteWritableView:
        return visitCommitDeleteWritableView((CommitDeleteWritableViewPlan) plan, context);
      case RenameWritableView:
        return visitRenameWritableView((RenameWritableViewPlan) plan, context);
      case SetWritableViewProperties:
        return visitSetWritableViewProperties((SetWritableViewPropertiesPlan) plan, context);
      case PreDeleteWritableViewColumn:
        return visitPreDeleteWritableViewColumn((PreDeleteWritableViewColumnPlan) plan, context);
      case PreDeleteWritableView:
        return visitPreDeleteWritableView((PreDeleteWritableViewPlan) plan, context);
      case RenameWritableViewColumn:
        return visitRenameWritableViewColumn((RenameWritableViewColumnPlan) plan, context);
      case PreAlterWritableViewColumnDataType:
        return visitPreAlterWritableViewColumnDataType(
            (PreAlterWritableViewColumnDataTypePlan) plan, context);
      case AlterWritableViewColumnDataType:
        return visitAlterWritableViewColumnDataType(
            (AlterWritableViewColumnDataTypePlan) plan, context);
      case SetWritableViewColumnComment:
        return visitSetWritableViewColumnComment((SetWritableViewColumnCommentPlan) plan, context);
      case SetWritableViewComment:
        return visitSetWritableViewComment((SetWritableViewCommentPlan) plan, context);
      default:
        return visitPlan(plan, context);
    }
  }

  public R visitPlan(final ConfigPhysicalPlan plan, final C context) {
    throw new UnsupportedOperationException(
        String.format(
            "Timecho config physical plan is not supported in Apache IoTDB: %s",
            plan.getPlanTypeId()));
  }

  public R visitRollbackCreateWritableView(
      final RollbackCreateWritableViewPlan rollbackCreateWritableViewPlan, final C context) {
    return apacheVisitor.visitPlan(rollbackCreateWritableViewPlan, context);
  }

  public R visitCommitCreateWritableView(
      final CommitCreateWritableViewPlan commitCreateWritableViewPlan, final C context) {
    return apacheVisitor.visitPlan(commitCreateWritableViewPlan, context);
  }

  public R visitAddWritableViewColumn(
      final AddWritableViewColumnPlan addWritableViewColumnPlan, final C context) {
    return apacheVisitor.visitAddTableColumn(addWritableViewColumnPlan, context);
  }

  public R visitCommitDeleteWritableViewColumn(
      final CommitDeleteWritableViewColumnPlan commitDeleteWritableViewColumnPlan,
      final C context) {
    return apacheVisitor.visitCommitDeleteColumn(commitDeleteWritableViewColumnPlan, context);
  }

  public R visitCommitDeleteWritableView(
      final CommitDeleteWritableViewPlan commitDeleteWritableViewPlan, final C context) {
    return apacheVisitor.visitCommitDeleteTable(commitDeleteWritableViewPlan, context);
  }

  public R visitRenameWritableView(
      final RenameWritableViewPlan renameWritableViewPlan, final C context) {
    return apacheVisitor.visitRenameTable(renameWritableViewPlan, context);
  }

  public R visitSetWritableViewProperties(
      final SetWritableViewPropertiesPlan setWritableViewPropertiesPlan, final C context) {
    return apacheVisitor.visitSetTableProperties(setWritableViewPropertiesPlan, context);
  }

  public R visitPreDeleteWritableViewColumn(
      final PreDeleteWritableViewColumnPlan preDeleteWritableViewColumnPlan, final C context) {
    return apacheVisitor.visitPlan(preDeleteWritableViewColumnPlan, context);
  }

  public R visitPreDeleteWritableView(
      final PreDeleteWritableViewPlan preDeleteWritableViewPlan, final C context) {
    return apacheVisitor.visitPlan(preDeleteWritableViewPlan, context);
  }

  public R visitRenameWritableViewColumn(
      final RenameWritableViewColumnPlan renameWritableViewColumnPlan, final C context) {
    return apacheVisitor.visitRenameTableColumn(renameWritableViewColumnPlan, context);
  }

  public R visitPreAlterWritableViewColumnDataType(
      final PreAlterWritableViewColumnDataTypePlan preAlterWritableViewColumnDataTypePlan,
      final C context) {
    return apacheVisitor.visitPlan(preAlterWritableViewColumnDataTypePlan, context);
  }

  public R visitAlterWritableViewColumnDataType(
      final AlterWritableViewColumnDataTypePlan alterWritableViewColumnDataTypePlan,
      final C context) {
    return apacheVisitor.visitAlterColumnDataType(alterWritableViewColumnDataTypePlan, context);
  }

  public R visitSetWritableViewColumnComment(
      final SetWritableViewColumnCommentPlan setWritableViewColumnCommentPlan, final C context) {
    return apacheVisitor.visitSetTableColumnComment(setWritableViewColumnCommentPlan, context);
  }

  public R visitSetWritableViewComment(
      final SetWritableViewCommentPlan setWritableViewCommentPlan, final C context) {
    return apacheVisitor.visitSetTableComment(setWritableViewCommentPlan, context);
  }
}
