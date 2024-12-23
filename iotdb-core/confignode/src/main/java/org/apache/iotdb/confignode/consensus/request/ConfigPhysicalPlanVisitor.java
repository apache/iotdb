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

import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;

public abstract class ConfigPhysicalPlanVisitor<R, C> {
  public R process(ConfigPhysicalPlan plan, C context) {
    switch (plan.getType()) {
      case CreateDatabase:
        return visitCreateDatabase((DatabaseSchemaPlan) plan, context);
      case AlterDatabase:
        return visitAlterDatabase((DatabaseSchemaPlan) plan, context);
      case DeleteDatabase:
        return visitDeleteDatabase((DeleteDatabasePlan) plan, context);
      case CreateSchemaTemplate:
        return visitCreateSchemaTemplate((CreateSchemaTemplatePlan) plan, context);
      case CommitSetSchemaTemplate:
        return visitCommitSetSchemaTemplate((CommitSetSchemaTemplatePlan) plan, context);
      case ExtendSchemaTemplate:
        return visitExtendSchemaTemplate((ExtendSchemaTemplatePlan) plan, context);
      case DropSchemaTemplate:
        return visitDropSchemaTemplate((DropSchemaTemplatePlan) plan, context);
      case PipeUnsetTemplate:
        return visitPipeUnsetSchemaTemplate((PipeUnsetSchemaTemplatePlan) plan, context);
      case PipeDeleteTimeSeries:
        return visitPipeDeleteTimeSeries((PipeDeleteTimeSeriesPlan) plan, context);
      case PipeDeleteLogicalView:
        return visitPipeDeleteLogicalView((PipeDeleteLogicalViewPlan) plan, context);
      case PipeDeactivateTemplate:
        return visitPipeDeactivateTemplate((PipeDeactivateTemplatePlan) plan, context);
      case CreateRole:
        return visitCreateRole((AuthorPlan) plan, context);
      case DropRole:
        return visitDropRole((AuthorPlan) plan, context);
      case GrantRole:
        return visitGrantRole((AuthorPlan) plan, context);
      case RevokeRole:
        return visitRevokeRole((AuthorPlan) plan, context);
      case CreateUser:
        return visitCreateUser((AuthorPlan) plan, context);
      case CreateUserWithRawPassword:
        return visitCreateRawUser((AuthorPlan) plan, context);
      case UpdateUser:
        return visitUpdateUser((AuthorPlan) plan, context);
      case DropUser:
        return visitDropUser((AuthorPlan) plan, context);
      case GrantUser:
        return visitGrantUser((AuthorPlan) plan, context);
      case RevokeUser:
        return visitRevokeUser((AuthorPlan) plan, context);
      case GrantRoleToUser:
        return visitGrantRoleToUser((AuthorPlan) plan, context);
      case RevokeRoleFromUser:
        return visitRevokeRoleFromUser((AuthorPlan) plan, context);
      case SetTTL:
        return visitTTL((SetTTLPlan) plan, context);
      default:
        return visitPlan(plan, context);
    }
  }

  /** Top Level Description */
  public abstract R visitPlan(ConfigPhysicalPlan plan, C context);

  public R visitCreateDatabase(DatabaseSchemaPlan createDatabasePlan, C context) {
    return visitPlan(createDatabasePlan, context);
  }

  public R visitAlterDatabase(DatabaseSchemaPlan alterDatabasePlan, C context) {
    return visitPlan(alterDatabasePlan, context);
  }

  public R visitDeleteDatabase(DeleteDatabasePlan deleteDatabasePlan, C context) {
    return visitPlan(deleteDatabasePlan, context);
  }

  public R visitCreateSchemaTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan, C context) {
    return visitPlan(createSchemaTemplatePlan, context);
  }

  public R visitCommitSetSchemaTemplate(
      CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan, C context) {
    return visitPlan(commitSetSchemaTemplatePlan, context);
  }

  public R visitPipeUnsetSchemaTemplate(
      PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, C context) {
    return visitPlan(pipeUnsetSchemaTemplatePlan, context);
  }

  public R visitExtendSchemaTemplate(ExtendSchemaTemplatePlan extendSchemaTemplatePlan, C context) {
    return visitPlan(extendSchemaTemplatePlan, context);
  }

  public R visitDropSchemaTemplate(DropSchemaTemplatePlan dropSchemaTemplatePlan, C context) {
    return visitPlan(dropSchemaTemplatePlan, context);
  }

  public R visitPipeDeleteTimeSeries(PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, C context) {
    return visitPlan(pipeDeleteTimeSeriesPlan, context);
  }

  public R visitPipeDeleteLogicalView(
      PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, C context) {
    return visitPlan(pipeDeleteLogicalViewPlan, context);
  }

  public R visitPipeDeactivateTemplate(
      PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, C context) {
    return visitPlan(pipeDeactivateTemplatePlan, context);
  }

  public R visitCreateUser(AuthorPlan createUserPlan, C context) {
    return visitPlan(createUserPlan, context);
  }

  public R visitCreateRawUser(AuthorPlan createRawUserPlan, C context) {
    return visitPlan(createRawUserPlan, context);
  }

  public R visitUpdateUser(AuthorPlan updateUserPlan, C context) {
    return visitPlan(updateUserPlan, context);
  }

  public R visitDropUser(AuthorPlan dropUserPlan, C context) {
    return visitPlan(dropUserPlan, context);
  }

  public R visitGrantUser(AuthorPlan grantUserPlan, C context) {
    return visitPlan(grantUserPlan, context);
  }

  public R visitRevokeUser(AuthorPlan revokeUserPlan, C context) {
    return visitPlan(revokeUserPlan, context);
  }

  public R visitCreateRole(AuthorPlan createRolePlan, C context) {
    return visitPlan(createRolePlan, context);
  }

  public R visitDropRole(AuthorPlan dropRolePlan, C context) {
    return visitPlan(dropRolePlan, context);
  }

  public R visitGrantRole(AuthorPlan grantRolePlan, C context) {
    return visitPlan(grantRolePlan, context);
  }

  public R visitRevokeRole(AuthorPlan revokeRolePlan, C context) {
    return visitPlan(revokeRolePlan, context);
  }

  public R visitGrantRoleToUser(AuthorPlan grantRoleToUserPlan, C context) {
    return visitPlan(grantRoleToUserPlan, context);
  }

  public R visitRevokeRoleFromUser(AuthorPlan revokeRoleFromUserPlan, C context) {
    return visitPlan(revokeRoleFromUserPlan, context);
  }

  public R visitTTL(SetTTLPlan setTTLPlan, C context) {
    return visitPlan(setTTLPlan, context);
  }
}
