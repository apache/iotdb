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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Optional;

public class PipeConfigTreePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, String> {
  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final String context) {
    return Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final String userName) {
    return visitDatabaseSchemaPlan(createDatabasePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final String userName) {
    return visitDatabaseSchemaPlan(alterDatabasePlan, userName);
  }

  public Optional<ConfigPhysicalPlan> visitDatabaseSchemaPlan(
      final DatabaseSchemaPlan databaseSchemaPlan, final String userName) {
    return ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(
                        userName,
                        new PrivilegeUnion(
                            PrivilegeType.READ_SCHEMA,
                            new PartialPath(databaseSchemaPlan.getSchema().getName())))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.SYSTEM))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(databaseSchemaPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final String userName) {
    return ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(
                        userName,
                        new PrivilegeUnion(
                            PrivilegeType.READ_SCHEMA,
                            new PartialPath(deleteDatabasePlan.getName())))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.SYSTEM))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final String userName) {}

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan, final String userName) {}

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, final String userName) {}

  @Override
  public Optional<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final String userName) {}

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantUser(
      final AuthorTreePlan grantUserPlan, final String userName) {
    return visitUserPlan(grantUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorTreePlan revokeUserPlan, final String userName) {
    return visitUserPlan(revokeUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(
      final AuthorTreePlan revokeUserPlan, final String userName) {
    return visitRolePlan(revokeUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorTreePlan revokeUserPlan, final String userName) {
    return visitRolePlan(revokeUserPlan, userName);
  }

  private Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorTreePlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_USER))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  private Optional<ConfigPhysicalPlan> visitRolePlan(
      final AuthorTreePlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final String userName) {
    return Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final String userName) {
    return Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final String userName) {
    return Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitTTL(final SetTTLPlan setTTLPlan, final String userName) {
    return Optional.empty();
  }
}
