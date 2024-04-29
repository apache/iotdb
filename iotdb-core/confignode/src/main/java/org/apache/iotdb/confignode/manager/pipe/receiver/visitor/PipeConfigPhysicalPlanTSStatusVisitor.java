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

package org.apache.iotdb.confignode.manager.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
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
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * This visitor translated some {@link TSStatus} to pipe related status to help sender classify them
 * and apply different error handling tactics. Please DO NOT modify the {@link TSStatus} returned by
 * the processes that generate the following {@link TSStatus}es in the class.
 */
public class PipeConfigPhysicalPlanTSStatusVisitor
    extends ConfigPhysicalPlanVisitor<TSStatus, TSStatus> {

  @Override
  public TSStatus visitPlan(ConfigPhysicalPlan plan, TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitCreateDatabase(DatabaseSchemaPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      if (context
          .getMessage()
          .contains(
              String.format(
                  "%s has already been created as database", plan.getSchema().getName()))) {
        // The same database has been created
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      }
      // Lower or higher level database has been created
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateDatabase(plan, context);
  }

  @Override
  public TSStatus visitAlterDatabase(DatabaseSchemaPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitAlterDatabase(plan, context);
  }

  @Override
  public TSStatus visitDeleteDatabase(DeleteDatabasePlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDeleteDatabase(plan, context);
  }

  @Override
  public TSStatus visitCreateSchemaTemplate(CreateSchemaTemplatePlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitExtendSchemaTemplate(ExtendSchemaTemplatePlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitExtendSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitCommitSetSchemaTemplate(CommitSetSchemaTemplatePlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      if (context.getMessage().contains("Template already exists")) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      } else if (context.getMessage().contains("Template t1 does not exist")) {
        return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      }
    } else if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCommitSetSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitPipeUnsetSchemaTemplate(
      PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, TSStatus context) {
    if (context.getCode() == TSStatusCode.TEMPLATE_NOT_SET.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()
        || context.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeUnsetSchemaTemplate(pipeUnsetSchemaTemplatePlan, context);
  }

  @Override
  public TSStatus visitDropSchemaTemplate(
      DropSchemaTemplatePlan dropSchemaTemplatePlan, TSStatus context) {
    if (context.getCode() == TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropSchemaTemplate(dropSchemaTemplatePlan, context);
  }

  @Override
  public TSStatus visitPipeDeleteTimeSeries(
      PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeleteTimeSeries(pipeDeleteTimeSeriesPlan, context);
  }

  @Override
  public TSStatus visitPipeDeleteLogicalView(
      PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeleteLogicalView(pipeDeleteLogicalViewPlan, context);
  }

  @Override
  public TSStatus visitPipeDeactivateTemplate(
      PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, TSStatus context) {
    if (context.getCode() == TSStatusCode.TEMPLATE_NOT_ACTIVATED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeactivateTemplate(pipeDeactivateTemplatePlan, context);
  }

  @Override
  public TSStatus visitCreateUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateUser(plan, context);
  }

  @Override
  public TSStatus visitCreateRawUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateRawUser(plan, context);
  }

  @Override
  public TSStatus visitUpdateUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitUpdateUser(plan, context);
  }

  @Override
  public TSStatus visitDropUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropUser(plan, context);
  }

  @Override
  public TSStatus visitGrantUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      // Admin user
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantUser(plan, context);
  }

  @Override
  public TSStatus visitRevokeUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()
        // Admin user
        || context.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeUser(plan, context);
  }

  @Override
  public TSStatus visitCreateRole(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateRole(plan, context);
  }

  @Override
  public TSStatus visitDropRole(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropRole(plan, context);
  }

  @Override
  public TSStatus visitGrantRole(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantRole(plan, context);
  }

  @Override
  public TSStatus visitRevokeRole(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeRole(plan, context);
  }

  @Override
  public TSStatus visitGrantRoleToUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_HAS_ROLE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantRoleToUser(plan, context);
  }

  @Override
  public TSStatus visitRevokeRoleFromUser(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeRoleFromUser(plan, context);
  }

  @Override
  public TSStatus visitTTL(SetTTLPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitTTL(plan, context);
  }
}
