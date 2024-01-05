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

package org.apache.iotdb.confignode.manager.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.PhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.rpc.TSStatusCode;

public class PipePlanTSStatusVisitor extends PhysicalPlanVisitor<TSStatus, TSStatus> {

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
    }
    return super.visitCreateDatabase(plan, context);
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
  public TSStatus visitCommitSetSchemaTemplate(CommitSetSchemaTemplatePlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.DATANODE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCommitSetSchemaTemplate(plan, context);
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
  public TSStatus visitCreateRole(AuthorPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateRole(plan, context);
  }
}
