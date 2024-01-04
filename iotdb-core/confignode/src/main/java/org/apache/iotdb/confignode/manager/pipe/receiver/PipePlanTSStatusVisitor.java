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
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.rpc.TSStatusCode;

public class PipePlanTSStatusVisitor extends PhysicalPlanVisitor<TSStatus, TSStatus> {

  @Override
  public TSStatus visitPlan(ConfigPhysicalPlan plan, TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitCreateDatabase(DatabaseSchemaPlan plan, TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      if (context.getMessage().contains("has already been created as database")) {
        // The same database or lower level database has been created
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      }
      // Lower level database has been created
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateDatabase(plan, context);
  }
}
