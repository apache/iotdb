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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.PhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * This visitor translated some exceptions to pipe related status to help sender classify them and
 * apply different error handling tactics. Please DO NOT modify the exceptions returned by the
 * processes that generate the following exceptions in the class.
 */
public class PipePlanExceptionVisitor extends PhysicalPlanVisitor<TSStatus, Exception> {
  @Override
  public TSStatus visitPlan(ConfigPhysicalPlan plan, Exception context) {
    return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
        .setMessage(context.getMessage());
  }

  @Override
  public TSStatus visitPipeUnsetSchemaTemplate(
      UnsetSchemaTemplatePlan unsetSchemaTemplatePlan, Exception context) {
    if (context instanceof MetadataException) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeUnsetSchemaTemplate(unsetSchemaTemplatePlan, context);
  }
}
