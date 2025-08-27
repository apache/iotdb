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

package org.apache.iotdb.db.queryengine.plan.execution.config.session;

import org.apache.iotdb.db.auth.LbacIntegration;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterUserLabelPolicyStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowUserLabelPolicyStatement.LabelPolicyScope;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * AlterUserLabelPolicyTask handles the execution of ALTER USER username DROP LABEL_POLICY
 * statements. This task removes label policies for users to support Label-Based Access Control
 * (LBAC) functionality.
 */
public class AlterUserLabelPolicyTask implements IConfigTask {

  private final AlterUserLabelPolicyStatement alterUserLabelPolicyStatement;

  public AlterUserLabelPolicyTask(AlterUserLabelPolicyStatement alterUserLabelPolicyStatement) {
    this.alterUserLabelPolicyStatement = alterUserLabelPolicyStatement;
  }

  public static void executeDropUserLabelPolicy(
      String username, LabelPolicyScope scope, SettableFuture<ConfigTaskResult> future) {
    try {
      // Drop user label policy
      LbacIntegration.dropUserLabelPolicy(username, scope);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      future.setException(e);
    }
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    return configTaskExecutor.alterUserLabelPolicy(
        alterUserLabelPolicyStatement.getUsername(), alterUserLabelPolicyStatement.getScope());
  }
}
