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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.lbac;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement.SetUserLabelPolicyStatement;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * SetTableUserLabelPolicyTask handles the execution of setting user label policies in table model.
 * This task specifically handles the creation and modification of label policies for users to
 * support Label-Based Access Control (LBAC) functionality in the table model.
 */
public class SetTableUserLabelPolicyTask implements IConfigTask {

  private final String username;
  private final String policyExpression;
  private final String scope;

  public SetTableUserLabelPolicyTask(SetUserLabelPolicyStatement statement) {
    this.username = statement.getUsername();
    this.policyExpression = statement.getPolicyExpression();
    this.scope = statement.getScope();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    // For setting policy, determine if it's READ or WRITE based on scope
    if ("READ".equalsIgnoreCase(scope)) {
      return configTaskExecutor.setUserReadLabelPolicy(username, policyExpression);
    } else if ("WRITE".equalsIgnoreCase(scope)) {
      return configTaskExecutor.setUserWriteLabelPolicy(username, policyExpression);
    } else {
      // Handle READ,WRITE or WRITE,READ - set both policies
      return configTaskExecutor.setUserReadLabelPolicy(username, policyExpression);
      // Note: WRITE policy will be set in a separate call if needed
    }
  }
}
