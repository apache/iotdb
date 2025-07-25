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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement.DropUserLabelPolicyStatement;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * DropTableUserLabelPolicyTask handles the execution of dropping user label policies in table
 * model. This task specifically handles the deletion of label policies for users to support
 * Label-Based Access Control (LBAC) functionality in the table model.
 */
public class DropTableUserLabelPolicyTask implements IConfigTask {

  private final String username;
  private final String scope;

  public DropTableUserLabelPolicyTask(DropUserLabelPolicyStatement statement) {
    this.username = statement.getUsername();
    this.scope = statement.getScope();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    // For dropping policy, use dropTableUserLabelPolicy
    return configTaskExecutor.dropTableUserLabelPolicy(username, parseScope(scope));
  }

  private org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement
          .ShowTableUserLabelPolicyStatement.PolicyScope
      parseScope(String scope) {
    if ("READ".equalsIgnoreCase(scope)) {
      return org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement
          .ShowTableUserLabelPolicyStatement.PolicyScope.READ;
    } else if ("WRITE".equalsIgnoreCase(scope)) {
      return org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement
          .ShowTableUserLabelPolicyStatement.PolicyScope.WRITE;
    } else {
      return org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement
          .ShowTableUserLabelPolicyStatement.PolicyScope.READ_WRITE;
    }
  }
}
