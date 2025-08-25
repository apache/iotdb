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

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetUserLabelPolicyStatement;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SetUserLabelPolicyTask handles the execution of ALTER USER username SET LABEL_POLICY statements.
 * This task sets or updates label policies for users to support Label-Based Access Control (LBAC)
 * functionality.
 */
public class SetUserLabelPolicyTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(SetUserLabelPolicyTask.class);

  private final SetUserLabelPolicyStatement setUserLabelPolicyStatement;

  public SetUserLabelPolicyTask(SetUserLabelPolicyStatement setUserLabelPolicyStatement) {
    this.setUserLabelPolicyStatement = setUserLabelPolicyStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    return configTaskExecutor.setUserLabelPolicy(
        setUserLabelPolicyStatement.getUsername(),
        setUserLabelPolicyStatement.getPolicyExpression(),
        setUserLabelPolicyStatement.getScope());
  }
}
