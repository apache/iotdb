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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseSecurityLabelStatement;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Task for showing database security labels in tree model
 *
 * <p>This task is responsible for executing SHOW DATABASES database? SECURITY_LABEL statements in
 * the tree model context.
 */
public class ShowDatabaseSecurityLabelTask implements IConfigTask {

  private final ShowDatabaseSecurityLabelStatement statement;

  public ShowDatabaseSecurityLabelTask(ShowDatabaseSecurityLabelStatement statement) {
    this.statement = statement;
  }

  @Override
  public SettableFuture<ConfigTaskResult> execute(IConfigTaskExecutor executor) {
    return executor.showDatabaseSecurityLabel(statement);
  }

  @Override
  public String toString() {
    return "ShowDatabaseSecurityLabelTask{statement=" + statement + "}";
  }
}
