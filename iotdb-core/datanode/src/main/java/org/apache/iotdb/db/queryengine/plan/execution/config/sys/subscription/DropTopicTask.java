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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropTopicStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class DropTopicTask implements IConfigTask {

  private final DropTopicStatement dropTopicStatement;

  public DropTopicTask(final DropTopicStatement dropTopicStatement) {
    this.dropTopicStatement = dropTopicStatement;
  }

  public DropTopicTask(final DropTopic dropTopic) {
    this.dropTopicStatement = new DropTopicStatement();
    this.dropTopicStatement.setTopicName(dropTopic.getTopicName());
    this.dropTopicStatement.setIfExists(dropTopic.hasIfExistsCondition());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.dropTopic(dropTopicStatement);
  }
}
