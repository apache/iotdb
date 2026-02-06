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

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Task for executing PREPARE statement. Stores the prepared statement AST in the session. The AST
 * is cached to avoid reparsing on EXECUTE (skipping Parser phase). Memory is allocated from
 * CoordinatorMemoryManager and shared across all sessions.
 */
public class PrepareTask implements IConfigTask {

  private final String statementName;
  private final Statement sql; // AST containing Parameter nodes

  public PrepareTask(String statementName, Statement sql) {
    this.statementName = statementName;
    this.sql = sql;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IClientSession session = SessionManager.getInstance().getCurrSession();
    if (session == null) {
      future.setException(
          new IllegalStateException("No current session available for PREPARE statement"));
      return future;
    }

    try {
      PreparedStatementHelper.register(session, statementName, sql);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }
}
