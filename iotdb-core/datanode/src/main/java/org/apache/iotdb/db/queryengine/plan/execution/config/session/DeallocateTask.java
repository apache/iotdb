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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Task for executing DEALLOCATE PREPARE statement. Removes the prepared statement from the session
 * and releases its allocated memory.
 */
public class DeallocateTask implements IConfigTask {

  private final String statementName;

  public DeallocateTask(String statementName) {
    this.statementName = statementName;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IClientSession session = SessionManager.getInstance().getCurrSession();
    if (session == null) {
      future.setException(
          new IllegalStateException("No current session available for DEALLOCATE statement"));
      return future;
    }

    // Remove the prepared statement
    PreparedStatementInfo removedInfo = session.removePreparedStatement(statementName);
    if (removedInfo == null) {
      future.setException(
          new SemanticException(
              String.format("Prepared statement '%s' does not exist", statementName)));
      return future;
    }

    // Release the memory allocated for this PreparedStatement from the shared MemoryBlock
    PreparedStatementMemoryManager.getInstance().release(removedInfo.getMemorySizeInBytes());

    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return future;
  }
}
