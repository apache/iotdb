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

package org.apache.iotdb.db.audit;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.RpcUtils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import jakarta.validation.constraints.NotNull;

/** Records the final result of a user-role modification at the concrete config task. */
public final class UserRoleModificationAuditTask implements IConfigTask {

  private final IConfigTask delegate;
  private final UserRoleModificationAuditContext auditContext;

  private UserRoleModificationAuditTask(
      IConfigTask delegate, UserRoleModificationAuditContext auditContext) {
    this.delegate = delegate;
    this.auditContext = auditContext;
  }

  public static IConfigTask wrap(
      IConfigTask delegate, UserRoleModificationAuditContext auditContext) {
    return auditContext.isEnabled()
        ? new UserRoleModificationAuditTask(delegate, auditContext)
        : delegate;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    try {
      ListenableFuture<ConfigTaskResult> future = delegate.execute(configTaskExecutor);
      Futures.addCallback(
          future,
          new FutureCallback<ConfigTaskResult>() {
            @Override
            public void onSuccess(ConfigTaskResult result) {
              auditContext.log(toStatus(result));
            }

            @Override
            public void onFailure(@NotNull Throwable throwable) {
              auditContext.log(toStatus(throwable));
            }
          },
          MoreExecutors.directExecutor());
      return future;
    } catch (InterruptedException | RuntimeException | Error e) {
      auditContext.log(toStatus(e));
      throw e;
    }
  }

  private static TSStatus toStatus(ConfigTaskResult result) {
    if (result == null) {
      return null;
    }
    if (result.getStatus() != null) {
      return result.getStatus();
    }
    return result.getStatusCode() == null ? null : RpcUtils.getStatus(result.getStatusCode());
  }

  private static TSStatus toStatus(Throwable throwable) {
    if (throwable instanceof IoTDBException) {
      return ((IoTDBException) throwable).getStatus();
    }
    if (throwable instanceof IoTDBRuntimeException) {
      return ((IoTDBRuntimeException) throwable).getStatus();
    }
    return null;
  }
}
