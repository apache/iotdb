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

package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.config.ConfigExecution;
import org.apache.iotdb.db.mpp.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class ConfigExecutionTest {

  @Test
  public void normalConfigTaskTest() {
    IConfigTask task = () -> immediateFuture(null);
    ConfigExecution execution =
        new ConfigExecution(genMPPQueryContext(), null, getExecutor(), task);
    execution.start();
    ExecutionResult result = execution.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.code);
  }

  @Test
  public void exceptionConfigTaskTest() {
    IConfigTask task =
        () -> {
          throw new RuntimeException("task throw exception when executing");
        };
    ConfigExecution execution =
        new ConfigExecution(genMPPQueryContext(), null, getExecutor(), task);
    execution.start();
    ExecutionResult result = execution.getStatus();
    Assert.assertEquals(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), result.status.code);
  }

  @Test
  public void configTaskCancelledTest() throws InterruptedException {
    SettableFuture<Void> taskResult = SettableFuture.create();
    class SimpleTask implements IConfigTask {
      private final ListenableFuture<Void> result;

      public SimpleTask(ListenableFuture<Void> future) {
        this.result = future;
      }

      @Override
      public ListenableFuture<Void> execute() throws InterruptedException {
        return result;
      }
    }
    IConfigTask task = new SimpleTask(taskResult);
    ConfigExecution execution =
        new ConfigExecution(genMPPQueryContext(), null, getExecutor(), task);
    execution.start();

    Thread resultThread =
        new Thread(
            () -> {
              ExecutionResult result = execution.getStatus();
              Assert.assertEquals(
                  TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), result.status.code);
            });
    resultThread.start();
    taskResult.cancel(true);
    resultThread.join();
  }

  @Test
  public void exceptionAfterInvokeGetStatusTest() {
    IConfigTask task =
        () -> {
          throw new RuntimeException("task throw exception when executing");
        };
    ConfigExecution execution =
        new ConfigExecution(genMPPQueryContext(), null, getExecutor(), task);
    Thread resultThread =
        new Thread(
            () -> {
              ExecutionResult result = execution.getStatus();
              Assert.assertEquals(
                  TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), result.status.code);
            });
    resultThread.start();
    execution.start();
    try {
      resultThread.join();
      Assert.fail("InterruptedException should be threw here");
    } catch (InterruptedException e) {
      execution.stop();
    }
  }

  private MPPQueryContext genMPPQueryContext() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("query1"));
    context.setQueryType(QueryType.WRITE);
    return context;
  }

  private ExecutorService getExecutor() {
    return IoTDBThreadPoolFactory.newSingleThreadExecutor("ConfigExecutionTest");
  }
}
