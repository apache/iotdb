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

package org.apache.iotdb.db.queryengine.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigExecutionTest {

  @Test
  public void normalConfigTaskTest() {
    IConfigTask task =
        (clientManager) -> immediateFuture(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    ConfigExecution execution = new ConfigExecution(genMPPQueryContext(), getExecutor(), task);
    execution.start();
    ExecutionResult result = execution.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.code);
  }

  @Test
  public void normalConfigTaskWithResultTest() {
    TsBlock tsBlock =
        new TsBlock(
            new TimeColumn(1, new long[] {0}), new IntColumn(1, Optional.empty(), new int[] {1}));
    DatasetHeader datasetHeader =
        new DatasetHeader(
            Collections.singletonList(new ColumnHeader("TestValue", TSDataType.INT32)), false);
    IConfigTask task =
        (clientManager) ->
            immediateFuture(
                new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, tsBlock, datasetHeader));
    ConfigExecution execution = new ConfigExecution(genMPPQueryContext(), getExecutor(), task);
    execution.start();
    ExecutionResult result = execution.getStatus();
    TsBlock tsBlockFromExecution = null;
    if (execution.hasNextResult()) {
      Optional<TsBlock> optionalTsBlock = execution.getBatchResult();
      assertTrue(optionalTsBlock.isPresent());
      tsBlockFromExecution = optionalTsBlock.get();
    }
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.code);
    assertEquals(tsBlock, tsBlockFromExecution);
  }

  @Test
  public void exceptionConfigTaskTest() {
    IConfigTask task =
        (clientManager) -> {
          throw new RuntimeException("task throw exception when executing");
        };
    ConfigExecution execution = new ConfigExecution(genMPPQueryContext(), getExecutor(), task);
    execution.start();
    ExecutionResult result = execution.getStatus();
    assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.code);
  }

  @Test
  public void configTaskCancelledTest() throws InterruptedException {
    SettableFuture<ConfigTaskResult> taskResult = SettableFuture.create();
    class SimpleTask implements IConfigTask {
      private final ListenableFuture<ConfigTaskResult> result;

      public SimpleTask(ListenableFuture<ConfigTaskResult> future) {
        this.result = future;
      }

      @Override
      public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskFetcher)
          throws InterruptedException {
        return result;
      }
    }
    IConfigTask task = new SimpleTask(taskResult);
    ConfigExecution execution = new ConfigExecution(genMPPQueryContext(), getExecutor(), task);
    execution.start();

    Thread resultThread =
        new Thread(
            () -> {
              ExecutionResult result = execution.getStatus();
              assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.code);
            });
    resultThread.start();
    taskResult.cancel(true);
    resultThread.join();
  }

  @Test
  public void exceptionAfterInvokeGetStatusTest() {
    IConfigTask task =
        (clientManager) -> {
          throw new RuntimeException("task throw exception when executing");
        };
    ConfigExecution execution = new ConfigExecution(genMPPQueryContext(), getExecutor(), task);
    Thread resultThread =
        new Thread(
            () -> {
              ExecutionResult result = execution.getStatus();
              assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.code);
            });
    resultThread.start();
    execution.start();
    try {
      resultThread.join();
      // There is a scenario that the InterruptedException won't throw here. If the
      // execution.start() runs faster and completes the whole process including
      // invoking Thread.interrupt() before join() is invoked, then the join won't
      // receive the interrupt signal. So we cannot assert fail here.
      // Assert.fail("InterruptedException should be threw here");
    } catch (InterruptedException e) {
      ExecutionResult result = execution.getStatus();
      assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.code);
      execution.stop(e);
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
