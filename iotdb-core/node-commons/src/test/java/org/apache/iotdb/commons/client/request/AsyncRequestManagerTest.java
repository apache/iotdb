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

package org.apache.iotdb.commons.client.request;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncRequestManagerTest {

  @Test
  public void dispatchFailureShouldNotBlockRetryWithoutTimeout() throws Exception {
    final TestAsyncRequestManager manager = new TestAsyncRequestManager();
    final AsyncRequestContext<String, String, TestRequestType, TestNodeLocation> context =
        new AsyncRequestContext<>(
            TestRequestType.TEST,
            "request",
            Collections.singletonMap(1, new TestNodeLocation(new TEndPoint("localhost", 6667))));

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      final Future<?> future =
          executorService.submit(() -> manager.sendAsyncRequest(context, 2, null, true));
      future.get(3, TimeUnit.SECONDS);
    } finally {
      executorService.shutdownNow();
    }

    Assert.assertEquals(2, manager.getBorrowAttempts());
    Assert.assertEquals("borrow failed", context.getResponseMap().get(1));
    Assert.assertEquals(Collections.singletonList(1), context.getRequestIndices());
  }

  private enum TestRequestType {
    TEST
  }

  private static class TestNodeLocation {

    private final TEndPoint endPoint;

    private TestNodeLocation(final TEndPoint endPoint) {
      this.endPoint = endPoint;
    }
  }

  private static class TestAsyncRequestManager
      extends AsyncRequestManager<TestRequestType, TestNodeLocation, Object> {

    private final AtomicInteger borrowAttempts = new AtomicInteger();

    private TestAsyncRequestManager() {
      super(1);
    }

    private int getBorrowAttempts() {
      return borrowAttempts.get();
    }

    @Override
    protected void initClientManager(final int selectorNumOfAsyncClientManager) {
      clientManager =
          new IClientManager<TEndPoint, Object>() {

            @Override
            public Object borrowClient(final TEndPoint node) throws ClientManagerException {
              borrowAttempts.incrementAndGet();
              throw new ClientManagerException("borrow failed");
            }

            @Override
            public void clear(final TEndPoint node) {
              // Do nothing
            }

            @Override
            public void clearAll() {
              // Do nothing
            }

            @Override
            public void close() {
              // Do nothing
            }
          };
    }

    @Override
    protected void initActionMapBuilder() {
      actionMapBuilder.put(
          TestRequestType.TEST,
          (request, client, handler) ->
              Assert.fail("The test client manager should fail before dispatch."));
    }

    @Override
    protected TEndPoint nodeLocationToEndPoint(final TestNodeLocation location) {
      return location.endPoint;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected AsyncRequestRPCHandler<?, TestRequestType, TestNodeLocation> buildHandler(
        final AsyncRequestContext<?, ?, TestRequestType, TestNodeLocation> requestContext,
        final int requestId,
        final TestNodeLocation targetNode) {
      return new TestAsyncRequestRPCHandler(
          requestContext.getRequestType(),
          requestId,
          targetNode,
          requestContext.getNodeLocationMap(),
          (Map<Integer, String>) requestContext.getResponseMap(),
          requestContext.getCountDownLatch());
    }
  }

  private static class TestAsyncRequestRPCHandler
      extends AsyncRequestRPCHandler<String, TestRequestType, TestNodeLocation> {

    private TestAsyncRequestRPCHandler(
        final TestRequestType requestType,
        final int requestId,
        final TestNodeLocation targetNode,
        final Map<Integer, TestNodeLocation> nodeLocationMap,
        final Map<Integer, String> responseMap,
        final CountDownLatch countDownLatch) {
      super(requestType, requestId, targetNode, nodeLocationMap, responseMap, countDownLatch);
    }

    @Override
    protected String generateFormattedTargetLocation(final TestNodeLocation location) {
      return location.endPoint.toString();
    }

    @Override
    public void onComplete(final String response) {
      responseMap.put(requestId, response);
      nodeLocationMap.remove(requestId);
      countDownLatch.countDown();
    }

    @Override
    public void onError(final Exception exception) {
      responseMap.put(requestId, exception.getMessage());
      countDownLatch.countDown();
    }
  }
}
