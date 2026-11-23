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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.Utils;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryCollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LastQueryCollectOperatorTest {

  @Test
  public void testUnblockAllLocalFragmentInstances() throws Exception {
    ExecutorService executor = newDirectExecutorService();
    LocalMemoryManager memoryManager = mock(LocalMemoryManager.class);
    MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
    when(memoryManager.getQueryPool()).thenReturn(memoryPool);
    TFragmentInstanceId downstreamId = new TFragmentInstanceId("q0", 0, "0");
    List<Operator> children = new ArrayList<>();
    List<LocalSinkChannel> sinks = new ArrayList<>();
    List<ListenableFuture<?>> producerBlocked = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      String planNodeId = "exchange_" + i;
      SharedTsBlockQueue queue =
          new SharedTsBlockQueue(downstreamId, planNodeId, memoryManager, executor);
      LocalSinkChannel sink =
          new LocalSinkChannel(
              new TFragmentInstanceId("q0", i + 1, "0"), queue, mock(SinkListener.class));
      sinks.add(sink);
      producerBlocked.add(sink.isFull());
      LocalSourceHandle source =
          new LocalSourceHandle(downstreamId, planNodeId, queue, mock(SourceHandleListener.class));
      OperatorContext context = mock(OperatorContext.class);
      when(context.getSpecifiedInfo()).thenReturn(new HashMap<>());
      children.add(new ExchangeOperator(context, source, new PlanNodeId(planNodeId)));
    }

    try (LastQueryCollectOperator collect =
        new LastQueryCollectOperator(mock(OperatorContext.class), children)) {
      for (ListenableFuture<?> blocked : producerBlocked) {
        assertFalse(blocked.isDone());
      }

      ListenableFuture<?> blocked = collect.isBlocked();
      assertFalse(blocked.isDone());
      for (ListenableFuture<?> producer : producerBlocked) {
        assertTrue(producer.isDone());
      }

      // Consume the first child's output without waiting for the second child to produce data.
      TsBlock first = Utils.createMockTsBlock(1024);
      sinks.get(0).send(first);
      assertTrue(blocked.isDone());
      assertTrue(collect.isBlocked().isDone());
      assertSame(first, collect.next());

      // The second fragment can produce data while the first fragment is still unfinished.
      blocked = collect.isBlocked();
      assertFalse(blocked.isDone());
      TsBlock second = Utils.createMockTsBlock(1024);
      sinks.get(1).send(second);
      sinks.get(1).setNoMoreTsBlocks();
      assertFalse(blocked.isDone());
      assertFalse(collect.isBlocked().isDone());

      sinks.get(0).setNoMoreTsBlocks();
      assertTrue(blocked.isDone());
      assertNull(collect.next());
      assertTrue(collect.hasNext());
      assertTrue(collect.isBlocked().isDone());
      assertSame(second, collect.next());
      assertTrue(collect.isBlocked().isDone());
      assertNull(collect.next());
      assertFalse(collect.hasNext());
      assertTrue(collect.isFinished());
      assertTrue(collect.isBlocked().isDone());
    } finally {
      for (LocalSinkChannel sink : sinks) {
        sink.close();
      }
      executor.shutdownNow();
    }
  }

  @Test
  public void testEmptyChildren() throws Exception {
    try (LastQueryCollectOperator collect =
        new LastQueryCollectOperator(mock(OperatorContext.class), Collections.emptyList())) {
      assertTrue(collect.isBlocked().isDone());
      assertFalse(collect.hasNext());
      assertTrue(collect.isFinished());
      assertTrue(collect.isBlocked().isDone());
    }
  }
}
