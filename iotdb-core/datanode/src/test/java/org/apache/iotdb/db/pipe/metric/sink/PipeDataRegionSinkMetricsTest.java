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

package org.apache.iotdb.db.pipe.metric.sink;

import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

public class PipeDataRegionSinkMetricsTest {

  @Test
  public void testDeregisterWaitsForBind() throws Exception {
    final String taskId = "data-region-sink-task";
    final CountDownLatch bindEntered = new CountDownLatch(1);
    final CountDownLatch continueBind = new CountDownLatch(1);
    final AtomicBoolean blockBindOnce = new AtomicBoolean(true);
    final PipeSinkSubtask subtask = Mockito.mock(PipeSinkSubtask.class);
    when(subtask.getTaskID()).thenReturn(taskId);
    when(subtask.getPipeName())
        .thenAnswer(
            invocation -> {
              if (blockBindOnce.compareAndSet(true, false)) {
                bindEntered.countDown();
                continueBind.await();
              }
              return "pipe";
            });
    when(subtask.getAttributeSortedString()).thenReturn("sink");
    when(subtask.getCreationTime()).thenReturn(1L);
    when(subtask.getSinkIndex()).thenReturn(0);

    final AbstractMetricService metricService =
        Mockito.mock(AbstractMetricService.class, Mockito.RETURNS_MOCKS);

    final Constructor<PipeDataRegionSinkMetrics> constructor =
        PipeDataRegionSinkMetrics.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final PipeDataRegionSinkMetrics metrics = constructor.newInstance();
    metrics.register(subtask);

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<?> bindFuture = executor.submit(() -> metrics.bindTo(metricService));
    try {
      Assert.assertTrue(bindEntered.await(30, TimeUnit.SECONDS));
      final AtomicReference<Thread> deregisterThread = new AtomicReference<>();
      final Future<?> deregisterFuture =
          executor.submit(
              () -> {
                deregisterThread.set(Thread.currentThread());
                metrics.deregister(taskId);
              });
      try {
        await()
            .atMost(30, TimeUnit.SECONDS)
            .until(
                () ->
                    deregisterFuture.isDone()
                        || (deregisterThread.get() != null
                            && deregisterThread.get().getState() == Thread.State.BLOCKED));
        Assert.assertFalse(
            "Deregistration must wait until metric binding completes", deregisterFuture.isDone());
      } finally {
        continueBind.countDown();
      }

      bindFuture.get(30, TimeUnit.SECONDS);
      deregisterFuture.get(30, TimeUnit.SECONDS);
    } finally {
      continueBind.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }
}
