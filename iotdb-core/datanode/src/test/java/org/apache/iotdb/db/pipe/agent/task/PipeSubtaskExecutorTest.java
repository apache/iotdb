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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeSubtask;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class PipeSubtaskExecutorTest {

  protected PipeSubtaskExecutor executor;
  protected PipeSubtask subtask;

  @After
  public void tearDown() throws Exception {
    executor.shutdown();
    Assert.assertTrue(executor.isShutdown());
  }

  @Test
  public void testRegister() {
    Assert.assertFalse(executor.isRegistered(subtask.getTaskID()));
    Assert.assertEquals(0, executor.getRegisteredSubtaskNumber());

    // test register a subtask which is not in the map
    executor.register(subtask);
    Assert.assertTrue(executor.isRegistered(subtask.getTaskID()));
    Assert.assertEquals(1, executor.getRegisteredSubtaskNumber());

    // test register a subtask which is in the map
    executor.register(subtask);
    Assert.assertTrue(executor.isRegistered(subtask.getTaskID()));
    Assert.assertEquals(1, executor.getRegisteredSubtaskNumber());
  }

  @Test
  public void testStart() throws Exception {
    // test start a subtask which is not in the map
    executor.start(subtask.getTaskID());
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    verify(subtask, times(0)).call();

    // test start a subtask which is in the map
    executor.register(subtask);
    executor.start(subtask.getTaskID());
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    verify(subtask, atLeast(2)).call();
    Assert.assertTrue(subtask.isSubmittingSelf());

    // test start a subtask which is in the map and is already running
    executor.start(subtask.getTaskID());
    Assert.assertTrue(subtask.isSubmittingSelf());
  }

  @Test
  public void testStop() {
    // test stop a subtask which is not in the map
    executor.stop(subtask.getTaskID());
    Assert.assertFalse(subtask.isSubmittingSelf());

    // test stop a subtask which is in the map
    executor.register(subtask);
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.stop(subtask.getTaskID());
    Assert.assertFalse(subtask.isSubmittingSelf());

    // test stop a running subtask
    executor.start(subtask.getTaskID());
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.stop(subtask.getTaskID());
    Assert.assertFalse(subtask.isSubmittingSelf());

    // test stop a stopped subtask
    executor.stop(subtask.getTaskID());
    Assert.assertFalse(subtask.isSubmittingSelf());
  }

  /**
   * A rejected submission must be rolled back, otherwise the following start() calls would take the
   * "already running" branch and report a false success.
   */
  @Test
  public void testStartWithRejectedSubmission() {
    // Processor registration starts workers that also invoke this spy. Stub it before sharing it.
    Mockito.doThrow(new RejectedExecutionException()).when(subtask).submitSelf();
    executor.register(subtask);

    try {
      executor.start(subtask.getTaskID());
      Assert.fail("The start of a subtask should fail if the submission is rejected");
    } catch (final RejectedExecutionException e) {
      // expected
    }

    verify(subtask).allowSubmittingSelf();
    verify(subtask).disallowSubmittingSelf();
    Assert.assertFalse(executor.isShutdown());
  }

  @Test
  public void testDeregister() {
    // test unregister a subtask which is not in the map
    executor.deregister(subtask.getTaskID());
    Assert.assertEquals(0, executor.getRegisteredSubtaskNumber());

    // test unregister a subtask which is in the map
    executor.register(subtask);
    Assert.assertEquals(1, executor.getRegisteredSubtaskNumber());

    // test unregister a running subtask
    executor.start(subtask.getTaskID());
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.deregister(subtask.getTaskID());
    Assert.assertEquals(0, executor.getRegisteredSubtaskNumber());
    Assert.assertFalse(subtask.isSubmittingSelf());

    // test unregister an unregistered subtask
    executor.deregister(subtask.getTaskID());
    Assert.assertEquals(0, executor.getRegisteredSubtaskNumber());
    Assert.assertFalse(subtask.isSubmittingSelf());
  }

  @Test(timeout = 15000)
  public void testDeregisterCloseDoesNotBlockOtherSubtaskStop() throws Exception {
    final String blockedSubtaskID = "blocked-close-subtask";
    final PipeSubtask blockedSubtask = Mockito.mock(PipeSubtask.class);
    Mockito.when(blockedSubtask.getTaskID()).thenReturn(blockedSubtaskID);
    Mockito.when(blockedSubtask.getDisplayTaskID()).thenReturn(blockedSubtaskID);

    final CountDownLatch closeStarted = new CountDownLatch(1);
    final CountDownLatch allowCloseToFinish = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              closeStarted.countDown();
              allowCloseToFinish.await();
              return null;
            })
        .when(blockedSubtask)
        .close();

    executor.register(blockedSubtask);
    executor.register(subtask);
    executor.start(subtask.getTaskID());

    final AtomicReference<Throwable> deregisterFailure = new AtomicReference<>();
    final Thread deregisterThread =
        new Thread(
            () -> {
              try {
                executor.deregister(blockedSubtaskID);
              } catch (final Throwable t) {
                deregisterFailure.set(t);
              }
            });
    deregisterThread.start();

    final AtomicReference<Throwable> stopFailure = new AtomicReference<>();
    final CountDownLatch stopFinished = new CountDownLatch(1);
    final Thread stopThread =
        new Thread(
            () -> {
              try {
                executor.stop(subtask.getTaskID());
              } catch (final Throwable t) {
                stopFailure.set(t);
              } finally {
                stopFinished.countDown();
              }
            });

    try {
      Assert.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      stopThread.start();
      Assert.assertTrue(
          "Stopping another subtask should not wait for a blocked close",
          stopFinished.await(5, TimeUnit.SECONDS));
      Assert.assertNull(stopFailure.get());
    } finally {
      allowCloseToFinish.countDown();
      deregisterThread.join(TimeUnit.SECONDS.toMillis(5));
      if (stopThread.isAlive()) {
        stopThread.join(TimeUnit.SECONDS.toMillis(5));
      }
    }

    Assert.assertFalse(deregisterThread.isAlive());
    Assert.assertFalse(stopThread.isAlive());
    Assert.assertNull(deregisterFailure.get());
    Assert.assertEquals(1, executor.getRegisteredSubtaskNumber());
  }

  @Test
  public void testShutdown() {
    // test shutdown a running executor
    executor.start(subtask.getTaskID());
    executor.shutdown();

    Assert.assertTrue(executor.isShutdown());
    Assert.assertFalse(subtask.isSubmittingSelf());

    // test shutdown a stopped executor
    executor.shutdown();
    Assert.assertTrue(executor.isShutdown());
    Assert.assertFalse(subtask.isSubmittingSelf());
  }
}
