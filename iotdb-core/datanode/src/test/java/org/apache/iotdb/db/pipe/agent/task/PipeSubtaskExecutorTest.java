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
