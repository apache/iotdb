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

package org.apache.iotdb.db.pipe.execution.executor;

import org.apache.iotdb.db.pipe.task.callable.PipeAssignerSubtask;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

public class PipeAssignerSubtaskExecutorTest {
  private PipeAssignerSubtaskExecutor executor;

  @Before
  public void setUp() throws Exception {
    executor = new PipeAssignerSubtaskExecutor();
  }

  @After
  public void tearDown() throws Exception {
    if (!executor.getExecutorService().isShutdown()) {
      executor.stop();
    }
  }

  @Test
  public void testSubmit() throws Exception {

    PipeAssignerSubtask subtask =
        new PipeAssignerSubtask("testProcessorSubtask") {
          @Override
          public Void call() {
            return null;
          }
        };
    subtask.setListeningExecutorService(executor.getExecutorService());
    PipeAssignerSubtask spySubtask = Mockito.spy(subtask);

    executor.submit(spySubtask);

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    verify(spySubtask, atLeast(10)).call();
  }

  @Test
  public void testStop() {
    executor.stop();
    ListeningExecutorService executorService = executor.getExecutorService();
    Assert.assertTrue(executorService.isShutdown());
  }
}
