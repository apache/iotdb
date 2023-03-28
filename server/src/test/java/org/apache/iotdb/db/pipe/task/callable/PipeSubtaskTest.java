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

package org.apache.iotdb.db.pipe.task.callable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PipeSubtaskTest {
  ListeningExecutorService service;
  private PipeSubtask alwaysSucceedSubtask;
  private PipeSubtask alwaysFailSubtask;

  @Before
  public void setUp() {
    service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
    alwaysSucceedSubtask =
        new PipeSubtask("alwaysSucceedSubtask") {
          @Override
          public Void call() {
            return null;
          }
        };
    alwaysFailSubtask =
        new PipeSubtask("alwaysFailSubtask") {
          @Override
          public Void call() throws Exception {
            throw new Exception("Always failing task");
          }
        };
  }

  @Test
  public void testOnSuccess() {
    PipeSubtask spySubtask = Mockito.spy(alwaysSucceedSubtask).setListeningExecutorService(service);
    ListenableFuture<Void> future = service.submit(spySubtask);
    Futures.addCallback(future, spySubtask, service);

    try {
      future.get();
    } catch (Exception ignored) {
    }

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    verify(spySubtask, atLeast(10)).onSuccess(null);

    service.shutdown();
  }

  @Test
  public void testOnError() {
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
    PipeSubtask spySubtask = Mockito.spy(alwaysFailSubtask).setListeningExecutorService(service);
    ListenableFuture<Void> future = service.submit(spySubtask);
    Futures.addCallback(future, spySubtask, service);

    try {
      future.get();
    } catch (Exception ignored) {
    }

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    assertEquals(5, spySubtask.getRetryCount());
    verify(spySubtask, times(6)).onFailure(any(Throwable.class));

    service.shutdown();
  }
}
