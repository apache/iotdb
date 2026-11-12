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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleQueryTerminatorTest {

  private ScheduledExecutorService scheduledExecutor;
  private SimpleQueryTerminator terminator;

  @Before
  public void setUp() {
    scheduledExecutor = mock(ScheduledExecutorService.class);
    final MPPQueryContext queryContext = mock(MPPQueryContext.class);
    when(queryContext.getQueryId()).thenReturn(new QueryId("test_query"));
    when(queryContext.getEndPointBlackList()).thenReturn(Collections.emptySet());
    terminator =
        new SimpleQueryTerminator(
            scheduledExecutor,
            queryContext,
            Collections.emptyList(),
            null,
            mock(IFragInstanceStateTracker.class));
  }

  @Test
  public void terminateWaitsForTheGracePeriod() {
    terminator.terminate(new RuntimeException("query failed"));

    verify(scheduledExecutor).schedule(any(Callable.class), eq(1000L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void terminateImmediatelySkipsTheGracePeriod() {
    terminator.terminateImmediately(new RuntimeException("dispatch failed"));

    verify(scheduledExecutor).schedule(any(Callable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }
}
