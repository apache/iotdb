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

package org.apache.iotdb.commons.pipe.task;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeAbstractSinkSubtask;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PipeSleepIntervalTest {
  private static final long INIT_SLEEP_INTERVAL_MS = 25L;
  private static final long MAX_SLEEP_INTERVAL_MS = 50L;
  private static final long SLEEP_ASSERTION_TOLERANCE_MS = 5L;

  private long oldPipeSinkSubtaskSleepIntervalInitMs;
  private long oldPipeSinkSubtaskSleepIntervalMaxMs;

  @Before
  public void setUp() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    oldPipeSinkSubtaskSleepIntervalInitMs = config.getPipeSinkSubtaskSleepIntervalInitMs();
    oldPipeSinkSubtaskSleepIntervalMaxMs = config.getPipeSinkSubtaskSleepIntervalMaxMs();
    config.setPipeSinkSubtaskSleepIntervalInitMs(INIT_SLEEP_INTERVAL_MS);
    config.setPipeSinkSubtaskSleepIntervalMaxMs(MAX_SLEEP_INTERVAL_MS);
  }

  @After
  public void tearDown() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setPipeSinkSubtaskSleepIntervalInitMs(oldPipeSinkSubtaskSleepIntervalInitMs);
    config.setPipeSinkSubtaskSleepIntervalMaxMs(oldPipeSinkSubtaskSleepIntervalMaxMs);
  }

  @Test
  public void testSleepIntervalStopsIncreasingAtMax() {
    try (final TestSinkSubtask subtask = new TestSinkSubtask()) {
      Assert.assertEquals(INIT_SLEEP_INTERVAL_MS, subtask.getSleepInterval());

      assertSleepAtLeast(subtask, MAX_SLEEP_INTERVAL_MS);
      Assert.assertEquals(MAX_SLEEP_INTERVAL_MS, subtask.getSleepInterval());

      assertSleepAtLeast(subtask, MAX_SLEEP_INTERVAL_MS);
      Assert.assertEquals(MAX_SLEEP_INTERVAL_MS, subtask.getSleepInterval());
    }
  }

  private static void assertSleepAtLeast(
      final PipeAbstractSinkSubtask subtask, final long expectedSleepMs) {
    final long startTime = System.nanoTime();
    subtask.sleep4NonReportException();

    Assert.assertTrue(
        System.nanoTime() - startTime
            >= TimeUnit.MILLISECONDS.toNanos(expectedSleepMs - SLEEP_ASSERTION_TOLERANCE_MS));
  }

  private static class TestSinkSubtask extends PipeAbstractSinkSubtask {

    private TestSinkSubtask() {
      super(null, 0, null);
    }

    private long getSleepInterval() {
      return sleepInterval;
    }

    @Override
    protected String getRootCause(final Throwable throwable) {
      return null;
    }

    @Override
    protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {}

    @Override
    protected boolean executeOnce() {
      return false;
    }
  }
}
