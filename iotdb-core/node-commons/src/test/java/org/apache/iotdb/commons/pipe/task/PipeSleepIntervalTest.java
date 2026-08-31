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
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PipeSleepIntervalTest {
  private static class TestSinkSubtask extends PipeAbstractSinkSubtask {

    TestSinkSubtask() {
      super(null, 0, null);
    }

    @Override
    protected String getRootCause(Throwable throwable) {
      return null;
    }

    @Override
    protected void report(EnrichedEvent event, PipeRuntimeException exception) {}

    @Override
    protected boolean executeOnce() {
      return false;
    }

    long getSleepInterval(final Throwable throwable) {
      return getSleepIntervalBasedOnThrowable(throwable);
    }

    boolean isAuthenticationFailureException(final Throwable throwable) {
      return isAuthenticationFailure(throwable);
    }

    void sleepWithoutHighPriorityTask(final long sleepMillis) throws InterruptedException {
      sleepIfNoHighPriorityTask(sleepMillis);
    }
  }

  private long oldPipeSinkSubtaskSleepIntervalInitMs;
  private long oldPipeSinkSubtaskSleepIntervalMaxMs;

  @Before
  public void setUp() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    oldPipeSinkSubtaskSleepIntervalInitMs = config.getPipeSinkSubtaskSleepIntervalInitMs();
    oldPipeSinkSubtaskSleepIntervalMaxMs = config.getPipeSinkSubtaskSleepIntervalMaxMs();
    config.setPipeSinkSubtaskSleepIntervalInitMs(25L);
    config.setPipeSinkSubtaskSleepIntervalMaxMs(50L);
  }

  @After
  public void tearDown() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setPipeSinkSubtaskSleepIntervalInitMs(oldPipeSinkSubtaskSleepIntervalInitMs);
    config.setPipeSinkSubtaskSleepIntervalMaxMs(oldPipeSinkSubtaskSleepIntervalMaxMs);
  }

  @Test
  public void test() {
    try (final TestSinkSubtask subtask = new TestSinkSubtask()) {
      long startTime = System.currentTimeMillis();
      subtask.sleep4NonReportException();
      Assert.assertTrue(
          System.currentTimeMillis() - startTime
              >= PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs());
      startTime = System.currentTimeMillis() - startTime;
      subtask.sleep4NonReportException();
      Assert.assertTrue(
          System.currentTimeMillis() - startTime
              >= PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs());
    }
  }

  @Test
  public void testAuthenticationFailureRetryInterval() {
    try (final TestSinkSubtask subtask = new TestSinkSubtask()) {
      Assert.assertTrue(
          subtask.isAuthenticationFailureException(
              new PipeConnectionException(
                  "Handshake error with receiver 127.0.0.1:6667, code: 801, message: Authentication failed.")));
      Assert.assertTrue(
          subtask.isAuthenticationFailureException(
              new PipeConnectionException("801: Failed to check password for pipe a2b.")));
      Assert.assertTrue(
          subtask.isAuthenticationFailureException(
              new PipeConnectionException("status code: 822, message: Account is blocked.")));
      Assert.assertFalse(
          subtask.isAuthenticationFailureException(
              new PipeConnectionException("Network error 801 bytes sent.")));

      final long authenticationFailureRetryInterval =
          subtask.getSleepInterval(new PipeConnectionException("code: 801"));
      Assert.assertTrue(authenticationFailureRetryInterval > TimeUnit.MINUTES.toMillis(3));
      Assert.assertTrue(authenticationFailureRetryInterval * 3 > TimeUnit.MINUTES.toMillis(10));
      Assert.assertTrue(
          subtask.getSleepInterval(new PipeConnectionException("network error")) <= 10000);
    }
  }

  @Test
  public void testSleepIfNoHighPriorityTaskWaits() throws Exception {
    try (final TestSinkSubtask subtask = new TestSinkSubtask()) {
      final long startTime = System.currentTimeMillis();
      subtask.sleepWithoutHighPriorityTask(20L);
      Assert.assertTrue(System.currentTimeMillis() - startTime >= 15L);
    }
  }
}
