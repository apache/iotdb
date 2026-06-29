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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipeSleepIntervalTest {
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
    try (final PipeAbstractSinkSubtask subtask =
        new PipeAbstractSinkSubtask(null, 0, null) {
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
        }) {
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
}
