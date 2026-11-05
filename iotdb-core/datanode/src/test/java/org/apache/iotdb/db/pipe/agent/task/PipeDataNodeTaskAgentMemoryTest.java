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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

public class PipeDataNodeTaskAgentMemoryTest {

  private static final int DATA_REGION_COUNT = 14;

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final PipeDataNodeTaskAgent taskAgent = new PipeDataNodeTaskAgent();
  private long originalQueueMemory;
  private double originalFloatingMemoryProportion;
  private long floatingMemorySizeInBytes;
  private Method memoryCheck;

  @Before
  public void setUp() throws Exception {
    originalQueueMemory = config.getPipeInsertNodeQueueMemory();
    originalFloatingMemoryProportion = config.getPipeTotalFloatingMemoryProportion();
    config.setPipeTotalFloatingMemoryProportion(0.5);
    floatingMemorySizeInBytes =
        PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes();
    Assert.assertTrue(floatingMemorySizeInBytes > 1);
    config.setPipeInsertNodeQueueMemory(floatingMemorySizeInBytes / 2);
    memoryCheck =
        PipeDataNodeTaskAgent.class.getDeclaredMethod(
            "calculateInsertNodeQueueMemory", PipeParameters.class, int.class);
    memoryCheck.setAccessible(true);
  }

  @After
  public void tearDown() {
    config.setPipeInsertNodeQueueMemory(originalQueueMemory);
    config.setPipeTotalFloatingMemoryProportion(originalFloatingMemoryProportion);
  }

  @Test
  public void testHybridQueueBudgetIsSharedAcrossDataRegions() throws Exception {
    checkMemory(Collections.emptyMap(), DATA_REGION_COUNT);
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_MODE_STREAMING_KEY,
          PipeSourceConstant.EXTRACTOR_MODE_STREAMING_KEY
        }) {
      checkMemory(Collections.singletonMap(key, "true"), DATA_REGION_COUNT);
    }
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_REALTIME_MODE_KEY,
          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY
        }) {
      for (final String mode :
          new String[] {
            PipeSourceConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE,
            PipeSourceConstant.EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE,
            PipeSourceConstant.EXTRACTOR_REALTIME_MODE_LOG_VALUE
          }) {
        checkMemory(Collections.singletonMap(key, mode), DATA_REGION_COUNT);
      }
    }
  }

  @Test
  public void testHybridQueueStillRequiresAvailableFloatingMemory() {
    config.setPipeInsertNodeQueueMemory(floatingMemorySizeInBytes + 1);
    final PipeException exception =
        Assert.assertThrows(
            PipeException.class, () -> checkMemory(Collections.emptyMap(), DATA_REGION_COUNT));
    Assert.assertTrue(
        exception.getMessage().contains(Long.toString(floatingMemorySizeInBytes + 1)));
  }

  @Test
  public void testForcedLogStillRequiresPerRegionMemory() throws Exception {
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_REALTIME_MODE_KEY,
          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY
        }) {
      final Map<String, String> attributes =
          Collections.singletonMap(
              key, PipeSourceConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE);
      checkMemory(attributes, 1);
      final PipeException exception =
          Assert.assertThrows(
              PipeException.class, () -> checkMemory(attributes, DATA_REGION_COUNT));
      Assert.assertTrue(
          exception
              .getMessage()
              .contains(Long.toString(config.getPipeInsertNodeQueueMemory() * DATA_REGION_COUNT)));
    }
  }

  @Test
  public void testSourcesWithoutTabletQueuesSkipFloatingMemoryCheck() throws Exception {
    config.setPipeInsertNodeQueueMemory(floatingMemorySizeInBytes + 1);
    checkMemory(Collections.emptyMap(), 0);
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY,
          PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY
        }) {
      checkMemory(Collections.singletonMap(key, "false"), DATA_REGION_COUNT);
    }
    for (final String mode :
        new String[] {
          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE,
          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE
        }) {
      for (final String key :
          new String[] {
            PipeSourceConstant.SOURCE_REALTIME_MODE_KEY,
            PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY
          }) {
        checkMemory(Collections.singletonMap(key, mode), DATA_REGION_COUNT);
      }
    }
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_MODE_STREAMING_KEY,
          PipeSourceConstant.EXTRACTOR_MODE_STREAMING_KEY
        }) {
      checkMemory(Collections.singletonMap(key, "false"), DATA_REGION_COUNT);
    }
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_MODE_SNAPSHOT_KEY,
          PipeSourceConstant.EXTRACTOR_MODE_SNAPSHOT_KEY
        }) {
      checkMemory(Collections.singletonMap(key, "true"), DATA_REGION_COUNT);
    }
    for (final String key :
        new String[] {PipeSourceConstant.SOURCE_MODE_KEY, PipeSourceConstant.EXTRACTOR_MODE_KEY}) {
      for (final String mode :
          new String[] {
            PipeSourceConstant.EXTRACTOR_MODE_SNAPSHOT_VALUE,
            PipeSourceConstant.EXTRACTOR_MODE_QUERY_VALUE
          }) {
        checkMemory(Collections.singletonMap(key, mode), DATA_REGION_COUNT);
      }
    }
  }

  private void checkMemory(final Map<String, String> attributes, final int dataRegionCount)
      throws Exception {
    try {
      memoryCheck.invoke(taskAgent, new PipeParameters(attributes), dataRegionCount);
    } catch (final InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }
}
