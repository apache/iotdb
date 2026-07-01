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

package org.apache.iotdb.commons.pipe.resource.log;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipePeriodicalLogReducerTest {

  @After
  public void tearDown() {
    PipePeriodicalLogReducer.setMemoryResizeFunction(null);
  }

  @Test
  public void testLogReducesDuplicateMessages() {
    final AtomicInteger logCount = new AtomicInteger(0);
    final String message = "PipePeriodicalLogReducerTest-" + System.nanoTime();

    Assert.assertTrue(PipePeriodicalLogReducer.log(log -> logCount.incrementAndGet(), message));
    Assert.assertFalse(PipePeriodicalLogReducer.log(log -> logCount.incrementAndGet(), message));
    Assert.assertEquals(1, logCount.get());
  }

  @Test
  public void testUpdateUsesMemoryResizeFunction() {
    final AtomicLong requestedSizeInBytes = new AtomicLong(-1);
    final long allocatedSizeInBytes = 1024;

    PipePeriodicalLogReducer.setMemoryResizeFunction(
        sizeInBytes -> {
          requestedSizeInBytes.set(sizeInBytes);
          return allocatedSizeInBytes;
        });

    Assert.assertEquals(
        PipeConfig.getInstance().getPipeLoggerCacheMaxSizeInBytes(), requestedSizeInBytes.get());
    Assert.assertEquals(
        allocatedSizeInBytes,
        PipePeriodicalLogReducer.LOGGER_CACHE.policy().eviction().get().getMaximum());
  }
}
