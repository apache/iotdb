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

package org.apache.iotdb.db.service;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConsensusReadinessTest {

  @Test
  public void testReadyOnlyAfterBothConsensusServicesStart() {
    DataNode.ConsensusReadinessContext context = new DataNode.ConsensusReadinessContext();

    context.markSchemaRegionConsensusStarted();
    Assert.assertFalse(context.isAllConsensusStarted());

    context.markDataRegionConsensusStarted();
    Assert.assertTrue(context.isAllConsensusStarted());
  }

  @Test
  public void testReadinessIsVisibleAcrossThreads() throws Exception {
    DataNode.ConsensusReadinessContext context = new DataNode.ConsensusReadinessContext();
    context.markSchemaRegionConsensusStarted();
    CountDownLatch waiterStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> result =
          executor.submit(
              () -> {
                waiterStarted.countDown();
                return context.awaitAllConsensusStarted(1, TimeUnit.SECONDS);
              });
      Assert.assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
      Assert.assertFalse(context.isAllConsensusStarted());
      Assert.assertFalse(result.isDone());

      context.markDataRegionConsensusStarted();

      Assert.assertTrue(result.get(1, TimeUnit.SECONDS));
      Assert.assertTrue(context.isAllConsensusStarted());
    } finally {
      executor.shutdownNow();
    }
  }
}
