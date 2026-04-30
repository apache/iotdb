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

package org.apache.iotdb.confignode.it.partition;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class DataPartitionTableIntegrityCheckProcedureIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataPartitionTableIntegrityCheckProcedureIT.class);

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataReplicationFactor(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testConcurrentSubmitDataPartitionTableIntegrityCheckProcedure()
      throws InterruptedException {
    final int threadCount = 10;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishLatch = new CountDownLatch(threadCount);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger failCount = new AtomicInteger(0);
    final List<String> failureMessages = Collections.synchronizedList(new ArrayList<>());

    // Concurrently submit the DataPartitionTableIntegrityCheckProcedure
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement stmt = connection.createStatement()) {
                stmt.execute("REPAIR DATA PARTITION TABLE");
                successCount.incrementAndGet();
                LOGGER.info("Thread {} submitted integrity check successfully", threadId);
              }
            } catch (final SQLException e) {
              failCount.incrementAndGet();
              failureMessages.add("Thread " + threadId + " failed: " + e.getMessage());
              LOGGER.info(
                  "Thread {} failed to submit integrity check: {}", threadId, e.getMessage());
            } catch (final Exception e) {
              failCount.incrementAndGet();
              failureMessages.add("Thread " + threadId + " failed unexpectedly: " + e.getMessage());
              LOGGER.error("Thread {} unexpected error: {}", threadId, e.getMessage(), e);
            } finally {
              finishLatch.countDown();
            }
          });
    }

    startLatch.countDown();

    final boolean completed = finishLatch.await(60, TimeUnit.SECONDS);
    Assert.assertTrue("Not all threads completed within timeout", completed);

    executor.shutdown();
    Assert.assertTrue(
        "Executor did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));

    LOGGER.info("Success count: {}, Fail count: {}", successCount.get(), failCount.get());
    LOGGER.info("Failure messages: {}", failureMessages);

    Assert.assertEquals(
        "Only one procedure should be submitted successfully", 1, successCount.get());
    Assert.assertEquals(
        "The other concurrent submissions should be rejected", threadCount - 1, failCount.get());
  }
}
