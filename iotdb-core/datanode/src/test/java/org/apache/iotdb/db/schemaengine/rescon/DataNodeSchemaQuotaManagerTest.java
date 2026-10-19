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

package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DataNodeSchemaQuotaManagerTest {

  private final DataNodeSchemaQuotaManager manager = DataNodeSchemaQuotaManager.getInstance();

  @After
  public void tearDown() {
    manager.updateRemain(-1, -1);
  }

  @Test
  public void testLocalCountExceedsStaleClusterCount() throws Exception {
    manager.updateQuota(100, 20, 90, -1);
    assertQuotaExceeded(11, 0);
    manager.check(10, 0);
    assertQuotaExceeded(1, 0);
  }

  @Test
  public void testClusterCountExceedsLocalCount() throws Exception {
    manager.updateQuota(100, 90, 20, -1);
    assertQuotaExceeded(11, 0);
    manager.check(10, 0);
    // The two counts describe overlapping series, so they must not be added together.
    manager.check(10, 0);
  }

  @Test
  public void testConsecutiveCreationsBetweenHeartbeats() throws Exception {
    manager.updateQuota(10, 0, 0, -1);
    manager.check(6, 1);
    assertQuotaExceeded(5, 1);
    manager.check(4, 1);
    assertQuotaExceeded(1, 1);
  }

  @Test
  public void testHeartbeatRefreshAndDeletion() throws Exception {
    manager.updateQuota(10, 0, 0, -1);
    manager.check(6, 0);
    manager.updateQuota(10, 2, 6, -1);
    assertQuotaExceeded(5, 0);
    manager.check(4, 0);

    // A later heartbeat accounts for deleted series and makes their quota available again.
    manager.updateQuota(10, 3, 3, -1);
    manager.check(7, 0);
    assertQuotaExceeded(1, 0);
  }

  @Test
  public void testQuotaChangeAndUnlimitedQuota() throws Exception {
    manager.updateQuota(-1, 100, 100, -1);
    assertFalse(manager.isMeasurementLimit());
    manager.check(Long.MAX_VALUE, 0);

    manager.updateQuota(5, 6, 6, -1);
    assertQuotaExceeded(1, 0);
    manager.check(0, 0);

    manager.updateQuota(10, 6, 6, -1);
    manager.check(4, 0);
    assertQuotaExceeded(1, 0);

    manager.updateQuota(0, 0, 0, -1);
    assertTrue(manager.isMeasurementLimit());
    assertQuotaExceeded(1, 0);
    manager.check(0, 0);
  }

  @Test
  public void testRequestCountDoesNotOverflow() throws Exception {
    manager.updateQuota(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, -1);
    assertQuotaExceeded(Long.MAX_VALUE, 0);
    manager.check(1, 0);
    assertQuotaExceeded(1, 0);
  }

  @Test
  public void testRejectedMeasurementRequestRollsBackDeviceQuota() throws Exception {
    manager.updateQuota(1, 0, 0, 1);
    assertQuotaExceeded(2, 1);
    manager.check(1, 1);
    final SchemaQuotaExceededException exception =
        assertThrows(SchemaQuotaExceededException.class, () -> manager.check(0, 1));
    assertEquals(
        new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.DEVICE).getMessage(),
        exception.getMessage());
  }

  @Test
  public void testBatchDeviceQuotaIsCheckedAndReservedTogether() throws Exception {
    manager.updateQuota(5, 0, 0, 2);
    final SchemaQuotaExceededException exception =
        assertThrows(SchemaQuotaExceededException.class, () -> manager.check(3, 3));
    assertEquals(
        new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.DEVICE).getMessage(),
        exception.getMessage());
    // Neither a device failure nor a measurement failure can leave a partial reservation.
    assertQuotaExceeded(6, 2);
    manager.check(5, 2);
    assertThrows(SchemaQuotaExceededException.class, () -> manager.check(0, 1));
  }

  @Test
  public void testOlderConfigNodeRemainingQuota() throws Exception {
    manager.updateQuota(0, 0, 0, -1);
    manager.updateRemain(3, -1);
    assertQuotaExceeded(4, 0);
    manager.check(3, 0);
    assertQuotaExceeded(1, 0);
    manager.check(0, 0);
    manager.updateRemain(-1, -1);
    manager.check(Long.MAX_VALUE, 0);
  }

  @Test
  public void testConcurrentCreationsShareLocalCount() throws Exception {
    manager.updateQuota(10, 0, 0, -1);
    final ExecutorService executor = Executors.newFixedThreadPool(8);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<Boolean>> results = new ArrayList<>();
    try {
      for (int i = 0; i < 32; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  try {
                    manager.check(1, 0);
                    return true;
                  } catch (SchemaQuotaExceededException e) {
                    return false;
                  }
                }));
      }
      start.countDown();
      int accepted = 0;
      for (Future<Boolean> result : results) {
        if (result.get(10, TimeUnit.SECONDS)) {
          accepted++;
        }
      }
      assertEquals(10, accepted);
      assertQuotaExceeded(1, 0);
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private void assertQuotaExceeded(long measurements, int devices) {
    final SchemaQuotaExceededException exception =
        assertThrows(
            SchemaQuotaExceededException.class, () -> manager.check(measurements, devices));
    assertEquals(TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode(), exception.getErrorCode());
    assertEquals(
        new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.TIMESERIES).getMessage(),
        exception.getMessage());
  }
}
