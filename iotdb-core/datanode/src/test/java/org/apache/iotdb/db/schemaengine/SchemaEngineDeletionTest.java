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

package org.apache.iotdb.db.schemaengine;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.schemaengine.metric.SchemaMetricManager;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaEngineDeletionTest {

  @Test
  public void deleteSchemaRegionHidesRegionDuringTeardown() throws Exception {
    final SchemaEngine schemaEngine = newSchemaEngine();
    final SchemaRegionId regionId = new SchemaRegionId(1);
    final ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn("root.sg");
    final CountDownLatch teardownStarted = new CountDownLatch(1);
    final CountDownLatch continueTeardown = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              teardownStarted.countDown();
              assertTrue(continueTeardown.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(schemaRegion)
        .deleteSchemaRegion();
    putSchemaRegion(schemaEngine, regionId, schemaRegion);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<Boolean> deletion =
          executor.submit(() -> schemaEngine.deleteSchemaRegion(regionId));
      assertTrue(teardownStarted.await(5, TimeUnit.SECONDS));
      assertNull(schemaEngine.getSchemaRegion(regionId));
      continueTeardown.countDown();
      assertTrue(deletion.get(5, TimeUnit.SECONDS));
      assertNull(schemaEngine.getSchemaRegion(regionId));
    } finally {
      continueTeardown.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void failedSchemaRegionDeletionRestoresRegion() throws Exception {
    assertFailedDeletionRestoresRegion(new MetadataException("delete failed"));
  }

  @Test
  public void runtimeFailureDuringSchemaRegionDeletionRestoresRegion() throws Exception {
    assertFailedDeletionRestoresRegion(new IllegalStateException("delete failed"));
  }

  private static void assertFailedDeletionRestoresRegion(final Throwable deletionFailure)
      throws Exception {
    final SchemaEngine schemaEngine = newSchemaEngine();
    final SchemaRegionId regionId = new SchemaRegionId(2);
    final ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn("root.sg");
    Mockito.doThrow(deletionFailure).when(schemaRegion).deleteSchemaRegion();
    putSchemaRegion(schemaEngine, regionId, schemaRegion);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<Boolean> deletion =
          executor.submit(() -> schemaEngine.deleteSchemaRegion(regionId));
      try {
        deletion.get(5, TimeUnit.SECONDS);
        fail("Expected schema region deletion to fail");
      } catch (final ExecutionException e) {
        assertSame(deletionFailure, e.getCause());
      }
      assertSame(schemaRegion, schemaEngine.getSchemaRegion(regionId));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static SchemaEngine newSchemaEngine() throws Exception {
    final Constructor<SchemaEngine> constructor = SchemaEngine.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final SchemaEngine schemaEngine = constructor.newInstance();
    final Field metricManagerField = SchemaEngine.class.getDeclaredField("schemaMetricManager");
    metricManagerField.setAccessible(true);
    metricManagerField.set(schemaEngine, Mockito.mock(SchemaMetricManager.class));
    return schemaEngine;
  }

  @SuppressWarnings("unchecked")
  private static void putSchemaRegion(
      final SchemaEngine schemaEngine,
      final SchemaRegionId regionId,
      final ISchemaRegion schemaRegion)
      throws Exception {
    final Field regionMapField = SchemaEngine.class.getDeclaredField("schemaRegionMap");
    regionMapField.setAccessible(true);
    ((Map<SchemaRegionId, ISchemaRegion>) regionMapField.get(schemaEngine))
        .put(regionId, schemaRegion);
  }
}
