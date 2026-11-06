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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.schemaengine.metric.SchemaMetricManager;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

  @Test
  public void errorDuringSchemaRegionDeletionRestoresRegion() throws Exception {
    assertFailedDeletionRestoresRegion(new AssertionError("delete failed"));
  }

  @Test
  public void inFlightSchemaWriteDuringDeletionRemainsRetryable() throws Exception {
    final SchemaEngine schemaEngine = newSchemaEngine();
    final SchemaRegionId regionId = new SchemaRegionId(3);
    final ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    Mockito.when(schemaRegion.getDatabaseFullPath()).thenReturn("root.sg");
    putSchemaRegion(schemaEngine, regionId, schemaRegion);
    final DataNodeRegionManager regionManager = newRegionManager(schemaEngine, regionId);
    final ReentrantReadWriteLock lock = regionManager.getRegionLock(regionId);
    final RegionWriteExecutor writeExecutor = newWriteExecutor(schemaEngine, regionManager);
    final CountDownLatch writeStarted = new CountDownLatch(1);
    final CountDownLatch continueWrite = new CountDownLatch(1);
    final CountDownLatch teardownStarted = new CountDownLatch(1);
    final CountDownLatch continueTeardown = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              // The write retains a region handle obtained before deletion starts.
              writeStarted.countDown();
              assertTrue(continueWrite.await(10, TimeUnit.SECONDS));
              throw new IllegalStateException("schema region was cleared during validation");
            })
        .when(schemaRegion)
        .checkSchemaQuota(Mockito.any(PartialPath.class), Mockito.anyInt());
    Mockito.doAnswer(
            invocation -> {
              teardownStarted.countDown();
              assertTrue(continueTeardown.await(10, TimeUnit.SECONDS));
              return null;
            })
        .when(schemaRegion)
        .deleteSchemaRegion();

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<RegionExecutionResult> write =
          executor.submit(() -> writeExecutor.execute(regionId, newCreateTimeSeriesNode()));
      assertTrue(writeStarted.await(10, TimeUnit.SECONDS));
      final Future<Boolean> deletion =
          executor.submit(() -> schemaEngine.deleteSchemaRegion(regionId));
      assertTrue(teardownStarted.await(10, TimeUnit.SECONDS));
      // Per-region deletion does not use the engine-wide shutdown flag or remove this lock.
      assertFalse(schemaEngine.isClearing());
      assertSame(lock, regionManager.getRegionLock(regionId));
      continueWrite.countDown();
      final RegionExecutionResult result = write.get(10, TimeUnit.SECONDS);
      assertFalse(result.isAccepted());
      assertEquals(
          TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode(), result.getStatus().getCode());
      continueTeardown.countDown();
      assertTrue(deletion.get(10, TimeUnit.SECONDS));
    } finally {
      continueWrite.countDown();
      continueTeardown.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void availableSchemaRegionWriteFailureRemainsInternalError() throws Exception {
    final SchemaEngine schemaEngine = newSchemaEngine();
    final SchemaRegionId regionId = new SchemaRegionId(4);
    final ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);
    putSchemaRegion(schemaEngine, regionId, schemaRegion);
    Mockito.doThrow(new IllegalStateException("validation failed"))
        .when(schemaRegion)
        .checkSchemaQuota(Mockito.any(PartialPath.class), Mockito.anyInt());

    final RegionExecutionResult result =
        newWriteExecutor(schemaEngine, newRegionManager(schemaEngine, regionId))
            .execute(regionId, newCreateTimeSeriesNode());

    assertFalse(result.isAccepted());
    assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.getStatus().getCode());
    assertSame(schemaRegion, schemaEngine.getSchemaRegion(regionId));
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
      Mockito.doNothing().when(schemaRegion).deleteSchemaRegion();
      assertTrue(schemaEngine.deleteSchemaRegion(regionId));
      assertNull(schemaEngine.getSchemaRegion(regionId));
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
  private static DataNodeRegionManager newRegionManager(
      final SchemaEngine schemaEngine, final SchemaRegionId regionId) throws Exception {
    final Constructor<DataNodeRegionManager> constructor =
        DataNodeRegionManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final DataNodeRegionManager regionManager = constructor.newInstance();
    final Field schemaEngineField = DataNodeRegionManager.class.getDeclaredField("schemaEngine");
    schemaEngineField.setAccessible(true);
    schemaEngineField.set(regionManager, schemaEngine);
    final Field lockMapField = DataNodeRegionManager.class.getDeclaredField("schemaRegionLockMap");
    lockMapField.setAccessible(true);
    ((Map<SchemaRegionId, ReentrantReadWriteLock>) lockMapField.get(regionManager))
        .put(regionId, new ReentrantReadWriteLock());
    return regionManager;
  }

  private static RegionWriteExecutor newWriteExecutor(
      final SchemaEngine schemaEngine, final DataNodeRegionManager regionManager) {
    return new RegionWriteExecutor(
        Mockito.mock(IConsensus.class),
        Mockito.mock(IConsensus.class),
        regionManager,
        schemaEngine,
        Mockito.mock(ClusterTemplateManager.class),
        Mockito.mock(TriggerFireVisitor.class));
  }

  private static CreateTimeSeriesNode newCreateTimeSeriesNode() throws Exception {
    return new CreateTimeSeriesNode(
        new PlanNodeId("create"),
        new MeasurementPath("root.sg.d.s"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        null,
        null);
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
