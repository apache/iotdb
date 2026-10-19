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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriver;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceFinishedException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import io.airlift.units.Duration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class SeriesScanUtilCancellationTest {

  private final boolean aligned;
  private final boolean ignoreAllNullRows;
  private final IDeviceID device;
  private int loadedFiles;
  private int previousDataNodeId;

  @Parameterized.Parameters(name = "aligned={0}, ignoreAllNullRows={1}")
  public static Object[][] parameters() {
    return new Object[][] {{false, true}, {true, true}, {true, false}};
  }

  public SeriesScanUtilCancellationTest(boolean aligned, boolean ignoreAllNullRows) {
    this.aligned = aligned;
    this.ignoreAllNullRows = ignoreAllNullRows;
    this.device =
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            ignoreAllNullRows ? new String[] {"root.sg", "d"} : new String[] {"table", "d"});
  }

  @Before
  public void setUp() {
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    // Resource cleanup initializes the query metrics and their coordinator.
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @Test
  public void testTerminalStateBeforeScanDoesNotLoadMetadata() {
    for (FragmentInstanceState state :
        new FragmentInstanceState[] {
          FragmentInstanceState.CANCELLED,
          FragmentInstanceState.ABORTED,
          FragmentInstanceState.FINISHED,
          FragmentInstanceState.FAILED
        }) {
      FragmentInstanceContext context = newContext(Runnable::run);
      SeriesScanUtil scanner = newScanner(context, Ordering.ASC, false, 3, count -> {});
      switch (state) {
        case CANCELLED:
          context.cancel();
          break;
        case ABORTED:
          context.abort();
          break;
        case FINISHED:
          context.finished();
          break;
        case FAILED:
          context.failed(new QueryTimeoutException());
          break;
        default:
          throw new AssertionError(state);
      }

      if (state == FragmentInstanceState.FINISHED) {
        assertThrows(FragmentInstanceFinishedException.class, scanner::hasNextFile);
      } else {
        IOException exception = assertThrows(IOException.class, scanner::hasNextFile);
        assertSame(context.getFailureCause().orElse(null), exception.getCause());
      }
      assertEquals(0, loadedFiles);
      assertEquals(state, context.getStateMachine().getState());
    }
  }

  @Test
  public void testCancellationDuringOverlappingFileScan() {
    for (Ordering ordering : new Ordering[] {Ordering.ASC, Ordering.DESC}) {
      for (boolean sequence : new boolean[] {true, false}) {
        FragmentInstanceContext context = newContext(Runnable::run);
        SeriesScanUtil scanner =
            newScanner(
                context,
                ordering,
                sequence,
                4,
                count -> {
                  if (count == 2) {
                    context.cancel();
                  }
                });

        assertThrows(IOException.class, scanner::hasNextFile);
        assertEquals(2, loadedFiles);
        assertEquals(FragmentInstanceState.CANCELLED, context.getStateMachine().getState());
      }
    }
  }

  @Test
  public void testTimeoutDuringLastFileReadPreservesCause() {
    for (boolean sequence : new boolean[] {true, false}) {
      FragmentInstanceContext context = newContext(Runnable::run);
      QueryTimeoutException timeout = new QueryTimeoutException();
      SeriesScanUtil scanner =
          newScanner(context, Ordering.ASC, sequence, 1, count -> context.failed(timeout));

      IOException exception = assertThrows(IOException.class, scanner::hasNextFile);
      assertSame(timeout, exception.getCause());
      assertEquals(1, loadedFiles);
      assertEquals(FragmentInstanceState.FAILED, context.getStateMachine().getState());
    }
  }

  @Test
  public void testActiveQueryStillLoadsOverlappingFiles() throws IOException {
    for (boolean flushing : new boolean[] {false, true}) {
      FragmentInstanceContext context = newContext(Runnable::run);
      if (flushing) {
        context.transitionToFlushing();
      }
      SeriesScanUtil scanner = newScanner(context, Ordering.ASC, false, 4, count -> {});

      assertTrue(scanner.hasNextFile().get());
      assertEquals(4, loadedFiles);
      assertEquals(
          flushing ? FragmentInstanceState.FLUSHING : FragmentInstanceState.RUNNING,
          context.getStateMachine().getState());
    }
  }

  @Test
  public void testCancellationWithCachedMetadata() throws IOException {
    FragmentInstanceContext context = newContext(Runnable::run);
    SeriesScanUtil scanner = newScanner(context, Ordering.ASC, false, 4, count -> {});
    assertTrue(scanner.hasNextFile().get());
    context.cancel();

    assertThrows(IOException.class, scanner::hasNextFile);
    assertEquals(4, loadedFiles);
  }

  @Test
  public void testCancellationDuringMissingMetadataRead() {
    for (boolean sequence : new boolean[] {true, false}) {
      FragmentInstanceContext context = newContext(Runnable::run);
      SeriesScanUtil scanner =
          newScanner(context, Ordering.ASC, sequence, 1, count -> context.cancel(), true);

      assertThrows(IOException.class, scanner::hasNextFile);
      assertEquals(1, loadedFiles);
    }
  }

  @Test
  public void testCompactionContextWithoutStateMachine() throws IOException {
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContextForCompaction(0);
    SeriesScanUtil scanner = newScanner(context, Ordering.ASC, false, 2, count -> {});

    assertTrue(scanner.hasNextFile().get());
    assertEquals(2, loadedFiles);
  }

  @Test(timeout = 15000)
  public void testTimeoutUnblocksDriverResourceCleanup() throws Exception {
    assertFailureUnblocksDriverResourceCleanup(new QueryTimeoutException());
  }

  @Test(timeout = 15000)
  public void testSemanticFailureUnblocksDriverResourceCleanup() throws Exception {
    assertFailureUnblocksDriverResourceCleanup(
        new SemanticException("Scalar sub-query has returned multiple rows."));
  }

  private void assertFailureUnblocksDriverResourceCleanup(RuntimeException failure)
      throws Exception {
    ExecutorService notifications = Executors.newSingleThreadExecutor();
    FragmentInstanceContext context = newContext(notifications);
    context.initializeNumOfDrivers(1);
    try {
      CountDownLatch closeRequested = new CountDownLatch(1);
      SeriesScanUtil scanner =
          newScanner(
              context,
              Ordering.ASC,
              false,
              4,
              count -> {
                if (count == 2) {
                  context.failed(failure);
                  try {
                    // The notification thread has requested close while this thread owns the
                    // driver lock, just as in a query that is still loading metadata.
                    assertTrue(closeRequested.await(5, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                }
              });
      Operator operator = mock(Operator.class);
      doReturn(NOT_BLOCKED).when(operator).isBlocked();
      when(operator.hasNextWithTimer()).thenReturn(true);
      when(operator.nextWithTimer())
          .thenAnswer(
              invocation -> {
                scanner.hasNextFile();
                return null;
              });
      ISink sink = mock(ISink.class);
      doReturn(NOT_BLOCKED).when(sink).isFull();
      DataDriverContext driverContext = new DataDriverContext(context, 0);
      driverContext.setSink(sink);
      DataDriver driver =
          new DataDriver(operator, driverContext, 0) {
            @Override
            public void close() {
              super.close();
              closeRequested.countDown();
            }
          };
      MPPDataExchangeManager exchangeManager = mock(MPPDataExchangeManager.class);
      FragmentInstanceExecution.createFragmentInstanceExecution(
          mock(IDriverScheduler.class),
          context.getId(),
          context,
          Collections.singletonList(driver),
          sink,
          context.getStateMachine(),
          1000,
          false,
          exchangeManager);

      assertSame(
          failure,
          assertThrows(
              RuntimeException.class, () -> driver.processFor(new Duration(1, TimeUnit.SECONDS))));
      // This can complete only after the preceding cleanup callback gets past allDriversClosed.
      notifications.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertEquals(2, loadedFiles);
      verify(operator).close();
      verify(context.getMemoryReservationContext()).releaseAllReservedMemory();
      verify(exchangeManager)
          .deRegisterFragmentInstanceFromMemoryPool(
              context.getId().getQueryId().getId(), context.getId().getFragmentInstanceId(), true);
    } finally {
      // Ensure a failed assertion cannot leave the notification thread waiting on the latch.
      context.decrementNumOfUnClosedDriver();
      notifications.shutdownNow();
      assertTrue(notifications.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 60000)
  public void testFinishedScanUnblocksDriverResourceCleanup() throws Exception {
    for (boolean sequence : new boolean[] {true, false}) {
      for (boolean waitForClose : new boolean[] {false, true}) {
        assertFinishedScanUnblocksDriverResourceCleanup(sequence, waitForClose);
      }
    }
  }

  private void assertFinishedScanUnblocksDriverResourceCleanup(
      boolean sequence, boolean waitForClose) throws Exception {
    ExecutorService notifications = Executors.newSingleThreadExecutor();
    ExecutorService worker = Executors.newSingleThreadExecutor();
    CountDownLatch allowNotifications = new CountDownLatch(waitForClose ? 0 : 1);
    CountDownLatch scanEntered = new CountDownLatch(1);
    CountDownLatch resumeScan = new CountDownLatch(1);
    CountDownLatch closeRequested = new CountDownLatch(1);
    // Cover both orderings: the FI is FINISHED before driver.close(), and close is already pending.
    notifications.submit(() -> await(allowNotifications));
    FragmentInstanceContext context = newContext(notifications);
    context.initializeNumOfDrivers(1);
    Operator operator = mock(Operator.class);
    doReturn(NOT_BLOCKED).when(operator).isBlocked();
    when(operator.hasNextWithTimer()).thenReturn(true);
    SeriesScanUtil scanner =
        newScanner(
            context,
            Ordering.ASC,
            sequence,
            4,
            count -> {
              if (count == 1) {
                scanEntered.countDown();
                await(resumeScan);
              }
            });
    when(operator.nextWithTimer())
        .thenAnswer(
            invocation -> {
              scanner.hasNextFile();
              return null;
            });
    ISink sink = mock(ISink.class);
    doReturn(NOT_BLOCKED).when(sink).isFull();
    DataDriverContext driverContext = new DataDriverContext(context, 0);
    driverContext.setSink(sink);
    DataDriver driver =
        new DataDriver(operator, driverContext, 0) {
          @Override
          public void close() {
            super.close();
            closeRequested.countDown();
          }
        };
    MPPDataExchangeManager exchangeManager = mock(MPPDataExchangeManager.class);
    IDriverScheduler scheduler = mock(IDriverScheduler.class);
    FragmentInstanceExecution.createFragmentInstanceExecution(
        scheduler,
        context.getId(),
        context,
        Collections.singletonList(driver),
        sink,
        context.getStateMachine(),
        1000,
        false,
        exchangeManager);
    try {
      Future<?> execution =
          worker.submit(() -> driver.processFor(new Duration(1, TimeUnit.SECONDS)));
      await(scanEntered);
      context.finished();
      if (waitForClose) {
        await(closeRequested);
      }
      resumeScan.countDown();
      execution.get(5, TimeUnit.SECONDS);
      assertTrue(driver.isFinished());
      assertEquals(1, loadedFiles);
      assertEquals(FragmentInstanceState.FINISHED, context.getStateMachine().getState());
      assertTrue(context.getStateMachine().getFailureCauses().isEmpty());
      allowNotifications.countDown();
      notifications.submit(() -> {}).get(5, TimeUnit.SECONDS);
      verify(operator).close();
      verify(context.getMemoryReservationContext()).releaseAllReservedMemory();
      verify(exchangeManager)
          .deRegisterFragmentInstanceFromMemoryPool(
              context.getId().getQueryId().getId(), context.getId().getFragmentInstanceId(), true);
      verify(scheduler, never()).abortFragmentInstance(any(), any());
    } finally {
      resumeScan.countDown();
      allowNotifications.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
      driver.close();
      context.decrementNumOfUnClosedDriver();
      // Let FI cleanup finish after opening its latch, without interrupting its driver-close wait.
      notifications.shutdown();
      assertTrue(notifications.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFinishedFragmentDoesNotSuppressIOException() throws Exception {
    FragmentInstanceContext context = newContext(Runnable::run);
    context.initializeNumOfDrivers(1);
    IOException failure = new IOException("Failed to read file metadata");
    Operator operator = mock(Operator.class);
    doReturn(NOT_BLOCKED).when(operator).isBlocked();
    when(operator.hasNextWithTimer()).thenReturn(true);
    when(operator.nextWithTimer())
        .thenAnswer(
            invocation -> {
              context.finished();
              throw failure;
            });
    ISink sink = mock(ISink.class);
    doReturn(NOT_BLOCKED).when(sink).isFull();
    DataDriverContext driverContext = new DataDriverContext(context, 0);
    driverContext.setSink(sink);
    DataDriver driver = new DataDriver(operator, driverContext, 0);
    try {
      RuntimeException exception =
          assertThrows(
              RuntimeException.class, () -> driver.processFor(new Duration(1, TimeUnit.SECONDS)));
      assertSame(failure, exception.getCause());
      assertSame(failure, context.getFailureCause().get());
    } finally {
      driver.close();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private FragmentInstanceContext newContext(Executor executor) {
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("scan_cancellation"), 0), "0");
    return FragmentInstanceContext.createFragmentInstanceContext(
        id, new FragmentInstanceStateMachine(id, executor), mock(MemoryReservationManager.class));
  }

  private SeriesScanUtil newScanner(
      FragmentInstanceContext context,
      Ordering ordering,
      boolean sequence,
      int fileCount,
      IntConsumer onRead) {
    return newScanner(context, ordering, sequence, fileCount, onRead, false);
  }

  private SeriesScanUtil newScanner(
      FragmentInstanceContext context,
      Ordering ordering,
      boolean sequence,
      int fileCount,
      IntConsumer onRead,
      boolean missingMetadata) {
    context.setIgnoreAllNullRows(ignoreAllNullRows);
    loadedFiles = 0;
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < fileCount; i++) {
      TsFileResource resource = mock(TsFileResource.class);
      when(resource.getOrderTimeForSeq(device, true)).thenReturn(0L);
      when(resource.getOrderTimeForSeq(device, false)).thenReturn(100L);
      when(resource.getOrderTimeForUnseq(device, true)).thenReturn(0L);
      when(resource.getOrderTimeForUnseq(device, false)).thenReturn(100L);
      resources.add(resource);
    }
    QueryDataSource source =
        new QueryDataSource(
            sequence ? resources : Collections.emptyList(),
            sequence ? Collections.emptyList() : resources);
    source.setSingleDevice(true);
    IntegerStatistics statistics = new IntegerStatistics();
    statistics.update(0L, 0);
    statistics.update(100L, 100);
    ITimeSeriesMetadata metadata =
        aligned ? mock(AbstractAlignedTimeSeriesMetadata.class) : mock(ITimeSeriesMetadata.class);
    doReturn(statistics).when(metadata).getStatistics();
    when(metadata.typeMatch(anyList())).thenReturn(true);
    MeasurementSchema schema = new MeasurementSchema("s", TSDataType.INT32);
    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(Collections.singleton("s"));
    SeriesScanUtil scanner;
    if (aligned) {
      scanner =
          new AlignedSeriesScanUtil(
              new AlignedFullPath(
                  device, Collections.singletonList("s"), Collections.singletonList(schema)),
              ordering,
              builder.build(),
              context) {
            @Override
            protected AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
                TsFileResource resource, boolean isSeq) {
              onRead.accept(++loadedFiles);
              return missingMetadata ? null : (AbstractAlignedTimeSeriesMetadata) metadata;
            }
          };
    } else {
      scanner =
          new SeriesScanUtil(
              new NonAlignedFullPath(device, schema), ordering, builder.build(), context) {
            @Override
            protected ITimeSeriesMetadata loadTimeSeriesMetadata(
                TsFileResource resource, boolean isSeq) {
              onRead.accept(++loadedFiles);
              return missingMetadata ? null : metadata;
            }
          };
    }
    scanner.initQueryDataSource(source);
    return scanner;
  }
}
