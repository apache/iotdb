/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PivotRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.memory.OperatorMemoryNotEnoughException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MPPQueryContextTest {

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  @Test
  public void cancellationPropagatesToDiscoveryContextAndListenersRegisteredAfterCancel() {
    MPPQueryContext parent = new MPPQueryContext(new QueryId("pivot_cancel_parent"));
    MPPQueryContext discovery = new MPPQueryContext(new QueryId("pivot_cancel_discovery"));
    AtomicInteger listenerCalls = new AtomicInteger();
    discovery.setParentQueryContext(parent);

    parent.cancelQuery();
    discovery.addCancellationListener(listenerCalls::incrementAndGet);

    Assert.assertTrue(discovery.isCancellationRequested());
    Assert.assertEquals(1, listenerCalls.get());
    IoTDBRuntimeException exception =
        Assert.assertThrows(IoTDBRuntimeException.class, discovery::checkTimeOut);
    Assert.assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), exception.getErrorCode());
  }

  @Test
  public void cancellationListenerIsInvokedOnceWhenRacingWithRegistration() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("pivot_cancel_listener"));
    AtomicInteger listenerCalls = new AtomicInteger();

    context.addCancellationListener(listenerCalls::incrementAndGet);
    context.cancelQuery();
    context.cancelQuery();

    Assert.assertEquals(1, listenerCalls.get());
  }

  @Test
  public void borrowedDynamicPivotInputSurvivesChildQueryCleanup() {
    Assume.assumeTrue(LocalExecutionPlanner.getInstance().getFreeMemoryForOperators() >= 128);
    MPPQueryContext owner = new MPPQueryContext(new QueryId("pivot_owner"));
    MPPQueryContext child = new MPPQueryContext(new QueryId("pivot_child"));
    PivotRelation pivot =
        new PivotRelation(
            null,
            new Table(QualifiedName.of("readings")),
            List.of(new PivotRelation.OnItem(new Identifier("region"), Optional.empty())),
            List.of(),
            Optional.empty(),
            true);
    NotThreadSafeMemoryReservationManager reservation =
        new NotThreadSafeMemoryReservationManager(owner.getQueryId(), "borrowed-pivot-test");
    reservation.reserveMemoryCumulatively(128);
    reservation.reserveMemoryImmediately();
    PivotInputStore store =
        new PivotInputStore(
            new CteDataStore(null, List.of(), 1024, 1), List.of(), List.of(), 0, reservation);
    owner.addPivotInputStore(pivot, store);
    child.borrowPivotInputStores(owner.getPivotInputStores());
    Assert.assertSame(store, child.getPivotInputStore(pivot));
    child.releasePivotInputStores();
    Assert.assertNull(child.getPivotInputStore(pivot));
    Assert.assertSame(store, owner.getPivotInputStore(pivot));
    Assert.assertEquals(128, reservation.getReservedBytesInTotalForTest());
    owner.releasePivotInputStores();
    Assert.assertNull(owner.getPivotInputStore(pivot));
    Assert.assertEquals(0, reservation.getReservedBytesInTotalForTest());
  }

  @Test
  public void dynamicPivotInputReservationOutlivesCoordinatorFrontEndCleanup() {
    Assume.assumeTrue(LocalExecutionPlanner.getInstance().getFreeMemoryForOperators() >= 128);
    MPPQueryContext owner = new MPPQueryContext(new QueryId("pivot_reservation"));
    NotThreadSafeMemoryReservationManager reservation =
        new NotThreadSafeMemoryReservationManager(owner.getQueryId(), "pivot-input-test");
    reservation.reserveMemoryCumulatively(128);
    reservation.reserveMemoryImmediately();
    PivotInputStore store =
        new PivotInputStore(
            new CteDataStore(null, List.of(), 1024, 1), List.of(), List.of(), 0, reservation);
    try {
      owner.releaseAllMemoryReservedForFrontEnd();
      Assert.assertEquals(128, reservation.getReservedBytesInTotalForTest());
    } finally {
      store.clear();
    }
    store.clear();
    Assert.assertEquals(0, reservation.getReservedBytesInTotalForTest());
  }

  @Test
  public void resultSetColumnMemoryNotEnoughExceptionContainsColumnDiagnostics() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("result_column_oom_test"));
    long failedRequestBytes = requestLargerThanFreeOperatorMemory();

    context.initResultSetColumnMemoryTracking(100, 5, false);
    context.recordMatchedSourceColumnsForResultSet(10);
    context.recordExpandedSourceColumnForResultSet(failedRequestBytes);
    context.recordGeneratedResultSetColumn(128);

    MemoryNotEnoughException exception =
        Assert.assertThrows(
            MemoryNotEnoughException.class,
            () -> context.reserveMemoryForFrontEnd(failedRequestBytes));

    String message = exception.getMessage();
    assertContains(message, "Not enough memory while analyzing metadata for query result columns.");
    assertContains(message, "The result set has too many columns.");
    assertContains(message, "matched 10 source columns");
    assertContains(message, "expanded 1 source columns");
    assertContains(message, "generated 1 result-set columns");
    assertContains(message, "exceed the estimated current memory capacity by at least");
    assertContains(message, "SLIMIT=100, SOFFSET=5");
    assertContains(message, "Use SLIMIT/SOFFSET");
    assertContains(message, "ALIGN BY DEVICE");
    assertContains(message, "increase query memory by at least");
    assertContains(message, "requested this time");
    assertContains(message, "current free memory");
    assertContains(message, "Original error:");
  }

  @Test
  public void resultSetColumnMemoryNotEnoughExceptionReportsOverageAfterExpansion() {
    // The old estimate returned zero when all (or all but one) source columns were expanded.
    Assume.assumeTrue(LocalExecutionPlanner.getInstance().getFreeMemoryForOperators() > 0);
    for (int unmatchedColumns = 0; unmatchedColumns <= 1; unmatchedColumns++) {
      MPPQueryContext context =
          new MPPQueryContext(new QueryId("result_column_overage_" + unmatchedColumns));
      context.initResultSetColumnMemoryTracking(0, 0, false);
      context.recordMatchedSourceColumnsForResultSet(2 + unmatchedColumns);
      context.recordExpandedSourceColumnForResultSet(1);
      context.recordExpandedSourceColumnForResultSet(1);
      context.recordGeneratedResultSetColumn(2);

      MemoryNotEnoughException exception =
          Assert.assertThrows(
              MemoryNotEnoughException.class,
              () -> context.reserveMemoryForFrontEnd(requestLargerThanFreeOperatorMemory()));

      String message = exception.getMessage();
      assertContains(message, "expanded 2 source columns, and generated 1 result-set columns");
      assertContains(message, "equivalent of at least 1 columns");
      Assert.assertFalse(message, message.contains("The matched source columns exceed"));
      assertContains(message, "increase query memory by at least");
    }
  }

  @Test
  public void resultSetColumnMemoryNotEnoughExceptionUsesFailedBatchSize() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("result_column_batch_oom_test"));
    context.initResultSetColumnMemoryTracking(0, 0, false);
    context.recordMatchedSourceColumnsForResultSet(2);
    context.recordExpandedSourceColumnForResultSet(1);
    context.recordExpandedSourceColumnForResultSet(1);
    context.recordGeneratedResultSetColumn(2);

    long failedBatchBytes = 1_048_840;
    long freeBytesAtFailure = 702_452;
    long lastExpressionBytes = 760;
    MemoryNotEnoughException original =
        new OperatorMemoryNotEnoughException(
            "the memory requested this time is 1048840B", failedBatchBytes, freeBytesAtFailure);

    MemoryNotEnoughException exception =
        context.enrichResultSetColumnMemoryNotEnoughException(original, lastExpressionBytes);
    String message = exception.getMessage();

    Assert.assertEquals(
        TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode(), exception.getErrorCode());
    assertContains(message, "requested this time 1.00 MB (1048840 B)");
    assertContains(message, "increase query memory by at least 338.27 KB (346388 B)");
    assertContains(message, "at least 173,194 columns");
    assertContains(message, "the memory requested this time is 1048840B");
    Assert.assertFalse(message, message.contains("requested this time 760 B"));
  }

  @Test
  public void schemaFetchMemoryNotEnoughExceptionContainsFetchedColumnDiagnostics() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("schema_fetch_oom_test"));
    long failedRequestBytes = requestLargerThanFreeOperatorMemory();

    context.initResultSetColumnMemoryTracking(0, 2, true);
    context.recordSchemaFetchDeserializedColumns(4);
    context.setReserveMemoryForSchemaTreeFunc(
        bytes -> {
          throw new MemoryNotEnoughException("schema fetch OOM");
        });

    MemoryNotEnoughException exception =
        Assert.assertThrows(
            MemoryNotEnoughException.class,
            () -> context.reserveMemoryForSchemaTree(failedRequestBytes));

    String message = exception.getMessage();
    assertContains(message, "Not enough memory while fetching metadata for query analysis.");
    assertContains(message, "deserialized 4 time-series columns");
    assertContains(message, "fetched schema columns exceed the estimated current memory capacity");
    assertContains(message, "SLIMIT=not set, SOFFSET=2");
    assertContains(message, "Use SLIMIT/SOFFSET");
    assertContains(message, "increase query memory by at least");
    assertContains(message, "fetched schema tree estimated memory");
    assertContains(message, "requested this time");
    assertContains(message, "schema fetch OOM");
  }

  @Test
  public void schemaFetchMemoryNotEnoughExceptionKeepsOriginalWithoutColumnContext() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("schema_fetch_without_context"));
    MemoryNotEnoughException expected = new MemoryNotEnoughException("original schema OOM");
    context.setReserveMemoryForSchemaTreeFunc(
        bytes -> {
          throw expected;
        });

    MemoryNotEnoughException actual =
        Assert.assertThrows(
            MemoryNotEnoughException.class, () -> context.reserveMemoryForSchemaTree(1));

    Assert.assertSame(expected, actual);
    Assert.assertEquals("original schema OOM", actual.getMessage());
  }

  private static long requestLargerThanFreeOperatorMemory() {
    long freeBytes = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    if (freeBytes < MEMORY_BATCH_THRESHOLD) {
      return MEMORY_BATCH_THRESHOLD;
    }
    if (freeBytes >= Long.MAX_VALUE - 1) {
      return Long.MAX_VALUE;
    }
    return freeBytes + 1;
  }

  private static void assertContains(String message, String expected) {
    Assert.assertTrue(
        String.format("Expected message to contain <%s>, but was <%s>.", expected, message),
        message.contains(expected));
  }
}
