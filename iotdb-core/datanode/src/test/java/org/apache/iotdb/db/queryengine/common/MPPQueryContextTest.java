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
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

import org.junit.Assert;
import org.junit.Test;

public class MPPQueryContextTest {

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

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
