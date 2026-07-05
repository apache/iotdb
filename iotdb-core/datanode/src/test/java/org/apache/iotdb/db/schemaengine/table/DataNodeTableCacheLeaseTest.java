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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.T_FENCE_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DataNodeTableCacheLeaseTest {

  private final AtomicLong nowNanos = new AtomicLong(100_000_000_000L);

  private MetadataLeaseManager leaseManager;
  private DataNodeTableCache tableCache;

  @Before
  public void setUp() {
    nowNanos.set(100_000_000_000L);
    leaseManager = MetadataLeaseTestUtils.newManager(nowNanos);
    tableCache = Mockito.spy((DataNodeTableCache) DataNodeTableCache.getInstance());
    tableCache.invalidateAll();
    Mockito.doAnswer(
            invocation -> {
              MetadataLeaseTestUtils.failIfMetadataLeaseFenced(leaseManager);
              return null;
            })
        .when(tableCache)
        .failIfMetadataLeaseFenced();
  }

  @After
  public void tearDown() {
    tableCache.invalidateAll();
  }

  @Test
  public void fencedLeaseFailsClosedForReadApis() {
    nowNanos.addAndGet((T_FENCE_MS + 1) * 1_000_000L);
    assertLeaseFenced(() -> tableCache.getTableInWrite("root.db", "t"));
    assertLeaseFenced(() -> tableCache.getTable("root.db", "t", false));
    assertLeaseFenced(() -> tableCache.isDatabaseExist("root.db"));
  }

  @Test
  public void fencedLeaseFailsClosedForUpdateApis() {
    final TsTable table = new TsTable("t");

    nowNanos.addAndGet((T_FENCE_MS + 1) * 1_000_000L);
    assertLeaseFenced(() -> tableCache.preUpdateTable("root.db", table, null));
    assertLeaseFenced(() -> tableCache.rollbackUpdateTable("root.db", "t", null));
    assertLeaseFenced(() -> tableCache.commitUpdateTable("root.db", "t", null));
  }

  private static void assertLeaseFenced(final Runnable runnable) {
    final MetadataLeaseFencedException e =
        assertThrows(MetadataLeaseFencedException.class, runnable::run);
    assertEquals(TSStatusCode.METADATA_LEASE_FENCED.getStatusCode(), e.getErrorCode());
  }
}
