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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

public class DataNodeTableCacheTest {

  private static final String DATABASE = "interrupted_fetch_database";
  private static final String TABLE_CACHE_TEST_DATABASE = "root.table_cache_test";
  private static final String TABLE_NAME = "table1";

  @Test
  public void interruptedFetchDoesNotLeakSemaphorePermit() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    cache.invalid(DATABASE);
    try {
      final Semaphore fetchTableSemaphore = getFetchTableSemaphore(cache);
      final int permitsBeforeFetch = fetchTableSemaphore.availablePermits();

      Thread.currentThread().interrupt();
      try {
        Assert.assertFalse(cache.isDatabaseExist(DATABASE));
      } finally {
        Thread.interrupted();
      }

      Assert.assertEquals(permitsBeforeFetch, fetchTableSemaphore.availablePermits());
    } finally {
      cache.invalid(DATABASE);
    }
  }

  @Test
  public void commitUpdateTableIsIdempotent() {
    final ITableCache cache = DataNodeTableCache.getInstance();
    cache.invalid(TABLE_CACHE_TEST_DATABASE);
    try {
      cache.preUpdateTable(TABLE_CACHE_TEST_DATABASE, createTable(TABLE_NAME), null);

      cache.commitUpdateTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, null);
      cache.commitUpdateTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, null);

      Assert.assertEquals(
          TABLE_NAME, cache.getTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME).getTableName());
    } finally {
      cache.invalid(TABLE_CACHE_TEST_DATABASE);
    }
  }

  @Test
  public void commitAfterRollbackUpdateTableIsIgnored() {
    final ITableCache cache = DataNodeTableCache.getInstance();
    cache.invalid(TABLE_CACHE_TEST_DATABASE);
    try {
      cache.preUpdateTable(TABLE_CACHE_TEST_DATABASE, createTable(TABLE_NAME), null);

      cache.rollbackUpdateTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, null);
      cache.commitUpdateTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, null);

      Assert.assertNull(cache.getTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, false));
    } finally {
      cache.invalid(TABLE_CACHE_TEST_DATABASE);
    }
  }

  @Test
  public void rollbackRenameTableRestoresOldName() {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String newTableName = "table2";
    cache.invalid(TABLE_CACHE_TEST_DATABASE);
    try {
      cache.preUpdateTable(TABLE_CACHE_TEST_DATABASE, createTable(TABLE_NAME), null);
      cache.commitUpdateTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME, null);

      cache.preUpdateTable(TABLE_CACHE_TEST_DATABASE, createTable(newTableName), TABLE_NAME);
      cache.rollbackUpdateTable(TABLE_CACHE_TEST_DATABASE, newTableName, TABLE_NAME);

      Assert.assertEquals(
          TABLE_NAME, cache.getTable(TABLE_CACHE_TEST_DATABASE, TABLE_NAME).getTableName());
      Assert.assertNull(cache.getTable(TABLE_CACHE_TEST_DATABASE, newTableName, false));
    } finally {
      cache.invalid(TABLE_CACHE_TEST_DATABASE);
    }
  }

  private Semaphore getFetchTableSemaphore(final ITableCache cache) throws Exception {
    final Field field = DataNodeTableCache.class.getDeclaredField("fetchTableSemaphore");
    field.setAccessible(true);
    return (Semaphore) field.get(cache);
  }

  private TsTable createTable(final String tableName) {
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(
        new FieldColumnSchema("s1", TSDataType.INT32, TSEncoding.RLE, CompressionType.GZIP));
    return table;
  }
}
