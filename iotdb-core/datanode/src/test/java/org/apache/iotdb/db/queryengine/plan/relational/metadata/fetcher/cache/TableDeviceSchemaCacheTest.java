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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableDeviceSchemaCacheTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private long originMemConfig;

  @Before
  public void setup() {
    originMemConfig = config.getAllocateMemoryForSchemaCache();
    config.setAllocateMemoryForSchemaCache(1300L);
  }

  @After
  public void rollback() {
    config.setAllocateMemoryForSchemaCache(originMemConfig);
  }

  @Test
  public void testDeviceCache() {
    final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

    final String database = "db";
    final String table1 = "t1";

    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        table1,
        new String[] {"hebei", "p_1", "d_0"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        table1,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("cycle", "daily");
    cache.putAttributes(
        database,
        table1,
        new String[] {"shandong", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));

    final String table2 = "t2";
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        table2,
        new String[] {"hebei", "p_1", "d_0"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        table2,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_1"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));

    cache.invalidateAttributes(database, table2, new String[] {"hebei", "p_1", "d_1"});
    Assert.assertNull(
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_1"}));
  }

  @Test
  public void testLastCache() {
    final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

    final String database = "db";
    final String table1 = "t1";

    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    // Test get from empty cache
    Assert.assertNull(cache.getLastEntry(database, table1, device0, "s0"));
    Assert.assertNull(cache.getLastTime(database, table1, device0, "s0"));
    Assert.assertNull(cache.getLastBy(database, table1, device0, "s0", "s1"));
    Assert.assertNull(
        cache.getLastRow(database, table1, device0, "s0", Collections.singletonList("s1")));
    Assert.assertNull(cache.getLastTime(database, table1, device0, ""));
    Assert.assertNull(cache.getLastBy(database, table1, device0, "", "s1"));
    Assert.assertNull(
        cache.getLastRow(database, table1, device0, "", Collections.singletonList("s1")));

    // Query update
    final Map<String, TimeValuePair> measurementQueryUpdateMap = new HashMap<>();

    final TimeValuePair tv0 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(0));
    final TimeValuePair tv1 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv2 = new TimeValuePair(2L, new TsPrimitiveType.TsInt(2));
    measurementQueryUpdateMap.put("s0", tv0);
    measurementQueryUpdateMap.put("s1", tv1);
    measurementQueryUpdateMap.put("s2", tv2);

    cache.updateLastCache(database, table1, device0, measurementQueryUpdateMap);

    Assert.assertEquals(tv0, cache.getLastEntry(database, table1, device0, "s0"));
    Assert.assertEquals(tv1, cache.getLastEntry(database, table1, device0, "s1"));
    Assert.assertEquals(tv2, cache.getLastEntry(database, table1, device0, "s2"));

    // Write update existing
    final Map<String, TimeValuePair> measurementWriteUpdateMap = new HashMap<>();

    final TimeValuePair tv3 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(3));
    measurementWriteUpdateMap.put("s0", tv3);
    measurementWriteUpdateMap.put("s1", tv3);
    measurementWriteUpdateMap.put("s2", tv3);
    measurementWriteUpdateMap.put("s3", tv3);

    cache.mayUpdateLastCacheWithoutLock(database, table1, device0, measurementWriteUpdateMap);

    Assert.assertEquals(tv3, cache.getLastEntry(database, table1, device0, "s0"));
    Assert.assertEquals(tv3, cache.getLastEntry(database, table1, device0, "s1"));
    Assert.assertEquals(tv2, cache.getLastEntry(database, table1, device0, "s2"));
    Assert.assertEquals(tv3, cache.getLastEntry(database, table1, device0, "s3"));

    Assert.assertNull(cache.getLastTime(database, table1, device0, "s4"));
    Assert.assertNull(cache.getLastBy(database, table1, device0, "s4", "s3"));
    Assert.assertEquals((Long) 2L, cache.getLastTime(database, table1, device0, ""));
    Assert.assertEquals((Long) 1L, cache.getLastTime(database, table1, device0, "s0"));
    Assert.assertNull(cache.getLastBy(database, table1, device0, "s2", "s1"));
    Assert.assertEquals(
        new TsPrimitiveType.TsInt(3), cache.getLastBy(database, table1, device0, "s0", "s1"));

    // Test lastRow
    Pair<Long, TsPrimitiveType[]> result =
        cache.getLastRow(database, table1, device0, "", Collections.singletonList("s2"));
    Assert.assertEquals((Long) 2L, result.getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {new TsPrimitiveType.TsInt(2)}, result.getRight());

    result = cache.getLastRow(database, table1, device0, "s0", Arrays.asList("s0", "s1"));
    Assert.assertEquals((Long) 1L, result.getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {new TsPrimitiveType.TsInt(3), new TsPrimitiveType.TsInt(3)},
        result.getRight());

    final String table2 = "t2";
    cache.invalidateLastCache(database, table1, device0);
    cache.invalidate(database);
    Assert.assertNull(cache.getLastEntry(database, table1, device0, "s2"));

    // Invalidate table
    final String[] device1 = new String[] {"hebei", "p_1", "d_1"};
    final String[] device2 = new String[] {"hebei", "p_1", "d_2"};

    cache.updateLastCache(database, table2, device0, measurementQueryUpdateMap);
    cache.updateLastCache(database, table2, device1, measurementQueryUpdateMap);
    cache.updateLastCache(database, table2, device2, measurementQueryUpdateMap);

    // Test cache eviction
    Assert.assertNull(cache.getLastEntry(database, table2, device0, "s2"));

    cache.invalidateLastCache(database, table2, null);

    Assert.assertNull(cache.getLastEntry(database, table2, device1, "s2"));
    Assert.assertNull(cache.getLastEntry(database, table2, device2, "s2"));
  }

  @Test
  public void testUpdateNonExistWhenWriting() {
    final String database = "db";
    final String database2 = "db2";

    final String table1 = "t1";
    final String table2 = "t2";
    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    final Map<String, TimeValuePair> measurementWriteUpdateMap = new HashMap<>();
    final TimeValuePair tv3 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(3));
    measurementWriteUpdateMap.put("s0", tv3);
    measurementWriteUpdateMap.put("s1", tv3);
    measurementWriteUpdateMap.put("s2", tv3);
    measurementWriteUpdateMap.put("s3", tv3);

    // Enable put cache by default
    final TableDeviceSchemaCache cache1 = new TableDeviceSchemaCache();

    cache1.mayUpdateLastCacheWithoutLock(database, table2, device0, measurementWriteUpdateMap);
    cache1.mayUpdateLastCacheWithoutLock(database2, table1, device0, measurementWriteUpdateMap);

    Assert.assertEquals(tv3, cache1.getLastEntry(database, table2, device0, "s2"));
    Assert.assertEquals(tv3, cache1.getLastEntry(database2, table1, device0, "s2"));

    // Disable put cache
    config.setPutLastCacheWhenWriting(false);
    final TableDeviceSchemaCache cache2 = new TableDeviceSchemaCache();

    cache2.mayUpdateLastCacheWithoutLock(database, table2, device0, measurementWriteUpdateMap);
    cache2.mayUpdateLastCacheWithoutLock(database2, table1, device0, measurementWriteUpdateMap);

    Assert.assertNull(cache2.getLastEntry(database, table2, device0, "s2"));
    Assert.assertNull(cache2.getLastEntry(database2, table1, device0, "s2"));
    config.setPutLastCacheWhenWriting(true);
  }

  @Test
  public void testIntern() {
    final String database = "sg";
    final String tableName = "t";
    final List<ColumnHeader> columnHeaderList =
        Arrays.asList(
            new ColumnHeader("hebei", TSDataType.STRING),
            new ColumnHeader("p_1", TSDataType.STRING),
            new ColumnHeader("d_1", TSDataType.STRING));
    final String attributeName = "attr";

    // Prepare table
    final TsTable testTable = new TsTable(tableName);
    columnHeaderList.forEach(
        columnHeader ->
            testTable.addColumnSchema(
                new IdColumnSchema(columnHeader.getColumnName(), columnHeader.getColumnType())));
    testTable.addColumnSchema(new AttributeColumnSchema(attributeName, TSDataType.STRING));
    testTable.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    testTable.addColumnSchema(
        new MeasurementColumnSchema(
            "s1", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.GZIP));
    DataNodeTableCache.getInstance().preUpdateTable(database, testTable);
    DataNodeTableCache.getInstance().commitUpdateTable(database, tableName);

    final String a = "s1";
    // Different from "a"
    final String b = new String(a.getBytes());

    Assert.assertSame(
        DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, a),
        DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, b));
  }
}
