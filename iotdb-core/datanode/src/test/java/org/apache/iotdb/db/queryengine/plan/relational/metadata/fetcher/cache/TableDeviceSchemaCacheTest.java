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
import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher.convertIdValuesToDeviceID;

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
    TableDeviceSchemaCache.getInstance().invalidateAll();
    config.setAllocateMemoryForSchemaCache(originMemConfig);
  }

  @Test
  public void testDeviceCache() {
    final TableDeviceSchemaCache cache = TableDeviceSchemaCache.getInstance();

    final String database = "db";
    final String table1 = "t1";

    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_0"}),
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_0"})));
    Assert.assertNull(
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_1"})));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_1"}),
        new HashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_1"})));

    attributeMap.put("cycle", "daily");
    cache.putAttributes(
        database,
        convertIdValuesToDeviceID(table1, new String[] {"shandong", "p_1", "d_1"}),
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertNull(
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_0"})));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"shandong", "p_1", "d_1"})));

    final String table2 = "t2";
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_0"}),
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_0"})));
    Assert.assertNull(
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"hebei", "p_1", "d_1"})));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_1"}),
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_1"})));
    Assert.assertNull(
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table1, new String[] {"shandong", "p_1", "d_1"})));

    cache.invalidateAttributes(
        database, convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_1"}));
    Assert.assertNull(
        cache.getDeviceAttribute(
            database, convertIdValuesToDeviceID(table2, new String[] {"hebei", "p_1", "d_1"})));
  }

  @Test
  public void testLastCache() {
    final TableDeviceSchemaCache cache = TableDeviceSchemaCache.getInstance();

    final String database = "db";
    final String table1 = "t1";

    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    // Test get from empty cache
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s0"));
    Assert.assertFalse(
        cache
            .getLastRow(
                database,
                convertIdValuesToDeviceID(table1, device0),
                "s0",
                Collections.singletonList("s1"))
            .isPresent());
    Assert.assertFalse(
        cache
            .getLastRow(
                database,
                convertIdValuesToDeviceID(table1, device0),
                "",
                Collections.singletonList("s1"))
            .isPresent());

    // Query update

    final TimeValuePair tv0 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(0));
    final TimeValuePair tv1 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv2 = new TimeValuePair(2L, new TsPrimitiveType.TsInt(2));

    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {"s0", "s1", "s2"},
        new TimeValuePair[] {tv0, tv1, tv2});

    Assert.assertEquals(
        tv0, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s0"));
    Assert.assertEquals(
        tv1, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s1"));
    Assert.assertEquals(
        tv2, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s2"));

    // Write update existing
    final TimeValuePair tv3 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(3));

    cache.updateLastCacheIfExists(
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {"s0", "s1", "s2", "s3"},
        new TimeValuePair[] {tv3, tv3, tv3, tv3});

    Assert.assertEquals(
        tv3, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s0"));
    Assert.assertEquals(
        tv3, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s1"));
    Assert.assertEquals(
        tv2, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s2"));
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s3"));

    // Test null hit measurements
    cache.updateLastCache(
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {"s4"},
        new TimeValuePair[] {TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR});

    // Miss if the "null" time value pair is not in cache, meaning that the
    // entry is evicted
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s4"));

    // Common query
    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {"s4"},
        new TimeValuePair[] {TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR});

    Assert.assertSame(
        TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR,
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s4"));

    // Test null miss measurements
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s5"));

    // Test lastRow
    Optional<Pair<OptionalLong, TsPrimitiveType[]>> result =
        cache.getLastRow(
            database,
            convertIdValuesToDeviceID(table1, device0),
            "",
            Collections.singletonList("s2"));
    Assert.assertFalse(result.isPresent());

    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {""},
        new TimeValuePair[] {new TimeValuePair(2L, TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE)});

    result =
        cache.getLastRow(
            database,
            convertIdValuesToDeviceID(table1, device0),
            "",
            Collections.singletonList("s2"));
    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get().getLeft().isPresent());
    Assert.assertEquals(OptionalLong.of(2L), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {new TsPrimitiveType.TsInt(2)}, result.get().getRight());

    result =
        cache.getLastRow(
            database,
            convertIdValuesToDeviceID(table1, device0),
            "s0",
            Arrays.asList("s0", "", "s1", "s4", "s5"));
    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get().getLeft().isPresent());
    Assert.assertEquals(OptionalLong.of(1L), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {
          new TsPrimitiveType.TsInt(3),
          new TsPrimitiveType.TsLong(1),
          new TsPrimitiveType.TsInt(3),
          TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE,
          null
        },
        result.get().getRight());

    // Test null source measurements
    result =
        cache.getLastRow(
            database,
            convertIdValuesToDeviceID(table1, device0),
            "s4",
            Arrays.asList("s0", "s1", "s5"));
    Assert.assertTrue(result.isPresent());
    Assert.assertFalse(result.get().getLeft().isPresent());

    Assert.assertFalse(
        cache
            .getLastRow(
                database,
                convertIdValuesToDeviceID(table1, device0),
                "s5",
                Arrays.asList("s0", "s1", "s5"))
            .isPresent());

    final String table2 = "t2";
    cache.invalidateLastCache(database, convertIdValuesToDeviceID(table1, device0));
    cache.invalidate(database);
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s2"));

    // Invalidate table
    final String[] device1 = new String[] {"hebei", "p_1", "d_1"};
    final String[] device2 = new String[] {"hebei", "p_1", "d_2"};

    final String[] tempMeasurements = new String[] {"s0", "s1", "s2", "s3", "s4"};
    final TimeValuePair[] tempTimeValuePairs = new TimeValuePair[] {tv0, tv0, tv0, tv0, tv0};

    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table2, device0),
        tempMeasurements,
        tempTimeValuePairs);
    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table2, device1),
        tempMeasurements,
        tempTimeValuePairs);
    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table2, device2),
        tempMeasurements,
        tempTimeValuePairs);

    // Test cache eviction
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table2, device0), "s2"));

    cache.invalidateLastCache(database, table2);

    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table2, device1), "s2"));
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table2, device2), "s2"));

    // Test Long.MIN_VALUE
    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table2, device0),
        new String[] {"", "s2"},
        new TimeValuePair[] {
          new TimeValuePair(Long.MIN_VALUE, TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE),
          TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR
        });

    result =
        cache.getLastRow(
            database, convertIdValuesToDeviceID(table2, device0), "", Arrays.asList("s2", "s3"));
    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get().getLeft().isPresent());
    Assert.assertEquals(OptionalLong.of(Long.MIN_VALUE), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE, null},
        result.get().getRight());

    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table2, device0),
        new String[] {"s3"},
        new TimeValuePair[] {new TimeValuePair(Long.MIN_VALUE, new TsPrimitiveType.TsInt(3))});

    result =
        cache.getLastRow(
            database, convertIdValuesToDeviceID(table2, device0), "s3", Arrays.asList("s2", "s3"));
    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get().getLeft().isPresent());
    Assert.assertEquals(OptionalLong.of(Long.MIN_VALUE), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {
          TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE, new TsPrimitiveType.TsInt(3),
        },
        result.get().getRight());

    result =
        cache.getLastRow(
            database, convertIdValuesToDeviceID(table2, device0), "", Arrays.asList("s2", "s3"));
    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get().getLeft().isPresent());
    Assert.assertEquals(OptionalLong.of(Long.MIN_VALUE), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {
          TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE, new TsPrimitiveType.TsInt(3),
        },
        result.get().getRight());
  }

  @Test
  public void testUpdateNonExistWhenWriting() {
    final String database = "db1";
    final String database2 = "db2";

    final String table1 = "t1";
    final String table2 = "t2";
    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    final TimeValuePair tv3 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(3));

    final String[] testMeasurements = new String[] {"s0", "s1", "s2", "s3"};
    final TimeValuePair[] testTimeValuePairs = new TimeValuePair[] {tv3, tv3, tv3, tv3};

    // Test disable put cache by writing
    final TableDeviceSchemaCache cache = TableDeviceSchemaCache.getInstance();

    cache.updateLastCacheIfExists(
        database, convertIdValuesToDeviceID(table2, device0), testMeasurements, testTimeValuePairs);
    cache.updateLastCacheIfExists(
        database2,
        convertIdValuesToDeviceID(table1, device0),
        testMeasurements,
        testTimeValuePairs);

    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table2, device0), "s2"));
    Assert.assertNull(
        cache.getLastEntry(database2, convertIdValuesToDeviceID(table1, device0), "s2"));

    updateLastCache4Query(
        cache,
        database,
        convertIdValuesToDeviceID(table1, device0),
        new String[] {"s0"},
        new TimeValuePair[] {new TimeValuePair(0L, new TsPrimitiveType.TsInt(2))});
    cache.updateLastCacheIfExists(
        database, convertIdValuesToDeviceID(table1, device0), testMeasurements, testTimeValuePairs);

    Assert.assertEquals(
        tv3, cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s0"));
    Assert.assertNull(
        cache.getLastEntry(database, convertIdValuesToDeviceID(table1, device0), "s2"));
  }

  private void updateLastCache4Query(
      final TableDeviceSchemaCache cache,
      final String database,
      final IDeviceID deviceID,
      final String[] measurement,
      final TimeValuePair[] data) {
    cache.updateLastCache(database, deviceID, measurement, null);
    cache.updateLastCache(database, deviceID, measurement, data);
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
