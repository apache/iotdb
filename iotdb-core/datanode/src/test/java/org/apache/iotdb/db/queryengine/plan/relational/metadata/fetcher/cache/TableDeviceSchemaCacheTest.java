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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableDeviceSchemaCacheTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private long originMemConfig;

  @Before
  public void setup() {
    originMemConfig = config.getAllocateMemoryForSchemaCache();
    config.setAllocateMemoryForSchemaCache(1500L);
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
    cache.put(
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
    cache.put(
        database,
        table1,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("cycle", "daily");
    cache.put(
        database,
        table1,
        new String[] {"shandong", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));

    final String table2 = "t1";
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.put(
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
    cache.put(
        database,
        table2,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_1"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));
  }

  @Test
  public void testLastCache() {
    final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

    final String database = "db";
    final String table1 = "t1";

    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    // Query update
    final Map<String, TimeValuePair> measurementQueryUpdateMap = new HashMap<>();

    final TimeValuePair tv1 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv2 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(2));
    measurementQueryUpdateMap.put("s1", tv1);
    measurementQueryUpdateMap.put("s2", tv2);

    cache.updateLastCache(database, table1, device0, measurementQueryUpdateMap);

    Assert.assertEquals(tv1, cache.getLastEntry(database, table1, device0, "s1"));
    Assert.assertEquals(tv2, cache.getLastEntry(database, table1, device0, "s2"));

    // Write update existing
    final Map<String, TimeValuePair> measurementWriteUpdateMap = new HashMap<>();

    final TimeValuePair tv3 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv4 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(2));
    measurementWriteUpdateMap.put("s2", tv3);
    measurementWriteUpdateMap.put("s3", tv4);

    cache.tryUpdateLastCacheWithoutLock(database, table1, device0, measurementWriteUpdateMap);

    Assert.assertEquals(tv3, cache.getLastEntry(database, table1, device0, "s2"));
    Assert.assertEquals(tv4, cache.getLastEntry(database, table1, device0, "s3"));

    // Write update non-exist
    final String database2 = "db2";
    final String table2 = "t2";

    cache.tryUpdateLastCacheWithoutLock(database, table2, device0, measurementWriteUpdateMap);
    cache.tryUpdateLastCacheWithoutLock(database2, table1, device0, measurementWriteUpdateMap);

    Assert.assertNull(cache.getLastEntry(database, table2, device0, "s2"));
    Assert.assertNull(cache.getLastEntry(database2, table1, device0, "s2"));

    // Invalidate device
    cache.invalidateLastCache(database, table1, device0);
    Assert.assertNull(cache.getLastEntry(database, table1, device0, "s2"));

    // Invalidate table
    final String[] device1 = new String[] {"hebei", "p_1", "d_1"};

    cache.updateLastCache(database, table2, device0, measurementQueryUpdateMap);
    cache.updateLastCache(database, table2, device1, measurementQueryUpdateMap);

    cache.invalidateLastCache(database, table2, null);

    Assert.assertNull(cache.getLastEntry(database, table2, device0, "s2"));
    Assert.assertNull(cache.getLastEntry(database, table2, device1, "s2"));
  }
}
