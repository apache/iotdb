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
}
