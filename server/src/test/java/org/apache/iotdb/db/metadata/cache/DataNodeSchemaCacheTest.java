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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DataNodeSchemaCacheTest {
  DataNodeSchemaCache dataNodeSchemaCache;
  private Map<String, String> s1TagMap;

  @Before
  public void setUp() throws Exception {
    dataNodeSchemaCache = DataNodeSchemaCache.getInstance();
    s1TagMap = new HashMap<>();
    s1TagMap.put("k1", "v1");
  }

  @After
  public void tearDown() throws Exception {
    dataNodeSchemaCache.cleanUp();
  }

  @Test
  public void testGetSchemaEntity() throws IllegalPathException {
    PartialPath device1 = new PartialPath("root.sg1.d1");
    String[] measurements = new String[3];
    measurements[0] = "s1";
    measurements[1] = "s2";
    measurements[2] = "s3";

    dataNodeSchemaCache.put((ClusterSchemaTree) generateSchemaTree1());

    Map<PartialPath, SchemaCacheEntry> schemaCacheEntryMap =
        dataNodeSchemaCache.get(device1, measurements).getAllMeasurement().stream()
            .collect(
                Collectors.toMap(
                    o -> new PartialPath(o.getNodes()),
                    o ->
                        new SchemaCacheEntry(
                            "root.sg1",
                            (MeasurementSchema) o.getMeasurementSchema(),
                            o.getTagMap(),
                            o.isUnderAlignedEntity())));
    Assert.assertEquals(
        TSDataType.INT32,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s1")).getTsDataType());
    Assert.assertEquals(
        s1TagMap, schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s1")).getTagMap());
    Assert.assertEquals(
        TSDataType.FLOAT,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s2")).getTsDataType());
    Assert.assertNull(schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s2")).getTagMap());
    Assert.assertEquals(
        TSDataType.BOOLEAN,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s3")).getTsDataType());
    Assert.assertNull(schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s3")).getTagMap());

    String[] otherMeasurements = new String[3];
    otherMeasurements[0] = "s3";
    otherMeasurements[1] = "s4";
    otherMeasurements[2] = "s5";

    dataNodeSchemaCache.put((ClusterSchemaTree) generateSchemaTree2());

    schemaCacheEntryMap =
        dataNodeSchemaCache.get(device1, otherMeasurements).getAllMeasurement().stream()
            .collect(
                Collectors.toMap(
                    o -> new PartialPath(o.getNodes()),
                    o ->
                        new SchemaCacheEntry(
                            "root.sg1",
                            (MeasurementSchema) o.getMeasurementSchema(),
                            o.getTagMap(),
                            o.isUnderAlignedEntity())));
    Assert.assertEquals(
        TSDataType.BOOLEAN,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s3")).getTsDataType());
    Assert.assertNull(schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s3")).getTagMap());
    Assert.assertEquals(
        TSDataType.TEXT,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s4")).getTsDataType());
    Assert.assertNull(schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s4")).getTagMap());
    Assert.assertEquals(
        TSDataType.INT64,
        schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s5")).getTsDataType());
    Assert.assertNull(schemaCacheEntryMap.get(new PartialPath("root.sg1.d1.s4")).getTagMap());
  }

  @Test
  public void testLastCache() throws IllegalPathException {
    // test no cache
    PartialPath seriesPath1 = new PartialPath("root.sg1.d1.s1");
    PartialPath seriesPath2 = new PartialPath("root.sg1.d1.s2");
    PartialPath seriesPath3 = new PartialPath("root.sg1.d1.s3");
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath1));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));
    // test no last cache
    dataNodeSchemaCache.put((ClusterSchemaTree) generateSchemaTree1());
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath1));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));
    // put cache
    long timestamp = 100;
    long timestamp2 = 101;
    TsPrimitiveType value = TsPrimitiveType.getByType(TSDataType.INT32, 101);
    TsPrimitiveType value2 = TsPrimitiveType.getByType(TSDataType.INT32, 100);
    TsPrimitiveType value3 = TsPrimitiveType.getByType(TSDataType.INT32, 99);

    // put into last cache when cache not exist
    TimeValuePair timeValuePair = new TimeValuePair(timestamp, value);
    dataNodeSchemaCache.updateLastCache(seriesPath1, timeValuePair, false, 99L);
    TimeValuePair cachedTimeValuePair = dataNodeSchemaCache.getLastCache(seriesPath1);
    Assert.assertNotNull(cachedTimeValuePair);
    Assert.assertEquals(timestamp, cachedTimeValuePair.getTimestamp());
    Assert.assertEquals(value, cachedTimeValuePair.getValue());
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));

    // same time but low priority
    TimeValuePair timeValuePair2 = new TimeValuePair(timestamp, value2);
    dataNodeSchemaCache.updateLastCache(seriesPath1, timeValuePair2, false, 100L);
    TimeValuePair cachedTimeValuePair2 = dataNodeSchemaCache.getLastCache(seriesPath1);
    Assert.assertNotNull(cachedTimeValuePair2);
    Assert.assertEquals(timestamp, cachedTimeValuePair2.getTimestamp());
    Assert.assertEquals(value, cachedTimeValuePair2.getValue());
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));

    // same time but high priority
    dataNodeSchemaCache.updateLastCache(seriesPath1, timeValuePair2, true, 100L);
    cachedTimeValuePair2 = dataNodeSchemaCache.getLastCache(seriesPath1);
    Assert.assertNotNull(cachedTimeValuePair2);
    Assert.assertEquals(timestamp, cachedTimeValuePair2.getTimestamp());
    Assert.assertEquals(value2, cachedTimeValuePair2.getValue());
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));

    // put into last cache when cache already exist
    TimeValuePair timeValuePair3 = new TimeValuePair(timestamp2, value3);
    dataNodeSchemaCache.updateLastCache(seriesPath1, timeValuePair3, false, 100L);
    TimeValuePair cachedTimeValuePair3 = dataNodeSchemaCache.getLastCache(seriesPath1);
    Assert.assertNotNull(cachedTimeValuePair3);
    Assert.assertEquals(timestamp2, cachedTimeValuePair3.getTimestamp());
    Assert.assertEquals(value3, cachedTimeValuePair3.getValue());
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath2));
    Assert.assertNull(dataNodeSchemaCache.getLastCache(seriesPath3));
  }

  private ISchemaTree generateSchemaTree1() throws IllegalPathException {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    Map<String, String> s1TagMap = new HashMap<>();
    s1TagMap.put("k1", "v1");
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s1"),
        new MeasurementSchema("s1", TSDataType.INT32),
        s1TagMap,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s2"),
        new MeasurementSchema("s2", TSDataType.FLOAT),
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s3"),
        new MeasurementSchema("s3", TSDataType.BOOLEAN),
        null,
        null,
        false);
    schemaTree.setDatabases(Collections.singleton("root.sg1"));
    return schemaTree;
  }

  private ISchemaTree generateSchemaTree2() throws IllegalPathException {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();

    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s3"),
        new MeasurementSchema("s3", TSDataType.BOOLEAN),
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s4"),
        new MeasurementSchema("s4", TSDataType.TEXT),
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s5"),
        new MeasurementSchema("s5", TSDataType.INT64),
        null,
        null,
        false);
    schemaTree.setDatabases(Collections.singleton("root.sg1"));
    return schemaTree;
  }
}
