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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.SchemaCacheEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;

public class TreeDeviceSchemaCacheManagerTest {
  private TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager;
  private Map<String, String> s1TagMap;

  @Before
  public void setUp() throws Exception {
    treeDeviceSchemaCacheManager = TreeDeviceSchemaCacheManager.getInstance();
    s1TagMap = new HashMap<>();
    s1TagMap.put("k1", "v1");
  }

  @After
  public void tearDown() throws Exception {
    treeDeviceSchemaCacheManager.cleanUp();
    ClusterTemplateManager.getInstance().clear();
  }

  @Test
  public void testGetSchemaEntity() throws IllegalPathException {
    final PartialPath device1 = new PartialPath("root.sg1.d1");
    final String[] measurements = new String[3];
    measurements[0] = "s1";
    measurements[1] = "s2";
    measurements[2] = "s3";

    treeDeviceSchemaCacheManager.put((ClusterSchemaTree) generateSchemaTree1());

    Map<PartialPath, SchemaCacheEntry> schemaCacheEntryMap =
        treeDeviceSchemaCacheManager.get(device1, measurements).getAllDevices().stream()
            .flatMap(deviceSchemaInfo -> deviceSchemaInfo.getMeasurementSchemaPathList().stream())
            .collect(
                Collectors.toMap(
                    o -> new PartialPath(o.getNodes()),
                    o -> new SchemaCacheEntry(o.getMeasurementSchema(), o.getTagMap())));
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

    final String[] otherMeasurements = new String[3];
    otherMeasurements[0] = "s3";
    otherMeasurements[1] = "s4";
    otherMeasurements[2] = "s5";

    treeDeviceSchemaCacheManager.put((ClusterSchemaTree) generateSchemaTree2());

    schemaCacheEntryMap =
        treeDeviceSchemaCacheManager.get(device1, otherMeasurements).getAllDevices().stream()
            .flatMap(deviceSchemaInfo -> deviceSchemaInfo.getMeasurementSchemaPathList().stream())
            .collect(
                Collectors.toMap(
                    o -> new PartialPath(o.getNodes()),
                    o -> new SchemaCacheEntry(o.getMeasurementSchema(), o.getTagMap())));
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

  private ISchemaTree generateSchemaTree1() throws IllegalPathException {
    final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    final Map<String, String> s1TagMap = new HashMap<>();
    s1TagMap.put("k1", "v1");
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s1"),
        new MeasurementSchema("s1", TSDataType.INT32),
        s1TagMap,
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s2"),
        new MeasurementSchema("s2", TSDataType.FLOAT),
        null,
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s3"),
        new MeasurementSchema("s3", TSDataType.BOOLEAN),
        null,
        null,
        null,
        false);
    schemaTree.setDatabases(Collections.singleton("root.sg1"));
    return schemaTree;
  }

  private ISchemaTree generateSchemaTree2() throws IllegalPathException {
    final ClusterSchemaTree schemaTree = new ClusterSchemaTree();

    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s3"),
        new MeasurementSchema("s3", TSDataType.BOOLEAN),
        null,
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s4"),
        new MeasurementSchema("s4", TSDataType.TEXT),
        null,
        null,
        null,
        false);
    schemaTree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s5"),
        new MeasurementSchema("s5", TSDataType.INT64),
        null,
        null,
        null,
        false);
    schemaTree.setDatabases(Collections.singleton("root.sg1"));
    return schemaTree;
  }

  @Test
  public void testUpdateLastCache() throws IllegalPathException {
    final String database = "root.db";
    final PartialPath device = new PartialPath("root.db.d");

    final String[] measurements = new String[] {"s1", "s2", "s3"};

    final MeasurementSchema s1 = new MeasurementSchema("s1", TSDataType.INT32);
    final MeasurementSchema s2 = new MeasurementSchema("s2", TSDataType.INT32);
    final MeasurementSchema s3 = new MeasurementSchema("s3", TSDataType.INT32);

    final TimeValuePair tv1 = new TimeValuePair(1, new TsPrimitiveType.TsInt(1));

    treeDeviceSchemaCacheManager.updateLastCache(
        database, new MeasurementPath(device.concatNode("s1"), s1), false);
    treeDeviceSchemaCacheManager.updateLastCache(
        database, new MeasurementPath(device.concatNode("s3"), s3), false);

    // Simulate "s1" revert when the query has failed in calculation
    treeDeviceSchemaCacheManager.updateLastCacheIfExists(
        database,
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(device.getNodes())),
        new String[] {"s1"},
        new TimeValuePair[] {
          new TimeValuePair(2, new TsPrimitiveType.TsInt(2)),
        },
        false,
        new MeasurementSchema[] {s1});
    treeDeviceSchemaCacheManager.updateLastCache(
        database, new MeasurementPath(device.concatNode("s1"), s1), true);

    // "s2" shall be null since the "null" timeValuePair has not been put
    treeDeviceSchemaCacheManager.updateLastCacheIfExists(
        database,
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(device.getNodes())),
        new String[] {"s2"},
        new TimeValuePair[] {tv1},
        false,
        new MeasurementSchema[] {s2});

    treeDeviceSchemaCacheManager.updateLastCacheIfExists(
        database,
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(device.getNodes())),
        new String[] {"s3"},
        new TimeValuePair[] {tv1},
        false,
        new MeasurementSchema[] {s3});

    Assert.assertNull(
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s1")));
    Assert.assertNull(
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s2")));
    Assert.assertNotNull(
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s3")));

    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[] {s1, s2, s3};

    treeDeviceSchemaCacheManager.updateLastCacheIfExists(
        database,
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            StringArrayDeviceID.splitDeviceIdString(device.getNodes())),
        measurements,
        new TimeValuePair[] {
          new TimeValuePair(2, new TsPrimitiveType.TsInt(2)),
          new TimeValuePair(2, new TsPrimitiveType.TsInt(2)),
          new TimeValuePair(2, new TsPrimitiveType.TsInt(2))
        },
        false,
        measurementSchemas);

    Assert.assertNull(
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s1")));
    Assert.assertNull(
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s2")));
    Assert.assertEquals(
        new TimeValuePair(2, new TsPrimitiveType.TsInt(2)),
        treeDeviceSchemaCacheManager.getLastCache(new MeasurementPath("root.db.d.s3")));
  }

  @Test
  public void testPut() throws Exception {
    final ClusterSchemaTree clusterSchemaTree = new ClusterSchemaTree();
    final Template template1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template1.setId(1);
    final Template template2 =
        new Template(
            "t2",
            Arrays.asList("t1", "t2", "t3"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.INT64),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLBE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    template2.setId(2);
    ClusterTemplateManager.getInstance().putTemplate(template1);
    ClusterTemplateManager.getInstance().putTemplate(template2);
    clusterSchemaTree.appendTemplateDevice(new PartialPath("root.sg1.d1"), false, 1, template1);
    clusterSchemaTree.appendTemplateDevice(new PartialPath("root.sg1.d2"), false, 2, template2);
    clusterSchemaTree.setDatabases(Collections.singleton("root.sg1"));
    clusterSchemaTree.appendSingleMeasurementPath(
        new MeasurementPath("root.sg1.d3.s1", TSDataType.FLOAT));
    treeDeviceSchemaCacheManager.put(clusterSchemaTree);
    final ClusterSchemaTree d1Tree =
        treeDeviceSchemaCacheManager.getMatchedSchemaWithTemplate(new PartialPath("root.sg1.d1"));
    final ClusterSchemaTree d2Tree =
        treeDeviceSchemaCacheManager.getMatchedSchemaWithTemplate(new PartialPath("root.sg1.d2"));
    final ClusterSchemaTree d3Tree =
        treeDeviceSchemaCacheManager.getMatchedSchemaWithoutTemplate(
            new MeasurementPath("root.sg1.d3.s1"));
    List<MeasurementPath> measurementPaths = d1Tree.searchMeasurementPaths(ALL_MATCH_PATTERN).left;
    Assert.assertEquals(2, measurementPaths.size());
    for (final MeasurementPath measurementPath : measurementPaths) {
      Assert.assertEquals(
          template1.getSchema(measurementPath.getMeasurement()),
          measurementPath.getMeasurementSchema());
    }
    measurementPaths = d2Tree.searchMeasurementPaths(ALL_MATCH_PATTERN).left;
    Assert.assertEquals(3, measurementPaths.size());
    for (final MeasurementPath measurementPath : measurementPaths) {
      Assert.assertEquals(
          template2.getSchema(measurementPath.getMeasurement()),
          measurementPath.getMeasurementSchema());
    }
    measurementPaths = d3Tree.searchMeasurementPaths(ALL_MATCH_PATTERN).left;
    Assert.assertEquals(1, measurementPaths.size());
    Assert.assertEquals(TSDataType.FLOAT, measurementPaths.get(0).getMeasurementSchema().getType());
    Assert.assertEquals("root.sg1.d3.s1", measurementPaths.get(0).getFullPath());
  }
}
