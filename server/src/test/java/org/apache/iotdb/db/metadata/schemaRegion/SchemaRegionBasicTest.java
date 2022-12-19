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
package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class define test cases for {@link ISchemaRegion}. All test cases will be run in both Memory
 * and Schema_File modes. In Schema_File mode, there are three kinds of test environment: full
 * memory, partial memory and non memory.
 */
public class SchemaRegionBasicTest extends AbstractSchemaRegionTest {

  public SchemaRegionBasicTest(SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testFetchSchema() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);

    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
                put("tag2", "t2");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
                put("attr2", "a2");
              }
            },
            "temp"),
        -1);
    List<MeasurementPath> schemas =
        schemaRegion.fetchSchema(
            new PartialPath("root.sg.wf01.wt01.*"), Collections.EMPTY_MAP, true);
    Assert.assertEquals(schemas.size(), 2);
    for (MeasurementPath measurementPath : schemas) {
      if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.status")) {
        Assert.assertTrue(StringUtils.isEmpty(measurementPath.getMeasurementAlias()));
        Assert.assertEquals(0, measurementPath.getTagMap().size());
        Assert.assertEquals(TSDataType.BOOLEAN, measurementPath.getMeasurementSchema().getType());
        Assert.assertEquals(
            TSEncoding.PLAIN, measurementPath.getMeasurementSchema().getEncodingType());
        Assert.assertEquals(
            CompressionType.SNAPPY, measurementPath.getMeasurementSchema().getCompressor());
      } else if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.temperature")) {
        // only when user query with alias, the alias in path will be set
        Assert.assertEquals("", measurementPath.getMeasurementAlias());
        Assert.assertEquals(2, measurementPath.getTagMap().size());
        Assert.assertEquals(TSDataType.FLOAT, measurementPath.getMeasurementSchema().getType());
        Assert.assertEquals(
            TSEncoding.RLE, measurementPath.getMeasurementSchema().getEncodingType());
        Assert.assertEquals(
            CompressionType.GZIP, measurementPath.getMeasurementSchema().getCompressor());
      } else {
        Assert.fail("Unexpected MeasurementPath " + measurementPath);
      }
    }
    schemas =
        schemaRegion.fetchSchema(
            new PartialPath("root.sg.wf01.wt01.temp"), Collections.EMPTY_MAP, false);
    Assert.assertEquals(schemas.size(), 1);
    Assert.assertEquals("root.sg.wf01.wt01.temperature", schemas.get(0).getFullPath());
    Assert.assertEquals("temp", schemas.get(0).getMeasurementAlias());
    Assert.assertNull(schemas.get(0).getTagMap());
    Assert.assertEquals(TSDataType.FLOAT, schemas.get(0).getMeasurementSchema().getType());
    Assert.assertEquals(TSEncoding.RLE, schemas.get(0).getMeasurementSchema().getEncodingType());
    Assert.assertEquals(
        CompressionType.GZIP, schemas.get(0).getMeasurementSchema().getCompressor());
  }

  @Test
  public void testCheckMeasurementExistence() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
                put("tag2", "t2");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
                put("attr2", "a2");
              }
            },
            "temp"),
        -1);
    // all non exist
    Map<Integer, MetadataException> res1 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01.wt01"),
            IntStream.range(0, 5).mapToObj(i -> "s" + i).collect(Collectors.toList()),
            IntStream.range(0, 5).mapToObj(i -> "alias" + i).collect(Collectors.toList()));
    Assert.assertEquals(0, res1.size());
    Map<Integer, MetadataException> res2 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01"),
            Collections.singletonList("wt01"),
            Collections.singletonList("alias1"));
    Assert.assertEquals(0, res2.size());
    // all exist
    Map<Integer, MetadataException> res3 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01.wt01"),
            Arrays.asList("status", "s1", "v1"),
            Arrays.asList("", "temp", ""));
    Assert.assertEquals(3, res3.size());
    Assert.assertTrue(res3.get(0) instanceof MeasurementAlreadyExistException);
    Assert.assertTrue(res3.get(1) instanceof AliasAlreadyExistException);
    Assert.assertTrue(res3.get(2) instanceof PathAlreadyExistException);
  }

  @Test
  public void testConstructSchemaBlackList() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt02.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf02.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.*"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.*.status"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion.constructSchemaBlackList(patternTree) >= 3);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.*"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"),
                new PartialPath("root.sg.wf02.wt01.temperature"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    PathPatternTree rollbackTree = new PathPatternTree();
    rollbackTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    rollbackTree.constructTree();
    schemaRegion.rollbackSchemaBlackList(rollbackTree);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.*"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    schemaRegion.deleteTimeseriesInBlackList(patternTree);
    List<MeasurementPath> schemas =
        schemaRegion.fetchSchema(new PartialPath("root.**"), Collections.EMPTY_MAP, false);
    Assert.assertEquals(1, schemas.size());
    Assert.assertEquals("root.sg.wf02.wt01.temperature", schemas.get(0).getFullPath());
  }
}
