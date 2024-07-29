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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilterFactory;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowNodesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateAlignedTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateTimeSeriesPlanImpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.checkSingleTimeSeries;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getAllTimeSeriesCount;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getChildNodePathInNextLevel;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getDevicesNum;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getMeasurementCountGroupByLevel;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getNodesListInGivenLevel;

/**
 * This class define test cases for {@link ISchemaRegion}. All test cases will be run in both Memory
 * and PBTree modes. In PBTree mode, there are three kinds of test environment: full memory, partial
 * memory and non memory.
 */
public class SchemaRegionBasicTest extends AbstractSchemaRegionTest {

  public SchemaRegionBasicTest(final SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  @Ignore("This is just a performance test and shall not be run in auto test environment")
  public void testFetchSchemaPerformance() throws Exception {
    System.out.println(testParams.getTestModeName());
    final int deviceNum = 1000;
    final int measurementNum = 40;
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    for (int i = 0; i < deviceNum; i++) {
      for (int j = 0; j < measurementNum; j++) {
        schemaRegion.createTimeSeries(
            SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
                new MeasurementPath("root.sg.d" + i + ".s" + j),
                TSDataType.BOOLEAN,
                TSEncoding.PLAIN,
                CompressionType.SNAPPY,
                null,
                null,
                null,
                null),
            -1);
      }
    }
    final PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < deviceNum; i++) {
      for (int j = 0; j < measurementNum; j++) {
        patternTree.appendFullPath(new MeasurementPath("root.sg.d" + i + ".s" + j));
      }
    }
    patternTree.constructTree();
    schemaRegion.fetchSeriesSchema(patternTree, Collections.emptyMap(), false, false, true, false);
    final long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      schemaRegion.fetchSeriesSchema(
          patternTree, Collections.emptyMap(), false, false, true, false);
    }
    System.out.println("Cost time: " + (System.currentTimeMillis() - startTime));
  }

  @Test
  public void testFetchSchema() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.temperature"),
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
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.*"));
    patternTree.constructTree();

    ClusterSchemaTree schemas =
        schemaRegion.fetchSeriesSchema(
            patternTree, Collections.emptyMap(), true, false, true, false);
    List<MeasurementPath> measurementPaths =
        schemas.searchMeasurementPaths(new PartialPath("root.sg.wf01.wt01.*")).left;
    Assert.assertEquals(2, measurementPaths.size());
    for (final MeasurementPath measurementPath : measurementPaths) {
      if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.status")) {
        Assert.assertTrue(StringUtils.isEmpty(measurementPath.getMeasurementAlias()));
        Assert.assertEquals(0, measurementPath.getTagMap().size());
        Assert.assertEquals(TSDataType.BOOLEAN, measurementPath.getMeasurementSchema().getType());
        Assert.assertEquals(
            TSEncoding.PLAIN, measurementPath.getMeasurementSchema().getEncodingType());
        Assert.assertEquals(
            CompressionType.SNAPPY, measurementPath.getMeasurementSchema().getCompressor());
      } else if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.temperature")) {
        // Only when user query with alias, the alias in path will be set
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
    patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.temp"));
    patternTree.constructTree();
    schemas =
        schemaRegion.fetchSeriesSchema(
            patternTree, Collections.emptyMap(), false, false, true, false);
    measurementPaths =
        schemas.searchMeasurementPaths(new PartialPath("root.sg.wf01.wt01.temp")).left;
    Assert.assertEquals(1, measurementPaths.size());
    Assert.assertEquals("root.sg.wf01.wt01.temperature", measurementPaths.get(0).getFullPath());
    Assert.assertEquals("temp", measurementPaths.get(0).getMeasurementAlias());
    Assert.assertNull(measurementPaths.get(0).getTagMap());
    Assert.assertEquals(TSDataType.FLOAT, measurementPaths.get(0).getMeasurementSchema().getType());
    Assert.assertEquals(
        TSEncoding.RLE, measurementPaths.get(0).getMeasurementSchema().getEncodingType());
    Assert.assertEquals(
        CompressionType.GZIP, measurementPaths.get(0).getMeasurementSchema().getCompressor());

    // Test blacklist
    patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.**"));
    patternTree.constructTree();

    Assert.assertEquals(new Pair<>(2L, false), schemaRegion.constructSchemaBlackList(patternTree));

    Assert.assertTrue(
        schemaRegion
            .fetchSeriesSchema(patternTree, Collections.emptyMap(), false, false, true, false)
            .isEmpty());

    patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.temperature"));
    patternTree.constructTree();
    Assert.assertTrue(
        schemaRegion
            .fetchSeriesSchema(patternTree, Collections.emptyMap(), false, false, true, false)
            .isEmpty());

    patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.*.temperature"));
    patternTree.constructTree();
    Assert.assertTrue(
        schemaRegion
            .fetchSeriesSchema(patternTree, Collections.emptyMap(), false, false, true, false)
            .isEmpty());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.wf02.wt01"),
            Arrays.asList("temperature", "status"),
            Arrays.asList(TSDataType.valueOf("FLOAT"), TSDataType.valueOf("INT32")),
            Arrays.asList(TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            null,
            null,
            null));
    final Map<Integer, MetadataException> checkRes =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf02.wt01"), Arrays.asList("temperature", "status"), null);
    Assert.assertEquals(2, checkRes.size());
    Assert.assertTrue(checkRes.get(0) instanceof MeasurementAlreadyExistException);
    Assert.assertTrue(checkRes.get(1) instanceof MeasurementAlreadyExistException);
  }

  @Test
  public void testCreateAlignedTimeSeriesWithMerge() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    final Map<String, String> oldTagMap = Collections.singletonMap("tagK", "tagV");
    final Map<String, String> oldAttrMap = Collections.singletonMap("attrK1", "attrV1");
    schemaRegion.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.wf02.wt01"),
            Arrays.asList("temperature", "status"),
            Arrays.asList(TSDataType.valueOf("FLOAT"), TSDataType.valueOf("INT32")),
            Arrays.asList(TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            null,
            Arrays.asList(Collections.emptyMap(), oldTagMap),
            Arrays.asList(Collections.emptyMap(), oldAttrMap)));

    final Map<String, String> newTagMap = Collections.singletonMap("tagK", "newTagV");
    final Map<String, String> newAttrMap = Collections.singletonMap("attrK2", "attrV2");
    ICreateAlignedTimeSeriesPlan mergePlan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.wf02.wt01"),
            // The lists must be mutable
            new ArrayList<>(Arrays.asList("status", "height")),
            new ArrayList<>(
                Arrays.asList(TSDataType.valueOf("INT32"), TSDataType.valueOf("INT64"))),
            new ArrayList<>(
                Arrays.asList(TSEncoding.valueOf("PLAIN"), TSEncoding.valueOf("PLAIN"))),
            new ArrayList<>(Arrays.asList(CompressionType.ZSTD, CompressionType.GZIP)),
            new ArrayList<>(Arrays.asList("alias2", null)),
            new ArrayList<>(Arrays.asList(newTagMap, oldTagMap)),
            new ArrayList<>(Arrays.asList(newAttrMap, oldAttrMap)));
    ((CreateAlignedTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
    schemaRegion.createAlignedTimeSeries(mergePlan);

    // The encoding and compressor won't be changed
    // The alias/tags/attributes are updated

    final Map<String, String> resultAttrMap = new HashMap<>(oldAttrMap);
    resultAttrMap.putAll(newAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf02.wt01.status"),
        true,
        TSDataType.INT32,
        TSEncoding.RLE,
        CompressionType.SNAPPY,
        "alias2",
        newTagMap,
        resultAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf02.wt01.height"),
        true,
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        oldTagMap,
        oldAttrMap);

    // Test illegal plan
    try {
      mergePlan =
          SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
              new PartialPath("root.sg.wf02.wt01"),
              Collections.singletonList("temperature"),
              Collections.singletonList(TSDataType.valueOf("BOOLEAN")),
              Collections.singletonList(TSEncoding.valueOf("PLAIN")),
              Collections.singletonList(CompressionType.ZSTD),
              Collections.singletonList("alias2"),
              Collections.singletonList(newTagMap),
              Collections.singletonList(newAttrMap));
      ((CreateAlignedTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
      schemaRegion.createAlignedTimeSeries(mergePlan);
      Assert.fail("Create aligned time series with merge shall fail if the types are different");
    } catch (final MeasurementAlreadyExistException e) {
      // Success
    }
  }

  @Test
  public void testCheckMeasurementExistence() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.temperature"),
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
    final Map<Integer, MetadataException> res1 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01.wt01"),
            IntStream.range(0, 5).mapToObj(i -> "s" + i).collect(Collectors.toList()),
            IntStream.range(0, 5).mapToObj(i -> "alias" + i).collect(Collectors.toList()));
    Assert.assertEquals(0, res1.size());
    final Map<Integer, MetadataException> res2 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01"),
            Collections.singletonList("wt01"),
            Collections.singletonList("alias1"));
    Assert.assertEquals(0, res2.size());
    // all exist
    final Map<Integer, MetadataException> res3 =
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
  public void testCreateTimeSeriesWithMerge() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    final Map<String, String> oldTagMap = Collections.singletonMap("tagK", "tagV");
    final Map<String, String> oldAttrMap = Collections.singletonMap("attrK1", "attrV1");
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            oldTagMap,
            oldAttrMap,
            null),
        -1);

    final Map<String, String> newTagMap = Collections.singletonMap("tagK", "newTagV");
    final Map<String, String> newAttrMap = Collections.singletonMap("attrK2", "attrV2");
    ICreateTimeSeriesPlan mergePlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.ZSTD,
            null,
            newTagMap,
            newAttrMap,
            "alias2");
    ((CreateTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
    schemaRegion.createTimeSeries(mergePlan, -1);

    // The encoding and compressor won't be changed
    // The alias/tags/attributes are updated

    final Map<String, String> resultAttrMap = new HashMap<>(oldAttrMap);
    resultAttrMap.putAll(newAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf01.wt01.v1.s1"),
        false,
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        "alias2",
        newTagMap,
        resultAttrMap);

    // Test illegal plan
    try {
      mergePlan =
          SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
              new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
              TSDataType.INT64,
              TSEncoding.PLAIN,
              CompressionType.ZSTD,
              null,
              oldTagMap,
              oldAttrMap,
              null);
      ((CreateTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
      schemaRegion.createTimeSeries(mergePlan, -1);
      Assert.fail("Create time series with merge shall fail if the types are different");
    } catch (final MeasurementAlreadyExistException e) {
      // Success
    }
  }

  /**
   * Test {@link ISchemaRegion#constructSchemaBlackList}, {@link
   * ISchemaRegion#rollbackSchemaBlackList}, {@link ISchemaRegion#fetchSchemaBlackList} and{@link
   * ISchemaRegion#deleteTimeseriesInBlackList}
   */
  @Test
  public void testDeleteTimeSeries() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt02.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf02.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.*"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.*.status"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion.constructSchemaBlackList(patternTree).getLeft() >= 4);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.temperature"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"),
                new PartialPath("root.sg.wf02.wt01.temperature"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    final PathPatternTree rollbackTree = new PathPatternTree();
    rollbackTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    rollbackTree.constructTree();
    schemaRegion.rollbackSchemaBlackList(rollbackTree);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.temperature"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    schemaRegion.deleteTimeseriesInBlackList(patternTree);
    final List<MeasurementPath> schemas =
        schemaRegion
            .fetchSeriesSchema(ALL_MATCH_SCOPE, Collections.emptyMap(), false, false, true, false)
            .searchMeasurementPaths(ALL_MATCH_PATTERN)
            .left;
    Assert.assertEquals(1, schemas.size());
    Assert.assertEquals("root.sg.wf02.wt01.temperature", schemas.get(0).getFullPath());
  }

  @Test
  public void testGetAllTimeSeriesCount() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    // for Non prefix matched path
    Assert.assertEquals(
        6, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.**"), null, false));
    Assert.assertEquals(
        6, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.**"), null, false));
    Assert.assertEquals(
        1, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.*"), null, false));
    Assert.assertEquals(
        4, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.*.*"), null, false));
    Assert.assertEquals(
        5, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.*.**"), null, false));
    Assert.assertEquals(
        1, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.*.*.t1"), null, false));
    Assert.assertEquals(
        2, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.*.s1"), null, false));
    Assert.assertEquals(
        3, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d1.**"), null, false));
    Assert.assertEquals(
        2, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d1.*"), null, false));
    Assert.assertEquals(
        1, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d2.s1"), null, false));
    Assert.assertEquals(
        2, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d2.**"), null, false));
    Assert.assertEquals(
        0, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop"), null, false));
    Assert.assertEquals(
        0, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d3.s1"), null, false));

    // for prefix matched path
    Assert.assertEquals(
        6, getAllTimeSeriesCount(schemaRegion, new PartialPath("root"), null, true));
    Assert.assertEquals(
        6, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop"), null, true));
    Assert.assertEquals(
        2, getAllTimeSeriesCount(schemaRegion, new PartialPath("root.laptop.d2"), null, true));
  }

  @Test
  public void testGetMeasurementCountGroupByLevel() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    final Map<PartialPath, Long> expected = new HashMap<>();
    expected.put(new PartialPath("root"), (long) 6);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(schemaRegion, new PartialPath("root.**"), 0, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 1);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(schemaRegion, new PartialPath("root.laptop.*"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d0"), (long) 1);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(schemaRegion, new PartialPath("root.laptop.d0"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 2);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.d1.*"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 3);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.d1.**"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d2"), (long) 2);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.d2.*"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 2);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.*.s1"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 1);
    expected.put(new PartialPath("root.laptop.d2"), (long) 1);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.*.s1"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 1);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.*.s2"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 2);
    expected.put(new PartialPath("root.laptop.d2"), (long) 2);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(
            schemaRegion, new PartialPath("root.laptop.*.*"), 2, false));
    expected.clear();

    // for prefix matched path
    expected.put(new PartialPath("root"), (long) 6);
    Assert.assertEquals(
        expected, getMeasurementCountGroupByLevel(schemaRegion, new PartialPath("root"), 0, true));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 3);
    Assert.assertEquals(
        expected,
        getMeasurementCountGroupByLevel(schemaRegion, new PartialPath("root.laptop.d1"), 2, true));
    expected.clear();
  }

  @Test
  public void testGetDevicesNum() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(4, getDevicesNum(schemaRegion, new PartialPath("root.**"), false));
    Assert.assertEquals(1, getDevicesNum(schemaRegion, new PartialPath("root.laptop"), false));
    Assert.assertEquals(2, getDevicesNum(schemaRegion, new PartialPath("root.laptop.*"), false));
    Assert.assertEquals(0, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d0"), false));
    Assert.assertEquals(1, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d1"), false));
    Assert.assertEquals(
        1, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d1.s2"), false));
    Assert.assertEquals(1, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d2"), false));
    Assert.assertEquals(2, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d*"), false));
    Assert.assertEquals(2, getDevicesNum(schemaRegion, new PartialPath("root.*.d*"), false));
    Assert.assertEquals(1, getDevicesNum(schemaRegion, new PartialPath("root.**.s2"), false));

    // for prefix matched path
    Assert.assertEquals(4, getDevicesNum(schemaRegion, new PartialPath("root"), true));
    Assert.assertEquals(4, getDevicesNum(schemaRegion, new PartialPath("root.laptop"), true));
    Assert.assertEquals(3, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d*"), true));
    Assert.assertEquals(1, getDevicesNum(schemaRegion, new PartialPath("root.laptop.d1.*"), true));
  }

  @Test
  public void testGetNodesListInGivenLevel() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(
        new LinkedList<>(Collections.singletonList(new PartialPath("root"))),
        getNodesListInGivenLevel(schemaRegion, new PartialPath("root.**"), 0, false));
    Assert.assertEquals(
        new LinkedList<>(Collections.singletonList(new PartialPath("root.laptop"))),
        getNodesListInGivenLevel(schemaRegion, new PartialPath("root.**"), 1, false));
    Assert.assertEquals(
        new LinkedList<>(Collections.singletonList(new PartialPath("root.laptop"))),
        getNodesListInGivenLevel(schemaRegion, new PartialPath("root.laptop"), 1, false));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d0"),
                new PartialPath("root.laptop.d1"),
                new PartialPath("root.laptop.d2"))),
        new HashSet<>(
            getNodesListInGivenLevel(schemaRegion, new PartialPath("root.laptop.**"), 2, false)));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d1.s1"),
                new PartialPath("root.laptop.d1.s2"),
                new PartialPath("root.laptop.d1.s3"),
                new PartialPath("root.laptop.d2.s1"),
                new PartialPath("root.laptop.d2.s2"))),
        new HashSet<>(
            getNodesListInGivenLevel(schemaRegion, new PartialPath("root.laptop.**"), 3, false)));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d1.s1"), new PartialPath("root.laptop.d2.s1"))),
        new HashSet<>(
            getNodesListInGivenLevel(schemaRegion, new PartialPath("root.laptop.*.s1"), 3, false)));
    // Empty return
    Assert.assertEquals(
        new HashSet<>(Collections.emptyList()),
        new HashSet<>(
            getNodesListInGivenLevel(
                schemaRegion, new PartialPath("root.laptop.notExists"), 1, false)));
  }

  @Test
  public void testGetChildNodePathInNextLevel() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(
        new HashSet<>(Collections.emptyList()),
        getChildNodePathInNextLevel(schemaRegion, new PartialPath("root.laptop.d0")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new ShowNodesResult("root.laptop.d1.s1", MNodeType.MEASUREMENT),
                new ShowNodesResult("root.laptop.d1.s2", MNodeType.DEVICE),
                new ShowNodesResult("root.laptop.d1.s3", MNodeType.MEASUREMENT))),
        getChildNodePathInNextLevel(schemaRegion, new PartialPath("root.laptop.d1")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new ShowNodesResult("root.laptop.d2.s1", MNodeType.MEASUREMENT),
                new ShowNodesResult("root.laptop.d2.s2", MNodeType.MEASUREMENT))),
        getChildNodePathInNextLevel(schemaRegion, new PartialPath("root.laptop.d2")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new ShowNodesResult("root.laptop.d0", MNodeType.MEASUREMENT),
                new ShowNodesResult("root.laptop.d1", MNodeType.DEVICE),
                new ShowNodesResult("root.laptop.d2", MNodeType.DEVICE))),
        getChildNodePathInNextLevel(schemaRegion, new PartialPath("root.laptop")));

    Assert.assertEquals(
        new HashSet<>(
            Collections.singletonList(
                new ShowNodesResult("root.laptop.d1.s2.t1", MNodeType.MEASUREMENT))),
        getChildNodePathInNextLevel(schemaRegion, new PartialPath("root.**.s2")));
  }

  @Test
  public void testGetMatchedDevices() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    // CASE 01. Query a timeseries, result should be empty set.
    Assert.assertEquals(
        Collections.emptyList(),
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.laptop.d0")));

    // CASE 02. Query an existing device.
    Assert.assertEquals(
        Collections.singletonList(new ShowDevicesResult("root.laptop.d1", false, -1)),
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.laptop.d1")));
    Assert.assertEquals(
        Collections.singletonList(new ShowDevicesResult("root.laptop.d2", false, -1)),
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.laptop.d2")));

    // CASE 03. Query an existing device, which has a sub device
    Assert.assertEquals(
        Collections.singletonList(new ShowDevicesResult("root.laptop", false, -1)),
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.laptop")));

    // CASE 04. Query devices using '*'
    Assert.assertEquals(
        Collections.singletonList(new ShowDevicesResult("root.laptop", false, -1)),
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.*")));

    // CASE 05. Query all devices using 'root.**'
    List<IDeviceSchemaInfo> expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop", false, -1),
            new ShowDevicesResult("root.laptop.d1", false, -1),
            new ShowDevicesResult("root.laptop.d2", false, -1),
            new ShowDevicesResult("root.laptop.d1.s2", false, -1));
    List<IDeviceSchemaInfo> actualResult =
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.**"));
    // Compare hash sets because the order does not matter.
    HashSet<IDeviceSchemaInfo> expectedHashset = new HashSet<>(expectedList);
    HashSet<IDeviceSchemaInfo> actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);

    // CASE 06. show devices root.**.d*
    expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop.d1", false, -1),
            new ShowDevicesResult("root.laptop.d2", false, -1));
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.**.d*"));
    // Compare hash sets because the order does not matter.
    expectedHashset = new HashSet<>(expectedList);
    actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);

    // CASE 07. show devices root.** limit 3 offset 0
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion, new PartialPath("root.**"), 3, 0, false);
    Assert.assertEquals(3, actualResult.size());
    // CASE 08. show devices root.** limit 3 offset 1
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion, new PartialPath("root.**"), 3, 1, false);
    Assert.assertEquals(3, actualResult.size());
    // CASE 09. show devices root.** limit 3 offset 2
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion, new PartialPath("root.**"), 3, 2, false);
    Assert.assertEquals(2, actualResult.size());
    // CASE 10. show devices root.** limit 3 offset 99
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion, new PartialPath("root.**"), 3, 99, false);
    Assert.assertEquals(0, actualResult.size());
    // CASE 11. show devices root.** where device contains 'laptop'
    expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop", false, -1),
            new ShowDevicesResult("root.laptop.d1", false, -1),
            new ShowDevicesResult("root.laptop.d1.s2", false, -1),
            new ShowDevicesResult("root.laptop.d2", false, -1));
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion,
            new PartialPath("root.**"),
            0,
            0,
            false,
            SchemaFilterFactory.createPathContainsFilter("laptop"));
    expectedHashset = new HashSet<>(expectedList);
    actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);
    // CASE 11. show devices root.** where device contains 'laptop.d' limit 2 offset 0
    expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop.d1", false, -1),
            new ShowDevicesResult("root.laptop.d1.s2", false, -1));
    actualResult =
        SchemaRegionTestUtil.getMatchedDevices(
            schemaRegion,
            new PartialPath("root.**"),
            2,
            0,
            false,
            SchemaFilterFactory.createPathContainsFilter("laptop.d"));
    expectedHashset = new HashSet<>(expectedList);
    actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);
  }

  @Test
  public void testShowTimeseries() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    // CASE 01: all timeseries
    List<ITimeSeriesSchemaInfo> result =
        SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath("root.**"));
    Set<String> expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d0",
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    int expectedSize = 6;
    Assert.assertEquals(expectedSize, result.size());
    Set<String> actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 02: some timeseries, pattern "root.**.s*"
    result = SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath("root.**.s*"));
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d1.s1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    expectedSize = 4;
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 03: show timeseries where path contains "s"
    result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            new PartialPath("root.**"),
            Collections.emptyMap(),
            0,
            0,
            false,
            SchemaFilterFactory.createPathContainsFilter("s"),
            false);
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 04: show timeseries where path contains "1"
    result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            new PartialPath("root.**"),
            Collections.emptyMap(),
            0,
            0,
            false,
            SchemaFilterFactory.createPathContainsFilter("1"),
            false);
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1"));
    expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 05: show timeseries where path contains "laptop.d"
    result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            new PartialPath("root.**"),
            Collections.emptyMap(),
            0,
            0,
            false,
            SchemaFilterFactory.createPathContainsFilter("laptop.d"),
            false);
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d0",
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 06: show timeseries where dataType=INT64
    result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            new PartialPath("root.**"),
            Collections.emptyMap(),
            0,
            0,
            false,
            SchemaFilterFactory.createDataTypeFilter(TSDataType.INT64),
            false);
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d0",
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // CASE 07: show timeseries where dataType=BOOLEAN
    result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            new PartialPath("root.**"),
            Collections.emptyMap(),
            0,
            0,
            false,
            SchemaFilterFactory.createDataTypeFilter(TSDataType.BOOLEAN),
            false);
    expectedPathList = new HashSet<>(Collections.emptyList());
    expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);
  }

  @Test
  public void testGetMatchedDevicesWithSpecialPattern() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.test", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList("root.test.d1.s", "root.test.dac.device1.s", "root.test.dac.device1.d1.s"));

    final List<IDeviceSchemaInfo> expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.test.d1", false, -1),
            new ShowDevicesResult("root.test.dac.device1", false, -1),
            new ShowDevicesResult("root.test.dac.device1.d1", false, -1));
    final List<IDeviceSchemaInfo> actualResult =
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.**.d*"));
    // Compare hash sets because the order does not matter.
    final Set<IDeviceSchemaInfo> expectedHashset = new HashSet<>(expectedList);
    final Set<IDeviceSchemaInfo> actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);

    final List<ITimeSeriesSchemaInfo> result =
        SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath("root.**.d*.*"));
    final Set<String> expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.test.d1.s", "root.test.dac.device1.s", "root.test.dac.device1.d1.s"));
    final int expectedSize = 3;
    Assert.assertEquals(expectedSize, result.size());
    final Set<String> actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);
  }

  @Test
  public void testGetMatchedDevicesWithSpecialPattern2() throws Exception {
    final ISchemaRegion schemaRegion = getSchemaRegion("root.test", 0);

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion,
        Arrays.asList(
            "root.test.abc57.bcde22.def89.efg1",
            "root.test.abc57.bcde22.def89.efg2",
            "root.test.abc57.bcd22.def89.efg1",
            "root.test.abc57.bcd22.def89.efg2"));

    // case1: show devices root.**.*b*.*
    final List<IDeviceSchemaInfo> expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.test.abc57.bcde22.def89", false, -1),
            new ShowDevicesResult("root.test.abc57.bcd22.def89", false, -1));
    final List<IDeviceSchemaInfo> actualResult =
        SchemaRegionTestUtil.getMatchedDevices(schemaRegion, new PartialPath("root.**.*b*.*"));
    // Compare hash sets because the order does not matter.
    final Set<IDeviceSchemaInfo> expectedHashset = new HashSet<>(expectedList);
    final Set<IDeviceSchemaInfo> actualHashset = new HashSet<>(actualResult);
    Assert.assertEquals(expectedHashset, actualHashset);

    // case2: show time series root.**.*e*.*e*
    List<ITimeSeriesSchemaInfo> result =
        SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath("root.**.*e*.*e*"));
    final Set<String> expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.test.abc57.bcde22.def89.efg1",
                "root.test.abc57.bcde22.def89.efg2",
                "root.test.abc57.bcd22.def89.efg1",
                "root.test.abc57.bcd22.def89.efg2"));
    final int expectedSize = expectedPathList.size();
    Assert.assertEquals(expectedSize, result.size());
    Set<String> actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // case3: show time series root.**.*e*
    result = SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath("root.**.*e*"));
    Assert.assertEquals(expectedSize, result.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.get(index).getFullPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);
  }
}
