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

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowDevicesResult;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowTimeSeriesResult;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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

  @Test
  public void testFetchSchema() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
  public void testCreateAlignedTimeseries() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
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
    Map<Integer, MetadataException> checkRes =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf02.wt01"), Arrays.asList("temperature", "status"), null);
    Assert.assertEquals(2, checkRes.size());
    Assert.assertTrue(checkRes.get(0) instanceof MeasurementAlreadyExistException);
    Assert.assertTrue(checkRes.get(1) instanceof MeasurementAlreadyExistException);
  }

  @Test
  public void testCheckMeasurementExistence() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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

  /**
   * Test {@link ISchemaRegion#constructSchemaBlackList}, {@link
   * ISchemaRegion#rollbackSchemaBlackList}, {@link ISchemaRegion#fetchSchemaBlackList} and{@link
   * ISchemaRegion#deleteTimeseriesInBlackList}
   */
  @Test
  public void testDeleteTimeseries() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
    Assert.assertTrue(schemaRegion.constructSchemaBlackList(patternTree) >= 4);
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

  @Test
  public void testGetAllTimeseriesCount() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
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
        6, schemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), null, false));
    Assert.assertEquals(
        6, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.**"), null, false));
    Assert.assertEquals(
        1, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.*"), null, false));
    Assert.assertEquals(
        4, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.*.*"), null, false));
    Assert.assertEquals(
        5, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.*.**"), null, false));
    Assert.assertEquals(
        1, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1"), null, false));
    Assert.assertEquals(
        2, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1"), null, false));
    Assert.assertEquals(
        3, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**"), null, false));
    Assert.assertEquals(
        2, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*"), null, false));
    Assert.assertEquals(
        1, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1"), null, false));
    Assert.assertEquals(
        2, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d2.**"), null, false));
    Assert.assertEquals(
        0, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop"), null, false));
    Assert.assertEquals(
        0, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1"), null, false));

    // for prefix matched path
    Assert.assertEquals(6, schemaRegion.getAllTimeseriesCount(new PartialPath("root"), null, true));
    Assert.assertEquals(
        6, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop"), null, true));
    Assert.assertEquals(
        2, schemaRegion.getAllTimeseriesCount(new PartialPath("root.laptop.d2"), null, true));
  }

  @Test
  public void testGetMeasurementCountGroupByLevel() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Map<PartialPath, Long> expected = new HashMap<>();
    expected.put(new PartialPath("root"), (long) 6);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.**"), 0, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 1);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.laptop.*"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d0"), (long) 1);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.laptop.d0"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 0);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.laptop.d1"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 2);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.d1.*"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 3);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.d1.**"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d2"), (long) 2);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.d2.*"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 2);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.*.s1"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d0"), (long) 0);
    expected.put(new PartialPath("root.laptop.d1"), (long) 1);
    expected.put(new PartialPath("root.laptop.d2"), (long) 1);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.*.s1"), 2, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop"), (long) 1);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(
            new PartialPath("root.laptop.*.s2"), 1, false));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d0"), (long) 0);
    expected.put(new PartialPath("root.laptop.d1"), (long) 2);
    expected.put(new PartialPath("root.laptop.d2"), (long) 2);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.laptop.*.*"), 2, false));
    expected.clear();

    // for prefix matched path
    expected.put(new PartialPath("root"), (long) 6);
    Assert.assertEquals(
        expected, schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root"), 0, true));
    expected.clear();

    expected.put(new PartialPath("root.laptop.d1"), (long) 3);
    Assert.assertEquals(
        expected,
        schemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.laptop.d1"), 2, true));
    expected.clear();
  }

  @Test
  public void testGetDevicesNum() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(4, schemaRegion.getDevicesNum(new PartialPath("root.**"), false));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.laptop"), false));
    Assert.assertEquals(2, schemaRegion.getDevicesNum(new PartialPath("root.laptop.*"), false));
    Assert.assertEquals(0, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d0"), false));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d1"), false));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d1.s2"), false));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d2"), false));
    Assert.assertEquals(2, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d*"), false));
    Assert.assertEquals(2, schemaRegion.getDevicesNum(new PartialPath("root.*.d*"), false));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.**.s2"), false));

    // for prefix matched path
    Assert.assertEquals(4, schemaRegion.getDevicesNum(new PartialPath("root"), true));
    Assert.assertEquals(4, schemaRegion.getDevicesNum(new PartialPath("root.laptop"), true));
    Assert.assertEquals(3, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d*"), true));
    Assert.assertEquals(1, schemaRegion.getDevicesNum(new PartialPath("root.laptop.d1.*"), true));
  }

  @Test
  public void testGetNodesListInGivenLevel() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(
        new LinkedList<>(Arrays.asList(new PartialPath("root"))),
        schemaRegion.getNodesListInGivenLevel(new PartialPath("root.**"), 0, false));
    Assert.assertEquals(
        new LinkedList<>(Arrays.asList(new PartialPath("root.laptop"))),
        schemaRegion.getNodesListInGivenLevel(new PartialPath("root.**"), 1, false));
    Assert.assertEquals(
        new LinkedList<>(Arrays.asList(new PartialPath("root.laptop"))),
        schemaRegion.getNodesListInGivenLevel(new PartialPath("root.laptop"), 1, false));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d0"),
                new PartialPath("root.laptop.d1"),
                new PartialPath("root.laptop.d2"))),
        new HashSet<>(
            schemaRegion.getNodesListInGivenLevel(new PartialPath("root.laptop.**"), 2, false)));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d1.s1"),
                new PartialPath("root.laptop.d1.s2"),
                new PartialPath("root.laptop.d1.s3"),
                new PartialPath("root.laptop.d2.s1"),
                new PartialPath("root.laptop.d2.s2"))),
        new HashSet<>(
            schemaRegion.getNodesListInGivenLevel(new PartialPath("root.laptop.**"), 3, false)));
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.laptop.d1.s1"), new PartialPath("root.laptop.d2.s1"))),
        new HashSet<>(
            schemaRegion.getNodesListInGivenLevel(new PartialPath("root.laptop.*.s1"), 3, false)));
    // Empty return
    Assert.assertEquals(
        new HashSet<>(Arrays.asList()),
        new HashSet<>(
            schemaRegion.getNodesListInGivenLevel(
                new PartialPath("root.laptop.notExists"), 1, false)));
  }

  @Test
  public void testGetChildNodePathInNextLevel() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(
        new HashSet<>(Arrays.asList()),
        schemaRegion.getChildNodePathInNextLevel(new PartialPath("root.laptop.d0")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new TSchemaNode("root.laptop.d1.s1", MNodeType.MEASUREMENT.getNodeType()),
                new TSchemaNode("root.laptop.d1.s2", MNodeType.DEVICE.getNodeType()),
                new TSchemaNode("root.laptop.d1.s3", MNodeType.MEASUREMENT.getNodeType()))),
        schemaRegion.getChildNodePathInNextLevel(new PartialPath("root.laptop.d1")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new TSchemaNode("root.laptop.d2.s1", MNodeType.MEASUREMENT.getNodeType()),
                new TSchemaNode("root.laptop.d2.s2", MNodeType.MEASUREMENT.getNodeType()))),
        schemaRegion.getChildNodePathInNextLevel(new PartialPath("root.laptop.d2")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new TSchemaNode("root.laptop.d0", MNodeType.MEASUREMENT.getNodeType()),
                new TSchemaNode("root.laptop.d1", MNodeType.DEVICE.getNodeType()),
                new TSchemaNode("root.laptop.d2", MNodeType.DEVICE.getNodeType()))),
        schemaRegion.getChildNodePathInNextLevel(new PartialPath("root.laptop")));

    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new TSchemaNode("root.laptop.d1.s2.t1", MNodeType.MEASUREMENT.getNodeType()))),
        schemaRegion.getChildNodePathInNextLevel(new PartialPath("root.**.s2")));
  }

  @Test
  public void testGetMatchedDevices() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    Assert.assertEquals(
        new Pair<>(Arrays.asList(), 0),
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.laptop.d0"))));
    Assert.assertEquals(
        new Pair<>(Arrays.asList(new ShowDevicesResult("root.laptop.d1", false)), 0),
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.laptop.d1"))));
    Assert.assertEquals(
        new Pair<>(Arrays.asList(new ShowDevicesResult("root.laptop.d2", false)), 0),
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.laptop.d2"))));
    Assert.assertEquals(
        new Pair<>(Arrays.asList(new ShowDevicesResult("root.laptop", false)), 0),
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.laptop"))));
    Assert.assertEquals(
        new Pair<>(Arrays.asList(new ShowDevicesResult("root.laptop", false)), 0),
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.*"))));

    List<ShowDevicesResult> expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop", false),
            new ShowDevicesResult("root.laptop.d1", false),
            new ShowDevicesResult("root.laptop.d2", false),
            new ShowDevicesResult("root.laptop.d1.s2", false));
    Integer expectedOffset = 0;
    Pair<List<ShowDevicesResult>, Integer> actualResult =
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.**")));
    // Compare hash sets because the order does not matter.
    HashSet<ShowDevicesResult> expectedHashset = new HashSet<>(expectedList);
    HashSet<ShowDevicesResult> actualHashset = new HashSet<>(actualResult.left);
    Assert.assertEquals(expectedHashset, actualHashset);
    Assert.assertEquals(expectedOffset, actualResult.right);

    expectedList =
        Arrays.asList(
            new ShowDevicesResult("root.laptop.d1", false),
            new ShowDevicesResult("root.laptop.d2", false));
    expectedOffset = 0;
    actualResult =
        schemaRegion.getMatchedDevices(
            SchemaRegionReadPlanFactory.getShowDevicesPlan(new PartialPath("root.**.d*")));
    // Compare hash sets because the order does not matter.
    expectedHashset = new HashSet<>(expectedList);
    actualHashset = new HashSet<>(actualResult.left);
    Assert.assertEquals(expectedHashset, actualHashset);
    Assert.assertEquals(expectedOffset, actualResult.right);
  }

  @Test
  public void testShowTimeseries() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.laptop", 0);

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion,
        Arrays.asList(
            "root.laptop.d0",
            "root.laptop.d1.s1",
            "root.laptop.d1.s2.t1",
            "root.laptop.d1.s3",
            "root.laptop.d2.s1",
            "root.laptop.d2.s2"));

    // case 01: all timeseries
    Pair<List<ShowTimeSeriesResult>, Integer> result =
        schemaRegion.showTimeseries(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(new PartialPath("root.**")));
    HashSet<String> expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d0",
                "root.laptop.d1.s1",
                "root.laptop.d1.s2.t1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    int expectedSize = 6;
    Assert.assertEquals(expectedSize, result.left.size());
    HashSet<String> actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.left.get(index).getPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);

    // case 02: some timeseries, pattern "root.**.s*"
    result =
        schemaRegion.showTimeseries(
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(new PartialPath("root.**.s*")));
    expectedPathList =
        new HashSet<>(
            Arrays.asList(
                "root.laptop.d1.s1",
                "root.laptop.d1.s3",
                "root.laptop.d2.s1",
                "root.laptop.d2.s2"));
    expectedSize = 4;
    Assert.assertEquals(expectedSize, result.left.size());
    actualPathList = new HashSet<>();
    for (int index = 0; index < expectedSize; index++) {
      actualPathList.add(result.left.get(index).getPath());
    }
    Assert.assertEquals(expectedPathList, actualPathList);
  }
}
