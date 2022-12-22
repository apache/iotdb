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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Ignore
public class MRocksDBUnitTest {

  private RSchemaRegion rSchemaRegion;

  @Before
  public void setUp() throws MetadataException {

    PartialPath storageGroup = new PartialPath("root.test.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId((int) (Math.random() * 10));
    MNode root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    IStorageGroupMNode storageGroupMNode = new StorageGroupMNode(root, "test", -1);
    rSchemaRegion =
        new RSchemaRegion(storageGroup, schemaRegionId, new RSchemaConfLoader());
  }

  @Test
  public void testCreateTimeSeries() throws MetadataException, IOException {
    PartialPath path = new PartialPath("root.test.sg.dd.m1");
    rSchemaRegion.createTimeseries(
        path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);

    IMeasurementMNode m1 = rSchemaRegion.getMeasurementMNode(path);
    Assert.assertNull(m1.getAlias());
    Assert.assertEquals(m1.getSchema().getCompressor(), CompressionType.UNCOMPRESSED);
    Assert.assertEquals(m1.getSchema().getEncodingType(), TSEncoding.PLAIN);
    Assert.assertEquals(m1.getSchema().getType(), TSDataType.TEXT);
    Assert.assertNull(m1.getSchema().getProps());

    PartialPath path2 = new PartialPath("root.tt.sg.dd.m2");
    rSchemaRegion.createTimeseries(
        path2, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.GZIP, null, "ma");

    rSchemaRegion.printScanAllKeys();

    IMeasurementMNode m2 = rSchemaRegion.getMeasurementMNode(path2);
    Assert.assertEquals(m2.getAlias(), "ma");
    Assert.assertEquals(m2.getSchema().getCompressor(), CompressionType.GZIP);
    Assert.assertEquals(m2.getSchema().getEncodingType(), TSEncoding.PLAIN);
    Assert.assertEquals(m2.getSchema().getType(), TSDataType.DOUBLE);
    Assert.assertNull(m2.getSchema().getProps());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws MetadataException, IOException {
    PartialPath prefixPath = new PartialPath("root.tt.sg.dd");
    List<String> measurements = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressions = new ArrayList<>();

    for (int i = 0; i < 6; i++) {
      measurements.add("mm" + i);
      dataTypes.add(TSDataType.INT32);
      encodings.add(TSEncoding.PLAIN);
      compressions.add(CompressionType.UNCOMPRESSED);
    }

    rSchemaRegion.createAlignedTimeSeries(
            SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(prefixPath, measurements, dataTypes, encodings, compressions, null, null, null));

    try {
      PartialPath path = new PartialPath("root.tt.sg.dd.mn");
      rSchemaRegion.createTimeseries(
          path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);
      assert false;
    } catch (MetadataException e) {
      assert true;
    }
    rSchemaRegion.printScanAllKeys();
  }

  @Test
  public void testNodeTypeCount() throws MetadataException, IOException {
    List<PartialPath> storageGroups = new ArrayList<>();
    storageGroups.add(new PartialPath("root.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg1"));
    storageGroups.add(new PartialPath("root.inner.sg2"));
    storageGroups.add(new PartialPath("root.inner1.inner2.inner3.sg"));
    storageGroups.add(new PartialPath("root.inner1.inner2.sg"));

    PartialPath path = new PartialPath("root.tt.sg.dd.m1");
    rSchemaRegion.createTimeseries(
        path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);

    PartialPath path2 = new PartialPath("root.tt.sg.ddd.m2");
    rSchemaRegion.createTimeseries(
        path2, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, "ma");

    rSchemaRegion.printScanAllKeys();

    // test all timeseries number
    Assert.assertEquals(
        1, rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.tt.sg.dd.m1"), Collections.emptyMap(),false));
    Assert.assertEquals(2, rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), Collections.emptyMap(), false));

    // test device number
    Assert.assertEquals(
        0, rSchemaRegion.getDevicesNum(new PartialPath("root.inner1.inner2"), false));
    Assert.assertEquals(
        0, rSchemaRegion.getDevicesNum(new PartialPath("root.inner1.inner2.**"), false));
    Assert.assertEquals(2, rSchemaRegion.getDevicesNum(new PartialPath("root.tt.sg.**"), false));
    Assert.assertEquals(1, rSchemaRegion.getDevicesNum(new PartialPath("root.tt.sg.dd"), false));

    // todo wildcard

    // test nodes count in given level
    Assert.assertEquals(
        2, rSchemaRegion.getNodesListInGivenLevel(new PartialPath("root.tt.sg"), 3, false).size());
  }

  @Test
  public void testPathPatternMatch() throws MetadataException, IOException {
    List<PartialPath> timeseries = new ArrayList<>();
    timeseries.add(new PartialPath("root.sg.d1.m1"));
    timeseries.add(new PartialPath("root.sg.d1.m2"));
    timeseries.add(new PartialPath("root.sg.d2.m1"));
    timeseries.add(new PartialPath("root.sg.d2.m2"));
    timeseries.add(new PartialPath("root.sg1.d1.m1"));
    timeseries.add(new PartialPath("root.sg1.d1.m2"));
    timeseries.add(new PartialPath("root.sg1.d2.m1"));
    timeseries.add(new PartialPath("root.sg1.d2.m2"));

    for (PartialPath path : timeseries) {
      rSchemaRegion.createTimeseries(
          path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);
    }

    //    mRocksDBManager.traverseByPatternPath(new PartialPath("root.sg.d1.*"));
  }

  @Test
  public void testDeleteTimeseries() throws MetadataException, IOException {
    List<PartialPath> timeseries = new ArrayList<>();
    timeseries.add(new PartialPath("root.sg.d1.m1"));
    timeseries.add(new PartialPath("root.sg.d1.m2"));
    timeseries.add(new PartialPath("root.sg.d2.m1"));
    timeseries.add(new PartialPath("root.sg.d2.m2"));
    timeseries.add(new PartialPath("root.sg.d3.m1"));
    timeseries.add(new PartialPath("root.sg.d3.m2"));
    timeseries.add(new PartialPath("root.sg1.d1.m1"));
    timeseries.add(new PartialPath("root.sg1.d1.m2"));
    timeseries.add(new PartialPath("root.sg1.d2.m1"));
    timeseries.add(new PartialPath("root.sg1.d2.m2"));

    for (PartialPath path : timeseries) {
      rSchemaRegion.createTimeseries(
          path, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null);
    }

    Assert.assertEquals(
        rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), Collections.emptyMap(), false), timeseries.size());

    int count = timeseries.size();
    rSchemaRegion.deleteTimeseries(new PartialPath("root.sg.d1.*"), false);
    Assert.assertEquals(
        rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), Collections.emptyMap(), false), count - 2);

    count = count - 2;
    rSchemaRegion.deleteTimeseries(new PartialPath("root.sg1.**"), false);
    Assert.assertEquals(
        rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), Collections.emptyMap(), false), count - 4);

    count = count - 4;
    rSchemaRegion.deleteTimeseries(new PartialPath("root.sg.*.m1"), false);
    Assert.assertEquals(
        rSchemaRegion.getAllTimeseriesCount(new PartialPath("root.**"), Collections.emptyMap(), false), count - 2);

    rSchemaRegion.printScanAllKeys();
  }

  @Test
  public void testUpsert() throws MetadataException, IOException {
    PartialPath path2 = new PartialPath("root.tt.sg.dd.m2");
    rSchemaRegion.createTimeseries(
        path2, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, "ma");

    IMeasurementMNode m1 = rSchemaRegion.getMeasurementMNode(new PartialPath("root.tt.sg.dd.m2"));
    Assert.assertEquals(m1.getAlias(), "ma");

    rSchemaRegion.upsertAliasAndTagsAndAttributes("test", null, null, new PartialPath("root.tt.sg.dd.m2"));

    IMeasurementMNode m2 = rSchemaRegion.getMeasurementMNode(new PartialPath("root.tt.sg.dd.m2"));
    Assert.assertEquals(m2.getAlias(), "test");

    IMeasurementMNode m3 = rSchemaRegion.getMeasurementMNode(new PartialPath("root.tt.sg.dd.test"));
    Assert.assertEquals(m3.getAlias(), "test");
  }

  @Test
  public void testGetMeasurementCountGroupByLevel() throws MetadataException {
    PartialPath path1 = new PartialPath("root.test.sg.dd.m1");
    rSchemaRegion.createTimeseries(
        path1, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.GZIP, null, "ma");
    PartialPath path2 = new PartialPath("root.test.sg.dd.m2");
    rSchemaRegion.createTimeseries(
        path2, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.GZIP, null, null);
    PartialPath path3 = new PartialPath("root.test.sg.dd.m3");
    rSchemaRegion.createTimeseries(
        path3, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.GZIP, null, null);
    PartialPath path4 = new PartialPath("root.test.sg.m4");
    rSchemaRegion.createTimeseries(
        path4, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.GZIP, null, null);

    Map<PartialPath, Long> result =
        rSchemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.**"), 3, false);
    Assert.assertEquals(3, (long) result.get(new PartialPath("root.test.sg.dd")));
    Assert.assertEquals(1, (long) result.get(new PartialPath("root.test.sg.m4")));

    result =
        rSchemaRegion.getMeasurementCountGroupByLevel(new PartialPath("root.test.**"), 2, false);
    Assert.assertEquals(4, (long) result.get(new PartialPath("root.test.sg")));
  }

  @After
  public void clean() throws MetadataException {
    rSchemaRegion.deleteSchemaRegion();
  }
}
