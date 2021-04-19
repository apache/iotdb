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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MManagerAdvancedTest {

  private static MManager mmanager = null;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mmanager = IoTDB.metaManager;

    mmanager.setStorageGroup(new PartialPath("root.vehicle.d0"));
    mmanager.setStorageGroup(new PartialPath("root.vehicle.d1"));
    mmanager.setStorageGroup(new PartialPath("root.vehicle.d2"));

    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d0.s5"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d1.s5"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() {

    try {
      // test file name
      List<PartialPath> fileNames = mmanager.getAllStorageGroupPaths();
      assertEquals(3, fileNames.size());
      if (fileNames.get(0).equals(new PartialPath("root.vehicle.d0"))) {
        assertEquals(new PartialPath("root.vehicle.d1"), fileNames.get(1));
      } else {
        assertEquals(new PartialPath("root.vehicle.d0"), fileNames.get(1));
      }
      // test filename by seriesPath
      assertEquals(
          new PartialPath("root.vehicle.d0"),
          mmanager.getStorageGroupPath(new PartialPath("root.vehicle.d0.s1")));
      List<PartialPath> pathList =
          mmanager.getAllTimeseriesPath(new PartialPath("root.vehicle.d1.*"));
      assertEquals(6, pathList.size());
      pathList = mmanager.getAllTimeseriesPath(new PartialPath("root.vehicle.d0"));
      assertEquals(6, pathList.size());
      pathList = mmanager.getAllTimeseriesPath(new PartialPath("root.vehicle.d*"));
      assertEquals(12, pathList.size());
      pathList = mmanager.getAllTimeseriesPath(new PartialPath("root.ve*.*"));
      assertEquals(12, pathList.size());
      pathList = mmanager.getAllTimeseriesPath(new PartialPath("root.vehicle*.d*.s1"));
      assertEquals(2, pathList.size());
      pathList = mmanager.getAllTimeseriesPath(new PartialPath("root.vehicle.d2"));
      assertEquals(0, pathList.size());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCache() throws MetadataException {
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d2.s0"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d2.s2.g0"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d2.s3"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    MNode node = mmanager.getNodeByPath(new PartialPath("root.vehicle.d0"));
    Assert.assertEquals(
        TSDataType.INT32, ((MeasurementMNode) node.getChild("s0")).getSchema().getType());

    try {
      mmanager.getNodeByPath(new PartialPath("root.vehicle.d100"));
    } catch (MetadataException e) {
      Assert.assertEquals("Path [root.vehicle.d100] does not exist", e.getMessage());
    }
  }

  @Test
  public void testCachedLastTimeValue() throws MetadataException {
    mmanager.createTimeseries(
        new PartialPath("root.vehicle.d2.s0"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    TimeValuePair tv1 = new TimeValuePair(1000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 1.0));
    TimeValuePair tv2 = new TimeValuePair(2000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 3.0));
    TimeValuePair tv3 = new TimeValuePair(1500, TsPrimitiveType.getByType(TSDataType.DOUBLE, 2.5));
    MNode node = mmanager.getNodeByPath(new PartialPath("root.vehicle.d2.s0"));
    ((MeasurementMNode) node).updateCachedLast(tv1, true, Long.MIN_VALUE);
    ((MeasurementMNode) node).updateCachedLast(tv2, true, Long.MIN_VALUE);
    Assert.assertEquals(
        tv2.getTimestamp(), ((MeasurementMNode) node).getCachedLast().getTimestamp());
    ((MeasurementMNode) node).updateCachedLast(tv3, true, Long.MIN_VALUE);
    Assert.assertEquals(
        tv2.getTimestamp(), ((MeasurementMNode) node).getCachedLast().getTimestamp());
  }
}
