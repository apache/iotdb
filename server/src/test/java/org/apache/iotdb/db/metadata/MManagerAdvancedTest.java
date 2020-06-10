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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MManagerAdvancedTest {

  private static MManager mmanager = null;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mmanager = MManager.getInstance();

    mmanager.setStorageGroup("root.vehicle.d0");
    mmanager.setStorageGroup("root.vehicle.d1");
    mmanager.setStorageGroup("root.vehicle.d2");

    mmanager.createTimeseries("root.vehicle.d0.s0", TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d0.s1", TSDataType.INT64, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d0.s2", TSDataType.FLOAT, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d0.s3", TSDataType.DOUBLE, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d0.s4", TSDataType.BOOLEAN, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d0.s5", TSDataType.TEXT, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());

    mmanager.createTimeseries("root.vehicle.d1.s0", TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d1.s2", TSDataType.FLOAT, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d1.s3", TSDataType.DOUBLE, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d1.s4", TSDataType.BOOLEAN, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d1.s5", TSDataType.TEXT, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());

  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() {

    try {
      // test file name
      List<String> fileNames = mmanager.getAllStorageGroupNames();
      assertEquals(3, fileNames.size());
      if (fileNames.get(0).equals("root.vehicle.d0")) {
        assertEquals("root.vehicle.d1", fileNames.get(1));
      } else {
        assertEquals("root.vehicle.d0", fileNames.get(1));
      }
      // test filename by seriesPath
      assertEquals("root.vehicle.d0", mmanager.getStorageGroupName("root.vehicle.d0.s1"));
      List<String> pathList = mmanager.getAllTimeseriesName("root.vehicle.d1.*");
      assertEquals(6, pathList.size());
      pathList = mmanager.getAllTimeseriesName("root.vehicle.d0");
      assertEquals(6, pathList.size());
      pathList = mmanager.getAllTimeseriesName("root.vehicle.d*");
      assertEquals(12, pathList.size());
      pathList = mmanager.getAllTimeseriesName("root.ve*.*");
      assertEquals(12, pathList.size());
      pathList = mmanager.getAllTimeseriesName("root.vehicle*.d*.s1");
      assertEquals(2, pathList.size());
      pathList = mmanager.getAllTimeseriesName("root.vehicle.d2");
      assertEquals(0, pathList.size());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCache() throws MetadataException {
    mmanager.createTimeseries("root.vehicle.d2.s0", TSDataType.DOUBLE, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d2.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d2.s2.g0", TSDataType.TEXT, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    mmanager.createTimeseries("root.vehicle.d2.s3", TSDataType.TEXT, TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());

    MNode node = mmanager.getNodeByPath("root.vehicle.d0");
    Assert.assertEquals(TSDataType.INT32, ((MeasurementMNode) node.getChild("s0")).getSchema().getType());

    try {
      mmanager.getNodeByPath("root.vehicle.d100");
      fail();
    } catch (MetadataException e) {
      // ignore
    }
  }

  @Test
  public void testCachedLastTimeValue() throws MetadataException {
    mmanager.createTimeseries("root.vehicle.d2.s0", TSDataType.DOUBLE, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());

    TimeValuePair tv1 = new TimeValuePair(1000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 1.0));
    TimeValuePair tv2 = new TimeValuePair(2000, TsPrimitiveType.getByType(TSDataType.DOUBLE, 3.0));
    TimeValuePair tv3 = new TimeValuePair(1500, TsPrimitiveType.getByType(TSDataType.DOUBLE, 2.5));
    MNode node = mmanager.getNodeByPath("root.vehicle.d2.s0");
    ((MeasurementMNode)node).updateCachedLast(tv1, true, Long.MIN_VALUE);
    ((MeasurementMNode)node).updateCachedLast(tv2, true, Long.MIN_VALUE);
    Assert.assertEquals(tv2.getTimestamp(), ((MeasurementMNode)node).getCachedLast().getTimestamp());
    ((MeasurementMNode)node).updateCachedLast(tv3, true, Long.MIN_VALUE);
    Assert.assertEquals(tv2.getTimestamp(), ((MeasurementMNode)node).getCachedLast().getTimestamp());
  }

  @Test
  public void testRemoteCache() throws MetadataException {
    mmanager.createTimeseries("root.vehicle.d2.s0", TSDataType.DOUBLE, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    MeasurementSchema schema1 = new MeasurementSchema("root.test1.a.b", TSDataType.INT32);
    MeasurementSchema schema2= new MeasurementSchema("root.test2.a.c", TSDataType.BOOLEAN);
    MeasurementSchema schema3 = new MeasurementSchema("root.test1.a.d", TSDataType.TEXT);

    mmanager.cacheSchema("root.test1.a.b", schema1);
    mmanager.cacheSchema("root.test2.a.c", schema2);
    mmanager.cacheSchema("root.test1.a.d", schema3);
    mmanager.cacheSchema("root.vehicle.d2.s0", schema2);

    Assert.assertEquals(TSDataType.INT32, mmanager.getSeriesType("root.test1.a.b"));
    Assert.assertEquals(TSDataType.BOOLEAN, mmanager.getSeriesType("root.test2.a.c"));
    Assert.assertEquals(TSDataType.TEXT, mmanager.getSeriesType("root.test1.a.d"));
    Assert.assertEquals(TSDataType.DOUBLE, mmanager.getSeriesType("root.vehicle.d2.s0"));

    List<String> groups = new ArrayList<>();
    groups.add("root.test2");
    try {
      mmanager.deleteStorageGroups(groups);
    } catch (MetadataException e) {
      // ignore
    }
    try {
      mmanager.getSeriesType("root.test2.a.c");
      fail();
    } catch (MetadataException e) {
      // ignore
    }
    try {
      mmanager.deleteTimeseries("root.test1.a.b");
    } catch (MetadataException e) {
      // ignore
    }
    try {
      mmanager.getSeriesType("root.test1.a.b");
      fail();
    } catch (MetadataException e) {
      //ignore
    }
  }
}
