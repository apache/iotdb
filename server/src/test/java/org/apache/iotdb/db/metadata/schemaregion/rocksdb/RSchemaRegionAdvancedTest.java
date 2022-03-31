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

import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RStorageGroupMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore
public class RSchemaRegionAdvancedTest {

  private static RSchemaRegion schemaRegion = null;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    PartialPath storageGroupPath = new PartialPath("root.vehicle.s0");
    SchemaRegionId schemaRegionId = new SchemaRegionId(1);
    RSchemaReadWriteHandler readWriteHandler = new RSchemaReadWriteHandler();
    RStorageGroupMNode storageGroupMNode =
        new RStorageGroupMNode(storageGroupPath.getFullPath(), -1, readWriteHandler);
    schemaRegion = new RSchemaRegion(storageGroupPath, schemaRegionId, storageGroupMNode);

    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s0.d0.s5"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s0"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s1"),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s2"),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s3"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s4"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s1.d1.s5"),
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
      List<MeasurementPath> pathList =
          schemaRegion.getMeasurementPaths(new PartialPath("root.vehicle.s1.d1.**"));
      assertEquals(6, pathList.size());
      pathList = schemaRegion.getMeasurementPaths(new PartialPath("root.vehicle.s0.d0.**"));
      assertEquals(6, pathList.size());
      //      pathList = schemaEngine.getMeasurementPaths(new PartialPath("root.vehicle.s*.**"));
      //      assertEquals(12, pathList.size());
      //      pathList = schemaEngine.getMeasurementPaths(new PartialPath("root.ve*.**"));
      //      assertEquals(12, pathList.size());
      //      pathList = schemaEngine.getMeasurementPaths(new
      // PartialPath("root.vehicle*.s*.d*.s1"));
      //      assertEquals(2, pathList.size());
      pathList = schemaRegion.getMeasurementPaths(new PartialPath("root.vehicle.s2.**"));
      assertEquals(0, pathList.size());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCache() throws MetadataException {
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s2.d2.s0"),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s2.d2.s1"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s2.d2.s2.g0"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schemaRegion.createTimeseries(
        new PartialPath("root.vehicle.s2.d2.s3"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    IMNode node = schemaRegion.getDeviceNode(new PartialPath("root.vehicle.s0.d0"));
    Assert.assertEquals(
        TSDataType.INT32, node.getChild("s0").getAsMeasurementMNode().getSchema().getType());

    Assert.assertFalse(schemaRegion.isPathExist(new PartialPath("root.vehicle.d100")));
  }
}
