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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.PathException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MManagerBasicTest {

  private CompressionType compressionType;

  @Before
  public void setUp() throws Exception {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddPathAndExist() {

    MManager manager = MManager.getInstance();
    assertTrue(manager.isPathExist("root"));

    assertFalse(manager.isPathExist("root.laptop"));

    try {
      manager.setStorageGroup("root.laptop.d1");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup("root.laptop");
    } catch (MetadataException e) {
      Assert.assertEquals("root.laptop has already been set to storage group",
          e.getMessage());
    }

    try {
      manager.createTimeseries("root.laptop.d1.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections
              .emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.isPathExist("root.laptop"));
    assertTrue(manager.isPathExist("root.laptop.d1"));
    assertTrue(manager.isPathExist("root.laptop.d1.s0"));
    assertFalse(manager.isPathExist("root.laptop.d1.s1"));
    try {
      manager.createTimeseries("root.laptop.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    assertTrue(manager.isPathExist("root.laptop.d1.s1"));
    try {
      manager.deleteTimeseries("root.laptop.d1.s1");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    // just delete s0, and don't delete root.laptop.d1??
    // delete storage group or not
    assertFalse(manager.isPathExist("root.laptop.d1.s1"));
    try {
      manager.deleteTimeseries("root.laptop.d1.s0");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist("root.laptop.d1.s0"));
    assertTrue(manager.isPathExist("root.laptop.d1"));
    assertTrue(manager.isPathExist("root.laptop"));
    assertTrue(manager.isPathExist("root"));

    try {
      manager.createTimeseries("root.laptop.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.createTimeseries("root.laptop.d1.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist("root.laptop.d2"));
    assertFalse(manager.checkStorageGroupByPath("root.laptop.d2"));

    try {
      manager.deleteTimeseries("root.laptop.d1.s0");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deleteTimeseries("root.laptop.d1.s1");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup("root.laptop.d2");
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop.d2"),
          e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroupAndExist() {

    MManager manager = MManager.getInstance();

    try {
      assertFalse(manager.isStorageGroup("root"));
      assertFalse(manager.isStorageGroup("root1.laptop.d2"));

      manager.setStorageGroup("root.laptop.d1");
      assertTrue(manager.isStorageGroup("root.laptop.d1"));
      assertFalse(manager.isStorageGroup("root.laptop.d2"));
      assertFalse(manager.isStorageGroup("root.laptop"));
      assertFalse(manager.isStorageGroup("root.laptop.d1.s1"));

      manager.setStorageGroup("root.laptop.d2");
      assertTrue(manager.isStorageGroup("root.laptop.d1"));
      assertTrue(manager.isStorageGroup("root.laptop.d2"));
      assertFalse(manager.isStorageGroup("root.laptop.d3"));
      assertFalse(manager.isStorageGroup("root.laptop"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    MManager manager = MManager.getInstance();
    try {
      manager.setStorageGroup("root.laptop.d1");
      manager.setStorageGroup("root.laptop.d2");
      manager.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getStorageGroupByPath("root.laptop.d1.s1"));
      assertEquals(list, manager.getStorageGroupByPath("root.laptop.d1"));

      list.add("root.laptop.d2");
      assertEquals(list, manager.getStorageGroupByPath("root.laptop"));
      assertEquals(list, manager.getStorageGroupByPath("root"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = MManager.getInstance();

    try {
      assertTrue(manager.getAllTimeseriesName("root").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle.device").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle.device.sensor").isEmpty());

      manager.setStorageGroup("root.vehicle");
      assertFalse(manager.getStorageGroupByPath("root.vehicle").isEmpty());
      assertFalse(manager.getStorageGroupByPath("root.vehicle.device").isEmpty());
      assertFalse(manager.getStorageGroupByPath("root.vehicle.device.sensor").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle1").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle1.device").isEmpty());

      manager.setStorageGroup("root.vehicle1.device");
      assertTrue(manager.getStorageGroupByPath("root.vehicle1.device1").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle1.device2").isEmpty());
      assertTrue(manager.getStorageGroupByPath("root.vehicle1.device3").isEmpty());
      assertFalse(manager.getStorageGroupByPath("root.vehicle1.device").isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaximalSeriesNumberAmongStorageGroup() throws MetadataException, PathException {
    MManager manager = MManager.getInstance();
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageGroup("root.laptop");
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    manager.createTimeseries("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageGroup("root.vehicle");
    manager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());

    manager.deleteTimeseries("root.laptop.d1.s1");
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.deleteTimeseries("root.laptop.d1.s2");
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
  }

  @Test
  public void testGetStorageGroupNameByAutoLevel() {
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    boolean caughtException;

    try {
      assertEquals("root.laptop",
          MetaUtils.getStorageGroupNameByLevel("root.laptop.d1.s1", level));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    caughtException = false;
    try {
      MetaUtils.getStorageGroupNameByLevel("root1.laptop.d1.s1", level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      MetaUtils.getStorageGroupNameByLevel("root", level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);
  }

  @Test
  public void testGetDevicesWithGivenPrefix() {
    MManager manager = MManager.getInstance();

    try {
      manager.setStorageGroup("root.laptop");
      manager.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      Set<String> devices = new TreeSet<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(devices, manager.getDevices("root.laptop"));
      manager.setStorageGroup("root.vehicle");
      manager.createTimeseries("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(devices, manager.getDevices("root.*"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
    MManager manager = MManager.getInstance();
    String[] res = new String[]{
        "[root.laptop, root.vehicle]",
        "[root.laptop.b1, root.laptop.b2]",
        "[root.laptop.b1.d1, root.laptop.b1.d2]",
        "[root.laptop.b1, root.laptop.b2, root.vehicle.b1, root.vehicle.b2]",
        "[root.laptop.b1.d1, root.laptop.b1.d2, root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3]",
        "[root.laptop.b1.d1, root.laptop.b1.d2]",
        "[root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3, root.vehicle.b2.d0]",
        "[root.laptop.b1.d1.s0, root.laptop.b1.d1.s1, root.laptop.b1.d2.s0, root.laptop.b2.d1.s1, root.laptop.b2.d1.s3, root.laptop.b2.d2.s2]"
    };

    try {
      manager.setStorageGroup("root.laptop");
      manager.setStorageGroup("root.vehicle");

      manager.createTimeseries("root.laptop.b1.d1.s0", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.b1.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.b1.d2.s0", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.b2.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.b2.d1.s3", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.laptop.b2.d2.s2", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.vehicle.b1.d0.s0", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.vehicle.b1.d2.s2", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.vehicle.b1.d3.s3", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries("root.vehicle.b2.d0.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      assertEquals(res[0], manager.getChildNodePathInNextLevel("root").toString());
      assertEquals(res[1], manager.getChildNodePathInNextLevel("root.laptop").toString());
      assertEquals(res[2], manager.getChildNodePathInNextLevel("root.laptop.b1").toString());
      assertEquals(res[3], manager.getChildNodePathInNextLevel("root.*").toString());
      assertEquals(res[4], manager.getChildNodePathInNextLevel("root.*.b1").toString());
      assertEquals(res[5], manager.getChildNodePathInNextLevel("root.l*.b1").toString());
      assertEquals(res[6], manager.getChildNodePathInNextLevel("root.v*.*").toString());
      assertEquals(res[7], manager.getChildNodePathInNextLevel("root.l*.b*.*").toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
