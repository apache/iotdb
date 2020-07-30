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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MManagerBasicTest {

  private CompressionType compressionType;
  private boolean canAdjust = IoTDBDescriptor.getInstance().getConfig().isEnableParameterAdapter();

  @Before
  public void setUp() throws Exception {
    canAdjust = IoTDBDescriptor.getInstance().getConfig().isEnableParameterAdapter();
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setEnableParameterAdapter(true);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableParameterAdapter(canAdjust);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddPathAndExist() {

    MManager manager = IoTDB.metaManager;
    assertTrue(manager.isPathExist(Collections.singletonList("root")));

    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop")));

    try {
      manager.setStorageGroup(Arrays.asList("root", "laptop", "d1"));
      manager.setStorageGroup(Arrays.asList("root", "1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(Arrays.asList("root", "1")));

    try {
      manager.setStorageGroup(Arrays.asList("root", "laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals("root.laptop has already been set to storage group",
          e.getMessage());
    }

    try {
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.isPathExist(Arrays.asList("root", "laptop")));
    assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1")));
    assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "s0")));
    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "s1")));
    try {
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "1_2"), TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.EMPTY_MAP);
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "\"1.2.3\""), TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.EMPTY_MAP);
      manager.createTimeseries(Arrays.asList("root", "1", "2", "3"), TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.EMPTY_MAP);

      assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "s1")));
      assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "1_2")));
      assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "\"1.2.3\"")));
      assertTrue(manager.isPathExist(Arrays.asList("root", "1", "2")));
      assertTrue(manager.isPathExist(Arrays.asList("root", "1", "2", "3")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "s1")));

    try {
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "s0")));
    assertTrue(manager.isPathExist(Arrays.asList("root", "laptop", "d1")));
    assertTrue(manager.isPathExist(Arrays.asList("root", "laptop")));
    assertTrue(manager.isPathExist(Collections.singletonList("root")));

    try {
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d2")));
    assertFalse(manager.checkStorageGroupByPath(Arrays.asList("root", "laptop", "d2")));

    try {
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(Arrays.asList("root", "laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "1_2"));
      manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "\"1.2.3\""));
      manager.deleteTimeseries(Arrays.asList("root", "1", "2", "3"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "1_2")));
    assertFalse(manager.isPathExist(Arrays.asList("root", "laptop", "d1", "\"1.2.3\"")));
    assertFalse(manager.isPathExist(Arrays.asList("root", "1", "2", "3")));
    assertFalse(manager.isPathExist(Arrays.asList("root", "1", "2")));
    assertTrue(manager.isPathExist(Arrays.asList("root", "1")));

    try {
      List<List<String>> storageGroups = new ArrayList<>();
      storageGroups.add(Arrays.asList("root", "1"));
      manager.deleteStorageGroups(storageGroups);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(Arrays.asList("root", "1")));
  }

  @Test
  public void testSetStorageGroupAndExist() {

    MManager manager = IoTDB.metaManager;

    try {
      assertFalse(manager.isStorageGroup(Collections.singletonList("root")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root1", "laptop", "d2")));

      manager.setStorageGroup(Arrays.asList("root", "laptop", "d1"));
      assertTrue(manager.isStorageGroup(Arrays.asList("root", "laptop", "d1")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root", "laptop", "d2")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root", "laptop")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root", "laptop", "d1", "s1")));
      manager.setStorageGroup(Arrays.asList("root", "laptop", "d2"));
      assertTrue(manager.isStorageGroup(Arrays.asList("root", "laptop", "d1")));
      assertTrue(manager.isStorageGroup(Arrays.asList("root", "laptop", "d2")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root", "laptop", "d3")));
      assertFalse(manager.isStorageGroup(Arrays.asList("root", "laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(Arrays.asList("root", "laptop", "d1"));
      manager.setStorageGroup(Arrays.asList("root", "laptop", "d2"));
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "d2", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getStorageGroupByPath(Arrays.asList("root", "laptop", "d1", "s1")));
      assertEquals(list, manager.getStorageGroupByPath(Arrays.asList("root", "laptop", "d1")));
      list.add("root.laptop.d2");
      assertEquals(list, manager.getStorageGroupByPath(Arrays.asList("root", "laptop")));
      assertEquals(list, manager.getStorageGroupByPath(Collections.singletonList("root")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = IoTDB.metaManager;

    try {
      assertTrue(manager.getAllTimeseries(Collections.singletonList("root")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle", "device")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle", "device", "sensor")).isEmpty());

      manager.setStorageGroup(Arrays.asList("root", "vehicle"));
      assertFalse(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle", "device")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle", "device", "sensor")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1", "device")).isEmpty());

      manager.setStorageGroup(Arrays.asList("root", "vehicle1", "device"));
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1", "device1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1", "device2")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1", "device3")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(Arrays.asList("root", "vehicle1", "device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaximalSeriesNumberAmongStorageGroup() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageGroup(Arrays.asList("root", "laptop"));
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s2"), TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageGroup(Arrays.asList("root", "vehicle"));
    manager.createTimeseries(Arrays.asList("root", "vehicle", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s1"));
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.deleteTimeseries(Arrays.asList("root", "laptop", "d1", "s2"));
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
  }

  @Test
  public void testGetStorageGroupNameByAutoLevel() {
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    boolean caughtException;

    try {
      List<String> storageGroup = new ArrayList<>(Arrays.asList("root", "laptop", "d1", "s1"));
      assertEquals(Arrays.asList("root", "laptop"),
          MetaUtils.getDetachedStorageGroupByLevel(storageGroup, level));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    caughtException = false;
    try {
      MetaUtils.getDetachedStorageGroupByLevel(Arrays.asList("root1", "laptop", "d1", "s1"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      MetaUtils.getDetachedStorageGroupByLevel(Collections.singletonList("root"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);
  }

  @Test
  public void testGetDevicesWithGivenPrefix() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(Arrays.asList("root", "laptop"));
      manager.createTimeseries(Arrays.asList("root", "laptop", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "d2", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      Set<String> devices = new TreeSet<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(devices, manager.getDevices(Arrays.asList("root", "laptop")));
      manager.setStorageGroup(Arrays.asList("root", "vehicle"));
      manager.createTimeseries(Arrays.asList("root", "vehicle", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(devices, manager.getDevices(Arrays.asList("root", "*")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
    MManager manager = IoTDB.metaManager;
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
      manager.setStorageGroup(Arrays.asList("root", "laptop"));
      manager.setStorageGroup(Arrays.asList("root", "vehicle"));

      manager.createTimeseries(Arrays.asList("root", "laptop", "b1", "d1", "s0"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "b1", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "b1", "d2", "s0"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "b2", "d1", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "b2", "d1", "s3"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "laptop", "b2", "d2", "s2"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "vehicle", "b1", "d0", "s0"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "vehicle", "b1", "d2", "s2"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "vehicle", "b1", "d3", "s3"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.createTimeseries(Arrays.asList("root", "vehicle", "b2", "d0", "s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      assertEquals(res[0], manager.getChildPathInNextLevel(Collections.singletonList("root")).toString());
      assertEquals(res[1], manager.getChildPathInNextLevel(Arrays.asList("root", "laptop")).toString());
      assertEquals(res[2], manager.getChildPathInNextLevel(Arrays.asList("root", "laptop", "b1")).toString());
      assertEquals(res[3], manager.getChildPathInNextLevel(Arrays.asList("root", "*")).toString());
      assertEquals(res[4], manager.getChildPathInNextLevel(Arrays.asList("root", "*", "b1")).toString());
      assertEquals(res[5], manager.getChildPathInNextLevel(Arrays.asList("root", "l*", "b1")).toString());
      assertEquals(res[6], manager.getChildPathInNextLevel(Arrays.asList("root", "v*", "*")).toString());
      assertEquals(res[7], manager.getChildPathInNextLevel(Arrays.asList("root", "l*", "b*", "*")).toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
