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
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  public void testAddPathAndExist() throws IllegalPathException {

    MManager manager = IoTDB.metaManager;
    assertTrue(manager.isPathExist(new PartialPath("root")));

    assertFalse(manager.isPathExist(new PartialPath("root.laptop")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals("root.laptop has already been set to storage group", e.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.1_2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.\"1.2.3\""),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);
      manager.createTimeseries(
          new PartialPath("root.1.2.3"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.EMPTY_MAP);

      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2.3")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root")));

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d2")));
    assertFalse(manager.checkStorageGroupByPath(new PartialPath("root.laptop.d2")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(new PartialPath("root.laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format(
              "The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.1_2"));
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.\"1.2.3\""));
      manager.deleteTimeseries(new PartialPath("root.1.2.3"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2.3")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2")));
    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.1")));
  }

  @Test
  public void testGetAllTimeseriesCount() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 5);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1")), 4);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2")), 2);

      try {
        manager.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1"));
        fail("Expected exception");
      } catch (MetadataException e) {
        assertEquals("Path [root.laptop.d3.s1] does not exist", e.getMessage());
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroupAndExist() {

    MManager manager = IoTDB.metaManager;

    try {
      assertFalse(manager.isStorageGroup(new PartialPath("root")));
      assertFalse(manager.isStorageGroup(new PartialPath("root1.laptop.d2")));

      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() {

    MManager manager = IoTDB.metaManager;

    try {

      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      Set<String> devices =
          new TreeSet<String>() {
            {
              add("root.laptop.d1");
              add("root.laptop.d2");
            }
          };
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop.d2")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.d2");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      MManager recoverManager = new MManager();
      recoverManager.init();

      assertTrue(recoverManager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          recoverManager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      recoverManager.clear();
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1")));
      list.add("root.laptop.d2");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = IoTDB.metaManager;

    try {
      assertTrue(manager.getAllTimeseriesPath(new PartialPath("root")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetStorageGroupNameByAutoLevel() {
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();

    try {
      assertEquals(
          "root.laptop",
          MetaUtils.getStorageGroupPathByLevel(new PartialPath("root.laptop.d1.s1"), level)
              .getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    boolean caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root1.laptop.d1.s1"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);
  }

  @Test
  public void testSetStorageGroupWithIllegalName() {
    MManager manager = IoTDB.metaManager;
    try {
      PartialPath path1 = new PartialPath("root.laptop\n");
      try {
        manager.setStorageGroup(path1);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop\t");
      try {
        manager.setStorageGroup(path2);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void testCreateTimeseriesWithIllegalName() {
    MManager manager = IoTDB.metaManager;
    try {
      PartialPath path1 = new PartialPath("root.laptop.d1\n.s1");
      try {
        manager.createTimeseries(
            path1, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop.d1\t.s1");
      try {
        manager.createTimeseries(
            path2, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void testGetDevicesWithGivenPrefix() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> devices = new TreeSet<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.laptop")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      manager.setStorageGroup(new PartialPath("root.vehicle"));
      manager.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
    MManager manager = IoTDB.metaManager;
    String[] res =
        new String[] {
          "[root.laptop, root.vehicle]",
          "[root.laptop.b1, root.laptop.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.laptop.b1, root.laptop.b2, root.vehicle.b1, root.vehicle.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2, root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3, root.vehicle.b2.d0]",
          "[root.laptop.b1.d1.s0, root.laptop.b1.d1.s1, root.laptop.b1.d2.s0, root.laptop.b2.d1.s1, root.laptop.b2.d1.s3, root.laptop.b2.d2.s2]",
          "[]"
        };

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.setStorageGroup(new PartialPath("root.vehicle"));

      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d2.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d3.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b2.d0.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(res[0], manager.getChildNodePathInNextLevel(new PartialPath("root")).toString());
      assertEquals(
          res[1], manager.getChildNodePathInNextLevel(new PartialPath("root.laptop")).toString());
      assertEquals(
          res[2],
          manager.getChildNodePathInNextLevel(new PartialPath("root.laptop.b1")).toString());
      assertEquals(
          res[3], manager.getChildNodePathInNextLevel(new PartialPath("root.*")).toString());
      assertEquals(
          res[4], manager.getChildNodePathInNextLevel(new PartialPath("root.*.b1")).toString());
      assertEquals(
          res[5], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b1")).toString());
      assertEquals(
          res[6], manager.getChildNodePathInNextLevel(new PartialPath("root.v*.*")).toString());
      assertEquals(
          res[7], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b*.*")).toString());
      assertEquals(
          res[8], manager.getChildNodePathInNextLevel(new PartialPath("root.laptopp")).toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
