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
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MManagerBasicTest {

  private CompressionType compressionType;

  @Before
  public void setUp() {
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
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
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
  public void testShowChildNodesWithGivenPrefix() {
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
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> nodes = new HashSet<>(Arrays.asList("s1", "s2"));
      Set<String> nodes2 = new HashSet<>(Arrays.asList("laptop"));
      Set<String> nodes3 = new HashSet<>(Arrays.asList("d1", "d2"));
      Set<String> nexLevelNodes1 =
          manager.getChildNodeInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 = manager.getChildNodeInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 = manager.getChildNodeInNextLevel(new PartialPath("root.laptop"));
      // usual condition
      assertEquals(nodes, nexLevelNodes1);
      assertEquals(nodes2, nexLevelNodes2);
      assertEquals(nodes3, nexLevelNodes3);

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

  @Test
  public void testTemplate() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    MManager manager = IoTDB.metaManager;
    manager.createDeviceTemplate(plan);

    // set device template
    SetDeviceTemplatePlan setDeviceTemplatePlan =
        new SetDeviceTemplatePlan("template1", "root.sg1.d1");

    manager.setDeviceTemplate(setDeviceTemplatePlan);

    MNode node = manager.getDeviceNode(new PartialPath("root.sg1.d1"));
    node.setUseTemplate(true);

    MeasurementSchema s11 =
        new MeasurementSchema("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    assertNotNull(node.getDeviceTemplate());
    assertEquals(node.getDeviceTemplate().getSchemaMap().get("s11"), s11);

    Set<MeasurementSchema> allSchema =
        new HashSet<>(node.getDeviceTemplate().getSchemaMap().values());
    for (MeasurementSchema schema :
        manager.getAllMeasurementByDevicePath(new PartialPath("root.sg1.d1"))) {
      allSchema.remove(schema);
    }

    assertTrue(allSchema.isEmpty());
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testTemplateAndTimeSeriesCompatibility() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();
    MManager manager = IoTDB.metaManager;
    manager.createDeviceTemplate(plan);

    // set device template
    SetDeviceTemplatePlan setDeviceTemplatePlan =
        new SetDeviceTemplatePlan("template1", "root.sg1.d1");

    manager.setDeviceTemplate(setDeviceTemplatePlan);

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.s20"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    manager.createTimeseries(createTimeSeriesPlan);

    CreateTimeSeriesPlan createTimeSeriesPlan2 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.s11"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    try {
      manager.createTimeseries(createTimeSeriesPlan2);
      fail();
    } catch (Exception e) {
      assertEquals(
          "Path [root.sg1.d1.s11 ( which is incompatible with template )] already exist",
          e.getMessage());
    }
  }

  @Test
  public void testSetDeviceTemplate() throws MetadataException {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("test_vector");

    CreateTemplatePlan plan1 =
        new CreateTemplatePlan(
            "template1",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    measurementList.add(Collections.singletonList("s12"));
    schemaNames.add("s12");
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    compressionTypes.add(CompressionType.SNAPPY);

    CreateTemplatePlan plan2 =
        new CreateTemplatePlan(
            "template2",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    measurementList.get(1).add("s13");
    dataTypeList.get(1).add(TSDataType.INT64);
    encodingList.get(1).add(TSEncoding.RLE);

    SetDeviceTemplatePlan setPlan1 = new SetDeviceTemplatePlan("template1", "root.sg1");
    SetDeviceTemplatePlan setPlan2 = new SetDeviceTemplatePlan("template2", "root.sg2.d1");

    SetDeviceTemplatePlan setPlan3 = new SetDeviceTemplatePlan("template1", "root.sg1.d1");
    SetDeviceTemplatePlan setPlan4 = new SetDeviceTemplatePlan("template2", "root.sg2");

    SetDeviceTemplatePlan setPlan5 = new SetDeviceTemplatePlan("template2", "root.sg1.d1");

    MManager manager = IoTDB.metaManager;

    manager.createDeviceTemplate(plan1);
    manager.createDeviceTemplate(plan2);

    manager.setStorageGroup(new PartialPath("root.sg1"));
    manager.setStorageGroup(new PartialPath("root.sg2"));
    manager.setStorageGroup(new PartialPath("root.sg3"));

    try {
      manager.setDeviceTemplate(setPlan1);
      manager.setDeviceTemplate(setPlan2);
    } catch (MetadataException e) {
      fail();
    }

    try {
      manager.setDeviceTemplate(setPlan3);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }

    try {
      manager.setDeviceTemplate(setPlan4);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg2.d1", e.getMessage());
    }

    try {
      manager.setDeviceTemplate(setPlan5);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }
  }

  @Test
  public void testShowTimeseriesWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("s1");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;
    try {
      manager.createDeviceTemplate(plan);

      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      // show timeseries root.laptop.d1.s0
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s0"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());

      // show timeseries root.laptop.d1.vector.s1
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s1"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s1", result.get(0).getName());

      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(new PartialPath("root"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(2, result.size());
      Set<String> set = new HashSet<>();
      set.add("root.laptop.d1.s0");
      set.add("root.laptop.d1.s1");

      for (int i = 0; i < result.size(); i++) {
        set.remove(result.get(i).getName());
      }

      assertTrue(set.isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCountTimeseriesWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("s1");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;
    try {
      manager.createDeviceTemplate(plan);

      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      manager.createTimeseries(
          new PartialPath("root.computer.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      setDeviceTemplatePlan = new SetDeviceTemplatePlan("template1", "root.computer");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.computer.d1")).setUseTemplate(true);

      Assert.assertEquals(2, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s2")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer")));
      Assert.assertEquals(5, manager.getAllTimeseriesCount(new PartialPath("root")));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCountDeviceWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("s1");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;

    try {
      manager.createDeviceTemplate(plan);
      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d2")));
      Assert.assertEquals(2, manager.getDevicesNum(new PartialPath("root.laptop")));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTotalSeriesNumber() throws Exception {
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

      assertEquals(6, manager.getTotalSeriesNumber());
      EnvironmentUtils.restartDaemon();
      assertEquals(6, manager.getTotalSeriesNumber());
      manager.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      assertEquals(5, manager.getTotalSeriesNumber());
      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop")));
      assertEquals(0, manager.getTotalSeriesNumber());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStorageGroupNameWithHyphen() throws IllegalPathException {
    MManager manager = IoTDB.metaManager;
    assertTrue(manager.isPathExist(new PartialPath("root")));

    assertFalse(manager.isPathExist(new PartialPath("root.group-with-hyphen")));

    try {
      manager.setStorageGroup(new PartialPath("root.group-with-hyphen"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.group-with-hyphen")));
  }

  @Test
  public void testGetStorageGroupNodeByPath() {
    MManager manager = IoTDB.metaManager;
    PartialPath partialPath = null;

    try {
      partialPath = new PartialPath("root.ln.sg1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(partialPath);
    } catch (MetadataException e) {
      fail(e.getMessage());
    }

    try {
      partialPath = new PartialPath("root.ln.sg2.device1.sensor1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      manager.getStorageGroupNodeByPath(partialPath);
    } catch (StorageGroupNotSetException e) {
      Assert.assertEquals(
          "Storage group is not set for current seriesPath: [root.ln.sg2.device1.sensor1]",
          e.getMessage());
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testMeasurementIdWhileInsert() throws Exception {
    MManager manager = IoTDB.metaManager;

    PartialPath deviceId = new PartialPath("root.sg.d");
    InsertPlan insertPlan;

    insertPlan = getInsertPlan("\"a.b\"");
    manager.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(manager.isPathExist(deviceId.concatNode("\"a.b\"")));

    insertPlan = getInsertPlan("\"a“（Φ）”b\"");
    manager.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(manager.isPathExist(deviceId.concatNode("\"a“（Φ）”b\"")));

    String[] illegalMeasurementIds = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP", "a\".\"c"};
    for (String measurementId : illegalMeasurementIds) {
      insertPlan = getInsertPlan(measurementId);
      try {
        manager.getSeriesSchemasAndReadLockDevice(insertPlan);
        assertFalse(manager.isPathExist(deviceId.concatNode(measurementId)));
      } catch (MetadataException e) {
        e.printStackTrace();
      }
    }
  }

  private InsertPlan getInsertPlan(String measurementId) throws MetadataException {
    PartialPath deviceId = new PartialPath("root.sg.d");
    String[] measurementList = {measurementId};
    String[] values = {"1"};
    MeasurementMNode[] measurementMNodes = new MeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);
    return insertPlan;
  }

  @Test
  public void testTemplateNameCheckWhileCreate() {
    MManager manager = IoTDB.metaManager;
    String[] illegalSchemaNames = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String schemaName : illegalSchemaNames) {
      CreateTemplatePlan plan = getCreateTemplatePlan(schemaName);
      try {
        manager.createDeviceTemplate(plan);
      } catch (MetadataException e) {
        Assert.assertEquals(
            String.format("%s is an illegal schema name", schemaName), e.getMessage());
      }
    }
  }

  private CreateTemplatePlan getCreateTemplatePlan(String schemaName) {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(compressionType);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add(schemaName);

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testDeviceNodeAfterAutoCreateTimeseriesFailure() throws Exception {
    MManager manager = IoTDB.metaManager;

    PartialPath sg1 = new PartialPath("root.a.sg");
    manager.setStorageGroup(sg1);

    PartialPath deviceId = new PartialPath("root.a.d");
    String[] measurementList = {"s"};
    String[] values = {"1"};
    MeasurementMNode[] measurementMNodes = new MeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);

    try {
      manager.getSeriesSchemasAndReadLockDevice(insertPlan);
      fail();
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.a have already been set to storage group", e.getMessage());
      Assert.assertFalse(manager.isPathExist(new PartialPath("root.a.d")));
    }
  }

  @Test
  public void testTagIndexRecovery() throws Exception {
    MManager manager = IoTDB.metaManager;
    PartialPath path = new PartialPath("root.sg.d.s");
    Map<String, String> tags = new HashMap<>();
    tags.put("description", "oldValue");
    manager.createTimeseries(
        new CreateTimeSeriesPlan(
            path,
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            null,
            tags,
            null,
            null));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    List<ShowTimeSeriesResult> results =
        manager.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("oldValue", resultTag.get("description"));

    tags.put("description", "newValue");
    manager.upsertTagsAndAttributes(null, tags, null, path);

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));

    manager.clear();
    manager.init();

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "oldValue", 0, 0, false);
    results = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(0, results.size());

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));
  }

  @Test
  public void testTagCreationViaMLogPlanDuringMetadataSync() throws Exception {
    MManager manager = IoTDB.metaManager;

    PartialPath path = new PartialPath("root.sg.d.s");
    Map<String, String> tags = new HashMap<>();
    tags.put("type", "test");
    CreateTimeSeriesPlan plan =
        new CreateTimeSeriesPlan(
            path,
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            null,
            tags,
            null,
            null);
    // mock that the plan has already been executed on sender and receiver will redo this plan
    plan.setTagOffset(10);

    manager.operation(plan);

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.sg.d.s"), true, "type", "test", 0, 0, false);
    List<ShowTimeSeriesResult> results =
        manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("test", resultTag.get("type"));

    assertEquals(0, ((MeasurementMNode) manager.getNodeByPath(path)).getOffset());
  }
}
