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
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.\"1.2.3\""),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.1.2.3"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());

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

  /**
   * Test if the PathNotExistException can be correctly thrown when the path to be deleted does not
   * exist. See {@link MManager#deleteTimeseries(PartialPath)}.
   */
  @Test
  public void testDeleteNonExistentTimeseries() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.deleteTimeseries(new PartialPath("root.non.existent"));
      fail();
    } catch (PathNotExistException e) {
      assertEquals("Path [root.non.existent] does not exist", e.getMessage());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Test
  public void testCreateAlignedTimeseries() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.aligned_device"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("INT32"),
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          Arrays.asList(compressionType, compressionType, compressionType));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    manager.deleteTimeseries(new PartialPath("root.laptop.d1.aligned_device.*"));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));

    try {
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.aligned_device"),
          Arrays.asList("s0", "s2", "s4"),
          Arrays.asList(
              TSDataType.valueOf("INT32"),
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          Arrays.asList(compressionType, compressionType, compressionType));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s4")));
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testGetAllTimeseriesCount() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d0"),
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
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
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

      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.**")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.**")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 4);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.**")), 5);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 2);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 2);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2.**")), 2);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop")), 0);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1")), 0);

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
          manager.getMatchedDevices(new PartialPath("root.**")).stream()
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
          manager.getMatchedDevices(new PartialPath("root.**")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      MManager recoverManager = new MManager();
      recoverManager.initForMultiMManagerTest();

      assertTrue(recoverManager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          recoverManager.getMatchedDevices(new PartialPath("root.**")).stream()
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

      List<PartialPath> list = new ArrayList<>();

      list.add(new PartialPath("root.laptop.d1"));
      assertEquals(list, manager.getBelongedStorageGroups(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, manager.getBelongedStorageGroups(new PartialPath("root.laptop.d1")));

      list.add(new PartialPath("root.laptop.d2"));
      assertEquals(list, manager.getBelongedStorageGroups(new PartialPath("root.laptop.**")));
      assertEquals(list, manager.getBelongedStorageGroups(new PartialPath("root.**")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = IoTDB.metaManager;

    try {
      assertTrue(manager.getMeasurementPaths(new PartialPath("root")).isEmpty());
      assertTrue(manager.getBelongedStorageGroups(new PartialPath("root")).isEmpty());
      assertTrue(manager.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          manager
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor"))
              .isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(manager.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          manager
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor"))
              .isEmpty());
      assertTrue(manager.getBelongedStorageGroups(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(
          manager.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());
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
          manager.getChildNodeNameInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 = manager.getChildNodeNameInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 =
          manager.getChildNodeNameInNextLevel(new PartialPath("root.laptop"));
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
          manager.getMatchedDevices(new PartialPath("root.laptop.**")).stream()
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
          manager.getMatchedDevices(new PartialPath("root.**")).stream()
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
    manager.createSchemaTemplate(plan);

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    manager.setSchemaTemplate(setTemplatePlan);

    IMNode node = manager.getDeviceNode(new PartialPath("root.sg1.d1"));
    node = manager.setUsingSchemaTemplate(node);

    UnaryMeasurementSchema s11 =
        new UnaryMeasurementSchema("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    assertNotNull(node.getSchemaTemplate());

    Set<String> allSchema = new HashSet<>();
    for (IMeasurementSchema schema : node.getSchemaTemplate().getSchemaMap().values()) {
      allSchema.add("root.sg1.d1" + TsFileConstant.PATH_SEPARATOR + schema.getMeasurementId());
    }
    for (MeasurementPath measurementPath :
        manager.getMeasurementPaths(new PartialPath("root.sg1.d1.**"))) {
      allSchema.remove(measurementPath.toString());
    }

    assertTrue(allSchema.isEmpty());

    IMeasurementMNode mNode = manager.getMeasurementMNode(new PartialPath("root.sg1.d1.s11"));
    IMeasurementMNode mNode2 =
        manager.getMeasurementMNode(new PartialPath("root.sg1.d1.vector.s2"));
    assertNotNull(mNode);
    assertEquals(mNode.getSchema(), s11);
    assertNotNull(mNode2);
    assertEquals(
        mNode2.getSchema(), manager.getTemplate("template1").getSchemaMap().get("vector.s2"));

    try {
      manager.getMeasurementMNode(new PartialPath("root.sg1.d1.s100"));
      fail();
    } catch (PathNotExistException e) {
      assertEquals("Path [root.sg1.d1.s100] does not exist", e.getMessage());
    }
  }

  @Test
  public void testTemplateInnerTree() {
    CreateTemplatePlan plan = getTreeTemplatePlan();
    Template template;
    MManager manager = IoTDB.metaManager;

    try {
      manager.createSchemaTemplate(plan);
      template = manager.getTemplate("treeTemplate");
      assertEquals(4, template.getMeasurementsCount());
      assertEquals("d1", template.getPathNodeInTemplate("d1").getName());
      assertNull(template.getPathNodeInTemplate("notExists"));
      assertEquals("[GPS]", template.getAllAlignedPrefix().toString());

      String[] alignedMeasurements = {"to.be.prefix.s1", "to.be.prefix.s2"};
      TSDataType[] dataTypes = {TSDataType.INT32, TSDataType.INT32};
      TSEncoding[] encodings = {TSEncoding.RLE, TSEncoding.RLE};
      CompressionType[] compressionTypes = {CompressionType.SNAPPY, CompressionType.SNAPPY};
      template.addAlignedMeasurements(alignedMeasurements, dataTypes, encodings, compressionTypes);

      assertEquals("[to.be.prefix, GPS]", template.getAllAlignedPrefix().toString());
      assertEquals("[s1, s2]", template.getAlignedMeasurements("to.be.prefix").toString());

      template.deleteAlignedPrefix("to.be.prefix");

      assertEquals("[GPS]", template.getAllAlignedPrefix().toString());
      assertEquals(null, template.getDirectNode("prefix"));
      assertEquals("to", template.getDirectNode("to").getName());

      assertFalse(template.isDirectAligned());
      template.addAlignedMeasurements(
          new String[] {"speed", "temperature"}, dataTypes, encodings, compressionTypes);
      assertTrue(template.isDirectAligned());

      try {
        template.deleteMeasurements("a.single");
        fail();
      } catch (IllegalPathException e) {
        assertEquals("Path [a.single] does not exist", e.getMessage());
      }
      assertEquals(
          "[d1.s1, GPS.x, to.be.prefix.s2, GPS.y, to.be.prefix.s1, s2]",
          template.getAllMeasurementsPaths().toString());

      template.deleteSeriesCascade("to");

      assertEquals("[d1.s1, GPS.x, GPS.y, s2]", template.getAllMeasurementsPaths().toString());

    } catch (MetadataException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("Duplicates")
  private CreateTemplatePlan getTreeTemplatePlan() {
    /**
     * Construct a template like: create schema template treeTemplate ( (d1.s1 INT32 GORILLA
     * SNAPPY), (s2 INT32 GORILLA SNAPPY), (GPS.x FLOAT RLE SNAPPY), (GPS.y FLOAT RLE SNAPPY), )with
     * aligned (GPS)
     */
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("d1.s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Arrays.asList("GPS.x", "GPS.y"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SDT));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));

    return new CreateTemplatePlan(
        "treeTemplate", measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @SuppressWarnings("Duplicates")
  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));

    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("vector.s" + i);
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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressorList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s21");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testUnsetSchemaTemplate() throws MetadataException {

    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Collections.singletonList("s3"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    }
    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s1");
    schemaNames.add("s2");
    schemaNames.add("s3");

    CreateTemplatePlan createTemplatePlan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg.1");
    UnsetTemplatePlan unsetTemplatePlan = new UnsetTemplatePlan("root.sg.1", "template1");
    MManager manager = IoTDB.metaManager;
    manager.createSchemaTemplate(createTemplatePlan);

    // path does not exist test
    try {
      manager.unsetSchemaTemplate(unsetTemplatePlan);
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("Path [root.sg.1] does not exist", e.getMessage());
    }

    manager.setSchemaTemplate(setTemplatePlan);

    // template unset test
    manager.unsetSchemaTemplate(unsetTemplatePlan);
    manager.setSchemaTemplate(setTemplatePlan);

    // no template on path test
    manager.unsetSchemaTemplate(unsetTemplatePlan);
    try {
      manager.unsetSchemaTemplate(unsetTemplatePlan);
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("NO template on root.sg.1", e.getMessage());
    }
  }

  @Test
  public void testTemplateAndTimeSeriesCompatibility() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();
    MManager manager = IoTDB.metaManager;
    manager.createSchemaTemplate(plan);
    manager.createSchemaTemplate(getTreeTemplatePlan());

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    manager.setSchemaTemplate(setTemplatePlan);
    manager.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.sg0"));

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
      assertEquals("Path [root.sg1.d1.s11] already exists in [template1]", e.getMessage());
    }

    CreateTimeSeriesPlan createTimeSeriesPlan3 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg0.GPS.s9"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    try {
      manager.createTimeseries(createTimeSeriesPlan3);
      fail();
    } catch (Exception e) {
      assertEquals(
          "Path [root.tree.sg0.GPS.s9] overlaps with [treeTemplate] on [GPS]", e.getMessage());
    }

    CreateTimeSeriesPlan createTimeSeriesPlan4 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg0.s3"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    manager.createTimeseries(createTimeSeriesPlan4);
  }

  @Test
  public void testTemplateAndNodePathCompatibility() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    CreateTemplatePlan plan = getCreateTemplatePlan();
    manager.createSchemaTemplate(plan);
    manager.createSchemaTemplate(getTreeTemplatePlan());

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    SetTemplatePlan setSchemaTemplatePlan2 = new SetTemplatePlan("treeTemplate", "root.tree.sg0");

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.s11"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    manager.createTimeseries(createTimeSeriesPlan);

    manager.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg0.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    manager.setSchemaTemplate(setSchemaTemplatePlan2);
    manager.unsetSchemaTemplate(new UnsetTemplatePlan("root.tree.sg0", "treeTemplate"));
    try {
      manager.setSchemaTemplate(setTemplatePlan);
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Node name s11 in template has conflict with node's child root.sg1.d1.s11",
          e.getMessage());
    }

    manager.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg0.GPS.speed"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    try {
      manager.setSchemaTemplate(setSchemaTemplatePlan2);
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Node name GPS in template has conflict with node's child root.tree.sg0.GPS",
          e.getMessage());
    }

    manager.deleteTimeseries(new PartialPath("root.sg1.d1.s11"));
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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressionTypeList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressionTypeList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(compressionTypeList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("test_aligned_device");

    CreateTemplatePlan plan1 =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);

    measurementList.add(Collections.singletonList("s12"));
    schemaNames.add("s12");
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));

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
    compressionTypes.get(1).add(CompressionType.SNAPPY);

    SetTemplatePlan setPlan1 = new SetTemplatePlan("template1", "root.sg1");
    SetTemplatePlan setPlan2 = new SetTemplatePlan("template2", "root.sg2.d1");

    SetTemplatePlan setPlan3 = new SetTemplatePlan("template1", "root.sg1.d1");
    SetTemplatePlan setPlan4 = new SetTemplatePlan("template2", "root.sg2");

    SetTemplatePlan setPlan5 = new SetTemplatePlan("template2", "root.sg1.d1");

    MManager manager = IoTDB.metaManager;

    manager.createSchemaTemplate(plan1);
    manager.createSchemaTemplate(plan2);

    manager.setStorageGroup(new PartialPath("root.sg1"));
    manager.setStorageGroup(new PartialPath("root.sg2"));
    manager.setStorageGroup(new PartialPath("root.sg3"));

    try {
      manager.setSchemaTemplate(setPlan1);
      manager.setSchemaTemplate(setPlan2);
    } catch (MetadataException e) {
      fail();
    }

    try {
      manager.setSchemaTemplate(setPlan3);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }

    try {
      manager.setSchemaTemplate(setPlan4);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg2.d1", e.getMessage());
    }

    try {
      manager.setSchemaTemplate(setPlan5);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }
  }

  @Test
  public void testShowTimeseries() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      // show timeseries root.laptop.d1.s0
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s0"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowTimeseriesWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      measurements.add("vector.s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.INT32);
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i <= 2; i++) {
      compressorList.add(compressionType);
    }
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("vector");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    CreateTemplatePlan treePlan = getTreeTemplatePlan();
    MManager manager = IoTDB.metaManager;
    try {
      manager.createSchemaTemplate(plan);
      manager.createSchemaTemplate(treePlan);

      // set device template

      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      SetTemplatePlan setSchemaTemplatePlan1 = new SetTemplatePlan("treeTemplate", "root.tree.d0");
      manager.setSchemaTemplate(setSchemaTemplatePlan);
      manager.setSchemaTemplate(setSchemaTemplatePlan1);
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.laptop.d1")));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0")));

      // show timeseries root.tree.d0
      ShowTimeSeriesPlan showTreeTSPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.tree.d0.**"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> treeShowResult =
          manager.showTimeseries(showTreeTSPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(4, treeShowResult.size());
      Set<String> checkSet = new HashSet<>();
      checkSet.add("root.tree.d0.d1.s1");
      checkSet.add("root.tree.d0.s2");
      checkSet.add("root.tree.d0.GPS.x");
      checkSet.add("root.tree.d0.GPS.y");
      for (ShowTimeSeriesResult res : treeShowResult) {
        checkSet.remove(res.getName());
      }
      assertTrue(checkSet.isEmpty());

      // show timeseries root.laptop.d1.s0
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s0"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());

      // show timeseries root.laptop.d1.(s1,s2,s3)
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.vector.s1"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.vector.s1", result.get(0).getName());

      // show timeseries root.laptop.d1.(s1,s2,s3)
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(new PartialPath("root.laptop.**"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(4, result.size());
      Set<String> set = new HashSet<>();
      for (int i = 1; i < result.size(); i++) {
        set.add("root.laptop.d1.vector.s" + i);
      }
      set.add("root.laptop.d1.s0");

      for (int i = 0; i < result.size(); i++) {
        set.remove(result.get(i).getName());
      }

      assertTrue(set.isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // show timeseries root.laptop.d1.(s0,s1)
    try {
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.(s0,s1)"), false, null, null, 0, 0, false);
      manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    } catch (MetadataException e) {
      assertEquals(
          "Cannot get node of children in different aligned timeseries (Path: (s0,s1))",
          e.getMessage());
    }
  }

  @Test
  public void minimumTestForWildcardInTemplate() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    CreateTemplatePlan treePlan = getTreeTemplatePlan();
    manager.createSchemaTemplate(treePlan);

    // set device template
    SetTemplatePlan setSchemaTemplatePlan1 = new SetTemplatePlan("treeTemplate", "root.tree.d0");
    manager.setSchemaTemplate(setSchemaTemplatePlan1);
    manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0")));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.tree.**.s1"), false, null, null, 0, 0, false);
    List<ShowTimeSeriesResult> result =
        manager.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertEquals(1, result.size());
  }

  @Test
  public void testCountTimeseriesWithTemplate() throws IOException {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(Collections.singletonList(compressionType));
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
      manager.createSchemaTemplate(plan);
      manager.createSchemaTemplate(getTreeTemplatePlan());

      // set device template
      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      manager.setSchemaTemplate(setSchemaTemplatePlan);
      manager.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.d0"));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.laptop.d1")));

      manager.createTimeseries(
          new PartialPath("root.computer.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.computer");
      manager.setSchemaTemplate(setTemplatePlan);
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.computer.d1")));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0")));
      manager.getDeviceNodeWithAutoCreate(new PartialPath("root.tree.d0.v0"));
      manager.getDeviceNodeWithAutoCreate(new PartialPath("root.tree.d0.v1"));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0.v0")));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0.v1")));

      Assert.assertEquals(2, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s2")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.**")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer.**")));
      Assert.assertEquals(12, manager.getAllTimeseriesCount(new PartialPath("root.tree.**")));
      Assert.assertEquals(17, manager.getAllTimeseriesCount(new PartialPath("root.**")));

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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(Collections.singletonList(compressionType));
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
      manager.createSchemaTemplate(plan);
      manager.createSchemaTemplate(getTreeTemplatePlan());
      // set device template
      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      manager.setSchemaTemplate(setSchemaTemplatePlan);
      manager.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.d0"));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.laptop.d1")));
      manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.tree.d0")));

      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d2")));
      Assert.assertEquals(2, manager.getDevicesNum(new PartialPath("root.laptop.*")));
      Assert.assertEquals(2, manager.getDevicesNum(new PartialPath("root.laptop.**")));
      Assert.assertEquals(3, manager.getDevicesNum(new PartialPath("root.tree.**")));
      Assert.assertEquals(5, manager.getDevicesNum(new PartialPath("root.**")));

      manager.createTimeseries(
          new PartialPath("root.laptop.d1.a.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      manager.createTimeseries(
          new PartialPath("root.laptop.d2.a.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(4, manager.getDevicesNum(new PartialPath("root.laptop.**")));

      manager.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      Assert.assertEquals(3, manager.getDevicesNum(new PartialPath("root.laptop.**")));
      manager.deleteTimeseries(new PartialPath("root.laptop.d2.a.s3"));
      Assert.assertEquals(2, manager.getDevicesNum(new PartialPath("root.laptop.**")));
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.a.s3"));
      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.**")));

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
          new PartialPath("root.laptop.d0"),
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
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
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
  public void testCreateAlignedTimeseriesAndInsertWithMismatchDataType() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.aligned_device"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT64"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          Arrays.asList(compressionType, compressionType, compressionType));

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.INT32};

      String[] columns = new String[3];
      columns[0] = 2.0 + "";
      columns[1] = 10000 + "";
      columns[2] = 100 + "";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2", "s3"},
              dataTypes,
              columns,
              true);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      IMNode node = manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(3, manager.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithNotAlignedData() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.aligned_device"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT64"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          Arrays.asList(compressionType, compressionType, compressionType));
    } catch (Exception e) {
      fail();
    }

    try {
      manager.createTimeseries(
          new CreateTimeSeriesPlan(
              new PartialPath("root.laptop.d1.aligned_device.s4"),
              TSDataType.valueOf("FLOAT"),
              TSEncoding.valueOf("RLE"),
              compressionType,
              null,
              null,
              null,
              null));
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity. (Path: root.laptop.d1.aligned_device)",
          e.getMessage());
    }

    try {
      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};

      String[] columns = new String[3];
      columns[0] = "1.0";
      columns[1] = "2";
      columns[2] = "3";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2", "s3"},
              dataTypes,
              columns,
              false);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is aligned , please set InsertPlan.isAligned() = true",
          e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithMismatchDataType() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT};

      String[] columns = new String[1];
      columns[0] = 2.0 + "";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1"), time, new String[] {"s0"}, dataTypes, columns);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      IMNode node = manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(1, manager.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));
      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
      assertEquals(1, insertRowPlan.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithAlignedData() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (Exception e) {
      fail();
    }

    try {
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.aligned_device"),
          Arrays.asList("s3", "s4", "s5"),
          Arrays.asList(
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT64"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          Arrays.asList(compressionType, compressionType, compressionType));
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under this entity is not aligned, please use createTimeseries or change entity. (Path: root.laptop.d1.aligned_device)",
          e.getMessage());
    }

    try {
      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};

      String[] columns = new String[2];
      columns[0] = "1";
      columns[1] = "2";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2"},
              dataTypes,
              columns,
              true);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is not aligned , please set InsertPlan.isAligned() = false",
          e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesWithIllegalNames() throws Exception {
    MManager manager = IoTDB.metaManager;
    manager.setStorageGroup(new PartialPath("root.laptop"));
    PartialPath deviceId = new PartialPath("root.laptop.d1");
    String[] measurementIds = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String measurementId : measurementIds) {
      PartialPath path = deviceId.concatNode(measurementId);
      try {
        manager.createAlignedTimeSeries(
            path,
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(
                TSDataType.valueOf("FLOAT"),
                TSDataType.valueOf("INT64"),
                TSDataType.valueOf("INT32")),
            Arrays.asList(
                TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(compressionType, compressionType, compressionType));
        fail();
      } catch (Exception e) {
        Assert.assertEquals(
            String.format(
                "%s is not a legal path, because %s",
                path.getFullPath(), String.format("%s is an illegal name.", measurementId)),
            e.getMessage());
      }
    }

    PartialPath path = deviceId.concatNode("t1");
    for (String measurementId : measurementIds) {
      try {
        manager.createAlignedTimeSeries(
            path,
            Arrays.asList(measurementId, "s2", "s3"),
            Arrays.asList(
                TSDataType.valueOf("FLOAT"),
                TSDataType.valueOf("INT64"),
                TSDataType.valueOf("INT32")),
            Arrays.asList(
                TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(compressionType, compressionType, compressionType));
        fail();
      } catch (Exception e) {
        Assert.assertEquals(String.format("%s is an illegal name.", measurementId), e.getMessage());
      }
    }
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

    insertPlan = getInsertPlan("\"a+b\"");
    manager.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(manager.isPathExist(deviceId.concatNode("\"a+b\"")));

    String[] illegalMeasurementIds = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP"};
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
    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);
    insertPlan.getDataTypes()[0] = TSDataType.INT32;
    return insertPlan;
  }

  @Test
  public void testTemplateSchemaNameCheckWhileCreate() {
    MManager manager = IoTDB.metaManager;
    String[] illegalSchemaNames = {"a+b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String schemaName : illegalSchemaNames) {
      CreateTemplatePlan plan = getCreateTemplatePlan(schemaName);
      try {
        manager.createSchemaTemplate(plan);
      } catch (MetadataException e) {
        Assert.assertEquals(String.format("%s is an illegal name.", schemaName), e.getMessage());
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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(compressionType));

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
    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);
    insertPlan.getDataTypes()[0] = TSDataType.INT32;

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
  public void testTimeseriesDeletionWithEntityUsingTemplate() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    manager.setStorageGroup(new PartialPath("root.sg"));

    CreateTemplatePlan plan = getCreateTemplatePlan("s1");
    manager.createSchemaTemplate(plan);
    SetTemplatePlan setPlan = new SetTemplatePlan("template1", "root.sg.d1");
    manager.setSchemaTemplate(setPlan);
    manager.createTimeseries(
        new PartialPath("root.sg.d1.s2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    manager.setUsingSchemaTemplate(manager.getDeviceNode(new PartialPath("root.sg.d1")));
    manager.deleteTimeseries(new PartialPath("root.sg.d1.s2"));
    assertTrue(manager.isPathExist(new PartialPath("root.sg.d1")));

    manager.createTimeseries(
        new PartialPath("root.sg.d2.s2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    manager.deleteTimeseries(new PartialPath("root.sg.d2.s2"));
    assertFalse(manager.isPathExist(new PartialPath("root.sg.d2")));
  }
}
