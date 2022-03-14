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
package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MTreeBelowSGTest {

  MTreeAboveSG root;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    root = new MTreeAboveSG();
  }

  @After
  public void tearDown() throws Exception {
    root = null;
    EnvironmentUtils.cleanEnv();
  }

  private MTreeBelowSG getStorageGroup(PartialPath path) throws MetadataException {
    root.setStorageGroup(path);
    try {
      return new MTreeBelowSG(root.getStorageGroupNodeByStorageGroupPath(path));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Test
  public void testAddLeftNodePathWithAlias() throws MetadataException {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");
    } catch (MetadataException e) {
      assertTrue(e instanceof AliasAlreadyExistException);
    }
  }

  @Test
  public void testAddAndPathExist() throws MetadataException {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1")));
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }
    assertTrue(storageGroup.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(storageGroup.isPathExist(new PartialPath("root.laptop")));
    assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1.s2")));
  }

  @Test
  public void testAddAndQueryPath() {
    MTreeBelowSG storageGroup = null;
    try {
      assertFalse(root.isPathExist(new PartialPath("root.a")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a")));
      storageGroup = getStorageGroup(new PartialPath("root.a"));

      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.b.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      assertNotNull(storageGroup);
      List<MeasurementPath> result =
          storageGroup.getMeasurementPaths(new PartialPath("root.a.*.s0"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = storageGroup.getMeasurementPaths(new PartialPath("root.a.*.*.s0"));
      assertEquals("root.a.b.d0.s0", result.get(0).getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPathWithAlias() {
    MTreeBelowSG storageGroup = null;
    try {
      assertFalse(root.isPathExist(new PartialPath("root.a")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a")));
      storageGroup = getStorageGroup(new PartialPath("root.a"));

      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");

      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.b.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      assertNotNull(storageGroup);

      List<MeasurementPath> result =
          storageGroup.getMeasurementPaths(new PartialPath("root.a.*.s0"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = storageGroup.getMeasurementPaths(new PartialPath("root.a.*.temperature"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      List<MeasurementPath> result2 =
          storageGroup.getMeasurementPathsWithAlias(new PartialPath("root.a.*.s0"), 0, 0, false)
              .left;
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.s0", result2.get(0).getFullPath());
      assertFalse(result2.get(0).isMeasurementAliasExists());
      assertEquals("root.a.d1.s0", result2.get(1).getFullPath());
      assertFalse(result2.get(1).isMeasurementAliasExists());

      result2 =
          storageGroup.getMeasurementPathsWithAlias(
                  new PartialPath("root.a.*.temperature"), 0, 0, false)
              .left;
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.temperature", result2.get(0).getFullPathWithAlias());
      assertEquals("root.a.d1.temperature", result2.get(1).getFullPathWithAlias());

      Pair<List<MeasurementPath>, Integer> result3 =
          storageGroup.getMeasurementPathsWithAlias(new PartialPath("root.a.**"), 2, 0, false);
      assertEquals(2, result3.left.size());
      assertEquals(2, result3.right.intValue());

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllChildNodeNamesByPath() {
    MTreeBelowSG storageGroup = null;
    try {
      storageGroup = getStorageGroup(new PartialPath("root.a"));

      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d5"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      // getChildNodeByPath
      Set<String> result1 = storageGroup.getChildNodeNameInNextLevel(new PartialPath("root.a.d0"));
      Set<String> result2 = storageGroup.getChildNodeNameInNextLevel(new PartialPath("root.a"));
      assertEquals(new HashSet<>(Arrays.asList("s0", "s1")), result1);
      assertEquals(new HashSet<>(Arrays.asList("d0", "d5")), result2);

      // if child node is nll   will return  null HashSet
      Set<String> result3 = storageGroup.getChildNodeNameInNextLevel(new PartialPath("root.a.d5"));
      assertEquals(result3, new HashSet<>(Collections.emptyList()));

      Set<String> result4 = storageGroup.getChildNodeNameInNextLevel(new PartialPath("root"));
      assertEquals(new HashSet<>(Collections.singletonList("a")), result4);
    } catch (MetadataException e1) {
      e1.printStackTrace();
    }
  }

  @Test
  public void testSetStorageGroup() throws IllegalPathException {
    // set storage group first
    MTreeBelowSG storageGroup = null;
    try {
      storageGroup = getStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isPathExist(new PartialPath("root.laptop.d1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1")).getFullPath());
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s1")).getFullPath());

      assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1.s1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
    }
    // check timeseries
    assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1.s1")));

    try {
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s0")).getFullPath());
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s1")).getFullPath());
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      storageGroup.deleteTimeseriesAndReturnEmptyStorageGroup(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(storageGroup.isPathExist(new PartialPath("root.laptop.d1.s0")));
    try {
      root.deleteStorageGroup(new PartialPath("root.laptop.d1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s1")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(root.isPathExist(new PartialPath("root.laptop")));
  }

  @Test
  public void testGetAllTimeseriesCount() {
    // set storage group first
    MTreeBelowSG storageGroup = null;
    try {
      storageGroup = getStorageGroup(new PartialPath("root.laptop"));
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);

      assertEquals(4, storageGroup.getAllTimeseriesCount(new PartialPath("root.laptop.**")));
      assertEquals(2, storageGroup.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")));
      assertEquals(0, storageGroup.getAllTimeseriesCount(new PartialPath("root.laptop.d1.s3")));

      assertEquals(
          1,
          storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.**.s1"), 1, false));
      assertEquals(
          1, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.*.*"), 1, false));
      assertEquals(
          2, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.*.*"), 2, false));
      assertEquals(
          2, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.*"), 2, false));
      assertEquals(
          4, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.*.*"), 3, false));
      assertEquals(
          2, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.**"), 2, false));
      assertEquals(
          4, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.**"), 3, false));
      assertEquals(
          2, storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.d1.*"), 3, false));
      assertEquals(
          0,
          storageGroup.getNodesCountInGivenLevel(new PartialPath("root.laptop.d1.**"), 4, false));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAddSubDevice() throws MetadataException {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d1.s2.b"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    assertEquals(2, storageGroup.getDevices(new PartialPath("root"), true).size());
    assertEquals(2, storageGroup.getDevices(new PartialPath("root.**"), false).size());
    assertEquals(2, storageGroup.getAllTimeseriesCount(new PartialPath("root.**")));
    assertEquals(2, storageGroup.getMeasurementPaths(new PartialPath("root.**")).size());
    assertEquals(
        2,
        storageGroup
            .getMeasurementPathsWithAlias(new PartialPath("root.**"), 0, 0, false)
            .left
            .size());
  }

  @Test
  public void testSearchStorageGroup() throws MetadataException {
    String path1 = "root";
    String sgPath1 = "root.vehicle";
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath(sgPath1));
    assertTrue(root.isPathExist(new PartialPath(path1)));
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.vehicle.d1.s2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }

    assertEquals(
        root.getBelongedStorageGroups(new PartialPath("root.vehicle.d1.s1")),
        Collections.singletonList(new PartialPath(sgPath1)));
  }

  @Test
  public void testCreateTimeseries() throws MetadataException {
    String sgPath = "root.sg1";
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath(sgPath));

    storageGroup.createTimeseries(
        new PartialPath("root.sg1.a.b.c"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    try {
      // mtree doesn't support nested timeseries which means MeasurementMNode is leaf of the tree.
      storageGroup.createTimeseries(
          new PartialPath("root.sg1.a.b"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (PathAlreadyExistException e) {
      assertEquals("Path [root.sg1.a.b] already exist", e.getMessage());
    }

    IMNode node = storageGroup.getNodeByPath(new PartialPath("root.sg1.a.b"));
    assertFalse(node instanceof MeasurementMNode);
  }

  @Test
  public void testCountEntity() throws MetadataException {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d2.s1.t1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d2.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.a.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(0, storageGroup.getDevicesNum(new PartialPath("root")));
    Assert.assertEquals(1, storageGroup.getDevicesNum(new PartialPath("root.laptop")));
    Assert.assertEquals(0, storageGroup.getDevicesNum(new PartialPath("root.laptop.s1")));
    Assert.assertEquals(1, storageGroup.getDevicesNum(new PartialPath("root.laptop.d1")));
    Assert.assertEquals(2, storageGroup.getDevicesNum(new PartialPath("root.laptop.*")));
    Assert.assertEquals(2, storageGroup.getDevicesNum(new PartialPath("root.laptop.*.*")));
    Assert.assertEquals(0, storageGroup.getDevicesNum(new PartialPath("root.laptop.*.*.*")));
    Assert.assertEquals(4, storageGroup.getDevicesNum(new PartialPath("root.laptop.**")));
    Assert.assertEquals(5, storageGroup.getDevicesNum(new PartialPath("root.**")));
    Assert.assertEquals(4, storageGroup.getDevicesNum(new PartialPath("root.**.*")));
    Assert.assertEquals(4, storageGroup.getDevicesNum(new PartialPath("root.*.**")));
    Assert.assertEquals(2, storageGroup.getDevicesNum(new PartialPath("root.**.d1")));
    Assert.assertEquals(1, storageGroup.getDevicesNum(new PartialPath("root.laptop.*.d1")));
    Assert.assertEquals(3, storageGroup.getDevicesNum(new PartialPath("root.**.d*")));
    Assert.assertEquals(1, storageGroup.getDevicesNum(new PartialPath("root.laptop.**.s1")));
    Assert.assertEquals(1, storageGroup.getDevicesNum(new PartialPath("root.*.d2.*")));
  }

  @Test
  public void testGetNodeListInLevel() throws MetadataException {
    MManager.StorageGroupFilter filter = sg -> sg.equals("root.sg1");

    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.sg1"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg1.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg1.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, null).size());

    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, null).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, null).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*.s1"), 2, null).size());

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, filter).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.**"), 2, filter).size());

    storageGroup = getStorageGroup(new PartialPath("root.sg2"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg2.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg2.d2.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, null).size());

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, null).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, null).size());
    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*.s1"), 2, null).size());

    Assert.assertEquals(
        0, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, filter).size());
    Assert.assertEquals(
        0, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.**"), 2, filter).size());
  }

  @Test
  public void testGetDeviceForTimeseries() throws MetadataException {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.sg"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a1.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a1.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    storageGroup.createTimeseries(
        new PartialPath("root.sg.a2.d2.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a2.d2.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(2, storageGroup.getDevicesByTimeseries(new PartialPath("root.**")).size());
    Assert.assertEquals(
        1, storageGroup.getDevicesByTimeseries(new PartialPath("root.*.*.d1.*")).size());
    Assert.assertEquals(
        2, storageGroup.getDevicesByTimeseries(new PartialPath("root.*.*.d*.*")).size());
  }

  @Test
  public void testGetMeasurementCountGroupByLevel() throws Exception {
    MTreeBelowSG storageGroup = getStorageGroup(new PartialPath("root.sg"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a1.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a1.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    storageGroup.createTimeseries(
        new PartialPath("root.sg.a2.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg.a2.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    PartialPath pattern = new PartialPath("root.sg.**");
    Map<PartialPath, Integer> result =
        storageGroup.getMeasurementCountGroupByLevel(pattern, 2, false);
    assertEquals(2, result.size());
    assertEquals(3, (int) result.get(new PartialPath("root.sg.a1")));
    assertEquals(2, (int) result.get(new PartialPath("root.sg.a2")));

    result = storageGroup.getMeasurementCountGroupByLevel(pattern, 3, false);
    assertEquals(4, result.size());
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a1.s1")));
    assertEquals(2, (int) result.get(new PartialPath("root.sg.a1.d1")));
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a2.s1")));
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a2.d1")));

    result = storageGroup.getMeasurementCountGroupByLevel(pattern, 5, false);
    assertEquals(0, result.size());

    pattern = new PartialPath("root.**.s1");
    result = storageGroup.getMeasurementCountGroupByLevel(pattern, 2, false);
    assertEquals(2, result.size());
    assertEquals(2, (int) result.get(new PartialPath("root.sg.a1")));
    assertEquals(2, (int) result.get(new PartialPath("root.sg.a2")));

    result = storageGroup.getMeasurementCountGroupByLevel(pattern, 3, false);
    assertEquals(4, result.size());
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a1.s1")));
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a1.d1")));
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a2.s1")));
    assertEquals(1, (int) result.get(new PartialPath("root.sg.a2.d1")));
  }
}
