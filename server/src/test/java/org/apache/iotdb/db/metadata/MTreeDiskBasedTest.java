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
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;

public class MTreeDiskBasedTest {

  private static final int CACHE_SIZE = 0;
  private static final String BASE_PATH = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static final String METAFILE_FILEPATH =
      BASE_PATH + File.separator + "MTreeDiskBasedTest_metafile.bin";
  private File metaFile = new File(METAFILE_FILEPATH);

  private MTreeDiskBased root;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    if (metaFile.exists()) {
      metaFile.delete();
    }
    root = new MTreeDiskBased(CACHE_SIZE, METAFILE_FILEPATH);
  }

  @After
  public void tearDown() throws Exception {
    root.clear();
    if (metaFile.exists()) {
      metaFile.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSetStorageGroupExceptionMessage() throws Exception {
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      root.setStorageGroup(new PartialPath("root.edge1"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals(
          "some children of root.edge1 have already been set to storage group", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge2"));
      root.setStorageGroup(new PartialPath("root.edge2.access"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals("root.edge2 has already been set to storage group", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals("root.edge1.access has already been set to storage group", e.getMessage());
    }
  }

  @Test
  public void testAddLeftNodePathWithAlias() throws Exception {
    root.setStorageGroup(new PartialPath("root.laptop"));
    try {
      createTimeseries(
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
      createTimeseries(
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
  public void testAddAndPathExist() throws Exception {
    String path1 = "root";
    root.setStorageGroup(new PartialPath("root.laptop"));
    assertTrue(root.isPathExist(new PartialPath(path1)));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1")));
    try {
      createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }
    assertTrue(root.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(root.isPathExist(new PartialPath("root.laptop")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s2")));
    try {
      createTimeseries(
          new PartialPath("aa.bb.cc"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e) {
      Assert.assertEquals(String.format("%s is not a legal path", "aa.bb.cc"), e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPath() throws Exception {
    try {
      assertFalse(root.isPathExist(new PartialPath("root.a.d0")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a.d0")));
      root.setStorageGroup(new PartialPath("root.a.d0"));
      createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      assertFalse(root.isPathExist(new PartialPath("root.a.d1")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a.d1")));
      root.setStorageGroup(new PartialPath("root.a.d1"));
      createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      root.setStorageGroup(new PartialPath("root.a.b.d0"));
      createTimeseries(
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
      List<PartialPath> result = root.getAllTimeseriesPath(new PartialPath("root.a.*.s0"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = root.getAllTimeseriesPath(new PartialPath("root.a.*.*.s0"));
      assertEquals("root.a.b.d0.s0", result.get(0).getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPathWithAlias() throws Exception {
    try {
      assertFalse(root.isPathExist(new PartialPath("root.a.d0")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a.d0")));
      root.setStorageGroup(new PartialPath("root.a.d0"));
      createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");

      assertFalse(root.isPathExist(new PartialPath("root.a.d1")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a.d1")));
      root.setStorageGroup(new PartialPath("root.a.d1"));
      createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      root.setStorageGroup(new PartialPath("root.a.b.d0"));
      createTimeseries(
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
      List<PartialPath> result = root.getAllTimeseriesPath(new PartialPath("root.a.*.s0"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = root.getAllTimeseriesPath(new PartialPath("root.a.*.temperature"));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      List<PartialPath> result2 =
          root.getAllTimeseriesPathWithAlias(new PartialPath("root.a.*.s0"), 0, 0).left;
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.s0", result2.get(0).getFullPath());
      assertFalse(result2.get(0).isMeasurementAliasExists());
      assertEquals("root.a.d1.s0", result2.get(1).getFullPath());
      assertFalse(result2.get(1).isMeasurementAliasExists());

      result2 =
          root.getAllTimeseriesPathWithAlias(new PartialPath("root.a.*.temperature"), 0, 0).left;
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.temperature", result2.get(0).getFullPathWithAlias());
      assertEquals("root.a.d1.temperature", result2.get(1).getFullPathWithAlias());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllChildNodeNamesByPath() throws Exception {
    try {
      root.setStorageGroup(new PartialPath("root.a.d0"));
      createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
          new PartialPath("root.a.d5"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      // getChildNodeByPath
      Set<String> result1 = root.getChildNodeInNextLevel(new PartialPath("root.a.d0"));
      Set<String> result2 = root.getChildNodeInNextLevel(new PartialPath("root.a"));
      Set<String> result3 = root.getChildNodeInNextLevel(new PartialPath("root"));
      assertEquals(result1, new HashSet<>(Arrays.asList("s0", "s1")));
      assertEquals(result2, new HashSet<>(Arrays.asList("d0", "d5")));
      assertEquals(result3, new HashSet<>(Arrays.asList("a")));

      // if child node is nll   will return  null HashSet
      Set<String> result5 = root.getChildNodeInNextLevel(new PartialPath("root.a.d5"));
      assertEquals(result5, new HashSet<>(Arrays.asList()));
    } catch (MetadataException e1) {
      e1.printStackTrace();
    }
  }

  @Test
  public void testSetStorageGroup() throws Exception {
    // set storage group first
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isPathExist(new PartialPath("root.laptop.d1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1")));
      assertEquals(
          "root.laptop.d1",
          root.getStorageGroupPath(new PartialPath("root.laptop.d1")).getFullPath());
      assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(
          "root.laptop.d1",
          root.getStorageGroupPath(new PartialPath("root.laptop.d1.s1")).getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d2"));
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
    }

    // check timeseries
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s1")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d2.s0")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d2.s1")));

    try {
      assertEquals(
          "root.laptop.d1",
          root.getStorageGroupPath(new PartialPath("root.laptop.d1.s0")).getFullPath());
      createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(
          "root.laptop.d1",
          root.getStorageGroupPath(new PartialPath("root.laptop.d1.s1")).getFullPath());
      createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(
          "root.laptop.d2",
          root.getStorageGroupPath(new PartialPath("root.laptop.d2.s0")).getFullPath());
      createTimeseries(
          new PartialPath("root.laptop.d2.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(
          "root.laptop.d2",
          root.getStorageGroupPath(new PartialPath("root.laptop.d2.s1")).getFullPath());
      createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
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
      root.deleteTimeseriesAndReturnEmptyStorageGroup(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s0")));
    try {
      root.deleteStorageGroup(new PartialPath("root.laptop.d1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1.s1")));
    assertFalse(root.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(root.isPathExist(new PartialPath("root.laptop")));
    assertTrue(root.isPathExist(new PartialPath("root.laptop.d2")));
    assertTrue(root.isPathExist(new PartialPath("root.laptop.d2.s0")));
  }

  @Test
  public void testCheckStorageGroup() throws Exception {
    // set storage group first
    try {
      assertFalse(root.isStorageGroup(new PartialPath("root")));
      assertFalse(root.isStorageGroup(new PartialPath("root1.laptop.d2")));

      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      root.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d3")));

      root.setStorageGroup(new PartialPath("root.1"));
      assertTrue(root.isStorageGroup(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() throws Exception {
    // set storage group first
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      root.setStorageGroup(new PartialPath("root.laptop.d2"));
      createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, root.getStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, root.getStorageGroupByPath(new PartialPath("root.laptop.d1")));

      list.add("root.laptop.d2");
      assertEquals(list, root.getStorageGroupByPath(new PartialPath("root.laptop")));
      assertEquals(list, root.getStorageGroupByPath(new PartialPath("root")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() throws Exception {
    // set storage group first
    try {
      assertTrue(root.getStorageGroupByPath(new PartialPath("root")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          root.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(root.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(root.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          root.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(root.getStorageGroupByPath(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(root.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllTimeseriesCount() throws Exception {
    // set storage group first
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
      createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);
      createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null,
          null);

      assertEquals(4, root.getAllTimeseriesCount(new PartialPath("root.laptop")));
      assertEquals(2, root.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")));
      PartialPath partialPath = new PartialPath("root.laptop.d1.s3");
      try {
        root.getAllTimeseriesCount(partialPath);
        fail("Expected exception");
      } catch (MetadataException e) {
        assertEquals("Path [root.laptop.d1.s3] does not exist", e.getMessage());
      }

      assertEquals(2, root.getNodesCountInGivenLevel(new PartialPath("root.laptop"), 2));
      assertEquals(4, root.getNodesCountInGivenLevel(new PartialPath("root.laptop"), 3));
      assertEquals(2, root.getNodesCountInGivenLevel(new PartialPath("root.laptop.*"), 2));
      assertEquals(4, root.getNodesCountInGivenLevel(new PartialPath("root.laptop.*"), 3));
      assertEquals(2, root.getNodesCountInGivenLevel(new PartialPath("root.laptop.d1"), 3));
      assertEquals(0, root.getNodesCountInGivenLevel(new PartialPath("root.laptop.d1"), 4));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAddSubDevice() throws Exception {
    root.setStorageGroup(new PartialPath("root.laptop"));
    createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);
    createTimeseries(
        new PartialPath("root.laptop.d1.s1.b"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    assertEquals(2, root.getDevices(new PartialPath("root")).size());
    assertEquals(2, root.getAllTimeseriesCount(new PartialPath("root")));
    assertEquals(2, root.getAllTimeseriesPath(new PartialPath("root")).size());
    assertEquals(2, root.getAllTimeseriesPathWithAlias(new PartialPath("root"), 0, 0).left.size());
  }

  @Test
  public void testIllegalStorageGroup() throws Exception {
    try {
      root.setStorageGroup(new PartialPath("root.\"sg.ln\""));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "The storage group name can only be characters, numbers and underscores. root.\"sg.ln\" is not a legal path",
          e.getMessage());
    }
  }

  @Test
  public void testSearchStorageGroup() throws Exception {
    String path1 = "root";
    String sgPath1 = "root.vehicle";
    root.setStorageGroup(new PartialPath(sgPath1));
    assertTrue(root.isPathExist(new PartialPath(path1)));
    try {
      createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
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
        root.searchAllRelatedStorageGroups(new PartialPath("root.vehicle.d1.s1")),
        Collections.singletonList(new PartialPath(sgPath1)));
  }

  @Test
  public void testDeleteChildOfMeasurementMNode() throws Exception {
    String sgPath = "root.sg1";
    root.setStorageGroup(new PartialPath(sgPath));
    try {
      createTimeseries(
          new PartialPath("root.sg1.a.b"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      createTimeseries(
          new PartialPath("root.sg1.a.b.c"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertTrue(root.isPathExist(new PartialPath("root.sg1.a.b")));
      assertTrue(root.isPathExist(new PartialPath("root.sg1.a.b.c")));

      root.deleteTimeseriesAndReturnEmptyStorageGroup(new PartialPath("root.sg1.a.b.c"));

      assertFalse(root.isPathExist(new PartialPath("root.sg1.a.b.c")));
      assertTrue(root.isPathExist(new PartialPath("root.sg1.a.b")));

    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }
  }

  @Test
  public void testGetMeasurementMNodeCount() throws Exception {
    PartialPath sgPath = new PartialPath("root.sg1");
    root.setStorageGroup(sgPath);
    try {
      createTimeseries(
          new PartialPath("root.sg1.a.b"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      IMNode sgNode = root.getNodeByPath(sgPath);
      assertEquals(1, root.getMeasurementMNodeCount(sgPath)); // b

      createTimeseries(
          new PartialPath("root.sg1.a.b.c"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(2, root.getMeasurementMNodeCount(sgPath)); // b and c
      PartialPath cNodePath = new PartialPath(sgPath + ".a.b.c");
      assertEquals(1, root.getMeasurementMNodeCount(cNodePath)); // c

      createTimeseries(
          new PartialPath("root.sg1.a.b.c.d"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(3, root.getMeasurementMNodeCount(sgPath)); // b, c and d
      assertEquals(2, root.getMeasurementMNodeCount(cNodePath)); // c and d
      PartialPath dNodePath = new PartialPath(cNodePath + ".d");
      assertEquals(1, root.getMeasurementMNodeCount(dNodePath)); // d

    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }
  }

  @Test
  public void testCreateTimeseries() throws Exception {
    String sgPath = "root.sg1";
    root.setStorageGroup(new PartialPath(sgPath));

    createTimeseries(
        new PartialPath("root.sg1.a.b.c"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    createTimeseries(
        new PartialPath("root.sg1.a.b"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    IMNode node = root.getNodeByPath(new PartialPath("root.sg1.a.b"));
    Assert.assertTrue(node instanceof MeasurementMNode);
  }

  private MeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    MeasurementMNode measurementMNode =
        root.createTimeseries(path, dataType, encoding, compressor, props, alias);
    root.unlockMNode(measurementMNode);
    return measurementMNode;
  }
}
