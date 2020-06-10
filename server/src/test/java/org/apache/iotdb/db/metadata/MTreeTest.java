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

import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MTreeTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddLeftNodePathWithAlias() throws MetadataException {
    MTree root = new MTree();
    root.setStorageGroup("root.laptop");
    try {
      root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), "status");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.createTimeseries("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), "status");
      fail();
    } catch (MetadataException e) {
      assertTrue(e instanceof AliasAlreadyExistException);
    }
  }

  @Test
  public void testAddAndPathExist() throws MetadataException {
    MTree root = new MTree();
    String path1 = "root";
    root.setStorageGroup("root.laptop");
    assertTrue(root.isPathExist(path1));
    assertFalse(root.isPathExist("root.laptop.d1"));
    try {
      root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }
    assertTrue(root.isPathExist("root.laptop.d1"));
    assertTrue(root.isPathExist("root.laptop"));
    assertFalse(root.isPathExist("root.laptop.d1.s2"));
    try {
      root.createTimeseries("aa.bb.cc", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
    } catch (MetadataException e) {
      Assert.assertEquals(String.format("%s is not a legal path", "aa.bb.cc"),
          e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPath() {
    MTree root = new MTree();
    try {
      assertFalse(root.isPathExist("root.a.d0"));
      assertFalse(root.checkStorageGroupByPath("root.a.d0"));
      root.setStorageGroup("root.a.d0");
      root.createTimeseries("root.a.d0.s0", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
      root.createTimeseries("root.a.d0.s1", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

      assertFalse(root.isPathExist("root.a.d1"));
      assertFalse(root.checkStorageGroupByPath("root.a.d1"));
      root.setStorageGroup("root.a.d1");
      root.createTimeseries("root.a.d1.s0", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
      root.createTimeseries("root.a.d1.s1", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

      root.setStorageGroup("root.a.b.d0");
      root.createTimeseries("root.a.b.d0.s0", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      List<String> result = root.getAllTimeseriesName("root.a.*.s0");
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0));
      assertEquals("root.a.d1.s0", result.get(1));

      result = root.getAllTimeseriesName("root.a.*.*.s0");
      assertEquals("root.a.b.d0.s0", result.get(0));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void testAddAndQueryPathWithAlias() {
    MTree root = new MTree();
    try {
      assertFalse(root.isPathExist("root.a.d0"));
      assertFalse(root.checkStorageGroupByPath("root.a.d0"));
      root.setStorageGroup("root.a.d0");
      root.createTimeseries("root.a.d0.s0", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), "temperature");
      root.createTimeseries("root.a.d0.s1", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), "status");

      assertFalse(root.isPathExist("root.a.d1"));
      assertFalse(root.checkStorageGroupByPath("root.a.d1"));
      root.setStorageGroup("root.a.d1");
      root.createTimeseries("root.a.d1.s0", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), "temperature");
      root.createTimeseries("root.a.d1.s1", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

      root.setStorageGroup("root.a.b.d0");
      root.createTimeseries("root.a.b.d0.s0", TSDataType.INT32, TSEncoding.RLE,
              TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      List<String> result = root.getAllTimeseriesName("root.a.*.s0");
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0));
      assertEquals("root.a.d1.s0", result.get(1));


      result = root.getAllTimeseriesName("root.a.*.temperature");
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0));
      assertEquals("root.a.d1.s0", result.get(1));


      List<Path> result2 = root.getAllTimeseriesPath("root.a.*.s0");
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.s0", result2.get(0).getFullPath());
      assertEquals(null, result2.get(0).getAlias());
      assertEquals("root.a.d1.s0", result2.get(1).getFullPath());
      assertEquals(null, result2.get(1).getAlias());

      result2 = root.getAllTimeseriesPath("root.a.*.temperature");
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.temperature", result2.get(0).getFullPathWithAlias());
      assertEquals("root.a.d1.temperature", result2.get(1).getFullPathWithAlias());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void testCombineMetadataInStrings() {
    MTree root = new MTree();
    MTree root1 = new MTree();
    MTree root2 = new MTree();
    MTree root3 = new MTree();
    try {
      CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();

      root.setStorageGroup("root.a.d0");
      root.createTimeseries("root.a.d0.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);
      root.createTimeseries("root.a.d0.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      root.setStorageGroup("root.a.d1");
      root.createTimeseries("root.a.d1.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);
      root.createTimeseries("root.a.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      root.setStorageGroup("root.a.b.d0");
      root.createTimeseries("root.a.b.d0.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      root1.setStorageGroup("root.a.d0");
      root1.createTimeseries("root.a.d0.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);
      root1.createTimeseries("root.a.d0.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      root2.setStorageGroup("root.a.d1");
      root2.createTimeseries("root.a.d1.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);
      root2.createTimeseries("root.a.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      root3.setStorageGroup("root.a.b.d0");
      root3.createTimeseries("root.a.b.d0.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap(), null);

      String[] metadatas = new String[3];
      metadatas[0] = root1.toString();
      metadatas[1] = root2.toString();
      metadatas[2] = root3.toString();
      assertEquals(MTree.combineMetadataInStrings(metadatas), root.toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroup() {
    // set storage group first
    MTree root = new MTree();
    try {
      root.setStorageGroup("root.laptop.d1");
      assertTrue(root.isPathExist("root.laptop.d1"));
      assertTrue(root.checkStorageGroupByPath("root.laptop.d1"));
      assertEquals("root.laptop.d1", root.getStorageGroupName("root.laptop.d1"));
      assertFalse(root.isPathExist("root.laptop.d1.s1"));
      assertTrue(root.checkStorageGroupByPath("root.laptop.d1.s1"));
      assertEquals("root.laptop.d1", root.getStorageGroupName("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup("root.laptop.d2");
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup("root.laptop");
    } catch (MetadataException e) {
      Assert.assertEquals(
          "root.laptop has already been set to storage group",
          e.getMessage());
    }
    // check timeseries
    assertFalse(root.isPathExist("root.laptop.d1.s0"));
    assertFalse(root.isPathExist("root.laptop.d1.s1"));
    assertFalse(root.isPathExist("root.laptop.d2.s0"));
    assertFalse(root.isPathExist("root.laptop.d2.s1"));

    try {
      assertEquals("root.laptop.d1", root.getStorageGroupName("root.laptop.d1.s0"));
      root.createTimeseries("root.laptop.d1.s0", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
      assertEquals("root.laptop.d1", root.getStorageGroupName("root.laptop.d1.s1"));
      root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
      assertEquals("root.laptop.d2", root.getStorageGroupName("root.laptop.d2.s0"));
      root.createTimeseries("root.laptop.d2.s0", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
      assertEquals("root.laptop.d2", root.getStorageGroupName("root.laptop.d2.s1"));
      root.createTimeseries("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.deleteTimeseriesAndReturnEmptyStorageGroup("root.laptop.d1.s0");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isPathExist("root.laptop.d1.s0"));
    try {
      root.deleteStorageGroup("root.laptop.d1");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isPathExist("root.laptop.d1.s1"));
    assertFalse(root.isPathExist("root.laptop.d1"));
    assertTrue(root.isPathExist("root.laptop"));
    assertTrue(root.isPathExist("root.laptop.d2"));
    assertTrue(root.isPathExist("root.laptop.d2.s0"));
  }

  @Test
  public void testCheckStorageGroup() {
    // set storage group first
    MTree root = new MTree();
    try {
      assertFalse(root.isStorageGroup("root"));
      assertFalse(root.isStorageGroup("root1.laptop.d2"));

      root.setStorageGroup("root.laptop.d1");
      assertTrue(root.isStorageGroup("root.laptop.d1"));
      assertFalse(root.isStorageGroup("root.laptop.d2"));
      assertFalse(root.isStorageGroup("root.laptop"));
      assertFalse(root.isStorageGroup("root.laptop.d1.s1"));

      root.setStorageGroup("root.laptop.d2");
      assertTrue(root.isStorageGroup("root.laptop.d1"));
      assertTrue(root.isStorageGroup("root.laptop.d2"));
      assertFalse(root.isStorageGroup("root.laptop.d3"));

      root.setStorageGroup("root.1");
      assertTrue(root.isStorageGroup("root.1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {
    // set storage group first
    MTree root = new MTree();
    try {
      root.setStorageGroup("root.laptop.d1");
      root.setStorageGroup("root.laptop.d2");
      root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);
      root.createTimeseries("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, root.getStorageGroupByPath("root.laptop.d1.s1"));
      assertEquals(list, root.getStorageGroupByPath("root.laptop.d1"));

      list.add("root.laptop.d2");
      assertEquals(list, root.getStorageGroupByPath("root.laptop"));
      assertEquals(list, root.getStorageGroupByPath("root"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    // set storage group first
    MTree root = new MTree();
    try {
      assertTrue(root.getStorageGroupByPath("root").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle.device").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle.device.sensor").isEmpty());

      root.setStorageGroup("root.vehicle");
      assertFalse(root.getStorageGroupByPath("root.vehicle").isEmpty());
      assertFalse(root.getStorageGroupByPath("root.vehicle.device").isEmpty());
      assertFalse(root.getStorageGroupByPath("root.vehicle.device.sensor").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle1").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle1.device").isEmpty());

      root.setStorageGroup("root.vehicle1.device");
      assertTrue(root.getStorageGroupByPath("root.vehicle1.device1").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle1.device2").isEmpty());
      assertTrue(root.getStorageGroupByPath("root.vehicle1.device3").isEmpty());
      assertFalse(root.getStorageGroupByPath("root.vehicle1.device").isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllTimeseriesCount() {
    // set storage group first
    MTree root = new MTree();
    try {
      root.setStorageGroup("root.laptop");
      root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);
      root.createTimeseries("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);
      root.createTimeseries("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);
      root.createTimeseries("root.laptop.d2.s2", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null, null);

      assertEquals(4, root.getAllTimeseriesCount("root.laptop"));

      assertEquals(2, root.getNodesCountInGivenLevel("root.laptop", 2));
      assertEquals(4, root.getNodesCountInGivenLevel("root.laptop", 3));
      assertEquals(2, root.getNodesCountInGivenLevel("root.laptop.d1", 3));
      assertEquals(0, root.getNodesCountInGivenLevel("root.laptop.d1", 4));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void addSubDevice() throws MetadataException {
    MTree root = new MTree();
    root.setStorageGroup("root.laptop");
    root.createTimeseries("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);
    root.createTimeseries("root.laptop.d1.s1.b", TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap(), null);

    assertEquals(2, root.getDevices("root").size());
    assertEquals(2, root.getAllTimeseriesCount("root"));
    assertEquals(2, root.getAllTimeseriesName("root").size());
    assertEquals(2, root.getAllTimeseriesPath("root").size());
  }
}
