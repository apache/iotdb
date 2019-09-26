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
import java.util.HashMap;
import java.util.List;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
  public void testAddLeftNodePath() {
    MTree root = new MTree("root");
    try {
      root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE");
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.addTimeseriesPath("root.laptop.d1.s1.b", "INT32", "RLE");
    } catch (PathErrorException e) {
      Assert.assertEquals(
          String.format("The Node [%s] is left node, the timeseries %s can't be created", "s1",
              "root.laptop.d1.s1.b"), e.getMessage());
    }
  }

  @Test
  public void testAddAndPathExist() {
    MTree root = new MTree("root");
    String path1 = "root";
    assertEquals(true, root.isPathExist(path1));
    assertEquals(false, root.isPathExist("root.laptop.d1"));
    try {
      root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE");
    } catch (PathErrorException e1) {
      fail(e1.getMessage());
    }
    assertEquals(true, root.isPathExist("root.laptop.d1"));
    assertEquals(true, root.isPathExist("root.laptop"));
    assertEquals(false, root.isPathExist("root.laptop.d1.s2"));
    try {
      root.addTimeseriesPath("aa.bb.cc", "INT32", "RLE");
    } catch (PathErrorException e) {
      Assert.assertEquals(String.format("Timeseries %s is not right.", "aa.bb.cc"), e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPath() {
    MTree root = new MTree("root");
    try {
      assertEquals(false, root.isPathExist("root.a.d0"));
      assertEquals(false, root.checkFileNameByPath("root.a.d0"));
      root.setStorageGroup("root.a.d0");
      root.addTimeseriesPath("root.a.d0.s0", "INT32", "RLE");
      root.addTimeseriesPath("root.a.d0.s1", "INT32", "RLE");

      assertEquals(false, root.isPathExist("root.a.d1"));
      assertEquals(false, root.checkFileNameByPath("root.a.d1"));
      root.setStorageGroup("root.a.d1");
      root.addTimeseriesPath("root.a.d1.s0", "INT32", "RLE");
      root.addTimeseriesPath("root.a.d1.s1", "INT32", "RLE");

      root.setStorageGroup("root.a.b.d0");
      root.addTimeseriesPath("root.a.b.d0.s0", "INT32", "RLE");

    } catch (PathErrorException | StorageGroupException e1) {
      e1.printStackTrace();
    }

    try {
      HashMap<String, ArrayList<String>> result = root.getAllPath("root.a.*.s0");
      assertEquals(2, result.size());
      assertTrue(result.containsKey("root.a.d1"));
      assertEquals("root.a.d1.s0", result.get("root.a.d1").get(0));
      assertTrue(result.containsKey("root.a.d0"));
      assertEquals("root.a.d0.s0", result.get("root.a.d0").get(0));

      result = root.getAllPath("root.a.*.*.s0");
      assertTrue(result.containsKey("root.a.b.d0"));
      assertEquals("root.a.b.d0.s0", result.get("root.a.b.d0").get(0));
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void testCombineMetadataInStrings() {
    MTree root = new MTree("root");
    MTree root1 = new MTree("root");
    MTree root2 = new MTree("root");
    MTree root3 = new MTree("root");
    try {
      root.setStorageGroup("root.a.d0");
      root.addTimeseriesPath("root.a.d0.s0", "INT32", "RLE");
      root.addTimeseriesPath("root.a.d0.s1", "INT32", "RLE");

      root.setStorageGroup("root.a.d1");
      root.addTimeseriesPath("root.a.d1.s0", "INT32", "RLE");
      root.addTimeseriesPath("root.a.d1.s1", "INT32", "RLE");

      root.setStorageGroup("root.a.b.d0");
      root.addTimeseriesPath("root.a.b.d0.s0", "INT32", "RLE");

      root1.setStorageGroup("root.a.d0");
      root1.addTimeseriesPath("root.a.d0.s0", "INT32", "RLE");
      root1.addTimeseriesPath("root.a.d0.s1", "INT32", "RLE");

      root2.setStorageGroup("root.a.d1");
      root2.addTimeseriesPath("root.a.d1.s0", "INT32", "RLE");
      root2.addTimeseriesPath("root.a.d1.s1", "INT32", "RLE");

      root3.setStorageGroup("root.a.b.d0");
      root3.addTimeseriesPath("root.a.b.d0.s0", "INT32", "RLE");

      String[] metadatas = new String[3];
      metadatas[0] = root1.toString();
      metadatas[1] = root2.toString();
      metadatas[2] = root3.toString();
      assertEquals(MTree.combineMetadataInStrings(metadatas), root.toString());
    } catch (PathErrorException | StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroup() {
    // set storage group first
    MTree root = new MTree("root");
    try {
      root.setStorageGroup("root.laptop.d1");
      assertEquals(true, root.isPathExist("root.laptop.d1"));
      assertEquals(true, root.checkFileNameByPath("root.laptop.d1"));
      assertEquals("root.laptop.d1", root.getStorageGroupNameByPath("root.laptop.d1"));
      assertEquals(false, root.isPathExist("root.laptop.d1.s1"));
      assertEquals(true, root.checkFileNameByPath("root.laptop.d1.s1"));
      assertEquals("root.laptop.d1", root.getStorageGroupNameByPath("root.laptop.d1.s1"));
    } catch (StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup("root.laptop.d2");
    } catch (StorageGroupException e) {
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup("root.laptop");
    } catch (StorageGroupException e) {
      Assert.assertEquals(
          "The seriesPath of root.laptop already exist, it can't be set to the storage group",
          e.getMessage());
    }
    // check timeseries
    assertEquals(false, root.isPathExist("root.laptop.d1.s0"));
    assertEquals(false, root.isPathExist("root.laptop.d1.s1"));
    assertEquals(false, root.isPathExist("root.laptop.d2.s0"));
    assertEquals(false, root.isPathExist("root.laptop.d2.s1"));

    try {
      assertEquals("root.laptop.d1", root.getStorageGroupNameByPath("root.laptop.d1.s0"));
      root.addTimeseriesPath("root.laptop.d1.s0", "INT32", "RLE");
      assertEquals("root.laptop.d1", root.getStorageGroupNameByPath("root.laptop.d1.s1"));
      root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE");
      assertEquals("root.laptop.d2", root.getStorageGroupNameByPath("root.laptop.d2.s0"));
      root.addTimeseriesPath("root.laptop.d2.s0", "INT32", "RLE");
      assertEquals("root.laptop.d2", root.getStorageGroupNameByPath("root.laptop.d2.s1"));
      root.addTimeseriesPath("root.laptop.d2.s1", "INT32", "RLE");
    } catch (PathErrorException | StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.deletePath("root.laptop.d1.s0");
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(false, root.isPathExist("root.laptop.d1.s0"));
    try {
      root.deletePath("root.laptop.d1");
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(false, root.isPathExist("root.laptop.d1.s1"));
    assertEquals(false, root.isPathExist("root.laptop.d1"));
    assertEquals(true, root.isPathExist("root.laptop"));
    assertEquals(true, root.isPathExist("root.laptop.d2"));
    assertEquals(true, root.isPathExist("root.laptop.d2.s0"));
  }

  @Test
  public void testCheckStorageGroup() {
    // set storage group first
    MTree root = new MTree("root");
    try {
      assertEquals(false, root.checkStorageGroup("root"));
      assertEquals(false, root.checkStorageGroup("root1.laptop.d2"));

      root.setStorageGroup("root.laptop.d1");
      assertEquals(true, root.checkStorageGroup("root.laptop.d1"));
      assertEquals(false, root.checkStorageGroup("root.laptop.d2"));
      assertEquals(false, root.checkStorageGroup("root.laptop"));
      assertEquals(false, root.checkStorageGroup("root.laptop.d1.s1"));

      root.setStorageGroup("root.laptop.d2");
      assertEquals(true, root.checkStorageGroup("root.laptop.d1"));
      assertEquals(true, root.checkStorageGroup("root.laptop.d2"));
      assertEquals(false, root.checkStorageGroup("root.laptop.d3"));
    } catch (StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {
    // set storage group first
    MTree root = new MTree("root");
    try {
      root.setStorageGroup("root.laptop.d1");
      root.setStorageGroup("root.laptop.d2");
      root.addTimeseriesPath("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.GZIP, null);
      root.addTimeseriesPath("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.GZIP, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, root.getAllFileNamesByPath("root.laptop.d1.s1"));
      assertEquals(list, root.getAllFileNamesByPath("root.laptop.d1"));

      list.add("root.laptop.d2");
      assertEquals(list, root.getAllFileNamesByPath("root.laptop"));
      assertEquals(list, root.getAllFileNamesByPath("root"));
    } catch (PathErrorException | StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    // set storage group first
    MTree root = new MTree("root");
    try {
      assertTrue(root.getAllFileNamesByPath("root").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle.device").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle.device.sensor").isEmpty());

      root.setStorageGroup("root.vehicle");
      assertFalse(root.getAllFileNamesByPath("root.vehicle").isEmpty());
      assertFalse(root.getAllFileNamesByPath("root.vehicle.device").isEmpty());
      assertFalse(root.getAllFileNamesByPath("root.vehicle.device.sensor").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle1").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle1.device").isEmpty());

      root.setStorageGroup("root.vehicle1.device");
      assertTrue(root.getAllFileNamesByPath("root.vehicle1.device1").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle1.device2").isEmpty());
      assertTrue(root.getAllFileNamesByPath("root.vehicle1.device3").isEmpty());
      assertFalse(root.getAllFileNamesByPath("root.vehicle1.device").isEmpty());
    } catch (PathErrorException | StorageGroupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
