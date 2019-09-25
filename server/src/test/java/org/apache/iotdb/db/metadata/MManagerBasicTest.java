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
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MManagerBasicTest {

  private CompressionType compressionType;

  @Before
  public void setUp() throws Exception {
    compressionType = CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddPathAndExist() {

    MManager manager = MManager.getInstance();
    assertTrue(manager.pathExist("root"));

    assertFalse(manager.pathExist("root.laptop"));

    try {
      manager.setStorageLevelToMTree("root.laptop.d1");
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageLevelToMTree("root.laptop");
    } catch (MetadataErrorException e) {
      Assert.assertEquals(
          "org.apache.iotdb.db.exception.PathErrorException: The seriesPath of"
              + " root.laptop already exist, it can't be set to the storage group",
          e.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections
              .emptyMap());
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.pathExist("root.laptop"));
    assertTrue(manager.pathExist("root.laptop.d1"));
    assertTrue(manager.pathExist("root.laptop.d1.s0"));
    assertFalse(manager.pathExist("root.laptop.d1.s1"));
    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    assertTrue(manager.pathExist("root.laptop.d1.s1"));
    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s1")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    // just delete s0, and don't delete root.laptop.d1??
    // delete storage group or not
    assertFalse(manager.pathExist("root.laptop.d1.s1"));
    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s0")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.pathExist("root.laptop.d1.s0"));
    assertTrue(manager.pathExist("root.laptop.d1"));
    assertTrue(manager.pathExist("root.laptop"));
    assertTrue(manager.pathExist("root"));

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.pathExist("root.laptop.d2"));
    assertFalse(manager.checkFileNameByPath("root.laptop.d2"));

    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s0")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s1")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageLevelToMTree("root.laptop.d2");
    } catch (MetadataErrorException e) {
      Assert.assertEquals(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop.d2"),
          e.getMessage());
    }
    /*
     * check file level
     */
    assertFalse(manager.pathExist("root.laptop.d2.s1"));
    List<Path> paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d2.s1"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d2.s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d2.s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d2.s0")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deletePaths(Collections.singletonList(new Path("root.laptop.d2.s1")));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s0"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s1"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s0"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s2"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s2"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree(new Path("root.laptop.d1.s3"), TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataErrorException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s3"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageLevelAndExist() {

    MManager manager = MManager.getInstance();

    try {
      assertFalse(manager.checkStorageLevelOfMTree("root"));
      assertFalse(manager.checkStorageLevelOfMTree("root1.laptop.d2"));

      manager.setStorageLevelToMTree("root.laptop.d1");
      assertTrue(manager.checkStorageLevelOfMTree("root.laptop.d1"));
      assertFalse(manager.checkStorageLevelOfMTree("root.laptop.d2"));
      assertFalse(manager.checkStorageLevelOfMTree("root.laptop"));
      assertFalse(manager.checkStorageLevelOfMTree("root.laptop.d1.s1"));

      manager.setStorageLevelToMTree("root.laptop.d2");
      assertTrue(manager.checkStorageLevelOfMTree("root.laptop.d1"));
      assertTrue(manager.checkStorageLevelOfMTree("root.laptop.d2"));
      assertFalse(manager.checkStorageLevelOfMTree("root.laptop.d3"));
      assertFalse(manager.checkStorageLevelOfMTree("root.laptop"));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    MManager manager = MManager.getInstance();
    try {
      manager.setStorageLevelToMTree("root.laptop.d1");
      manager.setStorageLevelToMTree("root.laptop.d2");
      manager.addPathToMTree(new Path("root.laptop.d1.s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.addPathToMTree(new Path("root.laptop.d2.s1"), TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getAllFileNamesByPath("root.laptop.d1.s1"));
      assertEquals(list, manager.getAllFileNamesByPath("root.laptop.d1"));

      list.add("root.laptop.d2");
      assertEquals(list, manager.getAllFileNamesByPath("root.laptop"));
      assertEquals(list, manager.getAllFileNamesByPath("root"));
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = MManager.getInstance();

    try {
      assertTrue(manager.getAllPathGroupByFileName("root").keySet().isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle.device").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle.device.sensor").isEmpty());

      manager.setStorageLevelToMTree("root.vehicle");
      assertFalse(manager.getAllFileNamesByPath("root.vehicle").isEmpty());
      assertFalse(manager.getAllFileNamesByPath("root.vehicle.device").isEmpty());
      assertFalse(manager.getAllFileNamesByPath("root.vehicle.device.sensor").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle1").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle1.device").isEmpty());

      manager.setStorageLevelToMTree("root.vehicle1.device");
      assertTrue(manager.getAllFileNamesByPath("root.vehicle1.device1").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle1.device2").isEmpty());
      assertTrue(manager.getAllFileNamesByPath("root.vehicle1.device3").isEmpty());
      assertFalse(manager.getAllFileNamesByPath("root.vehicle1.device").isEmpty());
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaximalSeriesNumberAmongStorageGroup() throws MetadataErrorException {
    MManager manager = MManager.getInstance();
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageLevelToMTree("root.laptop");
    assertEquals(0, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.addPathToMTree("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    manager.addPathToMTree("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageLevelToMTree("root.vehicle");
    manager.addPathToMTree("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());

    manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s1")));
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.deletePaths(Collections.singletonList(new Path("root.laptop.d1.s2")));
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
  }
}
