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
    compressionType = CompressionType
        .valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
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
      Assert.assertEquals(
          "Path [root.laptop] already exist",
          e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s0", TSDataType.valueOf("INT32"),
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
      manager.addPathToMTree("root.laptop.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    assertTrue(manager.isPathExist("root.laptop.d1.s1"));
    try {
      manager.deletePaths(Collections.singletonList("root.laptop.d1.s1"), false);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    // just delete s0, and don't delete root.laptop.d1??
    // delete storage group or not
    assertFalse(manager.isPathExist("root.laptop.d1.s1"));
    try {
      manager.deletePaths(Collections.singletonList("root.laptop.d1.s0"), false);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist("root.laptop.d1.s0"));
    assertTrue(manager.isPathExist("root.laptop.d1"));
    assertTrue(manager.isPathExist("root.laptop"));
    assertTrue(manager.isPathExist("root"));

    try {
      manager.addPathToMTree("root.laptop.d1.s1", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s0", TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist("root.laptop.d2"));
    assertFalse(manager.checkStorageGroup("root.laptop.d2"));

    try {
      manager.deletePaths(Collections.singletonList("root.laptop.d1.s0"), false);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deletePaths(Collections.singletonList("root.laptop.d1.s1"), false);
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
      manager.addPathToMTree("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.addPathToMTree("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getAllStorageGroupNamesByPath("root.laptop.d1.s1"));
      assertEquals(list, manager.getAllStorageGroupNamesByPath("root.laptop.d1"));

      list.add("root.laptop.d2");
      assertEquals(list, manager.getAllStorageGroupNamesByPath("root.laptop"));
      assertEquals(list, manager.getAllStorageGroupNamesByPath("root"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = MManager.getInstance();

    try {
      assertTrue(manager.getPaths("root").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle.device").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle.device.sensor").isEmpty());

      manager.setStorageGroup("root.vehicle");
      assertFalse(manager.getAllStorageGroupNamesByPath("root.vehicle").isEmpty());
      assertFalse(manager.getAllStorageGroupNamesByPath("root.vehicle.device").isEmpty());
      assertFalse(manager.getAllStorageGroupNamesByPath("root.vehicle.device.sensor").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle1").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle1.device").isEmpty());

      manager.setStorageGroup("root.vehicle1.device");
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle1.device1").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle1.device2").isEmpty());
      assertTrue(manager.getAllStorageGroupNamesByPath("root.vehicle1.device3").isEmpty());
      assertFalse(manager.getAllStorageGroupNamesByPath("root.vehicle1.device").isEmpty());
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
    manager.addPathToMTree("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    manager.addPathToMTree("root.laptop.d1.s2", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.setStorageGroup("root.vehicle");
    manager.addPathToMTree("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.GZIP, null);
    assertEquals(2, manager.getMaximalSeriesNumberAmongStorageGroups());

    manager.deletePaths(Collections.singletonList("root.laptop.d1.s1"), false);
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
    manager.deletePaths(Collections.singletonList("root.laptop.d1.s2"), false);
    assertEquals(1, manager.getMaximalSeriesNumberAmongStorageGroups());
  }

  @Test
  public void testGetStorageGroupNameByAutoLevel() {
    MManager manager = MManager.getInstance();
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    boolean caughtException;

    try {
      assertEquals("root.laptop",
          manager.getStorageGroupNameByAutoLevel("root.laptop.d1.s1", level));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    caughtException = false;
    try {
      manager.getStorageGroupNameByAutoLevel("root1.laptop.d1.s1", level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      manager.getStorageGroupNameByAutoLevel("root", level);
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
      manager.addPathToMTree("root.laptop.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      manager.addPathToMTree("root.laptop.d2.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      List<String> devices = new ArrayList<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(devices, manager.getDevices("root.laptop"));
      manager.setStorageGroup("root.vehicle");
      manager.addPathToMTree("root.vehicle.d1.s1", TSDataType.INT32, TSEncoding.PLAIN,
          CompressionType.GZIP, null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(devices, manager.getDevices("root.*"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
