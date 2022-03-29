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
package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.rocksdb.RSchemaReadWriteHandler.ROCKSDB_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RSchemaEngineBasicTest {

  private CompressionType compressionType;
  RSchemaEngine schemaEngine;
  private String rocksdbPath;

  @Before
  public void setUp() throws MetadataException {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    rocksdbPath = ROCKSDB_PATH + UUID.randomUUID();
    File file = new File(rocksdbPath);
    if (!file.exists()) {
      file.mkdirs();
    }
    schemaEngine = new RSchemaEngine(rocksdbPath);
  }

  @After
  public void tearDown() throws Exception {
    schemaEngine.deactivate();
    File rockdDbFile = new File(rocksdbPath);
    if (rockdDbFile.exists() && rockdDbFile.isDirectory()) {
      FileUtils.deleteDirectory(rockdDbFile);
    }
  }

  @Test
  public void testAddPathAndExist() throws MetadataException {

    assertTrue(schemaEngine.isPathExist(new PartialPath("root")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop")));

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop.d1"));
      schemaEngine.setStorageGroup(new PartialPath("root.1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(schemaEngine.isPathExist(new PartialPath("root.1")));

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals("MNode [root.laptop] is not a StorageGroupMNode.", e.getMessage());
    }

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      fail();
    } catch (MetadataException e) {
      assertEquals("parent of measurement could only be entity or internal node", e.getMessage());
    }
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.s1")));
    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.d.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.d.1_2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.d.\"1.2.3\""),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      schemaEngine.createTimeseries(
          new PartialPath("root.1.2.3"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());

      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.s1")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.1_2")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.\"1.2.3\"")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.1.2")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.1.2.3")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.s1")));

    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.s0")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root")));

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.d.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.d.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d2")));
    assertFalse(schemaEngine.checkStorageGroupByPath(new PartialPath("root.laptop.d2")));

    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format(
              "The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.1_2"));
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.d.\"1.2.3\""));
      schemaEngine.deleteTimeseries(new PartialPath("root.1.2.3"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.1_2")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.d.\"1.2.3\"")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.1.2.3")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.1.2")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.1")));

    try {
      schemaEngine.deleteStorageGroups(Collections.singletonList(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.1")));

    assertFalse(schemaEngine.isPathExist(new PartialPath("root.template")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.template.d1")));

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.template.d2.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test if the PathNotExistException can be correctly thrown when the path to be deleted does not
   * exist. See {@link SchemaEngine#deleteTimeseries(PartialPath)}.
   */
  @Test
  public void testDeleteNonExistentTimeseries() {
    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.non.existent"));
    } catch (Exception e) {
      fail();
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Test
  public void testCreateAlignedTimeseries() throws MetadataException {
    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaEngine.createAlignedTimeSeries(
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

    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.aligned_device.*"));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    try {
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertFalse(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.s0")));

    try {
      schemaEngine.createAlignedTimeSeries(
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

    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s0")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s4")));
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testGetAllTimeseriesCount() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.**")), 6);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.**")), 6);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 0);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 5);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.*.**")), 6);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 2);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**")), 3);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 2);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.d2.**")), 2);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop")), 0);
      assertEquals(schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1")), 0);

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroupAndExist() {

    try {
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root1.laptop.d2")));

      schemaEngine.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      schemaEngine.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() throws Exception {
    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop.s1"));
      schemaEngine.setStorageGroup(new PartialPath("root.laptop.s2"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.s1.d1.m1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.s2.d1.m1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s1")));
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s2")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s3")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop")));
      Set<String> devices =
          new TreeSet<String>() {
            {
              add("root.laptop.s1.d1");
              add("root.laptop.s2.d1");
            }
          };
      // prefix with *
      assertEquals(
          devices,
          schemaEngine.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      schemaEngine.deleteStorageGroups(
          Collections.singletonList(new PartialPath("root.laptop.s2")));
      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s1")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s2")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s3")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.s2.d1");
      // prefix with *
      assertEquals(
          devices,
          schemaEngine.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      schemaEngine.deactivate();
      schemaEngine = new RSchemaEngine(rocksdbPath);

      assertTrue(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s1")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s2")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop.s3")));
      assertFalse(schemaEngine.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          schemaEngine.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop.s1"));
      schemaEngine.setStorageGroup(new PartialPath("root.laptop.s2"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.s1.d1.m1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.s2.d1.m1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      List<PartialPath> list = new ArrayList<>();

      list.add(new PartialPath("root.laptop.s1"));
      assertEquals(
          list, schemaEngine.getBelongedStorageGroups(new PartialPath("root.laptop.s1.d1.m1")));
      // TODO: fix the case
      //      list.add(new PartialPath("root.laptop.s2"));
      //      assertEquals(list, schemaEngine.getBelongedStorageGroups(new
      //      PartialPath("root.laptop.**")));
      //      assertEquals(list, schemaEngine.getBelongedStorageGroups(new PartialPath("root.**")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {

    try {
      assertTrue(schemaEngine.getMeasurementPaths(new PartialPath("root")).isEmpty());
      assertTrue(schemaEngine.getBelongedStorageGroups(new PartialPath("root")).isEmpty());
      assertTrue(schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(
          schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          schemaEngine
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor"))
              .isEmpty());

      schemaEngine.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(
          schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          schemaEngine
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor"))
              .isEmpty());
      assertTrue(schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(
          schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());

      schemaEngine.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(
          schemaEngine
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device1"))
              .isEmpty());
      assertTrue(
          schemaEngine
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device2"))
              .isEmpty());
      assertTrue(
          schemaEngine
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device3"))
              .isEmpty());
      assertFalse(
          schemaEngine.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowChildNodesWithGivenPrefix() {
    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> nodes = new HashSet<>(Arrays.asList("s1", "s2"));
      Set<String> nodes2 = new HashSet<>(Arrays.asList("laptop"));
      Set<String> nodes3 = new HashSet<>(Arrays.asList("d1", "d2"));
      Set<String> nexLevelNodes1 =
          schemaEngine.getChildNodeNameInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 =
          schemaEngine.getChildNodeNameInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 =
          schemaEngine.getChildNodeNameInNextLevel(new PartialPath("root.laptop"));
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
    try {
      PartialPath path1 = new PartialPath("root.laptop\n");
      try {
        schemaEngine.setStorageGroup(path1);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop\t");
      try {
        schemaEngine.setStorageGroup(path2);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void testCreateTimeseriesWithIllegalName() {
    try {
      PartialPath path1 = new PartialPath("root.laptop.d1\n.s1");
      try {
        schemaEngine.createTimeseries(
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
        schemaEngine.createTimeseries(
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

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
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
          schemaEngine.getMatchedDevices(new PartialPath("root.laptop.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      schemaEngine.setStorageGroup(new PartialPath("root.vehicle"));
      schemaEngine.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(
          devices,
          schemaEngine.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
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
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.setStorageGroup(new PartialPath("root.vehicle"));

      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b1.d2.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.b2.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.vehicle.b1.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.vehicle.b1.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.vehicle.b1.d3.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.vehicle.b2.d0.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(
          res[0],
          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root")).stream()
              .sorted()
              .collect(Collectors.toList())
              .toString());
      assertEquals(
          res[1],
          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.laptop")).stream()
              .sorted()
              .collect(Collectors.toList())
              .toString());
      assertEquals(
          res[2],
          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.laptop.b1")).stream()
              .sorted()
              .collect(Collectors.toList())
              .toString());
      // TODO: support later
      //      assertEquals(
      //          res[3],
      //          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.*")).stream()
      //              .sorted()
      //              .collect(Collectors.toList())
      //              .toString());
      //      assertEquals(
      //          res[4],
      //          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.*.b1")).stream()
      //              .sorted()
      //              .collect(Collectors.toList())
      //              .toString());
      //      assertEquals(
      //          res[5],
      //          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.l*.b1")).stream()
      //              .sorted()
      //              .collect(Collectors.toList())
      //              .toString());
      //      assertEquals(
      //          res[6],
      //          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.v*.*")).stream()
      //              .sorted()
      //              .collect(Collectors.toList())
      //              .toString());
      //      assertEquals(
      //          res[7],
      //          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.l*.b*.*")).stream()
      //              .sorted()
      //              .collect(Collectors.toList())
      //              .toString());
      assertEquals(
          res[8],
          schemaEngine.getChildNodePathInNextLevel(new PartialPath("root.laptopp")).stream()
              .sorted()
              .collect(Collectors.toList())
              .toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // region template related cases region

  // end region

  @Test
  public void testShowTimeseries() {

    try {
      schemaEngine.createTimeseries(
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
          schemaEngine.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTotalSeriesNumber() throws Exception {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(5, schemaEngine.getTotalSeriesNumber());
      schemaEngine.deactivate();
      schemaEngine = new RSchemaEngine(rocksdbPath);
      assertEquals(5, schemaEngine.getTotalSeriesNumber());
      schemaEngine.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      assertEquals(4, schemaEngine.getTotalSeriesNumber());
      schemaEngine.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop")));
      assertEquals(0, schemaEngine.getTotalSeriesNumber());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      fail();
    } catch (MetadataException e) {
      Assert.assertEquals(
          "parent of measurement could only be entity or internal node", e.getMessage());
    }
  }

  @Test
  public void testStorageGroupNameWithHyphen() throws MetadataException {

    assertTrue(schemaEngine.isPathExist(new PartialPath("root")));

    assertFalse(schemaEngine.isPathExist(new PartialPath("root.group-with-hyphen")));

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.group-with-hyphen"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(schemaEngine.isPathExist(new PartialPath("root.group-with-hyphen")));
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithMismatchDataType() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createAlignedTimeSeries(
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
      IMNode node = schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(3, schemaEngine.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithNotAlignedData() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createAlignedTimeSeries(
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
      schemaEngine.createTimeseries(
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
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is aligned , please set InsertPlan.isAligned() = true",
          e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithMismatchDataType() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
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
      IMNode node = schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(1, schemaEngine.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));
      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
      assertEquals(1, insertRowPlan.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithAlignedData() {

    try {
      schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaEngine.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (Exception e) {
      fail();
    }

    try {
      schemaEngine.createAlignedTimeSeries(
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
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is not aligned , please set InsertPlan.isAligned() = false",
          e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesWithIllegalNames() throws Exception {

    schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
    PartialPath deviceId = new PartialPath("root.laptop.d1");
    String[] measurementIds = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String measurementId : measurementIds) {
      PartialPath path = deviceId.concatNode(measurementId);
      try {
        schemaEngine.createAlignedTimeSeries(
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
        schemaEngine.createAlignedTimeSeries(
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

  // TODO: fix the case, right now create aligned timeseries doesn't support alias create
  public void testCreateAlignedTimeseriesWithAliasAndTags() throws Exception {

    schemaEngine.setStorageGroup(new PartialPath("root.laptop"));
    PartialPath devicePath = new PartialPath("root.laptop.device");
    List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4", "s5");
    List<TSDataType> tsDataTypes =
        Arrays.asList(
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.FLOAT,
            TSDataType.BOOLEAN,
            TSDataType.INT32);
    List<TSEncoding> tsEncodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED);
    List<String> aliasList = Arrays.asList("alias1", null, "alias2", null, null);
    List<Map<String, String>> tagList = new ArrayList<>();
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    tagList.add(tags);
    tagList.add(null);
    tagList.add(null);
    tagList.add(tags);
    tagList.add(null);
    CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
        new CreateAlignedTimeSeriesPlan(
            devicePath,
            measurements,
            tsDataTypes,
            tsEncodings,
            compressionTypes,
            aliasList,
            tagList,
            null);
    schemaEngine.createAlignedTimeSeries(createAlignedTimeSeriesPlan);

    schemaEngine.printScanAllKeys();
    Assert.assertEquals(
        5, schemaEngine.getAllTimeseriesCount(new PartialPath("root.laptop.device.*")));
    Assert.assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.device.alias2")));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.**"), false, "key", "value", 0, 0, false);
    List<ShowTimeSeriesResult> showTimeSeriesResults =
        schemaEngine.showTimeseries(showTimeSeriesPlan, null);
    Assert.assertEquals(2, showTimeSeriesResults.size());
    showTimeSeriesResults =
        showTimeSeriesResults.stream()
            .sorted(Comparator.comparing(ShowResult::getName))
            .collect(Collectors.toList());
    ShowTimeSeriesResult result = showTimeSeriesResults.get(0);
    Assert.assertEquals("root.laptop.device.s1", result.getName());
    Assert.assertEquals("alias1", result.getAlias());
    Assert.assertEquals(tags, result.getTag());
    result = showTimeSeriesResults.get(1);
    Assert.assertEquals("root.laptop.device.s4", result.getName());
    Assert.assertEquals(tags, result.getTag());
  }

  @Test
  public void testAutoCreateAlignedTimeseriesWhileInsert() {

    try {
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT32};

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

      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);

      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));

      insertRowPlan.setMeasurements(new String[] {"s3", "s4"});
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s4")));

      insertRowPlan.setMeasurements(new String[] {"s2", "s5"});
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertTrue(schemaEngine.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s5")));

      insertRowPlan.setMeasurements(new String[] {"s2", "s3"});
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertRowPlan);

    } catch (MetadataException | IOException e) {
      fail();
    }
  }

  @Test
  public void testGetStorageGroupNodeByPath() {

    PartialPath partialPath = null;

    try {
      partialPath = new PartialPath("root.ln.sg1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      schemaEngine.setStorageGroup(partialPath);
    } catch (MetadataException e) {
      fail(e.getMessage());
    }

    try {
      partialPath = new PartialPath("root.ln.sg2.device1.sensor1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      schemaEngine.getStorageGroupNodeByPath(partialPath);
    } catch (StorageGroupNotSetException e) {
      Assert.assertEquals(
          "Storage group is not set for current seriesPath: [root.ln.sg2.device1.sensor1]",
          e.getMessage());
    } catch (StorageGroupAlreadySetException e) {
      Assert.assertEquals(
          "some children of root.ln have already been set to storage group", e.getMessage());
    } catch (MNodeTypeMismatchException e) {
      Assert.assertEquals("MNode [root.ln] is not a StorageGroupMNode.", e.getMessage());
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testMeasurementIdWhileInsert() throws Exception {

    PartialPath deviceId = new PartialPath("root.sg.d");
    InsertPlan insertPlan;

    insertPlan = getInsertPlan("\"a+b\"");
    schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaEngine.isPathExist(deviceId.concatNode("\"a+b\"")));

    insertPlan = getInsertPlan("\"a.b\"");
    schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaEngine.isPathExist(deviceId.concatNode("\"a.b\"")));

    insertPlan = getInsertPlan("\"a“（Φ）”b\"");
    schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaEngine.isPathExist(deviceId.concatNode("\"a“（Φ）”b\"")));

    String[] illegalMeasurementIds = {"a.b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String measurementId : illegalMeasurementIds) {
      insertPlan = getInsertPlan(measurementId);
      try {
        schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
        assertFalse(schemaEngine.isPathExist(deviceId.concatNode(measurementId)));
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
  public void testDeviceNodeAfterAutoCreateTimeseriesFailure() throws Exception {

    PartialPath sg1 = new PartialPath("root.a.sg");
    schemaEngine.setStorageGroup(sg1);

    PartialPath deviceId = new PartialPath("root.a.d");
    String[] measurementList = {"s"};
    String[] values = {"1"};
    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);
    insertPlan.getDataTypes()[0] = TSDataType.INT32;

    try {
      schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
      fail();
    } catch (MetadataException e) {
      Assert.assertEquals("MNode [root.a] is not a StorageGroupMNode.", e.getMessage());
      Assert.assertFalse(schemaEngine.isPathExist(new PartialPath("root.a.d")));
    }
  }

  // TODO: enable after tag completed
  public void testTagIndexRecovery() throws Exception {

    PartialPath path = new PartialPath("root.sg.d.s");
    Map<String, String> tags = new HashMap<>();
    tags.put("description", "oldValue");
    schemaEngine.createTimeseries(
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
        schemaEngine.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("oldValue", resultTag.get("description"));

    tags.put("description", "newValue");
    schemaEngine.upsertTagsAndAttributes(null, tags, null, path);

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = schemaEngine.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));

    schemaEngine.clear();
    schemaEngine.init();

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "oldValue", 0, 0, false);
    results = schemaEngine.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(0, results.size());

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = schemaEngine.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));
  }

  // TODO: fix the case
  public void testTagCreationViaMLogPlanDuringMetadataSync() throws Exception {

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

    schemaEngine.operation(plan);

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.sg.d.s"), true, "type", "test", 0, 0, false);
    List<ShowTimeSeriesResult> results =
        schemaEngine.showTimeseries(showTimeSeriesPlan, new QueryContext());
    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("test", resultTag.get("type"));

    assertEquals(0, schemaEngine.getMeasurementMNode(path).getOffset());
  }
}
