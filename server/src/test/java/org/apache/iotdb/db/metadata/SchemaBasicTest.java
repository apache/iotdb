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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SchemaBasicTest {

  private CompressionType compressionType;

  @Before
  public void setUp() {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    setConfig();
    EnvironmentUtils.envSetUp();
  }

  protected abstract void setConfig();

  protected abstract void rollBackConfig();

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    rollBackConfig();
  }

  @Test
  public void testAddPathAndExist() throws IllegalPathException {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root")));

    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop")));

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d1"));
      schemaProcessor.setStorageGroup(new PartialPath("root.`1`"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.`1`")));

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been created as database", e.getMessage());
    }

    try {
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s1")));
    try {
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.1_2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.`\"1.2.3\"`"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      schemaProcessor.createTimeseries(
          new PartialPath("root.`1`.`2`.`3`"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());

      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s1")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.1_2")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.`\"1.2.3\"`")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.`1`.`2`")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.`1`.`2`.`3`")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s1")));

    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root")));

    try {
      schemaProcessor.createTimeseries(
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
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d2")));
    assertFalse(schemaProcessor.checkStorageGroupByPath(new PartialPath("root.laptop.d2")));

    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format(
              "The seriesPath of %s already exist, it can't be set to the database",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.1_2"));
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.`\"1.2.3\"`"));
      schemaProcessor.deleteTimeseries(new PartialPath("root.`1`.`2`.`3`"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.1_2")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.`\"1.2.3\"`")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.`1`.`2`.`3`")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.`1`.`2`")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.`1`")));

    try {
      schemaProcessor.deleteStorageGroups(Collections.singletonList(new PartialPath("root.`1`")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.`1`")));
  }

  /**
   * Test if the PathNotExistException can be correctly thrown when the path to be deleted does not
   * exist. See {@link LocalSchemaProcessor#deleteTimeseries(PartialPath)}.
   */
  @Test
  public void testDeleteNonExistentTimeseries() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.non.existent"));
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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaProcessor.createAlignedTimeSeries(
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

    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.aligned_device.*"));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));

    try {
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.s0")));

    try {
      schemaProcessor.createAlignedTimeSeries(
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

    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s0")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s4")));
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testGetAllTimeseriesCount() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.**")), 6);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.**")), 6);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 1);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 4);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.*.**")), 5);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 2);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**")), 3);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 2);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d2.**")), 2);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop")), 0);
      assertEquals(schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1")), 0);

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroupAndExist() {

    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root1.laptop.d2")));

      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() throws Exception {

    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {

      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d1"));
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d2"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop")));
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
          schemaProcessor.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      schemaProcessor.deleteStorageGroups(
          Collections.singletonList(new PartialPath("root.laptop.d2")));
      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.d2");
      // prefix with *
      assertEquals(
          devices,
          schemaProcessor.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      EnvironmentUtils.restartDaemon();

      assertTrue(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(schemaProcessor.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          schemaProcessor.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d1"));
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop.d2"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      List<PartialPath> list = new ArrayList<>();

      list.add(new PartialPath("root.laptop.d1"));
      assertEquals(
          list, schemaProcessor.getBelongedStorageGroups(new PartialPath("root.laptop.d1.s1")));
      assertEquals(
          list, schemaProcessor.getBelongedStorageGroups(new PartialPath("root.laptop.d1")));

      list.add(new PartialPath("root.laptop.d2"));
      assertEquals(
          list, schemaProcessor.getBelongedStorageGroups(new PartialPath("root.laptop.**")));
      assertEquals(list, schemaProcessor.getBelongedStorageGroups(new PartialPath("root.**")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      assertTrue(schemaProcessor.getMeasurementPaths(new PartialPath("root")).isEmpty());
      assertTrue(schemaProcessor.getBelongedStorageGroups(new PartialPath("root")).isEmpty());
      assertTrue(
          schemaProcessor.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device0"))
              .isEmpty());
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device0.sensor"))
              .isEmpty());

      schemaProcessor.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(
          schemaProcessor.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device0"))
              .isEmpty());
      assertFalse(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle.device0.sensor"))
              .isEmpty());
      assertTrue(
          schemaProcessor.getBelongedStorageGroups(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device0"))
              .isEmpty());

      schemaProcessor.setStorageGroup(new PartialPath("root.vehicle1.device0"));
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device1"))
              .isEmpty());
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device2"))
              .isEmpty());
      assertTrue(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device3"))
              .isEmpty());
      assertFalse(
          schemaProcessor
              .getBelongedStorageGroups(new PartialPath("root.vehicle1.device0"))
              .isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowChildNodesWithGivenPrefix() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> nodes = new HashSet<>(Arrays.asList("s1", "s2"));
      Set<String> nodes2 = new HashSet<>(Arrays.asList("laptop"));
      Set<String> nodes3 = new HashSet<>(Arrays.asList("d1", "d2"));
      Set<String> nexLevelNodes1 =
          schemaProcessor.getChildNodeNameInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 =
          schemaProcessor.getChildNodeNameInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 =
          schemaProcessor.getChildNodeNameInNextLevel(new PartialPath("root.laptop"));
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
  public void testGetDevicesWithGivenPrefix() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
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
          schemaProcessor.getMatchedDevices(new PartialPath("root.laptop.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      schemaProcessor.setStorageGroup(new PartialPath("root.vehicle"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(
          devices,
          schemaProcessor.getMatchedDevices(new PartialPath("root.**"), false).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    String[] res =
        new String[] {
          "[TSchemaNode(nodeName:root.laptop, nodeType:2), TSchemaNode(nodeName:root.vehicle, nodeType:2)]",
          "[TSchemaNode(nodeName:root.laptop.b1, nodeType:3), TSchemaNode(nodeName:root.laptop.b2, nodeType:3)]",
          "[TSchemaNode(nodeName:root.laptop.b1.d1, nodeType:4), TSchemaNode(nodeName:root.laptop.b1.d2, nodeType:4)]",
          "[TSchemaNode(nodeName:root.laptop.b1, nodeType:3), TSchemaNode(nodeName:root.laptop.b2, nodeType:3), TSchemaNode(nodeName:root.vehicle.b1, nodeType:3), TSchemaNode(nodeName:root.vehicle.b2, nodeType:3)]",
          "[TSchemaNode(nodeName:root.laptop.b1.d1, nodeType:4), TSchemaNode(nodeName:root.laptop.b1.d2, nodeType:4), TSchemaNode(nodeName:root.vehicle.b1.d0, nodeType:4), TSchemaNode(nodeName:root.vehicle.b1.d2, nodeType:4), TSchemaNode(nodeName:root.vehicle.b1.d3, nodeType:4)]",
          "[TSchemaNode(nodeName:root.laptop.b1.d1, nodeType:4), TSchemaNode(nodeName:root.laptop.b1.d2, nodeType:4)]",
          "[TSchemaNode(nodeName:root.vehicle.b1.d0, nodeType:4), TSchemaNode(nodeName:root.vehicle.b1.d2, nodeType:4), TSchemaNode(nodeName:root.vehicle.b1.d3, nodeType:4), TSchemaNode(nodeName:root.vehicle.b2.d0, nodeType:4)]",
          "[TSchemaNode(nodeName:root.laptop.b1.d1.s0, nodeType:5), TSchemaNode(nodeName:root.laptop.b1.d1.s1, nodeType:5), TSchemaNode(nodeName:root.laptop.b1.d2.s0, nodeType:5), TSchemaNode(nodeName:root.laptop.b2.d1.s1, nodeType:5), TSchemaNode(nodeName:root.laptop.b2.d1.s3, nodeType:5), TSchemaNode(nodeName:root.laptop.b2.d2.s2, nodeType:5)]",
          "[]"
        };

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.setStorageGroup(new PartialPath("root.vehicle"));

      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b1.d2.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.b2.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.vehicle.b1.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.vehicle.b1.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.vehicle.b1.d3.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.vehicle.b2.d0.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(
          res[0], schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root")).toString());
      assertEquals(
          res[1],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.laptop")).toString());
      assertEquals(
          res[2],
          schemaProcessor
              .getChildNodePathInNextLevel(new PartialPath("root.laptop.b1"))
              .toString());
      assertEquals(
          res[3],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.*")).toString());
      assertEquals(
          res[4],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.*.b1")).toString());
      assertEquals(
          res[5],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.l*.b1")).toString());
      assertEquals(
          res[6],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.v*.*")).toString());
      assertEquals(
          res[7],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.l*.b*.*")).toString());
      assertEquals(
          res[8],
          schemaProcessor.getChildNodePathInNextLevel(new PartialPath("root.laptopp")).toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
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
    compressionTypes.add(Collections.singletonList(CompressionType.GZIP));
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
  public void testShowTimeseries() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.createTimeseries(
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
          schemaProcessor.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTotalSeriesNumber() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(6, schemaProcessor.getTotalSeriesNumber());
      EnvironmentUtils.restartDaemon();
      assertEquals(6, schemaProcessor.getTotalSeriesNumber());
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      assertEquals(5, schemaProcessor.getTotalSeriesNumber());
      schemaProcessor.deleteStorageGroups(
          Collections.singletonList(new PartialPath("root.laptop")));
      assertEquals(0, schemaProcessor.getTotalSeriesNumber());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStorageGroupNameWithHyphen() throws IllegalPathException {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root")));

    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.group_with_hyphen")));

    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.group_with_hyphen"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.group_with_hyphen")));
  }

  @Test
  public void testCreateTimeseriesAndInsertWithAlignedData() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.aligned_device.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (Exception e) {
      fail();
    }

    try {
      schemaProcessor.createAlignedTimeSeries(
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
  }

  @Test
  public void testCreateAlignedTimeseriesWithIllegalNames() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
    PartialPath deviceId = new PartialPath("root.laptop.d1");
    String[] measurementIds = {"time", "timestamp", "TIME", "TIMESTAMP"};
    for (String measurementId : measurementIds) {
      PartialPath path = deviceId.concatNode(measurementId);
      try {
        schemaProcessor.createAlignedTimeSeries(
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
        schemaProcessor.createAlignedTimeSeries(
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
  public void testCreateAlignedTimeseriesWithAliasAndTags() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
    PartialPath devicePath = new PartialPath("root.laptop.device0");
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
    schemaProcessor.createAlignedTimeSeries(createAlignedTimeSeriesPlan);

    Assert.assertEquals(
        5, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.device0.*")));
    Assert.assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.device0.alias2")));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.**"), false, "key", "value", 0, 0, false);
    List<ShowTimeSeriesResult> showTimeSeriesResults =
        schemaProcessor.showTimeseries(showTimeSeriesPlan, null);
    Assert.assertEquals(2, showTimeSeriesResults.size());
    showTimeSeriesResults =
        showTimeSeriesResults.stream()
            .sorted(Comparator.comparing(ShowResult::getName))
            .collect(Collectors.toList());
    ShowTimeSeriesResult result = showTimeSeriesResults.get(0);
    Assert.assertEquals("root.laptop.device0.s1", result.getName());
    Assert.assertEquals("alias1", result.getAlias());
    Assert.assertEquals(tags, result.getTag());
    result = showTimeSeriesResults.get(1);
    Assert.assertEquals("root.laptop.device0.s4", result.getName());
    Assert.assertEquals(tags, result.getTag());
  }

  @Test
  public void testGetStorageGroupNodeByPath() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    PartialPath partialPath = null;

    try {
      partialPath = new PartialPath("root.ln.sg1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      schemaProcessor.setStorageGroup(partialPath);
    } catch (MetadataException e) {
      fail(e.getMessage());
    }

    try {
      partialPath = new PartialPath("root.ln.sg2.device1.sensor1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      schemaProcessor.getStorageGroupNodeByPath(partialPath);
    } catch (StorageGroupNotSetException e) {
      Assert.assertEquals(
          "Database is not set for current seriesPath: [root.ln.sg2.device1.sensor1]",
          e.getMessage());
    } catch (StorageGroupAlreadySetException e) {
      Assert.assertEquals(
          "some children of root.ln have already been created as database", e.getMessage());
    } catch (MetadataException e) {
      fail(e.getMessage());
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
  public void testTagIndexRecovery() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    PartialPath path = new PartialPath("root.sg.d.s");
    Map<String, String> tags = new HashMap<>();
    tags.put("description", "oldValue");
    schemaProcessor.createTimeseries(
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
        schemaProcessor.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("oldValue", resultTag.get("description"));

    tags.put("description", "newValue");
    schemaProcessor.upsertTagsAndAttributes(null, tags, null, path);

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = schemaProcessor.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));

    EnvironmentUtils.restartDaemon();

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "oldValue", 0, 0, false);
    results = schemaProcessor.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(0, results.size());

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.sg.d.s"), true, "description", "Value", 0, 0, false);
    results = schemaProcessor.showTimeseries(showTimeSeriesPlan, new QueryContext());

    assertEquals(1, results.size());
    resultTag = results.get(0).getTag();
    assertEquals("newValue", resultTag.get("description"));
  }
}
