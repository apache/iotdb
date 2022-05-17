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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
          "some children of root.laptop have already been set to storage group", e.getMessage());
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
              "The seriesPath of %s already exist, it can't be set to the storage group",
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

    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.template0")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.template0.d1")));

    try {
      schemaProcessor.createTimeseries(
          new PartialPath("root.template0.d2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      schemaProcessor.createSchemaTemplate(getCreateTemplatePlan());
      schemaProcessor.setSchemaTemplate(new SetTemplatePlan("template1", "root.template0"));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.template0.d1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.template0.d1")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.template0.d1.s11")));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.template0.d2.s11")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.template0.d1.vector")));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.template0.d1.vector.s0")));
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

  @Test
  public void testTemplate() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.createSchemaTemplate(plan);

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    schemaProcessor.setSchemaTemplate(setTemplatePlan);

    schemaProcessor.setUsingSchemaTemplate(
        new ActivateTemplatePlan(new PartialPath("root.sg1.d1")));
    IMNode node = schemaProcessor.getDeviceNode(new PartialPath("root.sg1.d1"));

    MeasurementSchema s11 =
        new MeasurementSchema("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    assertNotNull(node.getSchemaTemplate());

    Set<String> allSchema = new HashSet<>();
    for (IMeasurementSchema schema : node.getSchemaTemplate().getSchemaMap().values()) {
      allSchema.add(
          "root.sg1.d1.vector" + TsFileConstant.PATH_SEPARATOR + schema.getMeasurementId());
    }
    for (MeasurementPath measurementPath :
        schemaProcessor.getMeasurementPaths(new PartialPath("root.sg1.**"))) {
      allSchema.remove(measurementPath.toString());
    }
    allSchema.remove("root.sg1.d1.vector.s11");
    assertTrue(allSchema.isEmpty());

    IMeasurementMNode mNode =
        schemaProcessor.getMeasurementMNode(new PartialPath("root.sg1.d1.s11"));
    IMeasurementMNode mNode2 =
        schemaProcessor.getMeasurementMNode(new PartialPath("root.sg1.d1.vector.s2"));
    assertNotNull(mNode);
    assertEquals(mNode.getSchema(), s11);
    assertNotNull(mNode2);
    assertEquals(
        mNode2.getSchema(),
        schemaProcessor.getTemplate("template1").getSchemaMap().get("vector.s2"));

    try {
      schemaProcessor.getMeasurementMNode(new PartialPath("root.sg1.d1.s100"));
      fail();
    } catch (PathNotExistException e) {
      assertEquals("Path [root.sg1.d1.s100] does not exist", e.getMessage());
    }
  }

  @Test
  public void testTemplateWithUnsupportedTypeEncoding() throws MetadataException {
    CreateTemplatePlan plan;
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("d1.s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Arrays.asList("GPS.x", "GPS.y"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Arrays.asList(TSDataType.TEXT, TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SDT));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));

    try {
      plan =
          new CreateTemplatePlan(
              "treeTemplate", measurementList, dataTypeList, encodingList, compressionTypes);
      IoTDB.schemaProcessor.createSchemaTemplate(plan);
    } catch (MetadataException e) {
      assertEquals("encoding RLE does not support TEXT", e.getMessage());
    }

    dataTypeList.get(2).set(0, TSDataType.FLOAT);
    CreateTemplatePlan planb =
        new CreateTemplatePlan(
            "treeTemplate", measurementList, dataTypeList, encodingList, compressionTypes);

    IoTDB.schemaProcessor.createSchemaTemplate(planb);
    Template template = IoTDB.schemaProcessor.getTemplate("treeTemplate");
    assertEquals("[d1.s1, GPS.x, GPS.y, s2]", template.getAllMeasurementsPaths().toString());

    List<String> appendMeasurements = Arrays.asList("a1", "a2");
    List<TSDataType> appendDataTypes = Arrays.asList(TSDataType.TEXT, TSDataType.FLOAT);
    List<TSEncoding> appendEncodings = Arrays.asList(TSEncoding.RLE, TSEncoding.RLE);
    List<CompressionType> appendCompressor =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4);
    AppendTemplatePlan plana =
        new AppendTemplatePlan(
            "treeTemplate",
            false,
            appendMeasurements,
            appendDataTypes,
            appendEncodings,
            appendCompressor);
    try {
      IoTDB.schemaProcessor.appendSchemaTemplate(plana);
    } catch (MetadataException e) {
      assertEquals("encoding RLE does not support TEXT", e.getMessage());
    }

    appendDataTypes.set(0, TSDataType.FLOAT);
    AppendTemplatePlan planab =
        new AppendTemplatePlan(
            "treeTemplate",
            false,
            appendMeasurements,
            appendDataTypes,
            appendEncodings,
            appendCompressor);
    IoTDB.schemaProcessor.appendSchemaTemplate(planab);
    assertEquals(
        "[a1, a2, d1.s1, GPS.x, GPS.y, s2]", template.getAllMeasurementsPaths().toString());
  }

  @Test
  public void testTemplateInnerTree() throws MetadataException {
    CreateTemplatePlan plan = getTreeTemplatePlan();
    Template template;
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    schemaProcessor.createSchemaTemplate(plan);
    template = schemaProcessor.getTemplate("treeTemplate");
    assertEquals(4, template.getMeasurementsCount());
    assertEquals("d1", template.getPathNodeInTemplate("d1").getName());
    assertNull(template.getPathNodeInTemplate("notExists"));
    assertEquals("[GPS]", template.getAllAlignedPrefix().toString());

    String[] alignedMeasurements = {"`to`.be.prefix.s1", "`to`.be.prefix.s2"};
    TSDataType[] dataTypes = {TSDataType.INT32, TSDataType.INT32};
    TSEncoding[] encodings = {TSEncoding.RLE, TSEncoding.RLE};
    CompressionType[] compressionTypes = {CompressionType.SNAPPY, CompressionType.SNAPPY};
    template.addAlignedMeasurements(alignedMeasurements, dataTypes, encodings, compressionTypes);

    assertEquals("[to.be.prefix, GPS]", template.getAllAlignedPrefix().toString());
    assertEquals("[s1, s2]", template.getAlignedMeasurements("`to`.be.prefix").toString());

    template.deleteAlignedPrefix("`to`.be.prefix");

    assertEquals("[GPS]", template.getAllAlignedPrefix().toString());
    assertEquals(null, template.getDirectNode("prefix"));
    assertEquals("to", template.getDirectNode("to").getName());

    assertFalse(template.isDirectAligned());
    try {
      template.addAlignedMeasurements(
          new String[] {"speed", "temperature"}, dataTypes, encodings, compressionTypes);
      fail();
    } catch (Exception e) {
      assertEquals(
          e.getMessage(), " is not a legal path, because path already exists but not aligned");
    }
    assertFalse(template.isDirectAligned());

    try {
      template.deleteMeasurements("a.single");
      fail();
    } catch (PathNotExistException e) {
      assertEquals("Path [a.single] does not exist", e.getMessage());
    }
    assertEquals(
        "[d1.s1, GPS.x, to.be.prefix.s2, GPS.y, to.be.prefix.s1, s2]",
        template.getAllMeasurementsPaths().toString());

    template.deleteSeriesCascade("`to`");

    assertEquals("[d1.s1, GPS.x, GPS.y, s2]", template.getAllMeasurementsPaths().toString());
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
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg.`1`");
    UnsetTemplatePlan unsetTemplatePlan = new UnsetTemplatePlan("root.sg.`1`", "template1");
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.createSchemaTemplate(createTemplatePlan);

    // path does not exist test
    try {
      schemaProcessor.unsetSchemaTemplate(unsetTemplatePlan);
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("Path [root.sg.`1`] does not exist", e.getMessage());
    }

    schemaProcessor.setSchemaTemplate(setTemplatePlan);

    // template unset test
    schemaProcessor.unsetSchemaTemplate(unsetTemplatePlan);
    schemaProcessor.setSchemaTemplate(setTemplatePlan);

    // no template on path test
    schemaProcessor.unsetSchemaTemplate(unsetTemplatePlan);
    try {
      schemaProcessor.unsetSchemaTemplate(unsetTemplatePlan);
      fail("No exception thrown.");
    } catch (Exception e) {
      assertEquals("NO template on root.sg.`1`", e.getMessage());
    }
  }

  @Test
  public void testTemplateAndTimeSeriesCompatibility() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.createSchemaTemplate(plan);
    schemaProcessor.createSchemaTemplate(getTreeTemplatePlan());

    // set device template
    SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg1.d1");

    schemaProcessor.setSchemaTemplate(setTemplatePlan);
    schemaProcessor.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.sg0"));

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

    schemaProcessor.createTimeseries(createTimeSeriesPlan);

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
      schemaProcessor.createTimeseries(createTimeSeriesPlan2);
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
      schemaProcessor.createTimeseries(createTimeSeriesPlan3);
      fail();
    } catch (Exception e) {
      assertEquals(
          "Path [root.tree.sg0.GPS] overlaps with [treeTemplate] on [GPS]", e.getMessage());
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

    schemaProcessor.createTimeseries(createTimeSeriesPlan4);
  }

  @Test
  public void testTemplateAndNodePathCompatibility() throws MetadataException {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    CreateTemplatePlan plan = getCreateTemplatePlan();
    schemaProcessor.createSchemaTemplate(plan);
    schemaProcessor.createSchemaTemplate(getTreeTemplatePlan());

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

    schemaProcessor.createTimeseries(createTimeSeriesPlan);

    schemaProcessor.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg0.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    schemaProcessor.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg1.dn.sn"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    schemaProcessor.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg2.dn.sn"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    schemaProcessor.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.tree.sg3.dn.sn"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));

    try {
      SetTemplatePlan planErr = new SetTemplatePlan("treeTemplate", "root.tree.*");
      fail();
    } catch (IllegalPathException e) {
      assertEquals(
          "root.tree.* is not a legal path, because template cannot be set on a path with wildcard.",
          e.getMessage());
    }

    SetTemplatePlan planEx1 = new SetTemplatePlan("treeTemplate", "root.tree.sg1");
    SetTemplatePlan planEx2 = new SetTemplatePlan("treeTemplate", "root.tree.sg2");
    SetTemplatePlan planEx3 = new SetTemplatePlan("treeTemplate", "root.tree.sg3");
    schemaProcessor.setSchemaTemplate(planEx1);
    schemaProcessor.setSchemaTemplate(planEx2);
    schemaProcessor.setSchemaTemplate(planEx3);

    try {
      schemaProcessor.unsetSchemaTemplate(new UnsetTemplatePlan("root.tree.*", "treeTemplate"));
      fail();
    } catch (IllegalPathException e) {
      assertEquals(
          "root.tree.* is not a legal path, because template cannot be unset on a path with wildcard.",
          e.getMessage());
    }

    schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan2);
    schemaProcessor.unsetSchemaTemplate(new UnsetTemplatePlan("root.tree.sg0", "treeTemplate"));
    try {
      schemaProcessor.setSchemaTemplate(setTemplatePlan);
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Node name s11 in template has conflict with node's child root.sg1.d1.s11",
          e.getMessage());
    }

    schemaProcessor.createTimeseries(
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
      schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan2);
      fail();
    } catch (MetadataException e) {
      assertEquals(
          "Node name GPS in template has conflict with node's child root.tree.sg0.GPS",
          e.getMessage());
    }

    schemaProcessor.deleteTimeseries(new PartialPath("root.sg1.d1.s11"));
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

    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    schemaProcessor.createSchemaTemplate(plan1);
    schemaProcessor.createSchemaTemplate(plan2);

    schemaProcessor.setStorageGroup(new PartialPath("root.sg1"));
    schemaProcessor.setStorageGroup(new PartialPath("root.sg2"));
    schemaProcessor.setStorageGroup(new PartialPath("root.sg3"));

    try {
      schemaProcessor.setSchemaTemplate(setPlan1);
      schemaProcessor.setSchemaTemplate(setPlan2);
    } catch (MetadataException e) {
      fail();
    }

    try {
      schemaProcessor.setSchemaTemplate(setPlan3);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }

    try {
      schemaProcessor.setSchemaTemplate(setPlan4);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg2.d1", e.getMessage());
    }

    try {
      schemaProcessor.setSchemaTemplate(setPlan5);
      fail();
    } catch (MetadataException e) {
      assertEquals("Template already exists on root.sg1", e.getMessage());
    }
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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.createSchemaTemplate(plan);
      schemaProcessor.createSchemaTemplate(treePlan);

      // set device template

      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      SetTemplatePlan setSchemaTemplatePlan1 = new SetTemplatePlan("treeTemplate", "root.tree.d0");
      schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan);
      schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan1);
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.laptop.d1")));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.tree.d0")));

      // show timeseries root.tree.d0
      ShowTimeSeriesPlan showTreeTSPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.tree.d0.**"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> treeShowResult =
          schemaProcessor.showTimeseries(showTreeTSPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
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
          schemaProcessor.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());

      // show timeseries root.laptop.d1.(s1,s2,s3)
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.vector.s1"), false, null, null, 0, 0, false);
      result =
          schemaProcessor.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.vector.s1", result.get(0).getName());

      // show timeseries root.laptop.d1.(s1,s2,s3)
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(new PartialPath("root.laptop.**"), false, null, null, 0, 0, false);
      result =
          schemaProcessor.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
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
  }

  @Test
  public void minimumTestForWildcardInTemplate() throws MetadataException {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    CreateTemplatePlan treePlan = getTreeTemplatePlan();
    schemaProcessor.createSchemaTemplate(treePlan);

    // set device template
    SetTemplatePlan setSchemaTemplatePlan1 = new SetTemplatePlan("treeTemplate", "root.tree.d0");
    schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan1);
    schemaProcessor.setUsingSchemaTemplate(
        new ActivateTemplatePlan(new PartialPath("root.tree.d0")));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.tree.**.s1"), false, null, null, 0, 0, false);
    List<ShowTimeSeriesResult> result =
        schemaProcessor.showTimeseries(showTimeSeriesPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.createSchemaTemplate(plan);
      schemaProcessor.createSchemaTemplate(getTreeTemplatePlan());

      // set device template
      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan);
      schemaProcessor.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.d0"));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.laptop.d1")));

      schemaProcessor.createTimeseries(
          new PartialPath("root.computer.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.computer");
      schemaProcessor.setSchemaTemplate(setTemplatePlan);
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.computer.d1")));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.tree.d0")));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.tree.d0.v0")));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.tree.d0.v1")));

      Assert.assertEquals(
          2, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d1.**")));
      Assert.assertEquals(
          1, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.laptop.d1.s1")));
      Assert.assertEquals(
          1, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.computer.d1.s1")));
      Assert.assertEquals(
          1, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.computer.d1.s2")));
      Assert.assertEquals(
          3, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.computer.d1.**")));
      Assert.assertEquals(
          3, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.computer.**")));
      Assert.assertEquals(
          12, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.tree.**")));
      Assert.assertEquals(17, schemaProcessor.getAllTimeseriesCount(new PartialPath("root.**")));

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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    try {
      schemaProcessor.createSchemaTemplate(plan);
      schemaProcessor.createSchemaTemplate(getTreeTemplatePlan());
      // set device template
      SetTemplatePlan setSchemaTemplatePlan = new SetTemplatePlan("template1", "root.laptop.d1");
      schemaProcessor.setSchemaTemplate(setSchemaTemplatePlan);
      schemaProcessor.setSchemaTemplate(new SetTemplatePlan("treeTemplate", "root.tree.d0"));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.laptop.d1")));
      schemaProcessor.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.tree.d0")));

      try {
        schemaProcessor.setUsingSchemaTemplate(
            new ActivateTemplatePlan(new PartialPath("root.non.existed.path")));
        fail();
      } catch (MetadataException e) {
        assertEquals("Path [root.non.existed.path] has not been set any template.", e.getMessage());
      }

      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(1, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.d2")));
      Assert.assertEquals(2, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.*")));
      Assert.assertEquals(2, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.**")));
      Assert.assertEquals(3, schemaProcessor.getDevicesNum(new PartialPath("root.tree.**")));
      Assert.assertEquals(5, schemaProcessor.getDevicesNum(new PartialPath("root.**")));

      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d1.a.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      schemaProcessor.createTimeseries(
          new PartialPath("root.laptop.d2.a.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(4, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.**")));

      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      Assert.assertEquals(3, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.**")));
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d2.a.s3"));
      Assert.assertEquals(2, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.**")));
      schemaProcessor.deleteTimeseries(new PartialPath("root.laptop.d1.a.s3"));
      Assert.assertEquals(1, schemaProcessor.getDevicesNum(new PartialPath("root.laptop.**")));

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
  public void testCreateAlignedTimeseriesAndInsertWithMismatchDataType() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createAlignedTimeSeries(
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
      IMNode node = schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(
          3, schemaProcessor.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithNotAlignedData() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createAlignedTimeSeries(
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
      schemaProcessor.createTimeseries(
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
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is aligned , please set InsertPlan.isAligned() = true",
          e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithMismatchDataType() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    try {
      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
      schemaProcessor.createTimeseries(
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
      IMNode node = schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(
          1, schemaProcessor.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));
      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
      assertEquals(1, insertRowPlan.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
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
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(
          "Timeseries under path [root.laptop.d1.aligned_device] is not aligned , please set InsertPlan.isAligned() = false",
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
  public void testAutoCreateAlignedTimeseriesWhileInsert() {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

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

      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);

      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s1")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s2")));

      insertRowPlan.setMeasurements(new String[] {"s3", "s4"});
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s3")));
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s4")));

      insertRowPlan.setMeasurements(new String[] {"s2", "s5"});
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertTrue(schemaProcessor.isPathExist(new PartialPath("root.laptop.d1.aligned_device.s5")));

      insertRowPlan.setMeasurements(new String[] {"s2", "s3"});
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertRowPlan);

    } catch (MetadataException | IOException e) {
      fail();
    }
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
          "Storage group is not set for current seriesPath: [root.ln.sg2.device1.sensor1]",
          e.getMessage());
    } catch (StorageGroupAlreadySetException e) {
      Assert.assertEquals(
          "some children of root.ln have already been set to storage group", e.getMessage());
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testMeasurementIdWhileInsert() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    PartialPath deviceId = new PartialPath("root.sg.d");
    InsertPlan insertPlan;

    insertPlan = getInsertPlan("`\"a+b\"`");
    schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaProcessor.isPathExist(deviceId.concatNode("`\"a+b\"`")));

    insertPlan = getInsertPlan("`\"a.b\"`");
    schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaProcessor.isPathExist(deviceId.concatNode("`\"a.b\"`")));

    insertPlan = getInsertPlan("`\"a“（Φ）”b\"`");
    schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
    assertTrue(schemaProcessor.isPathExist(deviceId.concatNode("`\"a“（Φ）”b\"`")));

    String[] illegalMeasurementIds = {"time", "timestamp", "TIME", "TIMESTAMP"};
    for (String measurementId : illegalMeasurementIds) {
      insertPlan = getInsertPlan(measurementId);
      try {
        schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
        assertFalse(schemaProcessor.isPathExist(deviceId.concatNode(measurementId)));
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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    String[] illegalSchemaNames = {"a+b", "time", "timestamp", "TIME", "TIMESTAMP"};
    for (String schemaName : illegalSchemaNames) {
      CreateTemplatePlan plan = getCreateTemplatePlan(schemaName);
      try {
        schemaProcessor.createSchemaTemplate(plan);
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
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

    PartialPath sg1 = new PartialPath("root.a.sg");
    schemaProcessor.setStorageGroup(sg1);

    PartialPath deviceId = new PartialPath("root.a.d");
    String[] measurementList = {"s"};
    String[] values = {"1"};
    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[1];
    InsertPlan insertPlan = new InsertRowPlan(deviceId, 1L, measurementList, values);
    insertPlan.setMeasurementMNodes(measurementMNodes);
    insertPlan.getDataTypes()[0] = TSDataType.INT32;

    try {
      schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
      fail();
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.a have already been set to storage group", e.getMessage());
      Assert.assertFalse(schemaProcessor.isPathExist(new PartialPath("root.a.d")));
    }
  }

  @Test
  public void testTimeseriesDeletionWithEntityUsingTemplate() throws MetadataException {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.setStorageGroup(new PartialPath("root.sg"));

    CreateTemplatePlan plan = getCreateTemplatePlan("s1");
    schemaProcessor.createSchemaTemplate(plan);
    SetTemplatePlan setPlan = new SetTemplatePlan("template1", "root.sg.d1");
    schemaProcessor.setSchemaTemplate(setPlan);
    schemaProcessor.createTimeseries(
        new PartialPath("root.sg.d1.s2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.setUsingSchemaTemplate(new ActivateTemplatePlan(new PartialPath("root.sg.d1")));
    schemaProcessor.deleteTimeseries(new PartialPath("root.sg.d1.s2"));
    assertTrue(schemaProcessor.isPathExist(new PartialPath("root.sg.d1")));

    schemaProcessor.createTimeseries(
        new PartialPath("root.sg.d2.s2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.deleteTimeseries(new PartialPath("root.sg.d2.s2"));
    assertFalse(schemaProcessor.isPathExist(new PartialPath("root.sg.d2")));
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

  @Test
  public void testTagCreationViaMLogPlanDuringMetadataSync() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

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

    schemaProcessor.operation(plan);

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.sg.d.s"), true, "type", "test", 0, 0, false);
    List<ShowTimeSeriesResult> results =
        schemaProcessor.showTimeseries(showTimeSeriesPlan, new QueryContext());
    assertEquals(1, results.size());
    Map<String, String> resultTag = results.get(0).getTag();
    assertEquals("test", resultTag.get("type"));

    assertEquals(0, schemaProcessor.getMeasurementMNode(path).getOffset());
  }

  @Test
  public void testCountNodesWithLevel() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.createTimeseries(
        new PartialPath("root.sgcc.wf03.wt01.temperature"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.createTimeseries(
        new PartialPath("root.sgcc.wf03.t01.status"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.createTimeseries(
        new PartialPath("root.ln.wf01.wt01.temperature"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.createTimeseries(
        new PartialPath("root.ln.wf01.wt01.status"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.createTimeseries(
        new PartialPath("root.ln.wf02.wt02.status"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    schemaProcessor.createTimeseries(
        new PartialPath("root.ln.wf02.wt02.hardware"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());

    Assert.assertEquals(
        2, schemaProcessor.getNodesCountInGivenLevel(new PartialPath("root.**.temperature"), 3));
  }
}
