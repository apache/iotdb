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

package org.apache.iotdb.db.metadata.idtable;
//
// import org.apache.iotdb.commons.exception.MetadataException;
// import org.apache.iotdb.commons.path.PartialPath;
// import org.apache.iotdb.db.conf.IoTDBDescriptor;
// import org.apache.iotdb.db.exception.StorageEngineException;
// import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
// import org.apache.iotdb.db.exception.query.QueryProcessException;
// import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
// import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
// import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
// import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
// import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
// import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
// import org.apache.iotdb.db.qp.Planner;
// import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
// import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
// import org.apache.iotdb.db.service.IoTDB;
// import org.apache.iotdb.db.utils.EnvironmentUtils;
// import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
// import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
// import org.apache.iotdb.tsfile.read.TimeValuePair;
// import org.apache.iotdb.tsfile.utils.Pair;
// import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
//
// import org.junit.After;
// import org.junit.Before;
// import org.junit.Test;
//
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Collection;
// import java.util.Collections;
// import java.util.List;
// import java.util.Set;
//
// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertFalse;
// import static org.junit.Assert.assertNotEquals;
// import static org.junit.Assert.assertNotNull;
// import static org.junit.Assert.assertNull;
// import static org.junit.Assert.assertTrue;
// import static org.junit.Assert.fail;
//
// public class IDTableTest {
//
//  private CompressionType compressionType;
//
//  private boolean isEnableIDTable = false;
//
//  private String originalDeviceIDTransformationMethod = null;
//
//  private boolean isEnableIDTableLogFile = false;
//
//  @Before
//  public void before() {
//    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
//    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
//    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
//    originalDeviceIDTransformationMethod =
//        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
//    isEnableIDTableLogFile = IoTDBDescriptor.getInstance().getConfig().isEnableIDTableLogFile();
//
//    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
//    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
//    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(true);
//    EnvironmentUtils.envSetUp();
//  }
//
//  @After
//  public void clean() throws IOException, StorageEngineException {
//    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
//    IoTDBDescriptor.getInstance()
//        .getConfig()
//        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
//    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
//    EnvironmentUtils.cleanEnv();
//  }
//
//  @Test
//  public void testCreateAlignedTimeseriesAndInsert() {
//    LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
//
//    try {
//      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
//      CreateAlignedTimeSeriesPlan plan =
//          new CreateAlignedTimeSeriesPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              Arrays.asList("s1", "s2", "s3"),
//              Arrays.asList(
//                  TSDataType.valueOf("FLOAT"),
//                  TSDataType.valueOf("INT64"),
//                  TSDataType.valueOf("INT32")),
//              Arrays.asList(
//                  TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"),
// TSEncoding.valueOf("RLE")),
//              Arrays.asList(compressionType, compressionType, compressionType),
//              null,
//              null,
//              null);
//
//      schemaProcessor.createAlignedTimeSeries(plan);
//
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      // construct an insertRowPlan with mismatched data type
//      long time = 1L;
//      TSDataType[] dataTypes =
//          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};
//
//      String[] columns = new String[3];
//      columns[0] = 2.0 + "";
//      columns[1] = 10000 + "";
//      columns[2] = 100 + "";
//
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2", "s3"},
//              dataTypes,
//              columns,
//              true);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      idTable.getSeriesSchemas(insertRowPlan);
//
//      // with type mismatch
//      dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.INT32};
//      InsertRowPlan insertRowPlan2 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2", "s3"},
//              dataTypes,
//              columns,
//              true);
//      insertRowPlan2.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // we should throw type mismatch exception here
//      try {
//        IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(false);
//        idTable.getSeriesSchemas(insertRowPlan2);
//        fail("should throw exception");
//      } catch (DataTypeMismatchException e) {
//        assertEquals(
//            "data type of root.laptop.d1.aligned_device.s2 is not consistent, registered type
// INT64, inserting type DOUBLE, timestamp 1, value 10000.0",
//            e.getMessage());
//      } catch (Exception e2) {
//        fail("throw wrong exception");
//      }
//
//      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail(e.getMessage());
//    }
//  }
//
//  @Test
//  public void testCreateAlignedTimeseriesAndInsertNotAlignedData() {
//    LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
//
//    try {
//      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
//      CreateAlignedTimeSeriesPlan plan =
//          new CreateAlignedTimeSeriesPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              Arrays.asList("s1", "s2", "s3"),
//              Arrays.asList(
//                  TSDataType.valueOf("FLOAT"),
//                  TSDataType.valueOf("INT64"),
//                  TSDataType.valueOf("INT32")),
//              Arrays.asList(
//                  TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"),
// TSEncoding.valueOf("RLE")),
//              Arrays.asList(compressionType, compressionType, compressionType),
//              null,
//              null,
//              null);
//
//      schemaProcessor.createAlignedTimeSeries(plan);
//
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      // construct an insertRowPlan with mismatched data type
//      long time = 1L;
//      TSDataType[] dataTypes =
//          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};
//
//      String[] columns = new String[3];
//      columns[0] = 2.0 + "";
//      columns[1] = 10000 + "";
//      columns[2] = 100 + "";
//
//      // non aligned plan
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2", "s3"},
//              dataTypes,
//              columns,
//              false);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // call getSeriesSchemasAndReadLockDevice
//      try {
//        idTable.getSeriesSchemas(insertRowPlan);
//        fail("should throw exception");
//      } catch (MetadataException e) {
//        assertEquals(
//            "Timeseries under path [root.laptop.d1.aligned_device]'s align value is [true], which
// is not consistent with insert plan",
//            e.getMessage());
//      } catch (Exception e2) {
//        fail("throw wrong exception");
//      }
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail(e.getMessage());
//    }
//  }
//
//  @Test
//  public void testCreateTimeseriesAndInsert() {
//    LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
//    try {
//      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
//      schemaProcessor.createTimeseries(
//          new PartialPath("root.laptop.d1.s0"),
//          TSDataType.valueOf("INT32"),
//          TSEncoding.valueOf("RLE"),
//          compressionType,
//          Collections.emptyMap());
//
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      long time = 1L;
//      String[] columns = new String[1];
//      columns[0] = 2 + "";
//
//      // correct insert plan
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1"),
//              time,
//              new String[] {"s0"},
//              new TSDataType[] {TSDataType.INT32},
//              columns);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      idTable.getSeriesSchemas(insertRowPlan);
//      assertEquals(insertRowPlan.getMeasurementMNodes()[0].getSchema().getType(),
// TSDataType.INT32);
//      assertEquals(0, insertRowPlan.getFailedMeasurementNumber());
//
//      // construct an insertRowPlan with mismatched data type
//      InsertRowPlan insertRowPlan2 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1"),
//              time,
//              new String[] {"s0"},
//              new TSDataType[] {TSDataType.FLOAT},
//              columns);
//      insertRowPlan2.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // get series schema
//      idTable.getSeriesSchemas(insertRowPlan2);
//      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
//      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail(e.getMessage());
//    }
//  }
//
//  @Test
//  public void testCreateTimeseriesAndInsertWithAlignedData() {
//    LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
//    try {
//      schemaProcessor.setStorageGroup(new PartialPath("root.laptop"));
//      schemaProcessor.createTimeseries(
//          new PartialPath("root.laptop.d1.non_aligned_device.s1"),
//          TSDataType.valueOf("INT32"),
//          TSEncoding.valueOf("RLE"),
//          compressionType,
//          Collections.emptyMap());
//      schemaProcessor.createTimeseries(
//          new PartialPath("root.laptop.d1.non_aligned_device.s2"),
//          TSDataType.valueOf("INT64"),
//          TSEncoding.valueOf("RLE"),
//          compressionType,
//          Collections.emptyMap());
//
//      // construct an insertRowPlan with mismatched data type
//      long time = 1L;
//      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};
//
//      String[] columns = new String[2];
//      columns[0] = "1";
//      columns[1] = "2";
//
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.non_aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              dataTypes,
//              columns,
//              true);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // call getSeriesSchemasAndReadLockDevice
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      try {
//        idTable.getSeriesSchemas(insertRowPlan);
//        fail("should throw exception");
//      } catch (MetadataException e) {
//        assertEquals(
//            "Timeseries under path [root.laptop.d1.non_aligned_device]'s align value is [false],
// which is not consistent with insert plan",
//            e.getMessage());
//      }
//    } catch (Exception e) {
//      fail("throw wrong exception");
//    }
//  }
//
//  @Test
//  public void testInsertAndAutoCreate() {
//    LocalSchemaProcessor schemaProcessor = LocalSchemaProcessor.getInstance();
//    try {
//      // construct an insertRowPlan with mismatched data type
//      long time = 1L;
//      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};
//
//      String[] columns = new String[2];
//      columns[0] = "1";
//      columns[1] = "2";
//
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.non_aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              dataTypes,
//              columns,
//              false);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // call getSeriesSchemasAndReadLockDevice
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      idTable.getSeriesSchemas(insertRowPlan);
//
//      // check SchemaProcessor
//      IMeasurementMNode s1Node =
//          schemaProcessor.getMeasurementMNode(
//              new PartialPath("root.laptop.d1.non_aligned_device.s1"));
//      assertEquals("s1", s1Node.getName());
//      assertEquals(TSDataType.INT32, s1Node.getSchema().getType());
//      IMeasurementMNode s2Node =
//          schemaProcessor.getMeasurementMNode(
//              new PartialPath("root.laptop.d1.non_aligned_device.s2"));
//      assertEquals("s2", s2Node.getName());
//      assertEquals(TSDataType.INT64, s2Node.getSchema().getType());
//
//      // insert type mismatch data
//      InsertRowPlan insertRowPlan2 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.non_aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
//              columns,
//              false);
//      insertRowPlan2.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      idTable.getSeriesSchemas(insertRowPlan2);
//
//      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
//      assertEquals(insertRowPlan.getMeasurementMNodes()[1].getSchema().getType(),
// TSDataType.INT64);
//      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());
//
//      // insert aligned data
//      InsertRowPlan insertRowPlan3 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.non_aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
//              columns,
//              true);
//      insertRowPlan3.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      try {
//        idTable.getSeriesSchemas(insertRowPlan3);
//        fail("should throw exception");
//      } catch (MetadataException e) {
//        assertEquals(
//            "Timeseries under path [root.laptop.d1.non_aligned_device]'s align value is [false],
// which is not consistent with insert plan",
//            e.getMessage());
//      } catch (Exception e) {
//        fail("throw wrong exception");
//      }
//    } catch (MetadataException e) {
//      e.printStackTrace();
//      fail("throw exception");
//    }
//  }
//
//  @Test
//  public void testAlignedInsertAndAutoCreate() {
//    LocalSchemaProcessor processor = LocalSchemaProcessor.getInstance();
//    try {
//      // construct an insertRowPlan with mismatched data type
//      long time = 1L;
//      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};
//
//      String[] columns = new String[2];
//      columns[0] = "1";
//      columns[1] = "2";
//
//      InsertRowPlan insertRowPlan =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              dataTypes,
//              columns,
//              true);
//      insertRowPlan.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      // call getSeriesSchemasAndReadLockDevice
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//
//      idTable.getSeriesSchemas(insertRowPlan);
//
//      // check SchemaProcessor
//      IMeasurementMNode s1Node =
//          processor.getMeasurementMNode(new PartialPath("root.laptop.d1.aligned_device.s1"));
//      assertEquals("s1", s1Node.getName());
//      assertEquals(TSDataType.INT32, s1Node.getSchema().getType());
//      IMeasurementMNode s2Node =
//          processor.getMeasurementMNode(new PartialPath("root.laptop.d1.aligned_device.s2"));
//      assertEquals("s2", s2Node.getName());
//      assertEquals(TSDataType.INT64, s2Node.getSchema().getType());
//      assertTrue(s2Node.getParent().isAligned());
//
//      // insert type mismatch data
//      InsertRowPlan insertRowPlan2 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
//              columns,
//              true);
//      insertRowPlan2.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      idTable.getSeriesSchemas(insertRowPlan2);
//
//      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
//      assertEquals(insertRowPlan.getMeasurementMNodes()[1].getSchema().getType(),
// TSDataType.INT64);
//      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());
//
//      // insert non-aligned data
//      InsertRowPlan insertRowPlan3 =
//          new InsertRowPlan(
//              new PartialPath("root.laptop.d1.aligned_device"),
//              time,
//              new String[] {"s1", "s2"},
//              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
//              columns,
//              false);
//      insertRowPlan3.setMeasurementMNodes(
//          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
//
//      try {
//        idTable.getSeriesSchemas(insertRowPlan3);
//        fail("should throw exception");
//      } catch (MetadataException e) {
//        assertEquals(
//            "Timeseries under path [root.laptop.d1.aligned_device]'s align value is [true], which
// is not consistent with insert plan",
//            e.getMessage());
//      } catch (Exception e) {
//        fail("throw wrong exception");
//      }
//    } catch (MetadataException e) {
//      e.printStackTrace();
//      fail("throw exception");
//    }
//  }
//
//  @Test
//  public void testGetDiskSchemaEntries() {
//    try {
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//      String sgPath = "root.laptop";
//      for (int i = 0; i < 10; i++) {
//        String devicePath = sgPath + ".d" + i;
//        IDeviceID iDeviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
//        String measurement = "s" + i;
//        idTable.putSchemaEntry(
//            devicePath,
//            measurement,
//            new SchemaEntry(
//                TSDataType.BOOLEAN,
//                TSEncoding.BITMAP,
//                CompressionType.UNCOMPRESSED,
//                iDeviceID,
//                new PartialPath(devicePath + "." + measurement),
//                false,
//                idTable.getIDiskSchemaManager()),
//            false);
//        SchemaEntry schemaEntry =
//            idTable.getDeviceEntry(iDeviceID.toStringID()).getSchemaEntry(measurement);
//        List<SchemaEntry> schemaEntries = new ArrayList<>();
//        schemaEntries.add(schemaEntry);
//        List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
//        assertNotNull(diskSchemaEntries);
//        assertEquals(diskSchemaEntries.size(), 1);
//        assertEquals(diskSchemaEntries.get(0).seriesKey, devicePath + "." + measurement);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("throw exception");
//    }
//  }
//
//  @Test
//  public void testDeleteTimeseries() {
//    try {
//      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath("root.laptop"));
//      String sgPath = "root.laptop";
//      for (int i = 0; i < 10; i++) {
//        String devicePath = sgPath + ".d" + i;
//        IDeviceID iDeviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
//        String measurement = "s" + i;
//        SchemaEntry schemaEntry =
//            new SchemaEntry(
//                TSDataType.BOOLEAN,
//                TSEncoding.BITMAP,
//                CompressionType.UNCOMPRESSED,
//                iDeviceID,
//                new PartialPath(devicePath + "." + measurement),
//                false,
//                idTable.getIDiskSchemaManager());
//        idTable.putSchemaEntry(devicePath, measurement, schemaEntry, false);
//      }
//      List<PartialPath> partialPaths = new ArrayList<>();
//      partialPaths.add(new PartialPath("root.laptop.d0.s0"));
//      partialPaths.add(new PartialPath("root.laptop.d8.s8"));
//      partialPaths.add(new PartialPath("root.laptop.d2.s3"));
//      Pair<Integer, Set<String>> pairs = idTable.deleteTimeseries(partialPaths);
//      assertNotNull(pairs);
//      assertEquals((int) pairs.left, 2);
//      assertTrue(pairs.right.contains("root.laptop.d2.s3"));
//      assertFalse(pairs.right.contains("root.laptop.d0.s0"));
//      assertFalse(pairs.right.contains("root.laptop.d8.s8"));
//      Collection<DiskSchemaEntry> diskSchemaEntries =
//          idTable.getIDiskSchemaManager().getAllSchemaEntry();
//      for (DiskSchemaEntry diskSchemaEntry : diskSchemaEntries) {
//        assertNotEquals("root.laptop.d0.s0", diskSchemaEntry.seriesKey);
//        assertNotEquals("root.laptop.d8.s8", diskSchemaEntry.seriesKey);
//      }
//      assertNull(idTable.getDeviceEntry("root.laptop.d0").getMeasurementMap().get("s0"));
//      assertNull(idTable.getDeviceEntry("root.laptop.d8").getMeasurementMap().get("s1"));
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("throw exception");
//    }
//  }
// }
