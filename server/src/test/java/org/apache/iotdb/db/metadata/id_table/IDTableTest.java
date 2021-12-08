/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */

package org.apache.iotdb.db.metadata.id_table;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IDTableTest {

  private CompressionType compressionType;

  @Before
  public void setUp() {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsert() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      CreateAlignedTimeSeriesPlan plan =
          new CreateAlignedTimeSeriesPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              Arrays.asList("s1", "s2", "s3"),
              Arrays.asList(
                  TSDataType.valueOf("FLOAT"),
                  TSDataType.valueOf("INT64"),
                  TSDataType.valueOf("INT32")),
              Arrays.asList(
                  TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
              Arrays.asList(compressionType, compressionType, compressionType),
              null);

      manager.createAlignedTimeSeriesEntry(plan);

      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};

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

      idTable.getSeriesSchemas(insertRowPlan);

      // with type mismatch
      dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.INT32};
      InsertRowPlan insertRowPlan2 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2", "s3"},
              dataTypes,
              columns,
              true);
      insertRowPlan2.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // we should throw type mismatch exception here
      try {
        IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(false);
        idTable.getSeriesSchemas(insertRowPlan2);
        fail("should throw exception");
      } catch (DataTypeMismatchException e) {
        assertEquals(
            "DataType mismatch, Insert measurement s2 type DOUBLE, metadata tree type INT64",
            e.getMessage());
      } catch (Exception e2) {
        fail("throw wrong exception");
      }

      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertNotAlignedData() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      CreateAlignedTimeSeriesPlan plan =
          new CreateAlignedTimeSeriesPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              Arrays.asList("s1", "s2", "s3"),
              Arrays.asList(
                  TSDataType.valueOf("FLOAT"),
                  TSDataType.valueOf("INT64"),
                  TSDataType.valueOf("INT32")),
              Arrays.asList(
                  TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
              Arrays.asList(compressionType, compressionType, compressionType),
              null);

      manager.createAlignedTimeSeriesEntry(plan);

      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};

      String[] columns = new String[3];
      columns[0] = 2.0 + "";
      columns[1] = 10000 + "";
      columns[2] = 100 + "";

      // non aligned plan
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
      try {
        idTable.getSeriesSchemas(insertRowPlan);
        fail("should throw exception");
      } catch (MetadataException e) {
        assertEquals(
            "Timeseries under path [root.laptop.d1.aligned_device]'s align value is [true], which is not consistent with insert plan",
            e.getMessage());
      } catch (Exception e2) {
        fail("throw wrong exception");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsert() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      long time = 1L;
      String[] columns = new String[1];
      columns[0] = 2 + "";

      // correct insert plan
      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1"),
              time,
              new String[] {"s0"},
              new TSDataType[] {TSDataType.INT32},
              columns);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      idTable.getSeriesSchemas(insertRowPlan);
      assertEquals(insertRowPlan.getMeasurementMNodes()[0].getSchema().getType(), TSDataType.INT32);
      assertEquals(0, insertRowPlan.getFailedMeasurementNumber());

      // construct an insertRowPlan with mismatched data type
      InsertRowPlan insertRowPlan2 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1"),
              time,
              new String[] {"s0"},
              new TSDataType[] {TSDataType.FLOAT},
              columns);
      insertRowPlan2.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // get series schema
      idTable.getSeriesSchemas(insertRowPlan2);
      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithAlignedData() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.non_aligned_device.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.non_aligned_device.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};

      String[] columns = new String[2];
      columns[0] = "1";
      columns[1] = "2";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.non_aligned_device"),
              time,
              new String[] {"s1", "s2"},
              dataTypes,
              columns,
              true);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      idTable.getSeriesSchemas(insertRowPlan);
      fail("should throw exception");
    } catch (MetadataException e) {
      assertEquals(
          "Timeseries under path [root.laptop.d1.non_aligned_device]'s align value is [false], which is not consistent with insert plan",
          e.getMessage());
    } catch (Exception e) {
      fail("throw wrong exception");
    }
  }

  @Test
  public void testInsertAndAutoCreate() {
    MManager manager = IoTDB.metaManager;
    try {
      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};

      String[] columns = new String[2];
      columns[0] = "1";
      columns[1] = "2";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.non_aligned_device"),
              time,
              new String[] {"s1", "s2"},
              dataTypes,
              columns,
              false);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      idTable.getSeriesSchemas(insertRowPlan);

      // check mmanager
      IMeasurementMNode s1Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.non_aligned_device.s1"));
      assertEquals("s1", s1Node.getName());
      assertEquals(TSDataType.INT32, s1Node.getSchema().getType());
      IMeasurementMNode s2Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.non_aligned_device.s2"));
      assertEquals("s2", s2Node.getName());
      assertEquals(TSDataType.INT64, s2Node.getSchema().getType());

      // insert type mismatch data
      InsertRowPlan insertRowPlan2 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.non_aligned_device"),
              time,
              new String[] {"s1", "s2"},
              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
              columns,
              false);
      insertRowPlan2.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      idTable.getSeriesSchemas(insertRowPlan2);

      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
      assertEquals(insertRowPlan.getMeasurementMNodes()[1].getSchema().getType(), TSDataType.INT64);
      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());

      // insert aligned data
      InsertRowPlan insertRowPlan3 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.non_aligned_device"),
              time,
              new String[] {"s1", "s2"},
              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
              columns,
              true);
      insertRowPlan3.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      try {
        idTable.getSeriesSchemas(insertRowPlan3);
        fail("should throw exception");
      } catch (MetadataException e) {
        assertEquals(
            "Timeseries under path [root.laptop.d1.non_aligned_device]'s align value is [false], which is not consistent with insert plan",
            e.getMessage());
      } catch (Exception e) {
        fail("throw wrong exception");
      }
    } catch (MetadataException | StorageEngineException e) {
      e.printStackTrace();
      fail("throw exception");
    }
  }

  @Test
  public void testAlignedInsertAndAutoCreate() {
    MManager manager = IoTDB.metaManager;
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
      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      idTable.getSeriesSchemas(insertRowPlan);

      // check mmanager
      IMeasurementMNode s1Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.aligned_device.s1"));
      assertEquals("s1", s1Node.getName());
      assertEquals(TSDataType.INT32, s1Node.getSchema().getType());
      IMeasurementMNode s2Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.aligned_device.s2"));
      assertEquals("s2", s2Node.getName());
      assertEquals(TSDataType.INT64, s2Node.getSchema().getType());
      assertTrue(s2Node.getParent().isAligned());

      // insert type mismatch data
      InsertRowPlan insertRowPlan2 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2"},
              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
              columns,
              true);
      insertRowPlan2.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      idTable.getSeriesSchemas(insertRowPlan2);

      assertNull(insertRowPlan2.getMeasurementMNodes()[0]);
      assertEquals(insertRowPlan.getMeasurementMNodes()[1].getSchema().getType(), TSDataType.INT64);
      assertEquals(1, insertRowPlan2.getFailedMeasurementNumber());

      // insert non-aligned data
      InsertRowPlan insertRowPlan3 =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.aligned_device"),
              time,
              new String[] {"s1", "s2"},
              new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
              columns,
              false);
      insertRowPlan3.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      try {
        idTable.getSeriesSchemas(insertRowPlan3);
        fail("should throw exception");
      } catch (MetadataException e) {
        assertEquals(
            "Timeseries under path [root.laptop.d1.aligned_device]'s align value is [true], which is not consistent with insert plan",
            e.getMessage());
      } catch (Exception e) {
        fail("throw wrong exception");
      }
    } catch (MetadataException | StorageEngineException e) {
      e.printStackTrace();
      fail("throw exception");
    }
  }

  @Test
  public void testTriggerAndInsert() {
    MManager manager = IoTDB.metaManager;
    try {
      long time = 1L;

      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.non_aligned_device.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.non_aligned_device.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      Planner processor = new Planner();

      String sql =
          "CREATE TRIGGER trigger1 BEFORE INSERT ON root.laptop.d1.non_aligned_device.s1 AS 'org.apache.iotdb.db.metadata.id_table.trigger_example.Counter'";

      CreateTriggerPlan plan = (CreateTriggerPlan) processor.parseSQLToPhysicalPlan(sql);

      TriggerRegistrationService.getInstance().register(plan);

      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};
      String[] columns = new String[2];
      columns[0] = "1";
      columns[1] = "2";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.non_aligned_device"),
              time,
              new String[] {"s1", "s2"},
              dataTypes,
              columns,
              false);
      insertRowPlan.setMeasurementMNodes(
          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      IDTable idTable =
          StorageEngine.getInstance().getProcessor(new PartialPath("root.laptop")).getIdTable();

      idTable.getSeriesSchemas(insertRowPlan);

      // check mmanager
      IMeasurementMNode s1Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.non_aligned_device.s1"));
      assertEquals("s1", s1Node.getName());
      assertEquals(TSDataType.INT32, s1Node.getSchema().getType());
      assertNotNull(s1Node.getTriggerExecutor());

      IMeasurementMNode s2Node =
          manager.getMeasurementMNode(new PartialPath("root.laptop.d1.non_aligned_device.s2"));
      assertEquals("s2", s2Node.getName());
      assertEquals(TSDataType.INT64, s2Node.getSchema().getType());
      assertNull(s2Node.getTriggerExecutor());

    } catch (MetadataException | StorageEngineException | QueryProcessException e) {
      e.printStackTrace();
      fail("throw exception");
    }
  }
}
