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

import static org.junit.Assert.fail;

import java.util.Arrays;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.id_table.entry.IDeviceID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        fail();
      } catch (Exception e) {

      }

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
      IDeviceID deviceID = idTable.getSeriesSchemas(insertRowPlan);
      // assertEquals(3, manager.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  //  @Test
  //  public void testCreateAlignedTimeseriesAndInsertWithNotAlignedData() {
  //    MManager manager = IoTDB.metaManager;
  //    try {
  //      manager.setStorageGroup(new PartialPath("root.laptop"));
  //      manager.createAlignedTimeSeries(
  //          new PartialPath("root.laptop.d1.aligned_device"),
  //          Arrays.asList("s1", "s2", "s3"),
  //          Arrays.asList(
  //              TSDataType.valueOf("FLOAT"),
  //              TSDataType.valueOf("INT64"),
  //              TSDataType.valueOf("INT32")),
  //          Arrays.asList(
  //              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
  //          Arrays.asList(compressionType, compressionType, compressionType));
  //
  //      // construct an insertRowPlan with mismatched data type
  //      long time = 1L;
  //      TSDataType[] dataTypes =
  //          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};
  //
  //      String[] columns = new String[3];
  //      columns[0] = "1.0";
  //      columns[1] = "2";
  //      columns[2] = "3";
  //
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
  //      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      Assert.assertEquals(
  //          "Timeseries under path [root.laptop.d1.aligned_device] is aligned , please
  // setInsertPlan.isAligned() = true",
  //          e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void testCreateTimeseriesAndInsertWithMismatchDataType() {
  //    MManager manager = IoTDB.metaManager;
  //    try {
  //      manager.setStorageGroup(new PartialPath("root.laptop"));
  //      manager.createTimeseries(
  //          new PartialPath("root.laptop.d1.s0"),
  //          TSDataType.valueOf("INT32"),
  //          TSEncoding.valueOf("RLE"),
  //          compressionType,
  //          Collections.emptyMap());
  //
  //      // construct an insertRowPlan with mismatched data type
  //      long time = 1L;
  //      TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT};
  //
  //      String[] columns = new String[1];
  //      columns[0] = 2.0 + "";
  //
  //      InsertRowPlan insertRowPlan =
  //          new InsertRowPlan(
  //              new PartialPath("root.laptop.d1"), time, new String[] {"s0"}, dataTypes, columns);
  //      insertRowPlan.setMeasurementMNodes(
  //          new IMeasurementMNode[insertRowPlan.getMeasurements().length]);
  //
  //      // call getSeriesSchemasAndReadLockDevice
  //      IMNode node = manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
  //      assertEquals(1, manager.getAllTimeseriesCount(node.getPartialPath().concatNode("**")));
  //      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
  //      assertEquals(1, insertRowPlan.getFailedMeasurementNumber());
  //
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void testCreateTimeseriesAndInsertWithAlignedData() {
  //    MManager manager = IoTDB.metaManager;
  //    try {
  //      manager.setStorageGroup(new PartialPath("root.laptop"));
  //      manager.createTimeseries(
  //          new PartialPath("root.laptop.d1.aligned_device.s1"),
  //          TSDataType.valueOf("INT32"),
  //          TSEncoding.valueOf("RLE"),
  //          compressionType,
  //          Collections.emptyMap());
  //      manager.createTimeseries(
  //          new PartialPath("root.laptop.d1.aligned_device.s2"),
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
  //      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      Assert.assertEquals(
  //          "Timeseries under path [root.laptop.d1.aligned_device] is not aligned , please set
  // InsertPlan.isAligned() = false",
  //          e.getMessage());
  //    }
  //  }
}
