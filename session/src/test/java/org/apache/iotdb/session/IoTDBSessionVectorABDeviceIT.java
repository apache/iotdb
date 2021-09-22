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
package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBSessionVectorABDeviceIT {
  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg1.d1.vector1";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;

  @BeforeClass
  public static void setUp() throws Exception {
    CONFIG.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    prepareAlignedTimeseriesData();
    prepareNonAlignedTimeSeriesData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
    CONFIG.setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
  }

  @Test
  @Ignore
  /** Ignore until the tablet interface. */
  public void vectorAlignByDeviceTest() {
    try {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select s1, s2 from root.sg1.d1.vector1 limit 1 align by device");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("vector1.s1", dataSet.getColumnNames().get(2));
      assertEquals("vector1.s2", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getFields().get(0).getLongV());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(2).getLongV());
        assertEquals(3, rowRecord.getFields().get(3).getIntV());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private static void prepareAlignedTimeseriesData()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(
        new VectorMeasurementSchema(
            "vector1",
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT32}));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);
    tablet.setAligned(true);

    for (long row = 1; row <= 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, row);
      tablet.addValue(schemaList.get(0).getSubMeasurementsList().get(0), rowIndex, row + 1);
      tablet.addValue(schemaList.get(0).getSubMeasurementsList().get(1), rowIndex, (int) (row + 2));

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
    session.executeNonQueryStatement("flush");
  }

  private static void prepareNonAlignedTimeSeriesData()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s3");
    measurements.add("s4");
    measurements.add("s5");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 3L);
      values.add(time + 4L);
      values.add(time + 5L);
      session.insertRecord(ROOT_SG1_D1, time, measurements, types, values);
    }
  }
}
