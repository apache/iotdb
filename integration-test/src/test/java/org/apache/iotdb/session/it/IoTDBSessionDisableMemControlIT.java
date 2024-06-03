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
package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
@Ignore
public class IoTDBSessionDisableMemControlIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSessionDisableMemControlIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableMemControl(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void insertPartialTabletTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!session.checkTimeseriesExists("root.sg.d.s1")) {
        session.createTimeseries(
            "root.sg.d.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      }
      if (!session.checkTimeseriesExists("root.sg.d.s2")) {
        session.createTimeseries(
            "root.sg.d.s2", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
      }
      if (!session.checkTimeseriesExists("root.sg.d.s3")) {
        session.createTimeseries(
            "root.sg.d.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      }
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 1D);
        tablet.addValue("s3", rowIndex, new Binary("1", TSFileConfig.STRING_CHARSET));
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          try {
            session.insertTablet(tablet, true);
          } catch (StatementExecutionException e) {
            LOGGER.error(e.getMessage());
            assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
            assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
          }
          tablet.reset();
        }
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        try {
          session.insertTablet(tablet);
        } catch (StatementExecutionException e) {
          LOGGER.error(e.getMessage());
          assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
          assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
        }
        tablet.reset();
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1), count(s2), count(s3) from root.sg.d");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(15L, rowRecord.getFields().get(0).getLongV());
        assertEquals(15L, rowRecord.getFields().get(1).getLongV());
        assertEquals(0L, rowRecord.getFields().get(2).getLongV());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertPartialAlignedTabletTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      List<String> multiMeasurementComponents = new ArrayList<>();
      for (int i = 1; i <= 3; i++) {
        multiMeasurementComponents.add("s" + i);
      }
      List<TSDataType> dataTypes = new ArrayList<>();
      dataTypes.add(TSDataType.INT64);
      dataTypes.add(TSDataType.DOUBLE);
      dataTypes.add(TSDataType.INT64);
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (int i = 1; i <= 3; i++) {
        encodings.add(TSEncoding.PLAIN);
        compressors.add(CompressionType.SNAPPY);
      }
      session.createAlignedTimeseries(
          "root.sg.d",
          multiMeasurementComponents,
          dataTypes,
          encodings,
          compressors,
          null,
          null,
          null);
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

      Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

      long timestamp = System.currentTimeMillis();

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 1D);
        tablet.addValue("s3", rowIndex, new Binary("1", TSFileConfig.STRING_CHARSET));
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          try {
            session.insertAlignedTablet(tablet, true);
          } catch (StatementExecutionException e) {
            LOGGER.error(e.getMessage());
            assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
            assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
          }
          tablet.reset();
        }
        timestamp++;
      }

      if (tablet.rowSize != 0) {
        try {
          session.insertAlignedTablet(tablet);
        } catch (StatementExecutionException e) {
          LOGGER.error(e.getMessage());
          assertTrue(e.getMessage().contains("insert measurements [s3] caused by"));
          assertTrue(e.getMessage().contains("data type of root.sg.d.s3 is not consistent"));
        }
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select s1, s2, s3 from root.sg.d");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getFields().get(0).getLongV());
        assertEquals(1.0, rowRecord.getFields().get(1).getDoubleV(), 0.01);
        assertNull(rowRecord.getFields().get(2).getObjectValue(TSDataType.TEXT));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
