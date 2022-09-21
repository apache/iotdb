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

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionAlignedABDeviceIT {

  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final String ROOT_SG1_D2 = "root.sg1.d2";
  private static int originCompactionThreadNum;

  private static void createAlignedTimeseries(ISession session)
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      measurements.add("s" + i);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT64);
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      encodings.add(TSEncoding.RLE);
      compressors.add(CompressionType.SNAPPY);
    }
    session.createAlignedTimeseries(
        ROOT_SG1_D1, measurements, dataTypes, encodings, compressors, null, null, null);
  }

  private static void prepareAlignedTimeSeriesData(ISession session)
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 1);
      values.add(time + 2);
      session.insertAlignedRecord(ROOT_SG1_D1, time, measurements, types, values);
    }
  }

  private static void prepareNonAlignedTimeSeriesData(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 1; time <= 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time + 3L);
      values.add(time + 4L);
      values.add(time + 5L);
      session.insertRecord(ROOT_SG1_D2, time, measurements, types, values);
    }
  }

  @Before
  public void setUp() throws Exception {
    originCompactionThreadNum = ConfigFactory.getConfig().getConcurrentCompactionThread();
    ConfigFactory.getConfig().setConcurrentCompactionThread(0);
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setConcurrentCompactionThread(originCompactionThreadNum);
  }

  @Test
  public void subMeasurementAlignByDeviceTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createAlignedTimeseries(session);
      prepareAlignedTimeSeriesData(session);
      prepareNonAlignedTimeSeriesData(session);

      SessionDataSet dataSet =
          session.executeQueryStatement("select s1, s2 from root.sg1.d1 limit 1 align by device");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("s1", dataSet.getColumnNames().get(2));
      assertEquals("s2", dataSet.getColumnNames().get(3));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getTimestamp());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(1).getLongV());
        assertEquals(3, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignByDeviceTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createAlignedTimeseries(session);
      prepareAlignedTimeSeriesData(session);
      prepareNonAlignedTimeSeriesData(session);

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from root.sg1.d1 limit 1 align by device");
      assertEquals(4, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("s1", dataSet.getColumnNames().get(2));
      assertEquals("s2", dataSet.getColumnNames().get(3));

      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(1, rowRecord.getTimestamp());
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(2, rowRecord.getFields().get(1).getLongV());
        assertEquals(3, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  /* Ignore until the tablet interface. */
  public void alignByDeviceWithWildcardTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createAlignedTimeseries(session);
      prepareAlignedTimeSeriesData(session);
      prepareNonAlignedTimeSeriesData(session);

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from root.sg1.* limit 1 align by device");
      assertEquals(5, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals("Device", dataSet.getColumnNames().get(1));
      assertEquals("s1", dataSet.getColumnNames().get(2));
      assertEquals("s2", dataSet.getColumnNames().get(3));

      long time = 1L;
      String device = "root.sg1.d1";
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(time, rowRecord.getTimestamp());
        assertEquals(device, rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(time + 1, rowRecord.getFields().get(1).getLongV());
        assertEquals(time + 2, rowRecord.getFields().get(2).getLongV());
        assertEquals("null", rowRecord.getFields().get(3).getStringValue());
        dataSet.next();
      }

      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // TODO: remove ignore after implementing aggregation
  @Ignore
  @Test
  public void aggregationAlignByDeviceTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createAlignedTimeseries(session);
      prepareAlignedTimeSeriesData(session);
      prepareNonAlignedTimeSeriesData(session);

      SessionDataSet dataSet =
          session.executeQueryStatement("select count(*) from root.sg1.d1 align by device");
      assertEquals(3, dataSet.getColumnNames().size());
      assertEquals("Device", dataSet.getColumnNames().get(0));
      assertEquals("count(s1)", dataSet.getColumnNames().get(1));
      assertEquals("count(s2)", dataSet.getColumnNames().get(2));
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("root.sg1.d1", rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals(100, rowRecord.getFields().get(1).getLongV());
        assertEquals(100, rowRecord.getFields().get(2).getLongV());
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
