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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.idtable.trigger_example.Counter;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBSessionComplexIT {
  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertByStrTest() {

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {

      session.setStorageGroup("root.sg1");
      createTimeseries(session);
      insertByStr(session);
      insertViaSQL(session);
      queryByDevice(session, "root.sg1.d1");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createTimeseries(ISession session)
      throws StatementExecutionException, IoTDBConnectionException {
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
  }

  private void insertByStr(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  private void insertViaSQL(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "insert into root.sg1.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");
  }

  private void queryByDevice(ISession session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from " + deviceId);
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    long expectedSum = 1 + 2 + 3;
    while (sessionDataSet.hasNext()) {
      count++;
      long actualSum = 0;
      for (Field f : sessionDataSet.next().getFields()) {
        actualSum += f.getLongV();
      }
      assertEquals(expectedSum, actualSum);
    }

    switch (deviceId) {
      case "root.sg1.d1":
        assertEquals(101, count);
        break;
      case "root.sg1.d2":
        assertEquals(500, count);
        break;
    }
    sessionDataSet.closeOperationHandle();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertByObjectTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {

      session.setStorageGroup("root.sg1");
      createTimeseries(session);

      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      for (long time = 0; time < 100; time++) {
        session.insertRecord(deviceId, time, measurements, types, 1L, 2L, 3L);
      }

      insertViaSQL(session);
      queryByDevice(session, "root.sg1.d1");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void alignByDeviceTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {

      session.setStorageGroup("root.sg1");
      createTimeseries(session);
      insertTablet(session, "root.sg1.d1");
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select s1 from root.sg1.d1 align by device");
      sessionDataSet.setFetchSize(1024);
      int count = 0;

      while (sessionDataSet.hasNext()) {
        count++;
        StringBuilder sb = new StringBuilder();
        List<Field> fields = sessionDataSet.next().getFields();
        for (Field f : fields) {
          sb.append(f.getStringValue()).append(",");
        }
        assertEquals("root.sg1.d1,0,", sb.toString());
      }
      assertEquals(100, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void insertTablet(ISession session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);

    for (long time = 0; time < 100; time++) {
      int rowIndex = tablet.rowSize++;
      long value = 0;
      tablet.addTimestamp(rowIndex, time);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
        value++;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testBatchInsertSeqAndUnseq()
      throws SQLException, IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createTimeseries(session);

      insertTablet(session, "root.sg1.d1");

      session.executeNonQueryStatement("FLUSH");
      session.executeNonQueryStatement("FLUSH root.sg1");
      session.executeNonQueryStatement("MERGE");
      session.executeNonQueryStatement("FULL MERGE");

      queryForBatch();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void queryForBatch() throws SQLException {
    List<String> standards =
        Arrays.asList(
            "Time",
            "root.sg1.d1.s1",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d2.s1",
            "root.sg1.d2.s2",
            "root.sg1.d2.s3");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      for (int i = 0; i < colCount; i++) {
        assertTrue(standards.contains(metaData.getColumnLabel(i + 1)));
      }

      int count = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          count++;
        }
      }
      assertEquals(700, count);
    }
  }

  @Ignore
  @Test
  public void insertTabletWithTriggersTest() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.setStorageGroup("root.sg1");
      createTimeseries(session);

      session.executeNonQueryStatement(
          "create trigger d1s1 after insert on root.sg1.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      session.executeNonQueryStatement(
          "create trigger d1s2 before insert on root.sg1.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      assertEquals(
          Counter.BASE,
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s1"))
              .getCounter());
      assertEquals(
          Counter.BASE,
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s2"))
              .getCounter());
      try {
        int counter =
            ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s3"))
                .getCounter();
        fail(String.valueOf(counter - Counter.BASE));
      } catch (TriggerManagementException e) {
        assertEquals("Trigger d1s3 does not exist.", e.getMessage());
      }

      insertTablet(session, "root.sg1.d1");

      assertEquals(
          Counter.BASE + 200,
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s1"))
              .getCounter());
      assertEquals(
          Counter.BASE + 200,
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s2"))
              .getCounter());
      try {
        int counter =
            ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s3"))
                .getCounter();
        fail(String.valueOf(counter - Counter.BASE));
      } catch (TriggerManagementException e) {
        assertEquals("Trigger d1s3 does not exist.", e.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({ClusterIT.class})
  public void sessionClusterTest() {
    ArrayList<String> nodeList = new ArrayList<>();
    List<DataNodeWrapper> dataNodeWrappersList = EnvFactory.getEnv().getDataNodeWrapperList();
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrappersList) {
      nodeList.add(dataNodeWrapper.getIpAndPortString());
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection(nodeList)) {
      session.setStorageGroup("root.sg1");

      createTimeseries(session);
      insertByStr(session);
      insertViaSQL(session);
      queryByDevice(session, "root.sg1.d1");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Category({ClusterIT.class})
  public void errorSessionClusterTest() {
    ArrayList<String> nodeList = new ArrayList<>();
    List<DataNodeWrapper> dataNodeWrappersList = EnvFactory.getEnv().getDataNodeWrapperList();
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrappersList) {
      nodeList.add(dataNodeWrapper.getIpAndPortString());
    }
    // test Format error
    nodeList.add("127.0.0.16669");
    try (ISession ignored = EnvFactory.getEnv().getSessionConnection(nodeList)) {
    } catch (Exception e) {
      assertEquals("NodeUrl Incorrect format", e.getMessage());
    }
  }
}
