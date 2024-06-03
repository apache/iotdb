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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.db.it.utils.TestUtils.revokeUserSeriesPrivilege;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBSessionComplexIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123");
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
    createTimeseries(session, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
  }

  private void createTimeseries(ISession session, List<String> deviceIds)
      throws IoTDBConnectionException, StatementExecutionException {
    for (String device : deviceIds) {
      session.createTimeseries(
          device + ".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      session.createTimeseries(
          device + ".s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      session.createTimeseries(
          device + ".s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
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

      // auth test
      try (ISession authSession = EnvFactory.getEnv().getSessionConnection("test", "test123")) {
        grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s1");
        grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s2");
        try {
          authSession.insertRecord(deviceId, 0, measurements, types, 1L, 2L, 3L);
        } catch (Exception e) {
          if (!e.getMessage()
              .contains(
                  "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg1.d1.s3]")) {
            fail(e.getMessage());
          }
        }

        grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s3");
        try {
          authSession.insertRecord(deviceId, 0, measurements, types, 1L, 2L, 3L);
        } catch (Exception e) {
          if (!e.getMessage()
              .contains(
                  "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d1.s3]")) {
            fail(e.getMessage());
          }
        }

        grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
        try {
          authSession.insertRecord(deviceId, 0, measurements, types, 1L, 2L, 3L);
        } catch (Exception e) {
          if (!e.getMessage()
              .contains(
                  "803: No permissions for this operation, please add privilege MANAGE_DATABASE")) {
            fail(e.getMessage());
          }
        }

        grantUserSystemPrivileges("test", PrivilegeType.MANAGE_DATABASE);
        try {
          authSession.insertRecord(deviceId, 0, measurements, types, 1L, 2L, 3L);
        } catch (Exception e) {
          fail(e.getMessage());
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }
      revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s1");
      revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s2");
      revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s3");
      revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
      revokeUserSeriesPrivilege("test", PrivilegeType.MANAGE_DATABASE, "root.**");

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

    List<IMeasurementSchema> schemaList = new ArrayList<>();
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

  private void insertRecords(ISession session, List<String> deviceIdList)
      throws IoTDBConnectionException, StatementExecutionException {
    long timePartition = CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 10 * timePartition; time += timePartition / 10) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.addAll(deviceIdList);

      measurementsList.add(measurements);
      measurementsList.add(measurements);

      valuesList.add(values);
      valuesList.add(values);
      typesList.add(types);
      typesList.add(types);
      timestamps.add(time);
      timestamps.add(time);

      if (time != 0 && time % (5 * timePartition) == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        typesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  private void insertMultiTablets(ISession session, List<String> deviceIdList)
      throws IoTDBConnectionException, StatementExecutionException {
    long timePartition = CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Map<String, Tablet> tabletMap = new HashMap<>();
    for (String device : deviceIdList) {
      tabletMap.put(device, new Tablet(device, schemaList, 100));
    }

    for (long time = 0; time < 10 * timePartition; time += timePartition / 10) {
      for (Tablet tablet : tabletMap.values()) {
        long value = 0;
        tablet.addTimestamp(tablet.rowSize, time);
        for (int s = 0; s < 3; s++) {
          tablet.addValue(schemaList.get(s).getMeasurementId(), tablet.rowSize, value);
          value++;
        }
        tablet.rowSize++;
      }
    }

    session.insertTablets(tabletMap);
  }

  private void insertRecordsOfOneDevice(ISession session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    long timePartition = CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 10 * timePartition; time += timePartition / 10) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);

      if (time != 0 && time % (5 * timePartition) == 0) {
        session.insertRecordsOfOneDevice(
            deviceId, timestamps, measurementsList, typesList, valuesList);
        measurementsList.clear();
        valuesList.clear();
        typesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecordsOfOneDevice(deviceId, timestamps, measurementsList, typesList, valuesList);
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

      List<String> deviceIds = new ArrayList<>();
      queryForBatch(Collections.singletonList("root.sg1.d1"), 400);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void queryForBatch(List<String> deviceIds, int pointNumPerDevice) throws SQLException {
    List<String> standards = new ArrayList<>();
    standards.add("Time");
    for (String device : deviceIds) {
      standards.add(device + ".s1");
      standards.add(device + ".s2");
      standards.add(device + ".s3");
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String device : deviceIds) {
        ResultSet resultSet = statement.executeQuery("SELECT * FROM " + device);
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
        assertEquals(pointNumPerDevice, count);
      }
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

  @Test
  @Category({ClusterIT.class})
  public void insertWithMultipleTimeSlotsTest() {

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      createTimeseries(session, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      insertRecords(session, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      queryForBatch(Arrays.asList("root.sg1.d1", "root.sg1.d2"), 400);

      createTimeseries(session, Arrays.asList("root.sg2.d1", "root.sg2.d2"));
      insertMultiTablets(session, Arrays.asList("root.sg2.d1", "root.sg2.d2"));
      queryForBatch(Arrays.asList("root.sg2.d1", "root.sg2.d2"), 400);

      createTimeseries(session, Collections.singletonList("root.sg3.d1"));
      insertRecordsOfOneDevice(session, "root.sg3.d1");
      queryForBatch(Collections.singletonList("root.sg3.d1"), 400);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAuth() {
    // auth test
    try (ISession authSession = EnvFactory.getEnv().getSessionConnection("test", "test123")) {
      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s1");
      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s2");
      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d2.**");
      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d2.**");
      try {
        insertRecords(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }
      try {
        insertTablet(authSession, "root.sg1.d1");
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }
      try {
        insertMultiTablets(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d1"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }

      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.d1.s3");
      try {
        insertRecords(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }
      try {
        insertTablet(authSession, "root.sg1.d1");
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }
      try {
        insertMultiTablets(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains(
                "No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d1.s3]")) {
          fail(e.getMessage());
        }
      }

      grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
      try {
        insertRecords(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains("No permissions for this operation, please add privilege MANAGE_DATABASE")) {
          fail(e.getMessage());
        }
      }
      try {
        insertTablet(authSession, "root.sg1.d1");
      } catch (Exception e) {
        if (!e.getMessage()
            .contains("No permissions for this operation, please add privilege MANAGE_DATABASE")) {
          fail(e.getMessage());
        }
      }
      try {
        insertMultiTablets(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        if (!e.getMessage()
            .contains("No permissions for this operation, please add privilege MANAGE_DATABASE")) {
          fail(e.getMessage());
        }
      }

      grantUserSystemPrivileges("test", PrivilegeType.MANAGE_DATABASE);
      try {
        insertRecords(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
        insertTablet(authSession, "root.sg1.d1");
        insertMultiTablets(authSession, Arrays.asList("root.sg1.d1", "root.sg1.d2"));
      } catch (Exception e) {
        fail(e.getMessage());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
    executeNonQuery("drop timeseries root.sg1.d1.**, root.sg1.d2.**");
  }
}
