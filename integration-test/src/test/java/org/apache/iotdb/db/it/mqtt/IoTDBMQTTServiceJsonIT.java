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
package org.apache.iotdb.db.it.mqtt;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for MQTT service with JSON payload formatter. JSON formatter supports tree
 * model data insertion.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBMQTTServiceJsonIT {

  private BlockingConnection connection;
  private static final String IP = System.getProperty("RemoteIp", "127.0.0.1");
  private static final String USER = System.getProperty("RemoteUser", SessionConfig.DEFAULT_USER);
  private static final String PASSWORD =
      System.getProperty("RemotePassword", SessionConfig.DEFAULT_PASSWORD);
  public static final String FORMATTER = "json";

  @Before
  public void setUp() throws Exception {
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableMQTTService(true);
    baseEnv.getConfig().getDataNodeConfig().setMqttPayloadFormatter(FORMATTER);
    baseEnv.initClusterEnvironment();
    DataNodeWrapper portConflictDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    int port = portConflictDataNodeWrapper.getMqttPort();
    MQTT mqtt = new MQTT();
    mqtt.setHost(IP, port);
    mqtt.setUserName(USER);
    mqtt.setPassword(PASSWORD);
    mqtt.setConnectAttemptsMax(3);
    mqtt.setReconnectDelay(10);
    mqtt.setClientId("jsonClientId1");

    connection = mqtt.blockingConnection();
    connection.connect();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (connection != null && connection.isConnected()) {
        connection.disconnect();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /** Test single JSON message with multiple measurements */
  @Test
  public void testSingleJsonMessage() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "{"
              + "\"device\":\"root.sg.d1\","
              + "\"timestamp\":1,"
              + "\"measurements\":[\"s1\",\"s2\"],"
              + "\"values\":[1.5,2.5]"
              + "}";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg.d1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement("select s1, s2 from root.sg.d1 where time = 1")) {
                  if (!dataSet.hasNext()) {
                    return false;
                  }
                  RowRecord row = dataSet.next();
                  List<Field> fields = row.getFields();
                  if (fields.size() != 2) {
                    return false;
                  }
                  if (Math.abs(fields.get(0).getDoubleV() - 1.5) > 0.001
                      || Math.abs(fields.get(1).getDoubleV() - 2.5) > 0.001) {
                    return false;
                  }
                  return true;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      try (final SessionDataSet dataSet =
          session.executeQueryStatement("select s1, s2 from root.sg.d1 where time = 1")) {
        assertTrue(dataSet.hasNext());
        List<Field> fields = dataSet.next().getFields();
        assertEquals(2, fields.size());
        assertEquals(1.5, fields.get(0).getDoubleV(), 0.001);
        assertEquals(2.5, fields.get(1).getDoubleV(), 0.001);
      }
    }
  }

  /** Test batch JSON message with timestamps array */
  @Test
  public void testBatchJsonMessage() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "{"
              + "\"device\":\"root.sg.d2\","
              + "\"timestamps\":[1,2,3],"
              + "\"measurements\":[\"s1\",\"s2\"],"
              + "\"values\":[[1.0,2.0],[3.0,4.0],[5.0,6.0]]"
              + "}";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg.d2", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement("select count(s1) from root.sg.d2")) {
                  if (!dataSet.hasNext()) {
                    return false;
                  }
                  RowRecord row = dataSet.next();
                  long count = row.getFields().get(0).getLongV();
                  // Retry when data not visible yet (count 0) or incomplete
                  if (count != 3) {
                    return false;
                  }
                  return true;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      // Assert after await so failure message is clear on timeout
      try (final SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from root.sg.d2")) {
        assertTrue(dataSet.hasNext());
        assertEquals(3, dataSet.next().getFields().get(0).getLongV());
      }
    }
  }

  /** Test JSON array with multiple messages */
  @Test
  public void testJsonArray() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "["
              + "{\"device\":\"root.sg.d3\",\"timestamp\":1,\"measurements\":[\"s1\"],\"values\":[10.0]},"
              + "{\"device\":\"root.sg.d3\",\"timestamp\":2,\"measurements\":[\"s1\"],\"values\":[20.0]},"
              + "{\"device\":\"root.sg.d3\",\"timestamp\":3,\"measurements\":[\"s1\"],\"values\":[30.0]}"
              + "]";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg.d3", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement("select s1 from root.sg.d3")) {
                  int count = 0;
                  double sum = 0;
                  while (dataSet.hasNext()) {
                    RowRecord row = dataSet.next();
                    sum += row.getFields().get(0).getDoubleV();
                    count++;
                  }
                  if (count != 3) {
                    return false;
                  }
                  // sum should be 10 + 20 + 30 = 60
                  if (Math.abs(sum - 60.0) > 0.001) {
                    return false;
                  }
                  return true;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      try (final SessionDataSet dataSet =
          session.executeQueryStatement("select s1 from root.sg.d3")) {
        int count = 0;
        double sum = 0;
        while (dataSet.hasNext()) {
          sum += dataSet.next().getFields().get(0).getDoubleV();
          count++;
        }
        assertEquals(3, count);
        assertEquals(60.0, sum, 0.001);
      }
    }
  }

  /** Test JSON with explicit data types */
  @Test
  public void testJsonWithDataTypes() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "{"
              + "\"device\":\"root.sg.d4\","
              + "\"timestamp\":1,"
              + "\"measurements\":[\"intVal\",\"floatVal\",\"boolVal\",\"textVal\"],"
              + "\"values\":[100,3.14,true,\"hello\"],"
              + "\"datatypes\":[\"INT32\",\"FLOAT\",\"BOOLEAN\",\"TEXT\"]"
              + "}";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg.d4", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement(
                        "select intVal, floatVal, boolVal, textVal from root.sg.d4 where time = 1")) {
                  if (!dataSet.hasNext()) {
                    return false;
                  }
                  List<Field> fields = dataSet.next().getFields();
                  if (fields.size() != 4) {
                    return false;
                  }
                  if (fields.get(0).getIntV() != 100
                      || Math.abs(fields.get(1).getFloatV() - 3.14f) > 0.01
                      || !fields.get(2).getBoolV()
                      || !"hello".equals(fields.get(3).getStringValue())) {
                    return false;
                  }
                  return true;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      try (final SessionDataSet dataSet =
          session.executeQueryStatement(
              "select intVal, floatVal, boolVal, textVal from root.sg.d4 where time = 1")) {
        assertTrue(dataSet.hasNext());
        List<Field> fields = dataSet.next().getFields();
        assertEquals(4, fields.size());
        assertEquals(100, fields.get(0).getIntV());
        assertEquals(3.14f, fields.get(1).getFloatV(), 0.01);
        assertTrue(fields.get(2).getBoolV());
        assertEquals("hello", fields.get(3).getStringValue());
      }
    }
  }

  /** Test multiple devices in single JSON array */
  @Test
  public void testMultipleDevicesJsonArray() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "["
              + "{\"device\":\"root.sg.device1\",\"timestamp\":1,\"measurements\":[\"temp\"],\"values\":[25.5]},"
              + "{\"device\":\"root.sg.device2\",\"timestamp\":1,\"measurements\":[\"temp\"],\"values\":[26.5]},"
              + "{\"device\":\"root.sg.device3\",\"timestamp\":1,\"measurements\":[\"temp\"],\"values\":[27.5]}"
              + "]";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try {
                  // Check device1
                  try (final SessionDataSet dataSet1 =
                      session.executeQueryStatement(
                          "select temp from root.sg.device1 where time = 1")) {
                    if (!dataSet1.hasNext()) {
                      return false;
                    }
                    if (Math.abs(dataSet1.next().getFields().get(0).getDoubleV() - 25.5) > 0.001) {
                      return false;
                    }
                  }
                  // Check device2
                  try (final SessionDataSet dataSet2 =
                      session.executeQueryStatement(
                          "select temp from root.sg.device2 where time = 1")) {
                    if (!dataSet2.hasNext()) {
                      return false;
                    }
                    if (Math.abs(dataSet2.next().getFields().get(0).getDoubleV() - 26.5) > 0.001) {
                      return false;
                    }
                  }
                  // Check device3
                  try (final SessionDataSet dataSet3 =
                      session.executeQueryStatement(
                          "select temp from root.sg.device3 where time = 1")) {
                    if (!dataSet3.hasNext()) {
                      return false;
                    }
                    if (Math.abs(dataSet3.next().getFields().get(0).getDoubleV() - 27.5) > 0.001) {
                      return false;
                    }
                  }
                  return true;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      try (final SessionDataSet dataSet1 =
          session.executeQueryStatement("select temp from root.sg.device1 where time = 1")) {
        assertTrue(dataSet1.hasNext());
        assertEquals(25.5, dataSet1.next().getFields().get(0).getDoubleV(), 0.001);
      }
      try (final SessionDataSet dataSet2 =
          session.executeQueryStatement("select temp from root.sg.device2 where time = 1")) {
        assertTrue(dataSet2.hasNext());
        assertEquals(26.5, dataSet2.next().getFields().get(0).getDoubleV(), 0.001);
      }
      try (final SessionDataSet dataSet3 =
          session.executeQueryStatement("select temp from root.sg.device3 where time = 1")) {
        assertTrue(dataSet3.hasNext());
        assertEquals(27.5, dataSet3.next().getFields().get(0).getDoubleV(), 0.001);
      }
    }
  }

  /** Test batch JSON with different values per timestamp */
  @Test
  public void testBatchJsonWithVariousValues() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String payload =
          "{"
              + "\"device\":\"root.sg.d5\","
              + "\"timestamps\":[100,200,300,400,500],"
              + "\"measurements\":[\"temperature\",\"humidity\"],"
              + "\"values\":[[20.1,60.0],[21.2,61.5],[22.3,62.0],[23.4,63.5],[24.5,64.0]]"
              + "}";

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                connection.publish("root.sg.d5", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement("select temperature, humidity from root.sg.d5")) {
                  int count = 0;
                  while (dataSet.hasNext()) {
                    RowRecord row = dataSet.next();
                    List<Field> fields = row.getFields();
                    if (fields.size() != 2) {
                      return false;
                    }
                    double temp = fields.get(0).getDoubleV();
                    double humidity = fields.get(1).getDoubleV();
                    if (temp < 20.0 || temp > 25.0 || humidity < 60.0 || humidity > 65.0) {
                      return false;
                    }
                    count++;
                  }
                  return count == 5;
                } catch (StatementExecutionException e) {
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  } else {
                    throw e;
                  }
                }
              });
      try (final SessionDataSet dataSet =
          session.executeQueryStatement("select temperature, humidity from root.sg.d5")) {
        int count = 0;
        while (dataSet.hasNext()) {
          RowRecord row = dataSet.next();
          List<Field> fields = row.getFields();
          assertEquals(2, fields.size());
          double temp = fields.get(0).getDoubleV();
          double humidity = fields.get(1).getDoubleV();
          assertTrue(temp >= 20.0 && temp <= 25.0);
          assertTrue(humidity >= 60.0 && humidity <= 65.0);
          count++;
        }
        assertEquals(5, count);
      }
    }
  }
}
