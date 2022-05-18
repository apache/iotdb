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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBTriggerForwardIT {
  private volatile long count = 0;
  private volatile Exception exception = null;

  private final Thread dataGenerator =
      new Thread() {
        @Override
        public void run() {
          try (Connection connection =
                  DriverManager.getConnection(
                      Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
              Statement statement = connection.createStatement()) {

            do {
              ++count;
              statement.execute(
                  String.format(
                      "insert into root.vehicle.a.b.c.d1(timestamp,s1,s2,s3,s4,s5,s6) values(%d,%d,%d,%d,%d,%s,'%d')",
                      count, count, count, count, count, count % 2 == 0 ? "true" : "false", count));
            } while (!isInterrupted());
          } catch (Exception e) {
            exception = e;
          }
        }
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    createTimeseries();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  @Ignore
  public void testForwardHTTPTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_forward_http_before before insert on root.vehicle.a.b.c.d1.s1 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ( 'protocol' = 'http', 'endpoint' = 'http://127.0.0.1:8079/demo/echo')");
      statement.execute(
          "create trigger trigger_forward_http_after after insert on root.vehicle.a.b.c.d1.s2 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'http', 'endpoint' = 'http://127.0.0.1:8079/demo/echo')");
      // TODO need to find a way to e2e test HTTPTrigger
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  @Ignore
  public void testForwardMQTTTrigger() throws InterruptedException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_forward_mqtt_before before insert on root.vehicle.a.b.c.d1.s3 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'mqtt', 'host' = '127.0.0.1', 'port' = '1883',"
              + " 'username' = 'root', 'password' = 'root', 'topic' = 'mqtt-test')");
      statement.execute(
          "create trigger trigger_forward_mqtt_after after insert on root.vehicle.a.b.c.d1.s4 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'mqtt', 'host' = '127.0.0.1', 'port' = '1883',"
              + " 'username' = 'root', 'password' = 'root', 'topic' = 'mqtt-test')");
      // TODO
      startDataGenerator();
      waitCountIncreaseBy(500);
      stopDataGenerator();
      if (exception != null) {
        return;
      }
      await()
          .atMost(1, MINUTES)
          .until(
              () ->
                  count
                      == checkSingleSensorHandlerResult(
                          "root.vehicle.a.b.c.d2",
                          "Time,root.vehicle.a.b.c.d2.s3,root.vehicle.a.b.c.d2.s4",
                          new int[] {Types.TIMESTAMP, Types.FLOAT, Types.FLOAT}));

    } catch (SQLException | InterruptedException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }

  private void createTimeseries() throws MetadataException {
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s5"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.vehicle.a.b.c.d1.s6"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private void startDataGenerator() {
    dataGenerator.start();
  }

  private void stopDataGenerator() throws InterruptedException {
    if (!dataGenerator.isInterrupted()) {
      dataGenerator.interrupt();
    }
    dataGenerator.join();
  }

  private void waitCountIncreaseBy(final long increment) throws InterruptedException {
    final long previous = count;
    while (count - previous < increment) {
      Thread.sleep(100);
    }
  }

  private int checkSingleSensorHandlerResult(
      String path, String expectedHeaderStrings, int[] expectedTypes)
      throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    int forwardCount = 0;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(statement.execute("select * from " + path));

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, expectedHeaderStrings, expectedTypes);

        while (resultSet.next()) {
          ++forwardCount;
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(forwardCount, Double.parseDouble(resultSet.getString(i)), 0.0);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    return forwardCount;
  }

  private void checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
    }
  }
}
