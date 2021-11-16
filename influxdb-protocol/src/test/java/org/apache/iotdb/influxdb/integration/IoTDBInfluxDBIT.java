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
package org.apache.iotdb.influxdb.integration;

import org.apache.iotdb.influxdb.IoTDBInfluxDBFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class IoTDBInfluxDBIT {

  private String host;
  private Integer port;
  private String username;
  private String password;
  private InfluxDB influxDB;
  private Session session;

  @Rule
  public GenericContainer<?> iotdb =
      new GenericContainer<>("apache/iotdb:latest").withExposedPorts(6667);

  @Before
  public void setUp() throws IoTDBConnectionException {
    host = iotdb.getContainerIpAddress();
    port = iotdb.getMappedPort(6667);
    username = "root";
    password = "root";
    influxDB = IoTDBInfluxDBFactory.connect(host, port, username, password);
    influxDB.createDatabase("monitor");
    influxDB.setDatabase("monitor");

    session = new Session(host, port, username, password);
    session.open(false);

    insertData();
  }

  private void insertData() {
    Point.Builder builder = Point.measurement("student");
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("name", "A");
    tags.put("phone", "B");
    tags.put("sex", "C");
    fields.put("score", 99);
    fields.put("tel", "110");
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Point point = builder.build();
    influxDB.write(point);

    builder = Point.measurement("student");
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("address", "D");
    fields.put("score", 98);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);

    builder = Point.measurement("student");
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("address", "D");
    tags.put("name", "A");
    tags.put("phone", "B");
    tags.put("sex", "C");
    fields.put("score", 97);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);
  }

  @After
  public void tearDown() {
    influxDB.close();
  }

  @Test
  public void testConnect1() {
    IoTDBInfluxDBFactory.connect("https://" + host + ":" + port, username, password).close();
  }

  @Test
  public void testConnect2() {
    IoTDBInfluxDBFactory.connect(host, port, username, password).close();
  }

  @Test
  public void testConnect3() {
    IoTDBInfluxDBFactory.connect(
            "https://" + host + ":" + port, username, password, new okhttp3.OkHttpClient.Builder())
        .close();
  }

  @Test
  public void testConnect4() {
    IoTDBInfluxDBFactory.connect(
            "https://" + host + ":" + port,
            username,
            password,
            new okhttp3.OkHttpClient.Builder(),
            InfluxDB.ResponseFormat.JSON)
        .close();
  }

  @Test
  public void testConnect5() {
    Session.Builder builder =
        new Session.Builder().host(host).port(port).username(username).password(password);
    IoTDBInfluxDBFactory.connect(builder).close();
  }

  @Test
  public void testConnect6() {
    Session session =
        new Session.Builder().host(host).port(port).username(username).password(password).build();
    session.setFetchSize(10000);
    IoTDBInfluxDBFactory.connect(session).close();
  }

  @Test(expected = InfluxDBException.class)
  public void testConnectRefusedFailed() {
    InfluxDB influxDB = IoTDBInfluxDBFactory.connect(host, 80, username, password);
  }

  @Test(expected = InfluxDBException.class)
  public void testConnectAuthFailed() {
    InfluxDB influxDB = IoTDBInfluxDBFactory.connect(host, port, "1", "1");
  }

  @Test
  public void testVersion() {
    String version = influxDB.version();
    assertNotNull(version);
    assertTrue(version.length() > 0);
  }

  @Test
  public void testPing() {
    assertTrue(influxDB.ping().getResponseTime() > 0);
  }

  @Test
  public void testInsert1() {
    String[] expected = {"china, 99, null, 110, ", "null, null, 97, null, "};
    int expectLength = 2;
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from root.monitor.student.A.B.C", 0);
      while (sessionDataSet.hasNext()) {
        for (int i = 0; i < expectLength; i++) {
          RowRecord record = sessionDataSet.next();
          List<Field> fields = record.getFields();
          StringBuilder actual = new StringBuilder();
          for (Field field : fields) {
            actual.append(field.toString()).append(", ");
          }
          assertEquals(expected[i], actual.toString());
        }
      }
      sessionDataSet.closeOperationHandle();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Test
  public void testInsert2() {
    String[] expected = {"98"};
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from root.monitor.student.PH.PH.PH.D", 0);
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<Field> fields = record.getFields();
        for (int i = 0; i < fields.size(); ++i) {
          assertEquals(expected[i], fields.get(i).toString());
        }
      }
      sessionDataSet.closeOperationHandle();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Test
  public void testInsert3() {
    String[] expected = {"97"};
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from root.monitor.student.A.B.C.D", 0);
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<Field> fields = record.getFields();
        for (int i = 0; i < fields.size(); ++i) {
          assertEquals(expected[i], fields.get(i).toString());
        }
      }
      sessionDataSet.closeOperationHandle();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }
}
