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
import org.apache.iotdb.session.Session;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBInfluxDBIT {

  private static String host;
  private static Integer port;
  private static String username;
  private static String password;
  private static InfluxDB influxDB;

  @ClassRule
  public static GenericContainer<?> iotdb =
      new GenericContainer(DockerImageName.parse("apache/iotdb:influxdb-protocol-on"))
          .withExposedPorts(8086);

  @BeforeClass
  public static void setUp() {
    host = iotdb.getContainerIpAddress();
    port = iotdb.getMappedPort(8086);
    username = "root";
    password = "root";
    influxDB = IoTDBInfluxDBFactory.connect(host, port, username, password);
    influxDB.createDatabase("database");
    influxDB.setDatabase("database");

    insertData();
  }

  private static void insertData() {
    // insert the build parameter to construct the influxdb
    Point.Builder builder = Point.measurement("student");
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    fields.put("score", 87);
    fields.put("tel", "110");
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Point point = builder.build();
    // after the build construction is completed, start writing
    influxDB.write(point);

    builder = Point.measurement("student");
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    tags.put("province", "anhui");
    fields.put("score", 99);
    fields.put("country", "china");
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
    InfluxDB influxDB = IoTDBInfluxDBFactory.connect(host, port, "error", "error");
  }

  @Test
  public void testCommonQueryColumn() {
    Query query =
        new Query(
            "select * from student where (name=\"xie\" and sex=\"m\")or time<now()-7d", "database");
    QueryResult result = influxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);

    String[] retArray = new String[] {"time", "name", "sex", "province", "country", "score", "tel"};
    Set<String> columnNames = new HashSet<>(Arrays.asList(retArray));
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertTrue(columnNames.contains(series.getColumns().get(i)));
    }
  }

  @Test
  public void testFuncWithoutFilter() {
    Query query =
        new Query(
            "select max(score),min(score),sum(score),count(score),spread(score),mean(score),first(score),last(score) from student ",
            "database");
    QueryResult result = influxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);

    Object[] retArray = new Object[] {0, 99.0, 87.0, 186.0, 2, 12.0, 93.0, 87, 99};
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertEquals(retArray[i], series.getValues().get(0).get(i));
    }
  }

  @Test
  public void testFunc() {
    Query query =
        new Query(
            "select count(score),first(score),last(country),max(score),mean(score),median(score),min(score),mode(score),spread(score),stddev(score),sum(score) from student where (name=\"xie\" and sex=\"m\")or score<99",
            "database");
    QueryResult result = influxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);

    Object[] retArray =
        new Object[] {0, 2, 87, "china", 99.0, 93.0, 93.0, 87.0, 87, 12.0, 6.0, 186.0};
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertEquals(retArray[i], series.getValues().get(0).get(i));
    }
  }
}
