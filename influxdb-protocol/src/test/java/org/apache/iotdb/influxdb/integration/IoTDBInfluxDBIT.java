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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class IoTDBInfluxDBIT {
  private String host;
  private Integer port;
  private String username;
  private String password;

  @Rule
  public GenericContainer IotDB =
      new GenericContainer("apache/iotdb:latest").withExposedPorts(6667);

  @Before
  public void setUp() {
    host = IotDB.getContainerIpAddress();
    port = IotDB.getMappedPort(6667);
    username = "root";
    password = "root";
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
            "https://" + host + ":" + port, username, password, new okhttp3.OkHttpClient.Builder()).close();
  }

  @Test
  public void testConnect4() {

        IoTDBInfluxDBFactory.connect(
            "https://" + host + ":" + port,
            username,
            password,
            new okhttp3.OkHttpClient.Builder(),
            InfluxDB.ResponseFormat.JSON).close();
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

  @Test(expected = IllegalArgumentException.class)
  public void testConnectRefusedFailed() {
    InfluxDB influxDB = IoTDBInfluxDBFactory.connect(host, 80, username, password);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectAuthFailed() {
    InfluxDB influxDB = IoTDBInfluxDBFactory.connect(host, port, "1", "1");
  }
}
