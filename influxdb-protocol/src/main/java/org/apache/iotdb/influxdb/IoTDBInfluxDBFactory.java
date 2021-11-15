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

package org.apache.iotdb.influxdb;

import org.apache.iotdb.influxdb.protocol.util.ParameterUtils;
import org.apache.iotdb.session.Session;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;

public enum IoTDBInfluxDBFactory {
  INSTANCE;

  private IoTDBInfluxDBFactory() {}

  public static InfluxDB connect(String url, String username, String password) {
    ParameterUtils.checkNonEmptyString(url, "url");
    ParameterUtils.checkNonEmptyString(username, "username");
    return new IoTDBInfluxDB(url, username, password);
  }

  public static InfluxDB connect(String host, int rpcPort, String userName, String password) {
    ParameterUtils.checkNonEmptyString(host, "host");
    ParameterUtils.checkNonEmptyString(userName, "username");
    return new IoTDBInfluxDB(host, rpcPort, userName, password);
  }

  public static InfluxDB connect(
      String url, String username, String password, OkHttpClient.Builder client) {
    ParameterUtils.checkNonEmptyString(url, "url");
    ParameterUtils.checkNonEmptyString(username, "username");
    return connect(url, username, password);
  }

  public static InfluxDB connect(
      String url,
      String username,
      String password,
      OkHttpClient.Builder client,
      InfluxDB.ResponseFormat responseFormat) {
    ParameterUtils.checkNonEmptyString(url, "url");
    ParameterUtils.checkNonEmptyString(username, "username");
    return connect(url, username, password);
  }

  public static InfluxDB connect(Session.Builder builder) {
    return new IoTDBInfluxDB(builder);
  }

  public static InfluxDB connect(Session session) {
    return new IoTDBInfluxDB(session);
  }
}
