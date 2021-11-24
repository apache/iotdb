/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.mqtt;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PublishHandlerTest {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void onPublish() throws ClassNotFoundException {
    PayloadFormatter payloadFormat = PayloadFormatManager.getPayloadFormat("json");
    PublishHandler handler = new PublishHandler(payloadFormat);

    String payload =
        "{\n"
            + "\"device\":\"root.sg.d1\",\n"
            + "\"timestamp\":1586076045524,\n"
            + "\"measurements\":[\"s1\"],\n"
            + "\"values\":[0.530635]\n"
            + "}";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

    // connect
    MqttConnectPayload mqttConnectPayload =
        new MqttConnectPayload(null, null, "test", "root", "root");
    MqttConnectMessage mqttConnectMessage = new MqttConnectMessage(null, null, mqttConnectPayload);
    InterceptConnectMessage interceptConnectMessage =
        new InterceptConnectMessage(mqttConnectMessage);
    handler.onConnect(interceptConnectMessage);

    // publish
    MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("root.sg.d1", 1);
    MqttFixedHeader fixedHeader =
        new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
    MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, variableHeader, buf);
    InterceptPublishMessage message = new InterceptPublishMessage(publishMessage, null, null);
    handler.onPublish(message);

    // disconnect
    InterceptDisconnectMessage interceptDisconnectMessage =
        new InterceptDisconnectMessage(null, null);
    handler.onDisconnect(interceptDisconnectMessage);

    String[] retArray = new String[] {"1586076045524,0.530635,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
