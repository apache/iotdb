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
package org.apache.iotdb.relational.it.mqtt;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.read.common.Field;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBMQTTServiceIT {
  private BlockingConnection connection;
  private int port = 1883;
  private final String ip = System.getProperty("RemoteIp", "127.0.0.1");
  private final String user = System.getProperty("RemoteUser", "root");
  private final String password = System.getProperty("RemotePassword", "root");

  @Before
  public void setUp() throws Exception {
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableMQTTService(true);
    baseEnv.getConfig().getDataNodeConfig().setMqttPayloadFormatter("line");
    baseEnv.initClusterEnvironment();
    DataNodeWrapper portConflictDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    port = portConflictDataNodeWrapper.getMqttPort();
    MQTT mqtt = new MQTT();
    mqtt.setHost(ip, port);
    mqtt.setUserName(user);
    mqtt.setPassword(password);
    mqtt.setConnectAttemptsMax(3);
    mqtt.setReconnectDelay(10);

    connection = mqtt.blockingConnection();
    connection.connect();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (connection != null) {
        connection.disconnect();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNoAttr() throws Exception {
    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("mqtttest")) {
      session.executeNonQueryStatement("CREATE DATABASE mqtttest");
      String payload1 = "test1,tag1=t1,tag2=t2 field1=1,field2=1f,field3=1i32 1";
      connection.publish("mqtttest/myTopic", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
      Thread.sleep(1000);
      try (final SessionDataSet dataSet =
          session.executeQueryStatement(
              "select tag1,tag2,field1,field2,field3 from test1 where time = 1")) {
        assertEquals(5, dataSet.getColumnNames().size());
        List<Field> fields = dataSet.next().getFields();
        assertEquals(fields.get(0).getStringValue(), "t1");
        assertEquals(fields.get(1).getStringValue(), "t2");
        assertEquals(fields.get(2).getDoubleV(), 1d, 0);
        assertEquals(fields.get(3).getFloatV(), 1f, 0);
        assertEquals(fields.get(4).getIntV(), 1, 0);
      }
    }
  }

  @Test
  public void testWithAttr() throws Exception {
    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("mqtttest")) {
      session.executeNonQueryStatement("CREATE DATABASE mqtttest");
      String payload1 = "test2,tag1=t1,tag2=t2 attr3=a3,attr4=a4 field1=1,field2=1f,field3=1i32 1";
      connection.publish("mqtttest/myTopic", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
      Thread.sleep(1000);
      try (final SessionDataSet dataSet =
          session.executeQueryStatement(
              "select tag1,tag2,attr3,attr4,field1,field2,field3 from test2 where time = 1")) {
        assertEquals(7, dataSet.getColumnNames().size());
        List<Field> fields = dataSet.next().getFields();
        assertEquals(fields.get(0).getStringValue(), "t1");
        assertEquals(fields.get(1).getStringValue(), "t2");
        assertEquals(fields.get(2).getStringValue(), "a3");
        assertEquals(fields.get(3).getStringValue(), "a4");
        assertEquals(1d, fields.get(4).getDoubleV(), 0);
        assertEquals(fields.get(5).getFloatV(), 1f, 0);
        assertEquals(fields.get(6).getIntV(), 1, 0);
      }
    }
  }
}
