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

package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.IPv6TestUtils;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.UrlUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.it.utils.IPv6TestUtils.IPV6_LOOPBACK_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBIPv6ExternalServiceIT {

  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private static String previousTestNodeAddress;

  @BeforeClass
  public static void setUp() {
    IPv6TestUtils.assumeIPv6LoopbackAvailable();
    previousTestNodeAddress = IPv6TestUtils.setTestNodeAddressToIPv6Loopback();
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setEnableRestService(true);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnableMQTTService(true)
        .setMqttPayloadFormatter("json");
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      IPv6TestUtils.restoreTestNodeAddress(previousTestNodeAddress);
    }
  }

  @Test
  public void restServiceCanCommunicateThroughIPv6Loopback() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNode.getIp());
    final String restAddress =
        UrlUtils.formatTEndPointIpv4AndIpv6Url(dataNode.getIp(), dataNode.getRestServicePort());

    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .ignoreExceptions()
          .untilAsserted(() -> assertRestPingSucceeds(httpClient, restAddress));

      executeRestNonQuery(
          httpClient, restAddress, "CREATE TIMESERIES root.ipv6_rest.d1.s1 WITH DATATYPE=INT64");
      executeRestNonQuery(
          httpClient, restAddress, "INSERT INTO root.ipv6_rest.d1(time, s1) VALUES (1, 100)");
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        SessionDataSet dataSet =
            session.executeQueryStatement("SELECT s1 FROM root.ipv6_rest.d1")) {
      assertTrue(dataSet.hasNext());
      final RowRecord row = dataSet.next();
      assertEquals(1L, row.getTimestamp());
      assertEquals(100L, row.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void mqttServiceCanCommunicateThroughIPv6Loopback() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNode.getIp());
    final String mqttAddress =
        UrlUtils.formatTEndPointIpv4AndIpv6Url(dataNode.getIp(), dataNode.getMqttPort());

    final MQTT mqtt = new MQTT();
    mqtt.setHost("tcp://" + mqttAddress);
    mqtt.setUserName(USER);
    mqtt.setPassword(PASSWORD);
    mqtt.setConnectAttemptsMax(3);
    mqtt.setReconnectDelay(1000);
    mqtt.setClientId("ipv6Client");

    final byte[] payload =
        ("{"
                + "\"device\":\"root.ipv6_mqtt.d1\","
                + "\"timestamp\":1,"
                + "\"measurements\":[\"s1\"],"
                + "\"values\":[200.0]"
                + "}")
            .getBytes(StandardCharsets.UTF_8);
    final BlockingConnection connection = mqtt.blockingConnection();
    try {
      connection.connect();
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(
                () -> {
                  connection.publish("root.ipv6_mqtt.d1", payload, QoS.AT_LEAST_ONCE, false);
                  return mqttValueIsVisible(session);
                });
      }
    } finally {
      if (connection.isConnected()) {
        connection.disconnect();
      }
    }
  }

  private static void assertRestPingSucceeds(
      final CloseableHttpClient httpClient, final String restAddress) throws IOException {
    try (CloseableHttpResponse response =
        httpClient.execute(new HttpGet("http://" + restAddress + "/ping"))) {
      assertEquals(200, response.getStatusLine().getStatusCode());
      final JsonObject result =
          JsonParser.parseString(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8))
              .getAsJsonObject();
      assertEquals(200, result.get("code").getAsInt());
    }
  }

  private static void executeRestNonQuery(
      final CloseableHttpClient httpClient, final String restAddress, final String sql)
      throws IOException {
    final HttpPost httpPost = new HttpPost("http://" + restAddress + "/rest/v2/nonQuery");
    httpPost.addHeader("Content-type", "application/json; charset=utf-8");
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader(
        "Authorization",
        Base64.getEncoder()
            .encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
    final JsonObject request = new JsonObject();
    request.addProperty("sql", sql);
    httpPost.setEntity(new StringEntity(request.toString(), StandardCharsets.UTF_8));

    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
      final JsonObject result =
          JsonParser.parseString(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8))
              .getAsJsonObject();
      assertEquals(200, result.get("code").getAsInt());
    }
  }

  private static boolean mqttValueIsVisible(final ISession session) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement("SELECT s1 FROM root.ipv6_mqtt.d1 WHERE time = 1")) {
      if (!dataSet.hasNext()) {
        return false;
      }
      final List<Field> fields = dataSet.next().getFields();
      return fields.size() == 1 && Math.abs(fields.get(0).getDoubleV() - 200.0) < 0.001;
    } catch (StatementExecutionException e) {
      if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
        return false;
      }
      throw e;
    }
  }
}
