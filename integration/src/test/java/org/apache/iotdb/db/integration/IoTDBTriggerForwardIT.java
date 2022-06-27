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
import org.apache.iotdb.db.engine.trigger.sink.forward.ForwardEvent;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.sun.net.httpserver.HttpServer;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

public class IoTDBTriggerForwardIT {
  private volatile long count = 0;
  private volatile Exception exception = null;

  private HttpServer httpServer;
  private Server mqttServer;

  private final Gson gson = new Gson();
  private final AtomicLong resultCount = new AtomicLong(0);

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
  public void testForwardHTTPTrigger() throws InterruptedException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      startHTTPService();
      statement.execute(
          "create trigger trigger_forward_http_before before insert on root.vehicle.a.b.c.d1.s1 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'http', 'endpoint' = 'http://127.0.0.1:8080/')");
      statement.execute(
          "create trigger trigger_forward_http_after after insert on root.vehicle.a.b.c.d1.s2 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'http', 'endpoint' = 'http://127.0.0.1:8080/')");
      startDataGenerator();
      waitCountIncreaseBy(500);
      stopDataGenerator();
      // ensure no exception occurs when inserting data
      if (exception != null) {
        fail(exception.getMessage());
      }

      await().atMost(1, TimeUnit.MINUTES).until(() -> 2 * count == resultCount.get());
      if (exception != null) {
        fail(exception.getMessage());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      if (httpServer != null) {
        httpServer.stop(0);
      }
      stopDataGenerator();
    }
  }

  @Test
  public void testForwardMQTTTrigger() throws InterruptedException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      startMQTTService();
      statement.execute(
          "create trigger trigger_forward_mqtt_before before insert on root.vehicle.a.b.c.d1.s3 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'mqtt', 'host' = '127.0.0.1', 'port' = '1884',"
              + " 'username' = 'root', 'password' = 'root', 'topic' = 'mqtt-test')");
      statement.execute(
          "create trigger trigger_forward_mqtt_after after insert on root.vehicle.a.b.c.d1.s4 "
              + "as 'org.apache.iotdb.db.engine.trigger.builtin.ForwardTrigger' "
              + "with ('protocol' = 'mqtt', 'host' = '127.0.0.1', 'port' = '1884',"
              + " 'username' = 'root', 'password' = 'root', 'topic' = 'mqtt-test')");
      startDataGenerator();
      waitCountIncreaseBy(500);
      stopDataGenerator();
      // ensure no exception occurs when inserting data
      if (exception != null) {
        fail(exception.getMessage());
      }

      await().atMost(1, TimeUnit.MINUTES).until(() -> 2 * count == resultCount.get());
      if (exception != null) {
        fail(exception.getMessage());
      }
    } catch (SQLException | InterruptedException | IOException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
      if (mqttServer != null) {
        mqttServer.stopServer();
      }
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

  private void startHTTPService() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(8080), 0);
    httpServer.createContext(
        "/",
        exchange -> {
          String entity = "";
          try {
            InputStream in = exchange.getRequestBody();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] b = new byte[1024 * 8];
            int len;
            while ((len = in.read(b)) != -1) {
              out.write(b, 0, len);
            }
            entity = out.toString();
          } catch (Exception e) {
            e.printStackTrace();
          }

          if (!checkPayload(entity)) {
            exception = new IOException("HTTP forward payload error");
          }
          JsonArray receiveData = gson.fromJson(entity, JsonArray.class);
          resultCount.addAndGet(receiveData.size());

          exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
        });
    httpServer.start();
  }

  private void startMQTTService() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, "0.0.0.0");
    properties.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "1884");
    properties.setProperty(BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE, "1");
    IConfig config = new MemoryConfig(properties);

    List<InterceptHandler> handlers = Lists.newArrayList(new ForwardTestHandler());

    mqttServer = new Server();
    mqttServer.startServer(config, handlers);
  }

  private class ForwardTestHandler extends AbstractInterceptHandler {
    @Override
    public String getID() {
      return "forward-test-handler";
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
      String payload = msg.getPayload().toString(StandardCharsets.UTF_8);
      if (!checkPayload(payload)) {
        exception = new IOException("MQTT forward payload error");
      }
      JsonArray receiveData = gson.fromJson(payload, JsonArray.class);
      resultCount.addAndGet(receiveData.size());
    }
  }

  private boolean checkPayload(String payload) {
    return payload.matches(ForwardEvent.PAYLOADS_FORMATTER_REGEX);
  }
}
