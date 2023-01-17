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
package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.protocol.mqtt.BrokerAuthenticator;
import org.apache.iotdb.db.protocol.mqtt.MPPPublishHandler;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** The IoTDB MQTT Service. */
public class MQTTService implements IService {
  private static final Logger LOG = LoggerFactory.getLogger(MQTTService.class);
  private final Server server = new Server();

  private MQTTService() {}

  @Override
  public void start() {
    startup();
  }

  @Override
  public void stop() {
    shutdown();
  }

  public void startup() {
    CommonConfig commonConfig = CommonDescriptor.getInstance().getConf();
    IConfig config = createBrokerConfig(commonConfig);
    List<InterceptHandler> handlers = new ArrayList<>(1);
    handlers.add(new MPPPublishHandler(commonConfig));
    IAuthenticator authenticator = new BrokerAuthenticator();

    server.startServer(config, handlers, null, authenticator, null);

    LOG.info(
        "Start MQTT service successfully, listening on ip {} port {}",
        commonConfig.getMqttHost(),
        commonConfig.getMqttPort());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping IoTDB MQTT service...");
                  shutdown();
                  LOG.info("IoTDB MQTT service stopped.");
                }));
  }

  private IConfig createBrokerConfig(CommonConfig commonConfig) {
    Properties properties = new Properties();
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, commonConfig.getMqttHost());
    properties.setProperty(
        BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(commonConfig.getMqttPort()));
    properties.setProperty(
        BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
        String.valueOf(commonConfig.getMqttHandlerPoolSize()));
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
        String.valueOf(commonConfig.getMqttMaxMessageSize()));
    return new MemoryConfig(properties);
  }

  public void shutdown() {
    server.stopServer();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MQTT_SERVICE;
  }

  public static MQTTService getInstance() {
    return MQTTServiceHolder.INSTANCE;
  }

  private static class MQTTServiceHolder {

    private static final MQTTService INSTANCE = new MQTTService();

    private MQTTServiceHolder() {}
  }
}
