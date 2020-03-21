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
package org.apache.iotdb.mqtt;

import com.google.common.collect.Lists;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * The IoTDB MQTT Broker.
 */
public class MQTTBroker {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTBroker.class);
    private static final MQTTBroker MQTT_BROKER = new MQTTBroker();
    private Server server = new Server();

    public void startup() {
        MQTTBrokerConfig brokerConfig = new MQTTBrokerConfig();
        IConfig config = createBrokerConfig(brokerConfig);
        List<InterceptHandler> handlers = Lists.newArrayList(new PublishHandler(brokerConfig));
        IAuthenticator authenticator = new BrokerAuthenticator(brokerConfig.getMqttBrokerUsername(), brokerConfig.getMqttBrokerPassword());

        server.startServer(config, handlers, null, authenticator, null);

        LOG.info("MQTT Broker IoTDB: start broker successfully, listening on ip {} port {}",
                brokerConfig.getMqttBrokerHost(), brokerConfig.getMqttBrokerPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping IoTDB MQTT Broker...");
            shutdown();
            LOG.info("IoTDB MQTT Broker stopped.");
        }));
    }

    private IConfig createBrokerConfig(MQTTBrokerConfig brokerConfig) {
        Properties properties = new Properties();
        properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, brokerConfig.getMqttBrokerHost());
        properties.setProperty(BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(brokerConfig.getMqttBrokerPort()));
        properties.setProperty(BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE, String.valueOf(brokerConfig.getMqttBrokerHanderPoolSize()));
        return new MemoryConfig(properties);
    }

    public void shutdown() {
        server.stopServer();
    }

    public static MQTTBroker getMqttBroker() {
        return MQTT_BROKER;
    }

    //  for testing
    void setServer(Server server) {
        this.server = server;
    }

    public static void main(String[] args) throws IOException {
        getMqttBroker().startup();
    }
}
