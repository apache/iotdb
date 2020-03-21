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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * The MQTT Broker configurations.
 */
public class MQTTBrokerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTBrokerConfig.class);
    public static final String CONFIG_FILE = "iotdb-mqtt-broker.properties";
    public static final String IOTDB_HOME = "IOTDB_HOME";
    public static final String IOTDB_CONF = "IOTDB_CONF";

    public static final String BROKER_HOST_PROPERTY_NAME = "mqtt_broker_host";
    public static final String BROKER_PORT_PROPERTY_NAME = "mqtt_broker_port";
    public static final String BROKER_USERNAME_PROPERTY_NAME = "mqtt_broker_user";
    public static final String BROKER_PASSWORD_PROPERTY_NAME = "mqtt_broker_password";
    public static final String BROKER_HANDLER_POOL_SIZE_PROPERTY_NAME = "mqtt_broker_handler_pool_size";
    public static final String BROKER_PAYLOAD_FORMATTER_PROPERTY_NAME = "mqtt_broker_payload_formatter";

    public static final String IOTDB_HOST_PROPERTY_NAME = "iotdb_host";
    public static final String IOTDB_PORT_PROPERTY_NAME = "iotdb_port";
    public static final String IOTDB_USERNAME_PROPERTY_NAME = "iotdb_user";
    public static final String IOTDB_PASSWORD_PROPERTY_NAME = "iotdb_password";

    private String mqttBrokerHost = "0.0.0.0";
    private int mqttBrokerPort = 1883;
    private String mqttBrokerUsername = "root";
    private String mqttBrokerPassword = "root";
    private int mqttBrokerHanderPoolSize = 1;

    private String iotDBHost = "127.0.0.1";
    private int iotDBPort = 6667;
    private String iotDBUsername = "root";
    private String iotDBPassword = "root";

    private String payloadFormatter = "json";

    public MQTTBrokerConfig() {
        load();
    }

    private void load() {
        Properties properties = new Properties();

        InputStream inputStream;
        String file = findConfigFileFromLocalIoTDB();
        if (file != null) {
            try {
                LOG.info("Load config from local iotdb server.");
                inputStream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
               throw new RuntimeException("Config file not found at " + file, e);
            }
        } else {
            inputStream = MQTTBrokerConfig.class.getResourceAsStream("/" + CONFIG_FILE);
        }

        if (inputStream == null) {
            LOG.warn("No MQTT broker config file found, uses default.");
            return;
        }

        try {
            properties.load(inputStream);
        } catch (IOException e) {
           throw new RuntimeException(e);
        }

        check(properties);

        setMqttBrokerHost(properties.getProperty(BROKER_HOST_PROPERTY_NAME));
        setMqttBrokerPort(Integer.parseInt(properties.getProperty(BROKER_PORT_PROPERTY_NAME)));
        setMqttBrokerUsername(properties.getProperty(BROKER_USERNAME_PROPERTY_NAME));
        setMqttBrokerPassword(properties.getProperty(BROKER_PASSWORD_PROPERTY_NAME));
        if (properties.getProperty(BROKER_HANDLER_POOL_SIZE_PROPERTY_NAME) != null) {
            setMqttBrokerHanderPoolSize(Integer.parseInt(properties.getProperty(BROKER_HANDLER_POOL_SIZE_PROPERTY_NAME)));
        }
        if (properties.getProperty(BROKER_PAYLOAD_FORMATTER_PROPERTY_NAME) != null) {
            setPayloadFormatter(properties.getProperty(BROKER_PAYLOAD_FORMATTER_PROPERTY_NAME));
        }

        setIotDBHost(properties.getProperty(IOTDB_HOST_PROPERTY_NAME));
        setIotDBPort(Integer.parseInt(properties.getProperty(IOTDB_PORT_PROPERTY_NAME)));
        setIotDBUsername(properties.getProperty(IOTDB_USERNAME_PROPERTY_NAME));
        setIotDBPassword(properties.getProperty(IOTDB_PASSWORD_PROPERTY_NAME));
    }

    private String findConfigFileFromLocalIoTDB() {
        String file = null;
        String conf = System.getProperty(IOTDB_CONF);
        if (conf == null) {
            String home = System.getProperty(IOTDB_HOME);
            if (home != null) {
                file = home + File.separatorChar + "conf" + File.separatorChar + CONFIG_FILE;
            }
        } else {
            file = conf + File.separatorChar + CONFIG_FILE;
        }
        return file;
    }

    private void check(Properties properties) {
        Preconditions.checkArgument(properties.getProperty(BROKER_HOST_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(BROKER_PORT_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(BROKER_USERNAME_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(BROKER_PASSWORD_PROPERTY_NAME) != null);

        Preconditions.checkArgument(properties.getProperty(IOTDB_HOST_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(IOTDB_PORT_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(IOTDB_USERNAME_PROPERTY_NAME) != null);
        Preconditions.checkArgument(properties.getProperty(IOTDB_PASSWORD_PROPERTY_NAME) != null);
    }

    public int getMqttBrokerPort() {
        return mqttBrokerPort;
    }

    public void setMqttBrokerPort(int mqttBrokerPort) {
        this.mqttBrokerPort = mqttBrokerPort;
    }

    public String getMqttBrokerUsername() {
        return mqttBrokerUsername;
    }

    public void setMqttBrokerUsername(String mqttBrokerUsername) {
        this.mqttBrokerUsername = mqttBrokerUsername;
    }

    public String getMqttBrokerPassword() {
        return mqttBrokerPassword;
    }

    public void setMqttBrokerPassword(String mqttBrokerPassword) {
        this.mqttBrokerPassword = mqttBrokerPassword;
    }

    public String getIotDBHost() {
        return iotDBHost;
    }

    public void setIotDBHost(String iotDBHost) {
        this.iotDBHost = iotDBHost;
    }

    public int getIotDBPort() {
        return iotDBPort;
    }

    public void setIotDBPort(int iotDBPort) {
        this.iotDBPort = iotDBPort;
    }

    public String getIotDBUsername() {
        return iotDBUsername;
    }

    public void setIotDBUsername(String iotDBUsername) {
        this.iotDBUsername = iotDBUsername;
    }

    public String getIotDBPassword() {
        return iotDBPassword;
    }

    public void setIotDBPassword(String iotDBPassword) {
        this.iotDBPassword = iotDBPassword;
    }

    public String getPayloadFormatter() {
        return payloadFormatter;
    }

    public void setPayloadFormatter(String payloadFormatter) {
        this.payloadFormatter = payloadFormatter;
    }

    public String getMqttBrokerHost() {
        return mqttBrokerHost;
    }

    public void setMqttBrokerHost(String mqttBrokerHost) {
        this.mqttBrokerHost = mqttBrokerHost;
    }

    public void setMqttBrokerHanderPoolSize(int mqttBrokerHanderPoolSize) {
        this.mqttBrokerHanderPoolSize = mqttBrokerHanderPoolSize;
    }

    public int getMqttBrokerHanderPoolSize() {
        return mqttBrokerHanderPoolSize;
    }
}
