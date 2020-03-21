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

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PublishHandler handle the messages from MQTT clients.
 */
public class PublishHandler extends AbstractInterceptHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    private Session session;
    private PayloadFormatter payloadFormat;
    static boolean testing = false;

    public PublishHandler(MQTTBrokerConfig config) {
        payloadFormat = PayloadFormatManager.getPayloadFormat(config.getPayloadFormatter());
        initSession(config);
    }

    public void initSession(MQTTBrokerConfig config) {
        if (testing) {
            return;
        }
        session = new Session(config.getIotDBHost(), config.getIotDBPort(),
                config.getIotDBUsername(), config.getIotDBPassword());
        try {
            session.open();
        } catch (IoTDBSessionException e) {
            throw new RuntimeException("Connect to IoTDB server failure, please check the db status.", e);
        }
    }

    //  for testing
    void setSession(Session session) {
        this.session = session;
    }

    @Override
    public String getID() {
        return "iotdb-mqtt-broker-listener";
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        String clientId = msg.getClientID();
        ByteBuf payload = msg.getPayload();
        String topic = msg.getTopicName();
        String username = msg.getUsername();
        MqttQoS qos = msg.getQos();

        LOG.debug("Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
                clientId, username, qos, topic, payload);

        Message event = payloadFormat.format(payload);
        if (event == null) {
            return;
        }

        TSStatus status;
        try {
            status = session.insert(event.getDevice(), event.getTimestamp(),
                    event.getMeasurements(), event.getValues());
        } catch (IoTDBSessionException e) {
           throw new RuntimeException(e);
        }
        LOG.debug("send event result: {}", status);
    }
}
