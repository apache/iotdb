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

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PublishHandler handle the messages from MQTT clients.
 */
public class PublishHandler extends AbstractInterceptHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    private IPlanExecutor executor;
    private PayloadFormatter payloadFormat;

    public PublishHandler(IoTDBConfig config) {
        this.payloadFormat = PayloadFormatManager.getPayloadFormat(config.getMqttPayloadFormatter());
        try {
            this.executor = new PlanExecutor();
        } catch (QueryProcessException e) {
            throw new RuntimeException(e);
        }
    }

    protected PublishHandler(IPlanExecutor executor, PayloadFormatter payloadFormat) {
        this.executor = executor;
        this.payloadFormat = payloadFormat;
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

        InsertPlan plan = new InsertPlan();
        plan.setDeviceId(event.getDevice());
        plan.setTime(event.getTimestamp());
        plan.setMeasurements(event.getMeasurements().toArray(new String[event.getMeasurements().size()]));
        plan.setValues(event.getValues().toArray(new String[event.getValues().size()]));

        boolean status;
        try {
            status = executeNonQuery(plan);
        } catch (QueryProcessException e) {
            throw new RuntimeException(e);
        }

        LOG.debug("event process result: {}", status);
    }

    private boolean executeNonQuery(PhysicalPlan plan) throws QueryProcessException {
        if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
            throw new QueryProcessException(
                    "Current system mode is read-only, does not support non-query operation");
        }
        return executor.processNonQuery(plan);
    }
}
