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
package org.apache.iotdb.db.protocol.mqtt;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** PublishHandler handle the messages from MQTT clients. */
public class PublishHandler extends AbstractInterceptHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);
  private final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final ConcurrentHashMap<String, Long> clientIdToSessionIdMap = new ConcurrentHashMap<>();

  private final PayloadFormatter payloadFormat;

  public PublishHandler(IoTDBConfig config) {
    this.payloadFormat = PayloadFormatManager.getPayloadFormat(config.getMqttPayloadFormatter());
  }

  protected PublishHandler(PayloadFormatter payloadFormat) {
    this.payloadFormat = payloadFormat;
  }

  @Override
  public String getID() {
    return "iotdb-mqtt-broker-listener";
  }

  @Override
  public void onConnect(InterceptConnectMessage msg) {
    if (!clientIdToSessionIdMap.containsKey(msg.getClientID())) {
      try {
        BasicOpenSessionResp basicOpenSessionResp =
            SESSION_MANAGER.openSession(
                msg.getUsername(),
                new String(msg.getPassword()),
                ZoneId.systemDefault().toString(),
                TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
        clientIdToSessionIdMap.put(msg.getClientID(), basicOpenSessionResp.getSessionId());
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void onDisconnect(InterceptDisconnectMessage msg) {
    Long sessionId = clientIdToSessionIdMap.remove(msg.getClientID());
    if (null != sessionId) {
      SESSION_MANAGER.closeSession(sessionId);
    }
  }

  @Override
  public void onPublish(InterceptPublishMessage msg) {
    String clientId = msg.getClientID();
    if (!clientIdToSessionIdMap.containsKey(clientId)) {
      return;
    }
    long sessionId = clientIdToSessionIdMap.get(msg.getClientID());
    ByteBuf payload = msg.getPayload();
    String topic = msg.getTopicName();
    String username = msg.getUsername();
    MqttQoS qos = msg.getQos();

    LOG.debug(
        "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
        clientId,
        username,
        qos,
        topic,
        payload);

    List<Message> events = payloadFormat.format(payload);
    if (events == null) {
      return;
    }

    // since device ids from messages maybe different, so we use the InsertPlan not
    // InsertTabletPlan.
    for (Message event : events) {
      if (event == null) {
        continue;
      }

      boolean status = false;
      try {
        PartialPath path = new PartialPath(event.getDevice());
        InsertRowPlan plan =
            new InsertRowPlan(
                path,
                event.getTimestamp(),
                event.getMeasurements().toArray(new String[0]),
                event.getValues().toArray(new String[0]));
        TSStatus tsStatus = SESSION_MANAGER.checkAuthority(plan, sessionId);
        if (tsStatus != null) {
          LOG.warn(tsStatus.message);
        } else {
          status = IoTDB.serviceProvider.executeNonQuery(plan);
        }
      } catch (Exception e) {
        LOG.warn(
            "meet error when inserting device {}, measurements {}, at time {}, because ",
            event.getDevice(),
            event.getMeasurements(),
            event.getTimestamp(),
            e);
      }

      LOG.debug("event process result: {}", status);
    }
  }
}
