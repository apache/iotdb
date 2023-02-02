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
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.MqttClientSession;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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
public class MPPPublishHandler extends AbstractInterceptHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MPPPublishHandler.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final ConcurrentHashMap<String, MqttClientSession> clientIdToSessionMap =
      new ConcurrentHashMap<>();
  private final PayloadFormatter payloadFormat;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  public MPPPublishHandler(IoTDBConfig config) {
    this.payloadFormat = PayloadFormatManager.getPayloadFormat(config.getMqttPayloadFormatter());
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  @Override
  public String getID() {
    return "iotdb-mqtt-broker-listener";
  }

  @Override
  public void onConnect(InterceptConnectMessage msg) {
    if (!clientIdToSessionMap.containsKey(msg.getClientID())) {
      try {
        MqttClientSession session = new MqttClientSession(msg.getClientID());
        SESSION_MANAGER.login(
            session,
            msg.getUsername(),
            new String(msg.getPassword()),
            ZoneId.systemDefault().toString(),
            TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
            ClientVersion.V_1_0);
        clientIdToSessionMap.put(msg.getClientID(), session);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void onDisconnect(InterceptDisconnectMessage msg) {
    MqttClientSession session = clientIdToSessionMap.remove(msg.getClientID());
    if (null != session) {
      SESSION_MANAGER.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
    }
  }

  @Override
  public void onPublish(InterceptPublishMessage msg) {
    String clientId = msg.getClientID();
    if (!clientIdToSessionMap.containsKey(clientId)) {
      return;
    }
    MqttClientSession session = clientIdToSessionMap.get(msg.getClientID());
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

    for (Message event : events) {
      if (event == null) {
        continue;
      }

      TSStatus tsStatus = null;
      try {
        InsertRowStatement statement = new InsertRowStatement();
        statement.setDevicePath(new PartialPath(event.getDevice()));
        statement.setTime(event.getTimestamp());
        statement.setMeasurements(event.getMeasurements().toArray(new String[0]));
        if (event.getDataTypes() == null) {
          statement.setDataTypes(new TSDataType[event.getMeasurements().size()]);
          statement.setValues(event.getValues().toArray(new Object[0]));
          statement.setNeedInferType(true);
        } else {
          List<TSDataType> dataTypes = event.getDataTypes();
          List<String> values = event.getValues();
          Object[] inferredValues = new Object[values.size()];
          for (int i = 0; i < values.size(); ++i) {
            inferredValues[i] = CommonUtils.parseValue(dataTypes.get(i), values.get(i));
          }
          statement.setDataTypes(dataTypes.toArray(new TSDataType[0]));
          statement.setValues(inferredValues);
        }
        statement.setAligned(false);

        tsStatus = AuthorityChecker.checkAuthority(statement, session);
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOG.warn(tsStatus.message);
        } else {
          long queryId = SESSION_MANAGER.requestQueryId();
          ExecutionResult result =
              Coordinator.getInstance()
                  .execute(
                      statement,
                      queryId,
                      SESSION_MANAGER.getSessionInfo(session),
                      "",
                      partitionFetcher,
                      schemaFetcher,
                      config.getQueryTimeoutThreshold());
          tsStatus = result.status;
        }
      } catch (Exception e) {
        LOG.warn(
            "meet error when inserting device {}, measurements {}, at time {}, because ",
            event.getDevice(),
            event.getMeasurements(),
            event.getTimestamp(),
            e);
      }
      LOG.debug("event process result: {}", tsStatus);
    }
  }
}
