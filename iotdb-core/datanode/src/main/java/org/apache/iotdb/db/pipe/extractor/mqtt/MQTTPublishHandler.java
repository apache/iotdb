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

package org.apache.iotdb.db.pipe.extractor.mqtt;

import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.pipe.event.common.statement.PipeStatementInsertionEvent;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatManager;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TableMessage;
import org.apache.iotdb.db.protocol.mqtt.TreeMessage;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.MqttClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** PublishHandler handle the messages from MQTT clients. */
public class MQTTPublishHandler extends AbstractInterceptHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTPublishHandler.class);

  private final SessionManager sessionManager = SessionManager.getInstance();

  private final ConcurrentHashMap<String, MqttClientSession> clientIdToSessionMap =
      new ConcurrentHashMap<>();
  private final PayloadFormatter payloadFormat;
  private final boolean useTableInsert;
  private final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue;
  private final String pipeName;
  private final long creationTime;
  private final PipeTaskMeta pipeTaskMeta;

  public MQTTPublishHandler(
      final PipeParameters pipeParameters,
      final PipeTaskExtractorRuntimeEnvironment environment,
      final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue) {
    this.payloadFormat =
        PayloadFormatManager.getPayloadFormat(
            pipeParameters.getStringOrDefault(
                PipeExtractorConstant.MQTT_PAYLOAD_FORMATTER_KEY,
                PipeExtractorConstant.MQTT_PAYLOAD_FORMATTER_DEFAULT_VALUE));
    useTableInsert = PayloadFormatter.TABLE_TYPE.equals(this.payloadFormat.getType());
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    this.pendingQueue = pendingQueue;
  }

  @Override
  public String getID() {
    return "mqtt-source-broker-listener";
  }

  @Override
  public void onConnect(InterceptConnectMessage msg) {
    if (!clientIdToSessionMap.containsKey(msg.getClientID())) {
      final MqttClientSession session = new MqttClientSession(msg.getClientID());
      sessionManager.login(
          session,
          msg.getUsername(),
          new String(msg.getPassword()),
          ZoneId.systemDefault().toString(),
          TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
          ClientVersion.V_1_0,
          useTableInsert ? IClientSession.SqlDialect.TABLE : IClientSession.SqlDialect.TREE);
      sessionManager.registerSession(session);
      clientIdToSessionMap.put(msg.getClientID(), session);
    }
  }

  @Override
  public void onDisconnect(InterceptDisconnectMessage msg) {
    final MqttClientSession session = clientIdToSessionMap.remove(msg.getClientID());
    if (null != session) {
      sessionManager.removeCurrSession();
      sessionManager.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
    }
  }

  @Override
  public void onPublish(InterceptPublishMessage msg) {
    try {
      final String clientId = msg.getClientID();
      if (!clientIdToSessionMap.containsKey(clientId)) {
        return;
      }
      final MqttClientSession session = clientIdToSessionMap.get(msg.getClientID());
      final ByteBuf payload = msg.getPayload();
      final String topic = msg.getTopicName();
      final String username = msg.getUsername();
      final MqttQoS qos = msg.getQos();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
            clientId,
            username,
            qos,
            topic,
            payload);
      }

      final List<Message> messages = payloadFormat.format(topic, payload);
      if (messages == null) {
        return;
      }

      for (Message message : messages) {
        if (message == null) {
          continue;
        }
        if (useTableInsert) {
          extractTable((TableMessage) message, session);
        } else {
          extractTree((TreeMessage) message, session);
        }
      }
    } catch (Throwable t) {
      LOGGER.warn("onPublish execution exception, msg is {}, error is ", msg, t);
    } finally {
      // release the payload of the message
      super.onPublish(msg);
    }
  }

  private void extractTable(final TableMessage message, final MqttClientSession session) {
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      InsertTabletStatement insertTabletStatement = constructInsertTabletStatement(message);
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);
      final EnrichedEvent event =
          new PipeStatementInsertionEvent(
              pipeName,
              creationTime,
              pipeTaskMeta,
              null,
              null,
              session.getUsername(),
              true,
              true,
              message.getDatabase(),
              insertTabletStatement);
      if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
        LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
        return;
      }
      pendingQueue.waitedOffer(event);
    } catch (Exception e) {
      LOGGER.warn(
          "meet error when polling mqtt source message database {}, table {}, tags {}, attributes {}, fields {}, at time {}, because {}",
          message.getDatabase(),
          message.getTable(),
          message.getTagKeys(),
          message.getAttributeKeys(),
          message.getFields(),
          message.getTimestamp(),
          e.getMessage(),
          e);
    }
  }

  private InsertTabletStatement constructInsertTabletStatement(TableMessage message)
      throws IllegalPathException {
    InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(message.getTable()));
    List<String> measurements =
        Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    statement.setMeasurements(measurements.toArray(new String[0]));
    long[] timestamps = new long[] {message.getTimestamp()};
    statement.setTimes(timestamps);
    int columnSize = measurements.size();
    int rowSize = 1;

    BitMap[] bitMaps = new BitMap[columnSize];
    Object[] columns =
        Stream.of(message.getValues(), message.getTagValues(), message.getAttributeValues())
            .flatMap(List::stream)
            .toArray(Object[]::new);
    statement.setColumns(columns);
    statement.setBitMaps(bitMaps);
    statement.setRowCount(rowSize);
    statement.setAligned(false);
    statement.setWriteToTable(true);
    TSDataType[] dataTypes = new TSDataType[measurements.size()];
    TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[measurements.size()];
    for (int i = 0; i < message.getFields().size(); i++) {
      dataTypes[i] = message.getDataTypes().get(i);
      columnCategories[i] = TsTableColumnCategory.FIELD;
    }
    for (int i = message.getFields().size();
        i < message.getFields().size() + message.getTagKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = TsTableColumnCategory.TAG;
    }
    for (int i = message.getFields().size() + message.getTagKeys().size();
        i
            < message.getFields().size()
                + message.getTagKeys().size()
                + message.getAttributeKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = TsTableColumnCategory.ATTRIBUTE;
    }
    statement.setDataTypes(dataTypes);
    statement.setColumnCategories(columnCategories);

    return statement;
  }

  private void extractTree(final TreeMessage message, final MqttClientSession session) {
    try {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(
          DataNodeDevicePathCache.getInstance().getPartialPath(message.getDevice()));
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      statement.setTime(message.getTimestamp());
      statement.setMeasurements(message.getMeasurements().toArray(new String[0]));
      if (message.getDataTypes() == null) {
        statement.setDataTypes(new TSDataType[message.getMeasurements().size()]);
        statement.setValues(message.getValues().toArray(new Object[0]));
        statement.setNeedInferType(true);
      } else {
        List<TSDataType> dataTypes = message.getDataTypes();
        List<String> values = message.getValues();
        Object[] inferredValues = new Object[values.size()];
        for (int i = 0; i < values.size(); ++i) {
          inferredValues[i] =
              values.get(i) == null
                  ? null
                  : CommonUtils.parseValue(dataTypes.get(i), values.get(i));
        }
        statement.setDataTypes(dataTypes.toArray(new TSDataType[0]));
        statement.setValues(inferredValues);
      }
      statement.setAligned(false);
      final EnrichedEvent event =
          new PipeStatementInsertionEvent(
              pipeName,
              creationTime,
              pipeTaskMeta,
              null,
              null,
              session.getUsername(),
              true,
              false,
              message.getDevice(),
              statement);
      if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
        LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
        return;
      }
      pendingQueue.waitedOffer(event);
    } catch (Exception e) {
      LOGGER.warn(
          "meet error when polling mqtt source device {}, measurements {}, at time {}, because {}",
          message.getDevice(),
          message.getMeasurements(),
          message.getTimestamp(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public void onSessionLoopError(Throwable throwable) {
    LOGGER.warn(
        "onSessionLoopError: {}",
        throwable.getMessage() == null ? "null" : throwable.getMessage(),
        throwable);
  }
}
