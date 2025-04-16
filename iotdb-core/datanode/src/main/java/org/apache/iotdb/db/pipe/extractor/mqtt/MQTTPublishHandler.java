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
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatManager;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TableMessage;
import org.apache.iotdb.db.protocol.mqtt.TreeMessage;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.MqttClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.utils.CommonUtils.parseBlobStringToByteArray;

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

      LOGGER.debug(
          "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
          clientId,
          username,
          qos,
          topic,
          payload);

      final List<Message> messages = payloadFormat.format(payload);
      if (messages == null) {
        return;
      }

      for (Message message : messages) {
        if (message == null) {
          continue;
        }
        if (useTableInsert) {
          final TableMessage tableMessage = (TableMessage) message;
          // '/' previously defined as a database name
          final String database =
              !msg.getTopicName().contains("/")
                  ? msg.getTopicName()
                  : msg.getTopicName().substring(0, msg.getTopicName().indexOf("/"));
          tableMessage.setDatabase(database.toLowerCase());
          extractTable(tableMessage, session);
        } else {
          extractTree((TreeMessage) message, session);
        }
      }
    } catch (Throwable t) {
      LOGGER.warn("onPublish execution exception, msg is [{}], error is ", msg, t);
    } finally {
      // release the payload of the message
      super.onPublish(msg);
    }
  }

  /** Inserting table using tablet */
  private void extractTable(final TableMessage message, final MqttClientSession session) {
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      session.setDatabaseName(message.getDatabase().toLowerCase());
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);
      final EnrichedEvent event = generateEvent(message, session.getUsername());
      if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
        LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
        return;
      }
      pendingQueue.waitedOffer(event);
    } catch (Exception e) {
      LOGGER.warn(
          "meet error when polling mqtt source message database {}, table {}, tags {}, attributes {}, fields {}, at time {}, because ",
          message.getDatabase(),
          message.getTable(),
          message.getTagKeys(),
          message.getAttributeKeys(),
          message.getFields(),
          message.getTimestamp(),
          e);
    }
  }

  private PipeRawTabletInsertionEvent generateEvent(
      final TableMessage message, final String userName) {
    final List<String> measurements =
        Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    final long[] timestamps = new long[] {message.getTimestamp()};
    final BitMap[] bitMaps = new BitMap[measurements.size()];
    final Object[] columns =
        Stream.of(message.getValues(), message.getTagValues(), message.getAttributeValues())
            .flatMap(List::stream)
            .toArray(Object[]::new);
    final TSDataType[] dataTypes = new TSDataType[measurements.size()];
    final Tablet.ColumnCategory[] columnCategories = new Tablet.ColumnCategory[measurements.size()];
    final MeasurementSchema[] schemas = new MeasurementSchema[measurements.size()];
    for (int i = 0; i < message.getFields().size(); i++) {
      dataTypes[i] = message.getDataTypes().get(i);
      columnCategories[i] = Tablet.ColumnCategory.FIELD;
      schemas[i] = new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }
    for (int i = message.getFields().size();
        i < message.getFields().size() + message.getTagKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = Tablet.ColumnCategory.TAG;
      schemas[i] = new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }
    for (int i = message.getFields().size() + message.getTagKeys().size();
        i
            < message.getFields().size()
                + message.getTagKeys().size()
                + message.getAttributeKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = Tablet.ColumnCategory.ATTRIBUTE;
      schemas[i] = new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }

    final Tablet eventTablet =
        new Tablet(
            message.getTable(),
            Arrays.asList(schemas),
            Arrays.asList(columnCategories),
            timestamps,
            columns,
            bitMaps,
            1);

    return new PipeRawTabletInsertionEvent(
        true,
        message.getDatabase().toLowerCase(),
        null,
        null,
        eventTablet,
        true,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        false,
        userName);
  }

  private void extractTree(final TreeMessage message, final MqttClientSession session) {
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      final EnrichedEvent event = generateEvent(message, session.getUsername());
      if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
        LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
        return;
      }
      pendingQueue.waitedOffer(event);
    } catch (Exception e) {
      LOGGER.warn(
          "meet error when polling mqtt source device {}, measurements {}, at time {}, because ",
          message.getDevice(),
          message.getMeasurements(),
          message.getTimestamp(),
          e);
    }
  }

  private EnrichedEvent generateEvent(final TreeMessage message, final String userName)
      throws QueryProcessException {
    final String deviceId = message.getDevice();
    final List<String> measurements = message.getMeasurements();
    final long[] timestamps = new long[] {message.getTimestamp()};
    final MeasurementSchema[] schemas = new MeasurementSchema[measurements.size()];
    final TSDataType[] dataTypes =
        message.getDataTypes() == null
            ? new TSDataType[message.getMeasurements().size()]
            : message.getDataTypes().toArray(new TSDataType[0]);
    final List<String> values = message.getValues();
    final BitMap[] bitMaps = new BitMap[schemas.length];
    final Object[] inferredValues = new Object[values.size()];

    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = new BitMap(1);
    }
    // For each measurement value, parse it and then wrap it in a one-dimensional array.
    for (int i = 0; i < values.size(); ++i) {
      Object parsedValue;
      if (message.getDataTypes() == null) {
        parsedValue = parseValue(values.get(i), dataTypes, i);
      } else {
        parsedValue =
            values.get(i) == null ? null : CommonUtils.parseValue(dataTypes[i], values.get(i));
      }
      if (parsedValue == null) {
        bitMaps[i].mark(0);
      }
      // Wrap the parsed value into a one-dimensional array based on its type.
      if (dataTypes[i] == TSDataType.INT32) {
        inferredValues[i] = new int[] {parsedValue == null ? 0 : (Integer) parsedValue};
      } else if (dataTypes[i] == TSDataType.INT64) {
        inferredValues[i] = new long[] {parsedValue == null ? 0 : (Long) parsedValue};
      } else if (dataTypes[i] == TSDataType.FLOAT) {
        inferredValues[i] = new float[] {parsedValue == null ? 0 : (Float) parsedValue};
      } else if (dataTypes[i] == TSDataType.DOUBLE) {
        inferredValues[i] = new double[] {parsedValue == null ? 0 : (Double) parsedValue};
      } else if (dataTypes[i] == TSDataType.BOOLEAN) {
        inferredValues[i] = new boolean[] {parsedValue == null ? false : (Boolean) parsedValue};
      } else if (dataTypes[i] == TSDataType.STRING) {
        inferredValues[i] = new String[] {(String) parsedValue};
      } else if (dataTypes[i] == TSDataType.TEXT) {
        inferredValues[i] = new Binary[] {(Binary) parsedValue};
      } else {
        // For any other type, wrap it as an Object array.
        inferredValues[i] = new Object[] {parsedValue};
      }
    }

    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }
    final Tablet eventTablet =
        new Tablet(deviceId, Arrays.asList(schemas), timestamps, inferredValues, bitMaps, 1);

    return new PipeRawTabletInsertionEvent(
        false,
        deviceId,
        null,
        null,
        eventTablet,
        false,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        false,
        userName);
  }

  public Object parseValue(String value, TSDataType[] dataType, final int index) {
    if (value == null) {
      dataType[index] = TSDataType.TEXT;
      return null;
    }
    Double parsedValue = null;
    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      dataType[index] = TSDataType.BOOLEAN;
      return Boolean.parseBoolean(value);
    }
    try {
      parsedValue = Double.parseDouble(value);
      if (Double.isInfinite(parsedValue)) {
        throw new NumberFormatException("The input double value is Infinity");
      }
    } catch (NumberFormatException e) {

    }
    if (parsedValue != null) {
      if (!value.endsWith("F")
          && !value.endsWith("f")
          && !value.endsWith("D")
          && !value.endsWith("d")) {
        dataType[index] = TSDataType.DOUBLE;
        return parsedValue;
      }
    }
    if ("null".equals(value) || "NULL".equals(value)) {
      dataType[index] = TSDataType.TEXT;
      return null;
      // "NaN" is returned if the NaN Literal is given in Parser
    }
    if ("NaN".equals(value)) {
      dataType[index] = TSDataType.DOUBLE;
      return Double.NaN;
    }
    if (value.length() >= 3 && value.startsWith("X'") && value.endsWith("'")) {
      dataType[index] = TSDataType.BLOB;
      if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
          || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
        return new Binary(parseBlobStringToByteArray(value.substring(1, value.length() - 1)));
      }
      return new Binary(parseBlobStringToByteArray(value));
    }
    dataType[index] = TSDataType.TEXT;
    if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
        || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
      if (value.length() == 1) {
        return new Binary(value, TSFileConfig.STRING_CHARSET);
      } else {
        return new Binary(value.substring(1, value.length() - 1), TSFileConfig.STRING_CHARSET);
      }
    }
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  @Override
  public void onSessionLoopError(Throwable throwable) {
    // TODO: Implement something sensible here ...
  }
}
