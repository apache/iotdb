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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.MqttClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.TSStatusCode;
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
public class MPPPublishHandler extends AbstractInterceptHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MPPPublishHandler.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final SessionManager sessionManager = SessionManager.getInstance();

  private final ConcurrentHashMap<String, MqttClientSession> clientIdToSessionMap =
      new ConcurrentHashMap<>();
  private final PayloadFormatter payloadFormat;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final boolean useTableInsert;

  public MPPPublishHandler(IoTDBConfig config) {
    this.payloadFormat = PayloadFormatManager.getPayloadFormat(config.getMqttPayloadFormatter());
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
    useTableInsert = PayloadFormatter.TABLE_TYPE.equals(this.payloadFormat.getType());
  }

  @Override
  public String getID() {
    return "iotdb-mqtt-broker-listener";
  }

  @Override
  public void onConnect(InterceptConnectMessage msg) {
    if (!clientIdToSessionMap.containsKey(msg.getClientID())) {
      MqttClientSession session = new MqttClientSession(msg.getClientID());
      sessionManager.login(
          session,
          msg.getUsername(),
          new String(msg.getPassword()),
          ZoneId.systemDefault().toString(),
          TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
          ClientVersion.V_1_0,
          useTableInsert ? IClientSession.SqlDialect.TABLE : IClientSession.SqlDialect.TREE);
      sessionManager.registerSessionForMqtt(session);
      clientIdToSessionMap.put(msg.getClientID(), session);
    }
  }

  @Override
  public void onDisconnect(InterceptDisconnectMessage msg) {
    MqttClientSession session = clientIdToSessionMap.remove(msg.getClientID());
    if (null != session) {
      sessionManager.removeCurrSessionForMqtt(session);
      sessionManager.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
    }
  }

  @Override
  public void onPublish(InterceptPublishMessage msg) {
    try {
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

      List<Message> messages = payloadFormat.format(topic, payload);
      if (messages == null) {
        return;
      }

      for (Message message : messages) {
        if (message == null) {
          continue;
        }
        if (useTableInsert) {
          insertTable((TableMessage) message, session);
        } else {
          insertTree((TreeMessage) message, session);
        }
      }
    } catch (Throwable t) {
      LOG.warn("onPublish execution exception, msg is [{}], error is ", msg, t);
    } finally {
      // release the payload of the message
      super.onPublish(msg);
    }
  }

  /** Inserting table using tablet */
  private void insertTable(TableMessage message, MqttClientSession session) {
    TSStatus tsStatus = null;
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      InsertTabletStatement insertTabletStatement = constructInsertTabletStatement(message);
      session.setDatabaseName(message.getDatabase().toLowerCase());
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);
      long queryId = sessionManager.requestQueryId();
      SqlParser relationSqlParser = new SqlParser();
      Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTableModel(
                  insertTabletStatement,
                  relationSqlParser,
                  session,
                  queryId,
                  sessionManager.getSessionInfo(session),
                  "",
                  metadata,
                  config.getQueryTimeoutThreshold());

      tsStatus = result.status;
      LOG.debug("process result: {}", tsStatus);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOG.warn("mqtt line insert error , message = {}", tsStatus.message);
      }
    } catch (Exception e) {
      LOG.warn(
          "meet error when inserting database {}, table {}, tags {}, attributes {}, fields {}, at time {}, because ",
          message.getDatabase(),
          message.getTable(),
          message.getTagKeys(),
          message.getAttributeKeys(),
          message.getFields(),
          message.getTimestamp(),
          e);
    }
  }

  private InsertTabletStatement constructInsertTabletStatement(TableMessage message)
      throws IllegalPathException {
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(message.getTable()));
    List<String> measurements =
        Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    insertStatement.setMeasurements(measurements.toArray(new String[0]));
    long[] timestamps = new long[] {message.getTimestamp()};
    insertStatement.setTimes(timestamps);
    int columnSize = measurements.size();
    int rowSize = 1;

    BitMap[] bitMaps = new BitMap[columnSize];
    Object[] columns =
        Stream.of(message.getValues(), message.getTagValues(), message.getAttributeValues())
            .flatMap(List::stream)
            .toArray(Object[]::new);
    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(rowSize);
    insertStatement.setAligned(false);
    insertStatement.setWriteToTable(true);
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
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setColumnCategories(columnCategories);

    return insertStatement;
  }

  private void insertTree(TreeMessage message, MqttClientSession session) {
    TSStatus tsStatus = null;
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
        long queryId = sessionManager.requestQueryId();
        ExecutionResult result =
            Coordinator.getInstance()
                .executeForTreeModel(
                    statement,
                    queryId,
                    sessionManager.getSessionInfo(session),
                    "",
                    partitionFetcher,
                    schemaFetcher,
                    config.getQueryTimeoutThreshold(),
                    false);
        tsStatus = result.status;
        LOG.debug("process result: {}", tsStatus);
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOG.warn("mqtt json insert error , message = {}", tsStatus.message);
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "meet error when inserting device {}, measurements {}, at time {}, because ",
          message.getDevice(),
          message.getMeasurements(),
          message.getTimestamp(),
          e);
    }
  }

  @Override
  public void onSessionLoopError(Throwable throwable) {
    // TODO: Implement something sensible here ...
  }
}
