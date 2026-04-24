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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import com.timecho.iotdb.utils.AsyncBatchUtils;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.mqtt.MqttUtils.constructTabletFromMultipleMessages;

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

  private final ConcurrentHashMap<String, AsyncBatchUtils<? extends Message>> clientBatchMap =
      new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

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
    if (msg.getClientID() == null || msg.getClientID().trim().isEmpty()) {
      LOG.error(
          "Connection refused: client_id is missing or empty. A valid client_id is required to establish a connection.");
    }
    if (!clientIdToSessionMap.containsKey(msg.getClientID())) {
      MqttClientSession session = new MqttClientSession(msg.getClientID());
      sessionManager.login(
          session,
          msg.getUsername(),
          new String(msg.getPassword()),
          ZoneId.systemDefault().toString(),
          TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
          ClientVersion.V_1_0,
          useTableInsert ? SqlDialect.TABLE : SqlDialect.TREE);
      sessionManager.registerSessionForMqtt(session);
      clientIdToSessionMap.put(msg.getClientID(), session);
    }
  }

  @Override
  public void onDisconnect(InterceptDisconnectMessage msg) {
    String clientId = msg.getClientID();

    // Clean up batch utils
    AsyncBatchUtils<? extends Message> utils = clientBatchMap.remove(clientId);
    if (utils != null) {
      utils.flush();
      utils.shutdown();
      LOG.info("batch utils for client {} cleaned", clientId);
    }

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

      if (LOG.isDebugEnabled()) {
        String username = msg.getUsername();
        MqttQoS qos = msg.getQos();
        LOG.debug(
            "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
            clientId,
            username,
            qos,
            topic,
            payload);
      }

      List<Message> messages = payloadFormat.format(topic, payload);
      if (messages == null) {
        return;
      }

      for (Message message : messages) {
        if (message == null) {
          continue;
        }
        insertWithBatch(message, session);
      }
    } catch (Throwable t) {
      LOG.warn("onPublish execution exception, msg is [{}], error is ", msg, t);
    } finally {
      // release the payload of the message
      super.onPublish(msg);
    }
  }

  /**
   * Push message to batch queue and handle completion. Dispatches to table or tree logic based on
   * message type.
   */
  private void insertWithBatch(Message message, MqttClientSession session) {
    if (message instanceof TableMessage) {
      doInsertWithBatch(
          (TableMessage) message,
          session,
          TableMessage::getTable,
          (status, m) ->
              status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                  && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(),
          (m, e) ->
              LOG.warn(
                  "meet error when inserting database {}, table {}, tags {}, attributes {}, fields {}, at time {}",
                  m.getDatabase(),
                  m.getTable(),
                  m.getTagKeys(),
                  m.getAttributeKeys(),
                  m.getFields(),
                  m.getTimestamp(),
                  e));
    } else if (message instanceof TreeMessage) {
      doInsertWithBatch(
          (TreeMessage) message,
          session,
          TreeMessage::getDevice,
          (status, m) ->
              status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                  && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(),
          (m, e) ->
              LOG.warn(
                  "meet error when inserting device {}, measurements {}, at time {}",
                  m.getDevice(),
                  m.getMeasurements(),
                  m.getTimestamp(),
                  e));
    }
  }

  private <T extends Message> void doInsertWithBatch(
      T message,
      MqttClientSession session,
      Function<T, String> targetName,
      BiPredicate<TSStatus, T> isFailure,
      BiConsumer<T, Exception> onInsertException) {
    AsyncBatchUtils<T> utils;
    try {
      utils = getOrCreateBatchUtils(session.getClientID());
    } catch (IllegalStateException e) {
      LOG.warn("Server is shutting down, reject msg for client {}", session.getClientID());
      return;
    }
    try {
      CompletableFuture<TSStatus> future = utils.push(message);
      future.whenComplete(
          (status, ex) -> {
            if (ex != null) {
              LOG.warn("Insert failed permanently for {}", targetName.apply(message), ex);
            } else if (isFailure.test(status, message)) {
              LOG.warn("Insert failed for {}: {}", targetName.apply(message), status.message);
            }
          });
    } catch (Exception e) {
      onInsertException.accept(message, e);
    }
  }

  @Override
  public void onSessionLoopError(Throwable throwable) {
    LOG.warn("Session loop error,{}", throwable.getMessage());
  }

  private InsertRowStatement buildStatement(TreeMessage event) {
    try {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(
          DataNodeDevicePathCache.getInstance().getPartialPath(event.getDevice()));
      TimestampPrecisionUtils.checkTimestampPrecision(event.getTimestamp());
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
      return statement;
    } catch (Exception e) {
      LOG.warn("build statement failed", e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Message> AsyncBatchUtils<T> getOrCreateBatchUtils(String clientId) {
    if (closed.get()) {
      throw new IllegalStateException("Server is shutting down");
    }
    return (AsyncBatchUtils<T>)
        clientBatchMap.computeIfAbsent(
            clientId,
            id -> {
              if (useTableInsert) {
                AsyncBatchUtils<TableMessage> utils =
                    new AsyncBatchUtils<>(
                        "mqtt-table-" + id,
                        config.getMqttPayloadBatchIntervalInMS(),
                        config.getMqttPayloadBatchMaxQueueBytes(),
                        new MqttTableInsertConsumer(id));
                utils.start();
                LOG.info("create table batch utils for client {}", id);
                return utils;
              } else {
                AsyncBatchUtils<TreeMessage> utils =
                    new AsyncBatchUtils<>(
                        "mqtt-tree-" + id,
                        config.getMqttPayloadBatchIntervalInMS(),
                        config.getMqttPayloadBatchMaxQueueBytes(),
                        new MqttTreeInsertConsumer(id));
                utils.start();
                LOG.info("create tree batch utils for client {}", id);
                return utils;
              }
            });
  }

  private class MqttTreeInsertConsumer implements AsyncBatchUtils.BatchConsumer<TreeMessage> {
    private final String clientId;

    public MqttTreeInsertConsumer(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public ExecutionResult consume(List<TreeMessage> batch) {
      if (batch == null || batch.isEmpty()) {
        return new ExecutionResult(null, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      }

      // Build InsertRowStatement list from TreeMessage batch
      List<InsertRowStatement> statements = new ArrayList<>();
      for (TreeMessage message : batch) {
        InsertRowStatement stmt = buildStatement(message);
        if (stmt != null) {
          statements.add(stmt);
        }
      }

      if (statements.isEmpty()) {
        return new ExecutionResult(null, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      }

      InsertRowsStatement rowsStatement = new InsertRowsStatement();
      rowsStatement.setInsertRowStatementList(statements);

      MqttClientSession session = clientIdToSessionMap.get(clientId);

      // Check privilege
      TSStatus status =
          AuthorityChecker.checkAuthority(
              rowsStatement,
              new TreeAccessCheckContext(
                  session.getUserId(), session.getUsername(), session.getClientAddress()));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return new ExecutionResult(null, status);
      }

      long queryId = sessionManager.requestQueryId();
      return Coordinator.getInstance()
          .executeForTreeModel(
              rowsStatement,
              queryId,
              sessionManager.getSessionInfo(session),
              "",
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold(),
              false);
    }
  }

  private class MqttTableInsertConsumer implements AsyncBatchUtils.BatchConsumer<TableMessage> {
    private final String clientId;

    public MqttTableInsertConsumer(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public ExecutionResult consume(List<TableMessage> batch) {
      if (batch == null || batch.isEmpty()) {
        return new ExecutionResult(null, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      }

      try {
        MqttClientSession session = clientIdToSessionMap.get(clientId);
        if (session == null) {
          LOG.warn("Session not found for client {}", clientId);
          return new ExecutionResult(
              null,
              new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                  .setMessage("Session not found"));
        }

        // Group messages by (database, table) to ensure data consistency
        // Messages with different tables cannot be merged into one statement
        java.util.Map<String, List<TableMessage>> groupedMessages =
            batch.stream()
                .collect(Collectors.groupingBy(msg -> msg.getDatabase() + "." + msg.getTable()));

        // Execute batch insert for each group
        TSStatus finalStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        int successCount = 0;
        int failCount = 0;

        for (java.util.Map.Entry<String, List<TableMessage>> entry : groupedMessages.entrySet()) {
          ExecutionResult result = executeBatchInsert(entry.getValue(), session);
          if (result.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
            successCount += entry.getValue().size();
          } else {
            failCount += entry.getValue().size();
            finalStatus = result.status; // Keep last error status
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Batch insert completed for client {}: {} groups, {} success, {} failed",
              clientId,
              groupedMessages.size(),
              successCount,
              failCount);
        }

        return new ExecutionResult(null, finalStatus);
      } catch (Exception e) {
        LOG.warn("Failed to execute table batch insert for client {}", clientId, e);
        return new ExecutionResult(
            null,
            new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                .setMessage(e.getMessage()));
      }
    }

    private ExecutionResult executeBatchInsert(
        List<TableMessage> messages, MqttClientSession session) {
      try {
        // Set session context
        String databaseName = messages.get(0).getDatabase().toLowerCase();
        session.setDatabaseName(databaseName);
        session.setSqlDialect(SqlDialect.TABLE);

        // Construct batch InsertTabletStatement
        Tablet tablet = constructTabletFromMultipleMessages(messages);
        if (!RpcUtils.checkSorted(tablet)) {
          RpcUtils.sortTablet(tablet);
        }
        InsertTabletStatement insertTabletStatement =
            new InsertTabletStatement(tablet, true, databaseName);

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

        TSStatus tsStatus = result.status;
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Batch insert result for {} rows in table {}: {}",
              messages.size(),
              messages.get(0).getTable(),
              tsStatus);
        }
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && tsStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          LOG.warn(
              "mqtt table batch insert error for table {}, code={}, message={}",
              messages.get(0).getTable(),
              tsStatus.getCode(),
              tsStatus.getMessage());
        }

        return result;
      } catch (Exception e) {
        LOG.warn("Failed to execute batch insert for table {}", messages.get(0).getTable(), e);
        return new ExecutionResult(
            null,
            new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                .setMessage(e.getMessage()));
      }
    }
  }
}
