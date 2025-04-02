package org.apache.iotdb.db.pipe.extractor.external.MQTT;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.external.PipeExternalExtractor;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatManager;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TableMessage;
import org.apache.iotdb.db.protocol.mqtt.TreeMessage;
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
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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

/** PublishHandler handle the messages from MQTT clients. */
public class MQTTPublishHandler extends AbstractInterceptHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTPublishHandler.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final SessionManager sessionManager = SessionManager.getInstance();

  private final ConcurrentHashMap<String, MqttClientSession> clientIdToSessionMap =
      new ConcurrentHashMap<>();
  private final PayloadFormatter payloadFormat;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final boolean useTableInsert;
  private final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue;
  private final String pipeName;
  private final long creationTime;
  private final PipeTaskMeta pipeTaskMeta;

  public MQTTPublishHandler(
      PipeParameters pipeParameters,
      PipeTaskExtractorRuntimeEnvironment environment,
      UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue) {
    this.payloadFormat =
        PayloadFormatManager.getPayloadFormat(
            pipeParameters.getStringOrDefault("payloadFormat", "json"));
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
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
      MqttClientSession session = new MqttClientSession(msg.getClientID());
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
    MqttClientSession session = clientIdToSessionMap.remove(msg.getClientID());
    if (null != session) {
      sessionManager.removeCurrSession();
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

      LOGGER.debug(
          "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}",
          clientId,
          username,
          qos,
          topic,
          payload);

      List<Message> messages = payloadFormat.format(payload);
      if (messages == null) {
        return;
      }

      for (Message message : messages) {
        if (message == null) {
          continue;
        }
        if (useTableInsert) {
          TableMessage tableMessage = (TableMessage) message;
          // '/' previously defined as a database name
          String database =
              !msg.getTopicName().contains("/")
                  ? msg.getTopicName()
                  : msg.getTopicName().substring(0, msg.getTopicName().indexOf("/"));
          tableMessage.setDatabase(database);
          ExtractTable(tableMessage, session);
        } else {
          ExtractTree((TreeMessage) message, session);
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
  private void ExtractTable(TableMessage message, MqttClientSession session) {
    TSStatus tsStatus = null;
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      InsertTabletStatement insertTabletStatement = constructInsertTabletStatement(message);
      tsStatus = AuthorityChecker.checkAuthority(insertTabletStatement, session);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(tsStatus.message);
      } else {
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
        LOGGER.debug("process result: {}", tsStatus);
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn("mqtt line insert error , message = {}", tsStatus.message);
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
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

  private void ExtractTree(TreeMessage message, MqttClientSession session) {
    try {
      EnrichedEvent event = generateEvent((TreeMessage) message);
      if (!event.increaseReferenceCount(PipeExternalExtractor.class.getName())) {
        LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
      }
      pendingQueue.waitedOffer(event);
    } catch (Exception e) {
      LOGGER.error("Error polling external source", e);
    }
  }

  private EnrichedEvent generateEvent(TreeMessage message) throws QueryProcessException {
    String deviceId = message.getDevice();
    List<String> measurements = message.getMeasurements();
    long timestamp = message.getTimestamp();
    final MeasurementSchema[] schemas = new MeasurementSchema[measurements.size()];
    TSDataType[] dataTypes =
        message.getDataTypes() == null
            ? new TSDataType[message.getMeasurements().size()]
            : message.getDataTypes().toArray(new TSDataType[0]);
    List<String> values = message.getValues();
    if (message.getDataTypes() == null) {
      for (int index = 0; index < values.size(); ++index) {
        dataTypes[index] = TypeInferenceUtils.getPredictedDataType(values.get(index), true);
      }
    }

    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }
    Object[] inferredValues = new Object[values.size()];
    // For each measurement value, parse it and then wrap it in a one-dimensional array.
    for (int i = 0; i < values.size(); ++i) {
      Object parsedValue = CommonUtils.parseValue(dataTypes[i], values.get(i));
      // Wrap the parsed value into a one-dimensional array based on its type.
      if (parsedValue instanceof Integer) {
        inferredValues[i] = new int[] {(Integer) parsedValue};
      } else if (parsedValue instanceof Long) {
        inferredValues[i] = new long[] {(Long) parsedValue};
      } else if (parsedValue instanceof Float) {
        inferredValues[i] = new float[] {(Float) parsedValue};
      } else if (parsedValue instanceof Double) {
        inferredValues[i] = new double[] {(Double) parsedValue};
      } else if (parsedValue instanceof Boolean) {
        inferredValues[i] = new boolean[] {(Boolean) parsedValue};
      } else if (parsedValue instanceof String) {
        inferredValues[i] = new String[] {(String) parsedValue};
      } else {
        // For any other type, wrap it as an Object array.
        inferredValues[i] = new Object[] {parsedValue};
      }
    }
    BitMap[] bitMapsForInsertRowNode = new BitMap[schemas.length];

    Tablet tabletForInsertRowNode =
        new Tablet(
            deviceId,
            Arrays.asList(schemas),
            new long[] {timestamp},
            inferredValues,
            bitMapsForInsertRowNode,
            1);

    return new PipeRawTabletInsertionEvent(
        false,
        deviceId,
        null,
        null,
        tabletForInsertRowNode,
        false,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        false);
  }

  @Override
  public void onSessionLoopError(Throwable throwable) {
    // TODO: Implement something sensible here ...
  }
}
