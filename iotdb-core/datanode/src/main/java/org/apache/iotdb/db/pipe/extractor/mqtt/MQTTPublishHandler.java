package org.apache.iotdb.db.pipe.extractor.mqtt;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
            pipeParameters.getStringOrDefault(PipeExtractorConstant.MQTT_PAYLOAD_FORMATTER_KEY, PipeExtractorConstant.MQTT_PAYLOAD_FORMATTER_DEFAULT_VALUE));
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
          tableMessage.setDatabase(database.toLowerCase());
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
      tsStatus = checkAuthority(session);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(tsStatus.message);
      }else {
        session.setDatabaseName(message.getDatabase().toLowerCase());
        session.setSqlDialect(IClientSession.SqlDialect.TABLE);
        EnrichedEvent event = generateEvent(message);
        if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
          LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
        }
        pendingQueue.waitedOffer(event);
      }
    } catch (Exception e) {
      LOGGER.warn(
              "meet error when extracting message database {}, table {}, tags {}, attributes {}, fields {}, at time {}, because ",
              message.getDatabase(),
              message.getTable(),
              message.getTagKeys(),
              message.getAttributeKeys(),
              message.getFields(),
              message.getTimestamp(),
              e);
    }
  }

  private static TSStatus checkAuthority(MqttClientSession session) {
    TSStatus tsStatus;
    long startTime = System.nanoTime();
    try {
      tsStatus = AuthorityChecker.getTSStatus(
              AuthorityChecker.SUPER_USER.equals(session.getUsername()),
              "Only the admin user can perform this operation");
    } finally {
      PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
    }
    return tsStatus;
  }

  private PipeRawTabletInsertionEvent generateEvent(TableMessage message) {
    List<String> measurements =
        Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    long[] timestamps = new long[] {message.getTimestamp()};
    int columnSize = measurements.size();

    BitMap[] bitMaps = new BitMap[columnSize];
    Object[] columns =
        Stream.of(message.getValues(), message.getTagValues(), message.getAttributeValues())
            .flatMap(List::stream)
            .toArray(Object[]::new);
    TSDataType[] dataTypes = new TSDataType[measurements.size()];
    Tablet.ColumnCategory[] columnCategories = new Tablet.ColumnCategory[measurements.size()];
    for (int i = 0; i < message.getFields().size(); i++) {
      dataTypes[i] = message.getDataTypes().get(i);
      columnCategories[i] = Tablet.ColumnCategory.FIELD;
    }
    for (int i = message.getFields().size();
        i < message.getFields().size() + message.getTagKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = Tablet.ColumnCategory.TAG;
    }
    for (int i = message.getFields().size() + message.getTagKeys().size();
        i
            < message.getFields().size()
                + message.getTagKeys().size()
                + message.getAttributeKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = Tablet.ColumnCategory.ATTRIBUTE;
    }
    final MeasurementSchema[] schemas = new MeasurementSchema[measurements.size()];
    for (int i = 0; i < measurements.size(); i++) {
      schemas[i] =  new MeasurementSchema(measurements.get(i), dataTypes[i]);
    }
    Tablet tabletForInsertRowNode =
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
            message.getDatabase(),
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

  private void ExtractTree(TreeMessage message, MqttClientSession session) {
    TSStatus tsStatus = null;
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
        tsStatus = checkAuthority(session);
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(tsStatus.message);
        }else {
          EnrichedEvent event = generateEvent(message);
          if (!event.increaseReferenceCount(MQTTPublishHandler.class.getName())) {
            LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
          }
          pendingQueue.waitedOffer(event);
        }
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
