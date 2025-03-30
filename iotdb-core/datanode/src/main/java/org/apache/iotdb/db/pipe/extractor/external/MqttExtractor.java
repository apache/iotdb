package org.apache.iotdb.db.pipe.extractor.external;

import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatManager;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TreeMessage;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MqttExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttExtractor.class);

  private PayloadFormatter payloadFormat;
  private String brokerUrl;
  private String topic;
  private BlockingConnection connection;
  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final AtomicLong index = new AtomicLong(0);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    payloadFormat = PayloadFormatManager.getPayloadFormat("json");
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    brokerUrl = parameters.getString("brokerUrl");
    topic = parameters.getString("topic");
  }

  @Override
  public void start() throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setHost(brokerUrl);
    mqtt.setUserName("root");
    mqtt.setPassword("root");
    mqtt.setConnectAttemptsMax(3);
    mqtt.setReconnectDelay(10);
    try {
      connection = mqtt.blockingConnection();
      connection.connect();
      // Subscribe to the topic
      Topic[] topics = {new Topic(topic, QoS.AT_LEAST_ONCE)};
      connection.subscribe(topics);
    } catch (Exception e) {
      LOGGER.error("Failed to connect to broker", e);
    }
    // Start a thread to continuously poll messages
    new Thread(
            () -> {
              while (true) {
                try {
                  org.fusesource.mqtt.client.Message msg = connection.receive();
                  if (msg != null) {
                    ByteBuf payload = Unpooled.wrappedBuffer(msg.getPayload());

                    extractPayload(payload);
                    msg.ack();
                  }
                } catch (Exception e) {
                  // Log or handle exceptions
                  LOGGER.warn(e.getMessage());
                }
              }
            },
            "MqttExtractor-MessagePoller")
        .start();
  }

  private void extractPayload(ByteBuf payload) {
    List<Message> messages = payloadFormat.format(payload);
    if (messages == null) {
      return;
    }

    for (Message message : messages) {
      if (message == null) {
        continue;
      }
      try {
        EnrichedEvent event = generateEvent((TreeMessage) message);
        if (!event.increaseReferenceCount(PipeExternalExtractor.class.getName())) {
          LOGGER.warn(
              "The reference count of the event {} cannot be increased, skipping it.", event);
          continue;
        }
        pendingQueue.waitedOffer(event);
      } catch (Exception e) {
        LOGGER.error("Error polling external source", e);
      }
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
        true,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        false);
  }

  @Override
  public Event supply() throws Exception {
    if (isClosed.get()) {
      return null;
    }
    EnrichedEvent event = pendingQueue.directPoll();
    if (Objects.nonNull(event)) {
      event.bindProgressIndex(new IoTProgressIndex(1, index.getAndIncrement()));
    }
    return event;
  }

  @Override
  public void close() throws Exception {
    if (connection != null && connection.isConnected()) {
      connection.disconnect();
    }
  }
}
