package org.apache.iotdb.db.pipe.extractor.external.MQTT;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.protocol.mqtt.BrokerAuthenticator;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MqttExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttExtractor.class);

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected IConfig config;
  protected List<InterceptHandler> handlers;
  protected IAuthenticator authenticator;



  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final AtomicLong index = new AtomicLong(0);
  private final Server server = new Server();

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {

  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    config = createBrokerConfig(parameters);
    handlers = new ArrayList<>(1);
    handlers.add(new MQTTPublishHandler(parameters,environment,pendingQueue));
    authenticator = new BrokerAuthenticator();
  }

  @Override
  public void start() throws Exception {
    try {
      server.startServer(config, handlers, null, authenticator, null);
    } catch (IOException e) {
      throw new RuntimeException("Exception while starting server", e);
    }

    LOGGER.info(
            "Start MQTT Extractor successfully"
            );

    Runtime.getRuntime()
            .addShutdownHook(
                    new Thread(
                            () -> {
                              LOGGER.info("Stopping IoTDB MQTT EXTRACTOR...");
                              shutdown();
                              LOGGER.info("MQTT EXTRACTOR stopped.");
                            }));
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
    if(!isClosed.get()){
      shutdown();
      isClosed.set(true);
    }
  }
  private IConfig createBrokerConfig(PipeParameters pipeParameters) {
    Properties properties = new Properties();
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, pipeParameters.getStringOrDefault("host","127.0.0.1"));
    properties.setProperty(
            BrokerConstants.PORT_PROPERTY_NAME, pipeParameters.getStringOrDefault("port","1883"));
    properties.setProperty(
            BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
            pipeParameters.getStringOrDefault("threadPoolSize","1"));
    properties.setProperty(
            BrokerConstants.DATA_PATH_PROPERTY_NAME,pipeParameters.getStringOrDefault("dataPath","C:/Users/22503/Desktop/mqtt/data"));
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME,"true");
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    properties.setProperty(
            BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
            String.valueOf(pipeParameters.getIntOrDefault("maxBytes",1048576)));
    return new MemoryConfig(properties);
  }

  public void shutdown() {
    server.stopServer();
  }
}
