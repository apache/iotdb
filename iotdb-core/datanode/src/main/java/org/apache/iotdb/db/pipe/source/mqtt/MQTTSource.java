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

package org.apache.iotdb.db.pipe.source.mqtt;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.protocol.mqtt.BrokerAuthenticator;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTTExtractor is an external Extractor that uses the MQTT protocol to receive data. It starts an
 * MQTT broker and listens for incoming messages, which are then processed and passed to the pending
 * queue.
 */
@TreeModel
@TableModel
public class MQTTSource implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTSource.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public static final String WILD_CARD_ADDRESS = "0.0.0.0";

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected IConfig brokerConfig;
  protected List<InterceptHandler> handlers;
  protected IAuthenticator authenticator;
  private final Server server = new Server();

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    if (!validator
        .getParameters()
        .getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_KEY,
                PipeSourceConstant.EXTERNAL_SOURCE_SINGLE_INSTANCE_PER_NODE_KEY),
            PipeSourceConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_DEFAULT_VALUE)) {
      throw new PipeParameterNotValidException("single mode should be true in MQTT extractor");
    }
    if(checkMoquetteConfigConflict(
        validator
            .getParameters()
            .getStringOrDefault(
                PipeSourceConstant.MQTT_BROKER_HOST_KEY,
                PipeSourceConstant.MQTT_BROKER_HOST_DEFAULT_VALUE),
        Integer.parseInt(
            validator
                .getParameters()
                .getStringOrDefault(
                    PipeSourceConstant.MQTT_BROKER_PORT_KEY,
                    PipeSourceConstant.MQTT_BROKER_PORT_DEFAULT_VALUE)),
        validator
            .getParameters()
            .getStringOrDefault(
                PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_KEY,
                PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE))) {
      throw new PipeParameterNotValidException("Moquette config is not valid");
    }

    if (CONFIG.isEnableMQTTService()) {
      if (Objects.equals(
          CONFIG.getMqttDataPath(),
          validator
              .getParameters()
              .getStringOrDefault(
                  PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_KEY,
                  PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE))) {
        throw new PipeParameterNotValidException(
            "The data path of the MQTT extractor is used by the MQTT service. "
                + "Please change the data path of the MQTT extractor.");
      }
      if (Objects.equals(
              CONFIG.getMqttHost(),
              validator
                  .getParameters()
                  .getStringOrDefault(
                      PipeSourceConstant.MQTT_BROKER_HOST_KEY,
                      PipeSourceConstant.MQTT_BROKER_HOST_DEFAULT_VALUE))
          || ((WILD_CARD_ADDRESS.equals(CONFIG.getMqttHost())
                  || WILD_CARD_ADDRESS.equals(
                      validator
                          .getParameters()
                          .getStringOrDefault(
                              PipeSourceConstant.MQTT_BROKER_HOST_KEY,
                              PipeSourceConstant.MQTT_BROKER_HOST_DEFAULT_VALUE)))
              && Objects.equals(
                  String.valueOf(CONFIG.getMqttPort()),
                  validator
                      .getParameters()
                      .getStringOrDefault(
                          PipeSourceConstant.MQTT_BROKER_PORT_KEY,
                          PipeSourceConstant.MQTT_BROKER_PORT_DEFAULT_VALUE)))) {
        throw new PipeParameterNotValidException(
            "The host and port of the MQTT extractor are used by the MQTT service. "
                + "Please change the host and port of the MQTT extractor.");
      }
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    brokerConfig = createBrokerConfig(parameters);
    handlers = new ArrayList<>(1);
    handlers.add(new MQTTPublishHandler(parameters, environment, pendingQueue));
    authenticator = new BrokerAuthenticator();
  }

  private IConfig createBrokerConfig(final PipeParameters pipeParameters) {
    final Properties properties = new Properties();
    properties.setProperty(
        BrokerConstants.HOST_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_BROKER_HOST_KEY,
            PipeSourceConstant.MQTT_BROKER_HOST_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.PORT_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_BROKER_PORT_KEY,
            PipeSourceConstant.MQTT_BROKER_PORT_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_KEY,
            String.valueOf(
                PipeSourceConstant.MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_DEFAULT_VALUE)));
    properties.setProperty(
        BrokerConstants.DATA_PATH_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_KEY,
            PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_KEY,
            String.valueOf(
                PipeSourceConstant.MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_DEFAULT_VALUE)));
    properties.setProperty(
        BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_KEY,
            String.valueOf(PipeSourceConstant.MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_DEFAULT_VALUE)));
    properties.setProperty(
        BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_KEY,
            String.valueOf(
                PipeSourceConstant.MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_DEFAULT_VALUE)));
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeSourceConstant.MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_KEY,
            String.valueOf(PipeSourceConstant.MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_DEFAULT_VALUE)));
    return new MemoryConfig(properties);
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);

    try {
      server.startServer(brokerConfig, handlers, null, authenticator, null);
    } catch (MVStoreException e) {
      throw new PipeException(
          "Failed to start MQTT Extractor "
              + pipeName
              + ". The data file is used by another MQTT Source or MQTT Service. "
              + "please check if another MQTT service is using the same data path: "
              + brokerConfig.getProperty(BrokerConstants.DATA_PATH_PROPERTY_NAME),
          e);
    } catch (Exception e) {
      throw new PipeException(
          "Failed to start MQTT Extractor "
              + pipeName
              + ". The port might already be in use, or there could be other issues. "
              + "Please check the logs for more details.",
          e);
    }

    LOGGER.info(
        "Start MQTT Extractor successfully, listening on ip {}, port {}",
        brokerConfig.getProperty(BrokerConstants.HOST_PROPERTY_NAME),
        brokerConfig.getProperty(BrokerConstants.PORT_PROPERTY_NAME));
  }

  @Override
  public Event supply() throws Exception {
    return isClosed.get() ? null : pendingQueue.directPoll();
  }

  @Override
  public void close() throws Exception {
    if (!isClosed.get()) {
      server.stopServer();
      isClosed.set(true);
    }
  }

  public static boolean checkMoquetteConfigConflict(String ip, int port, String dataPath)  {
    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(ip, port));
    } catch (IOException e) {
      return true;
    }

    File file = Paths.get(dataPath).resolve("moquette_store.h2").toAbsolutePath().toFile();
    return file.exists() && file.canWrite();
  }

}
