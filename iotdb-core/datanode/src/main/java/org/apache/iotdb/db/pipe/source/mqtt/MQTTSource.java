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
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.protocol.mqtt.BrokerAuthenticator;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatManager;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected PayloadFormatter payloadFormat;
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

    validateMoquetteConfig(validator.getParameters());
  }

  public void validateMoquetteConfig(final PipeParameters parameters) {
    final String sqlDialect =
        parameters.getStringOrDefault(
            SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final String formatType =
        parameters.getStringOrDefault(
            PipeSourceConstant.MQTT_PAYLOAD_FORMATTER_KEY,
            SystemConstant.SQL_DIALECT_TREE_VALUE.equals(sqlDialect)
                ? PipeSourceConstant.MQTT_PAYLOAD_FORMATTER_TREE_DIALECT_VALUE
                : PipeSourceConstant.MQTT_PAYLOAD_FORMATTER_TABLE_DIALECT_VALUE);
    try {
      payloadFormat = PayloadFormatManager.getPayloadFormat(formatType);
    } catch (IllegalArgumentException e) {
      throw new PipeParameterNotValidException("Invalid payload format type: " + formatType);
    }

    final String ip =
        parameters.getStringOrDefault(
            PipeSourceConstant.MQTT_BROKER_HOST_KEY,
            PipeSourceConstant.MQTT_BROKER_HOST_DEFAULT_VALUE);
    final int port =
        Integer.parseInt(
            parameters.getStringOrDefault(
                PipeSourceConstant.MQTT_BROKER_PORT_KEY,
                PipeSourceConstant.MQTT_BROKER_PORT_DEFAULT_VALUE));
    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(ip, port));
    } catch (IOException e) {
      throw new PipeParameterNotValidException(
          "Cannot bind MQTT broker to "
              + ip
              + ":"
              + port
              + ". The port might already be in use, or the IP address is invalid.");
    }

    final String dataPath =
        parameters.getStringOrDefault(
            PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_KEY,
            PipeSourceConstant.MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE);
    final File file = Paths.get(dataPath).resolve("moquette_store.h2").toAbsolutePath().toFile();
    if (file.exists()) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
          FileChannel channel = raf.getChannel();
          FileLock lock = channel.tryLock()) {
        if (lock == null) {
          throw new PipeParameterNotValidException(
              " The data file is used by another MQTT Source or MQTT Service. "
                  + "Please use another data path");
        }
      } catch (Exception e) {
        throw new PipeParameterNotValidException(
            " The data file is used by another MQTT Source or MQTT Service. "
                + "Please use another data path");
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
    handlers.add(new MQTTPublishHandler(payloadFormat, environment, pendingQueue));
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
    } catch (Exception e) {
      throw new PipeException(
          "Failed to start MQTT Extractor "
              + pipeName
              + ". Possible reasons: the data file might be used by another MQTT Source or MQTT Service, "
              + "the port might already be in use, or there could be other issues. "
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
    if (!isClosed.get() && hasBeenStarted.get()) {
      server.stopServer();
      isClosed.set(true);
    }
  }
}
