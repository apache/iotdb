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

package org.apache.iotdb.commons.pipe.plugin.builtin.connector.schema;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.ddl.ISchemaEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_AUTHORITY_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DATA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_FUNCTION_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_MODEL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_SCHEMA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_TRIGGER_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_TTL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class IoTDBSchemaConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaConnector.class);
  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private boolean enableSchemaSync = false;
  private boolean enableTtlSync = false;
  private boolean enableFunctionSync = false;
  private boolean enableTriggerSync = false;
  private boolean enableModelSync = false;
  private boolean enableAuthoritySync = false;
  private boolean atLeastOneEnable = false;

  private final List<IoTDBThriftSyncConnectorClient> clients = new ArrayList<>();
  private final List<Boolean> isClientAlive = new ArrayList<>();

  private long currentClientIndex = 0;

  public IoTDBSchemaConnector() {
    // Do nothing
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    validator.validate(
        arg -> {
          Set<String> inclusionList =
              new HashSet<>(Arrays.asList(((String) arg).replace(" ", "").split(",")));
          if (inclusionList.contains(EXTRACTOR_INCLUSION_SCHEMA_VALUE)) {
            enableSchemaSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_TTL_VALUE)) {
            enableTtlSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_FUNCTION_VALUE)) {
            enableFunctionSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_TRIGGER_VALUE)) {
            enableTriggerSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_MODEL_VALUE)) {
            enableModelSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_AUTHORITY_VALUE)) {
            enableAuthoritySync = true;
          }
          atLeastOneEnable =
              enableSchemaSync
                  || enableTtlSync
                  || enableFunctionSync
                  || enableTriggerSync
                  || enableModelSync
                  || enableAuthoritySync;
          // If none of above are present and data is also absent, then validation will fail.
          return atLeastOneEnable || inclusionList.contains(EXTRACTOR_INCLUSION_DATA_VALUE);
        },
        String.format(
            "At least one of %s, %s, %s, %s, %s, %s, %s should be present in %s.",
            EXTRACTOR_INCLUSION_DATA_VALUE,
            EXTRACTOR_INCLUSION_SCHEMA_VALUE,
            EXTRACTOR_INCLUSION_TTL_VALUE,
            EXTRACTOR_INCLUSION_FUNCTION_VALUE,
            EXTRACTOR_INCLUSION_TRIGGER_VALUE,
            EXTRACTOR_INCLUSION_MODEL_VALUE,
            EXTRACTOR_INCLUSION_AUTHORITY_VALUE,
            SOURCE_INCLUSION_KEY),
        validator
            .getParameters()
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    if (!atLeastOneEnable) {
      return;
    }

    super.customize(parameters, configuration);

    for (int i = 0; i < nodeUrls.size(); i++) {
      isClientAlive.add(false);
      clients.add(null);
    }
  }

  @Override
  public void handshake() throws Exception {
    if (!atLeastOneEnable) {
      return;
    }

    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // Close the client if necessary
      if (clients.get(i) != null) {
        try {
          clients.set(i, null).close();
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      clients.set(
          i,
          new IoTDBThriftSyncConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs((int) PIPE_CONFIG.getPipeConnectorTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              ip,
              port,
              false,
              null,
              null));

      // TODO: validate client connectivity here, just like in ThriftSync.
      isClientAlive.set(i, true);
      LOGGER.info("Handshake success. Target server ip: {}, port: {}", ip, port);
    }

    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", nodeUrls));
  }

  @Override
  public void heartbeat() throws Exception {
    if (!atLeastOneEnable) {
      return;
    }

    // TODO: heartbeat
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(ISchemaEvent schemaEvent) throws Exception {
    if (!atLeastOneEnable) {
      return;
    }

    // TODO
  }

  @Override
  public void transfer(Event event) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaConnector currently can't transfer heartbeat or generic event.");
  }

  private int nextClientIndex() {
    final int clientSize = clients.size();
    // Round-robin, find the next alive client
    for (int tryCount = 0; tryCount < clientSize; ++tryCount) {
      final int clientIndex = (int) (currentClientIndex++ % clientSize);
      if (Boolean.TRUE.equals(isClientAlive.get(clientIndex))) {
        return clientIndex;
      }
    }
    throw new PipeConnectionException(
        "All clients are dead, please check the connection to the receiver.");
  }

  @Override
  public void close() {
    for (int i = 0; i < clients.size(); ++i) {
      try {
        if (clients.get(i) != null) {
          clients.set(i, null).close();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to close client {}.", i, e);
      } finally {
        isClientAlive.set(i, false);
      }
    }
  }
}
