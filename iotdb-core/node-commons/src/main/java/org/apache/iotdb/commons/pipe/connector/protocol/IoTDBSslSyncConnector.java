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

package org.apache.iotdb.commons.pipe.connector.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK;

public abstract class IoTDBSslSyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSslSyncConnector.class);

  protected IoTDBSyncClientManager clientManager;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();

    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();

    validator.validate(
        args -> !((boolean) args[0]) || ((boolean) args[1] && (boolean) args[2]),
        String.format(
            "When ssl transport is enabled, %s and %s must be specified",
            SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY),
        IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
            || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
            || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false),
        parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
        parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // ssl transport configuration
    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();
    final boolean useSSL =
        IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
            || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
            || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false);
    final String trustStorePath = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY);
    final String trustStorePwd = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY);

    // leader cache configuration
    final boolean useLeaderCache =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
            CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE);

    clientManager =
        constructClient(nodeUrls, useSSL, trustStorePath, trustStorePwd, useLeaderCache);
  }

  protected abstract IoTDBSyncClientManager constructClient(
      List<TEndPoint> nodeUrls,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache);

  @Override
  public void handshake() throws Exception {
    clientManager.checkClientStatusAndTryReconstructIfNecessary();
  }

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
    }
  }

  @Override
  public void close() {
    if (clientManager != null) {
      clientManager.close();
    }
  }
}
