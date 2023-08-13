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

package org.apache.iotdb.db.pipe.connector.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.session.util.SessionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;

public abstract class IoTDBThriftConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftConnector.class);

  protected final List<TEndPoint> nodeUrls = new ArrayList<>();

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (boolean) args[0] || ((boolean) args[1] && (boolean) args[2]),
        String.format(
            "Either %s or %s:%s must be specified",
            CONNECTOR_IOTDB_NODE_URLS_KEY, CONNECTOR_IOTDB_IP_KEY, CONNECTOR_IOTDB_PORT_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final Set<TEndPoint> givenNodeUrls = new HashSet<>(nodeUrls);

    if (parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY)
        && parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY)) {
      givenNodeUrls.add(
          new TEndPoint(
              parameters.getString(CONNECTOR_IOTDB_IP_KEY),
              parameters.getInt(CONNECTOR_IOTDB_PORT_KEY)));
    }

    if (parameters.hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY)) {
      givenNodeUrls.addAll(
          SessionUtils.parseSeedNodeUrls(
              Arrays.asList(parameters.getString(CONNECTOR_IOTDB_NODE_URLS_KEY).split(","))));
    }

    nodeUrls.clear();
    nodeUrls.addAll(givenNodeUrls);

    LOGGER.info("IoTDBThriftConnector nodeUrls: {}", nodeUrls);
  }
}
