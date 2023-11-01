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

package org.apache.iotdb.db.pipe.config.constant;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class PipeConnectorConstant {

  public static final String CONNECTOR_KEY = "connector";
  public static final String SINK_KEY = "sink";

  public static final String CONNECTOR_IOTDB_IP_KEY = "connector.ip";
  public static final String SINK_IOTDB_IP_KEY = "sink.ip";
  public static final String CONNECTOR_IOTDB_PORT_KEY = "connector.port";
  public static final String SINK_IOTDB_PORT_KEY = "sink.port";
  public static final String CONNECTOR_IOTDB_NODE_URLS_KEY = "connector.node-urls";
  public static final String SINK_IOTDB_NODE_URLS_KEY = "sink.node-urls";

  public static final String CONNECTOR_IOTDB_PARALLEL_TASKS_KEY = "connector.parallel.tasks";
  public static final String SINK_IOTDB_PARALLEL_TASKS_KEY = "sink.parallel.tasks";
  public static final int CONNECTOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE =
      PipeConfig.getInstance().getPipeSubtaskExecutorMaxThreadNum();

  public static final String CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY = "connector.batch.enable";
  public static final String SINK_IOTDB_BATCH_MODE_ENABLE_KEY = "sink.batch.enable";
  public static final boolean CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE = true;

  public static final String CONNECTOR_IOTDB_BATCH_DELAY_KEY = "connector.batch.max-delay-seconds";
  public static final String SINK_IOTDB_BATCH_DELAY_KEY = "sink.batch.max-delay-seconds";
  public static final int CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE = 1;

  public static final String CONNECTOR_IOTDB_BATCH_SIZE_KEY = "connector.batch.size-bytes";
  public static final String SINK_IOTDB_BATCH_SIZE_KEY = "sink.batch.size-bytes";
  public static final long CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE = 16 * MB;

  public static final String CONNECTOR_IOTDB_USER_KEY = "connector.user";
  public static final String SINK_IOTDB_USER_KEY = "sink.user";
  public static final String CONNECTOR_IOTDB_USER_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_IOTDB_PASSWORD_KEY = "connector.password";
  public static final String SINK_IOTDB_PASSWORD_KEY = "sink.password";
  public static final String CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY =
      "connector.air-gap.e-language.enable";
  public static final String SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY = "sink.air-gap.e-language.enable";
  public static final boolean CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE = false;

  public static final String CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY =
      "connector.air-gap.handshake-timeout-ms";
  public static final String SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY =
      "sink.air-gap.handshake-timeout-ms";
  public static final int CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE = 5000;

  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_KEY = "connector.version";
  public static final String SINK_IOTDB_SYNC_CONNECTOR_VERSION_KEY = "sink.version";
  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_DEFAULT_VALUE = "1.1";

  public static final String CONNECTOR_WEBSOCKET_PORT_KEY = "connector.websocket.port";
  public static final String SINK_WEBSOCKET_PORT_KEY = "sink.websocket.port";
  public static final int CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE = 8080;

  public static final String CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY = "connector.opcua.tcp.port";
  public static final String SINK_OPC_UA_TCP_BIND_PORT_KEY = "sink.opcua.tcp.port";
  public static final int CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE = 12686;

  public static final String CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY = "connector.opcua.https.port";
  public static final String SINK_OPC_UA_HTTPS_BIND_PORT_KEY = "sink.opcua.https.port";
  public static final int CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE = 8443;

  public static final String CONNECTOR_OPC_UA_SECURITY_DIR_KEY = "connector.opcua.security.dir";
  public static final String SINK_OPC_UA_SECURITY_DIR_KEY = "sink.opcua.security.dir";
  public static final String CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE =
      IoTDBDescriptor.getInstance().getConfDir() != null
          ? IoTDBDescriptor.getInstance().getConfDir() + File.separatorChar + "opc_security"
          : System.getProperty("user.home") + File.separatorChar + "iotdb_opc_security";

  private PipeConnectorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
