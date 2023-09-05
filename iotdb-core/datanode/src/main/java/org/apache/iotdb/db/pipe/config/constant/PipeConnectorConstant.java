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

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class PipeConnectorConstant {

  public static final String CONNECTOR_KEY = "connector";

  public static final String CONNECTOR_IOTDB_IP_KEY = "connector.ip";
  public static final String CONNECTOR_IOTDB_PORT_KEY = "connector.port";
  public static final String CONNECTOR_IOTDB_NODE_URLS_KEY = "connector.node-urls";

  public static final String CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY = "connector.batch.enable";
  public static final boolean CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE = true;

  public static final String CONNECTOR_IOTDB_BATCH_DELAY_KEY = "connector.batch.max-delay-seconds";
  public static final int CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE = 1;

  public static final String CONNECTOR_IOTDB_BATCH_SIZE_KEY = "connector.batch.size-bytes";
  public static final long CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE = 16 * MB;

  public static final String CONNECTOR_IOTDB_USER_KEY = "connector.user";
  public static final String CONNECTOR_IOTDB_USER_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_IOTDB_PASSWORD_KEY = "connector.password";
  public static final String CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY =
      "connector.air-gap.handshake-timeout-ms";
  public static final int CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE = 5000;

  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_KEY = "connector.version";
  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_DEFAULT_VALUE = "1.1";

  public static final String CONNECTOR_WEBSOCKET_PORT_KEY = "connector.websocket.port";
  public static final int CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE = 8080;

  private PipeConnectorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
