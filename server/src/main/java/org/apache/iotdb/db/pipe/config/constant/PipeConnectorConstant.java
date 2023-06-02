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

public class PipeConnectorConstant {

  public static final String CONNECTOR_KEY = "connector";

  public static final String CONNECTOR_IOTDB_IP_KEY = "connector.ip";
  public static final String CONNECTOR_IOTDB_PORT_KEY = "connector.port";

  public static final String CONNECTOR_IOTDB_USER_KEY = "connector.user";
  public static final String CONNECTOR_IOTDB_USER_DEFAULT_VALUE = "root";
  public static final String CONNECTOR_IOTDB_PASSWORD_KEY = "connector.password";
  public static final String CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE = "root";

  private PipeConnectorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
