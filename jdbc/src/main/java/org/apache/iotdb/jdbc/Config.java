/**
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
package org.apache.iotdb.jdbc;

public class Config {

  /**
   * The required prefix for the connection URL.
   */
  public static final String IOTDB_URL_PREFIX = "jdbc:iotdb://";

  public static final String IOTDB_DEFAULT_HOST = "localhost";
  /**
   * If host is provided, without a port.
   */
  public static final int IOTDB_DEFAULT_PORT = 6667;

  /**
   * tsfile's default series name.
   */
  public static final String DEFAULT_SERIES_NAME = "default";

  public static final String AUTH_USER = "user";
  public static final String DEFAULT_USER = "user";

  public static final String AUTH_PASSWORD = "password";
  public static final String DEFALUT_PASSWORD = "password";

  public static final int RETRY_NUM = 3;
  public static final long RETRY_INTERVAL = 1000;

  public static int fetchSize = 10000;
  public static int connectionTimeoutInMs = 0;

  public static String JDBC_DRIVER_NAME = "org.apache.iotdb.jdbc.IoTDBDriver";

}
