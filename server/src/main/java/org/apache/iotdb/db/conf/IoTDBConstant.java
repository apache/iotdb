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
package org.apache.iotdb.db.conf;

public class IoTDBConstant {

  private IoTDBConstant() {
  }

  public static final String ENV_FILE_NAME = "iotdb-env";
  public static final String IOTDB_CONF = "IOTDB_CONF";
  public static final String GLOBAL_DB_NAME = "IoTDB";
  public static final String VERSION = "0.9.0-SNAPSHOT";
  public static final String REMOTE_JMX_PORT_NAME = "com.sun.management.jmxremote.port";
  public static final String IOTDB_LOCAL_JMX_PORT_NAME = "iotdb.jmx.local.port";
  public static final String IOTDB_REMOTE_JMX_PORT_NAME = "iotdb.jmx.remote.port";
  public static final String SERVER_RMI_ID = "java.rmi.server.randomIDs";
  public static final String RMI_SERVER_HOST_NAME = "java.rmi.server.hostname";
  public static final String JMX_REMOTE_RMI_PORT = "com.sun.management.jmxremote.rmi.port";
  public static final String IOTDB_PACKAGE = "org.apache.iotdb.service";
  public static final String JMX_TYPE = "type";

  public static final long GB = 1024 * 1024 * 1024L;
  public static final long MB = 1024 * 1024L;
  public static final long KB = 1024L;

  public static final String IOTDB_HOME = "IOTDB_HOME";

  public static final String SEQFILE_LOG_NODE_SUFFIX = "-seq";
  public static final String UNSEQFILE_LOG_NODE_SUFFIX = "-unseq";

  public static final String PATH_ROOT = "root";
  public static final char PATH_SEPARATOR = '.';
  public static final String ADMIN_NAME = "root";
  public static final String ADMIN_PW = "root";
  public static final String PROFILE_SUFFIX = ".profile";
  public static final String MAX_TIME = "max_time";
  public static final String MIN_TIME = "min_time";
  public static final int MIN_SUPPORTED_JDK_VERSION = 8;

  // for cluster, set read consistency level
  public static final String SET_READ_CONSISTENCY_LEVEL_PATTERN = "set\\s+read.*level.*";

  public static final String SHOW_FLUSH_TASK_INFO = "show\\s+flush\\s+task\\s+info";

  public static final String SHOW_DYNAMIC_PARAMETERS = "show\\s+dynamic\\s+parameters";

  public static final String ROLE = "role";
  public static final String USER = "user";
  public static final String PRIVILEGE = "privilege";

  public static final String STORAGE_GROUP = "storage group";
  public static final String TTL = "ttl";
}
