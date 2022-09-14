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
package org.apache.iotdb.commons.conf;

import java.util.HashSet;
import java.util.Set;

public class IoTDBConstant {

  private IoTDBConstant() {}

  public static final String ENV_FILE_NAME = "datanode-env";
  public static final String IOTDB_CONF = "IOTDB_CONF";
  public static final String GLOBAL_DB_NAME = "IoTDB";

  public static final String RPC_ADDRESS = "rpc_address";
  public static final String RPC_PORT = "rpc_port";
  public static final String INTERNAL_ADDRESS = "internal_address";
  public static final String INTERNAL_PORT = "internal_port";
  public static final String CONSENSUS_PORT = "consensus_port";
  public static final String TARGET_CONFIG_NODES = "target_config_nodes";

  // when running the program in IDE, we can not get the version info using
  // getImplementationVersion()
  public static final String VERSION =
      IoTDBConstant.class.getPackage().getImplementationVersion() != null
          ? IoTDBConstant.class.getPackage().getImplementationVersion()
          : "UNKNOWN";
  public static final String MAJOR_VERSION =
      "UNKNOWN".equals(VERSION)
          ? "UNKNOWN"
          : VERSION.split("\\.")[0] + "." + VERSION.split("\\.")[1];

  public static final String AUDIT_LOGGER_NAME = "IoTDB_AUDIT_LOGGER";
  public static final String SLOW_SQL_LOGGER_NAME = "SLOW_SQL";
  public static final String COMPACTION_LOGGER_NAME = "COMPACTION";

  public static final String IOTDB_JMX_PORT = "iotdb.jmx.port";

  public static final String IOTDB_PACKAGE = "org.apache.iotdb.service";
  public static final String IOTDB_THREADPOOL_PACKAGE = "org.apache.iotdb.threadpool";
  public static final String JMX_TYPE = "type";

  public static final long GB = 1024 * 1024 * 1024L;
  public static final long MB = 1024 * 1024L;
  public static final long KB = 1024L;

  public static final String IOTDB_HOME = "IOTDB_HOME";

  public static final String SEQFILE_LOG_NODE_SUFFIX = "-seq";
  public static final String UNSEQFILE_LOG_NODE_SUFFIX = "-unseq";

  public static final String PATH_ROOT = "root";
  public static final char PATH_SEPARATOR = '.';
  public static final String PROFILE_SUFFIX = ".profile";
  public static final String MAX_TIME = "max_time";
  public static final String MIN_TIME = "min_time";
  public static final String LAST_VALUE = "last_value";
  public static final int MIN_SUPPORTED_JDK_VERSION = 8;
  public static final Set<String> reservedWords =
      new HashSet<String>() {
        {
          add("TIME");
          add("TIMESTAMP");
          add("ROOT");
        }
      };

  // show info
  public static final String COLUMN_ITEM = "                             item";
  public static final String COLUMN_VALUE = "value";
  public static final String COLUMN_VERSION = "        version";
  public static final String COLUMN_TIMESERIES = "timeseries";
  public static final String COLUMN_TIMESERIES_ALIAS = "alias";
  public static final String COLUMN_TIMESERIES_DATATYPE = "dataType";
  public static final String COLUMN_TIMESERIES_ENCODING = "encoding";
  public static final String COLUMN_TIMESERIES_COMPRESSION = "compression";
  public static final String COLUMN_TIMESERIES_COMPRESSOR = "compressor";
  public static final String COLUMN_CHILD_PATHS = "child paths";
  public static final String COLUMN_CHILD_PATHS_TYPES = "node types";
  public static final String COLUMN_CHILD_NODES = "child nodes";
  public static final String COLUMN_DEVICES = "devices";
  public static final String COLUMN_COLUMN = "column";
  public static final String COLUMN_COUNT = "count";
  public static final String COLUMN_TAGS = "tags";
  public static final String COLUMN_ATTRIBUTES = "attributes";
  public static final String COLUMN_IS_ALIGNED = "isAligned";
  public static final String COLUMN_DISTRIBUTION_PLAN = "distribution plan";
  public static final String QUERY_ID = "queryId";
  public static final String STATEMENT = "statement";

  public static final String COLUMN_ROLE = "role";
  public static final String COLUMN_USER = "user";
  public static final String COLUMN_PRIVILEGE = "privilege";

  public static final String COLUMN_STORAGE_GROUP = "storage group";
  public static final String COLUMN_LOCK_INFO = "lock holder";
  public static final String COLUMN_TTL = "ttl";

  public static final String COLUMN_TASK_NAME = "task name";
  public static final String COLUMN_CREATED_TIME = "created time";
  public static final String COLUMN_PROGRESS = "progress";
  public static final String COLUMN_CANCELLED = "cancelled";
  public static final String COLUMN_DONE = "done";

  public static final String COLUMN_FUNCTION_NAME = "function name";
  public static final String COLUMN_FUNCTION_TYPE = "function type";
  public static final String COLUMN_FUNCTION_CLASS = "class name (UDF)";

  public static final String COLUMN_CONTINUOUS_QUERY_NAME = "cq name";
  public static final String COLUMN_CONTINUOUS_QUERY_EVERY_INTERVAL = "every interval";
  public static final String COLUMN_CONTINUOUS_QUERY_FOR_INTERVAL = "for interval";
  public static final String COLUMN_CONTINUOUS_QUERY_BOUNDARY = "boundary";
  public static final String COLUMN_CONTINUOUS_QUERY_TARGET_PATH = "target path";
  public static final String COLUMN_CONTINUOUS_QUERY_QUERY_SQL = "query sql";

  public static final String COLUMN_SCHEMA_TEMPLATE = "template name";

  public static final String FUNCTION_TYPE_NATIVE = "native";
  public static final String FUNCTION_TYPE_BUILTIN_UDAF = "built-in UDAF";
  public static final String FUNCTION_TYPE_BUILTIN_UDTF = "built-in UDTF";
  public static final String FUNCTION_TYPE_EXTERNAL_UDAF = "external UDAF";
  public static final String FUNCTION_TYPE_EXTERNAL_UDTF = "external UDTF";

  public static final String COLUMN_TRIGGER_NAME = "trigger name";
  public static final String COLUMN_TRIGGER_STATUS = "status";
  public static final String COLUMN_TRIGGER_EVENT = "event";
  public static final String COLUMN_TRIGGER_PATH = "path";
  public static final String COLUMN_TRIGGER_CLASS = "class name";
  public static final String COLUMN_TRIGGER_ATTRIBUTES = "attributes";

  public static final String COLUMN_TRIGGER_STATUS_STARTED = "started";
  public static final String COLUMN_TRIGGER_STATUS_STOPPED = "stopped";

  // sync module
  // TODO(sync): delete this in new-standalone version
  public static final String COLUMN_PIPESERVER_STATUS = "enable";
  public static final String COLUMN_PIPESINK_NAME = "name";
  public static final String COLUMN_PIPESINK_TYPE = "type";
  public static final String COLUMN_PIPESINK_ATTRIBUTES = "attributes";
  public static final String COLUMN_PIPE_NAME = "name";
  public static final String COLUMN_PIPE_CREATE_TIME = "create time";
  public static final String COLUMN_PIPE_ROLE = "role";
  public static final String COLUMN_PIPE_REMOTE = "remote";
  public static final String COLUMN_PIPE_STATUS = "status";
  public static final String COLUMN_PIPE_MSG = "message";
  public static final String COLUMN_PIPE_ERRORS = "errors";
  public static final String COLUMN_PIPE_PERF_INFO = "performance_info";

  public static final String ONE_LEVEL_PATH_WILDCARD = "*";
  public static final String MULTI_LEVEL_PATH_WILDCARD = "**";
  public static final String TIME = "time";
  public static final String SYNC_SENDER_ROLE = "sender";
  public static final String SYNC_RECEIVER_ROLE = "receiver";

  // sdt parameters
  public static final String LOSS = "loss";
  public static final String SDT = "sdt";
  public static final String SDT_COMP_DEV = "compdev";
  public static final String SDT_COMP_MIN_TIME = "compmintime";
  public static final String SDT_COMP_MAX_TIME = "compmaxtime";

  // default base dir, stores all IoTDB runtime files
  public static final String DEFAULT_BASE_DIR = "data";

  // data folder name
  public static final String DATA_FOLDER_NAME = "data";
  public static final String SEQUENCE_FLODER_NAME = "sequence";
  public static final String UNSEQUENCE_FLODER_NAME = "unsequence";
  public static final String FILE_NAME_SEPARATOR = "-";
  public static final String UPGRADE_FOLDER_NAME = "upgrade";
  public static final String CONSENSUS_FOLDER_NAME = "consensus";

  // system folder name
  public static final String SYSTEM_FOLDER_NAME = "system";
  public static final String SCHEMA_FOLDER_NAME = "schema";
  public static final String SYNC_FOLDER_NAME = "sync";
  public static final String QUERY_FOLDER_NAME = "query";
  public static final String TRACING_FOLDER_NAME = "tracing";
  public static final String TRACING_LOG = "tracing.txt";
  public static final String EXT_FOLDER_NAME = "ext";
  public static final String UDF_FOLDER_NAME = "udf";
  public static final String UDF_TMP_FOLDER_NAME = "udf_temporary";
  public static final String TRIGGER_FOLDER_NAME = "trigger";

  public static final String TRIGGER_TMP_FOLDER_NAME = "trigger_temporary";
  public static final String MQTT_FOLDER_NAME = "mqtt";
  public static final String WAL_FOLDER_NAME = "wal";
  public static final String EXT_PIPE_FOLDER_NAME = "extPipe";

  public static final String EXT_PROPERTIES_LOADER_FOLDER_NAME = "loader";

  public static final String EXT_LIMITER = "limiter";

  // mqtt
  public static final String ENABLE_MQTT = "enable_mqtt_service";
  public static final String MQTT_HOST_NAME = "mqtt_host";
  public static final String MQTT_PORT_NAME = "mqtt_port";
  public static final String MQTT_HANDLER_POOL_SIZE_NAME = "mqtt_handler_pool_size";
  public static final String MQTT_PAYLOAD_FORMATTER_NAME = "mqtt_payload_formatter";
  public static final String MQTT_MAX_MESSAGE_SIZE = "mqtt_max_message_size";

  // thrift
  public static final int LEFT_SIZE_IN_REQUEST = 4 * 1024 * 1024;
  public static final int DEFAULT_FETCH_SIZE = 5000;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 0;

  // change tsFile name
  public static final int FILE_NAME_SUFFIX_INDEX = 0;
  public static final int FILE_NAME_SUFFIX_TIME_INDEX = 0;
  public static final int FILE_NAME_SUFFIX_VERSION_INDEX = 1;
  public static final int FILE_NAME_SUFFIX_MERGECNT_INDEX = 2;
  public static final int FILE_NAME_SUFFIX_UNSEQMERGECNT_INDEX = 3;
  public static final String FILE_NAME_SUFFIX_SEPARATOR = "\\.";

  // inner space compaction
  public static final String INNER_COMPACTION_TMP_FILE_SUFFIX = ".inner";

  // cross space compaction
  public static final String CROSS_COMPACTION_TMP_FILE_SUFFIX = ".cross";

  // cross space compaction of previous version (<0.13)
  public static final String CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD = ".merge";

  // compaction mods of previous version (<0.13)
  public static final String COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD = "merge.mods";

  // write ahead log
  public static final String WAL_FILE_PREFIX = "_";
  public static final String WAL_FILE_SUFFIX = ".wal";
  public static final String WAL_CHECKPOINT_FILE_SUFFIX = ".checkpoint";
  public static final String WAL_VERSION_ID = "versionId";
  public static final String WAL_START_SEARCH_INDEX = "startSearchIndex";
  public static final String WAL_STATUS_CODE = "statusCode";

  // show cluster status
  public static final String NODE_TYPE_CONFIG_NODE = "ConfigNode";
  public static final String NODE_TYPE_DATA_NODE = "DataNode";
  public static final String NODE_STATUS_RUNNING = "Running";
  public static final String NODE_STATUS_Down = "Down";

  // client version number
  public enum ClientVersion {
    V_0_12,
    V_0_13
  }
}
