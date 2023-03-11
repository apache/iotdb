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
package org.apache.iotdb.db.audit;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.db.sync.pipedata.load.ILoader.SCHEMA_FETCHER;

public class AuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  private static final String LOG = "log";
  private static final String USERNAME = "username";
  private static final String ADDRESS = "address";
  private static final String AUDIT_LOG_DEVICE = "root.__system.audit._%s";
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<AuditLogStorage> auditLogStorageList = config.getAuditLogStorage();

  private static final List<AuditLogOperation> auditLogOperationList =
      config.getAuditLogOperation();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private AuditLogger() {
    // empty constructor
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      String log, String address, String username)
      throws IoTDBConnectionException, IllegalPathException, QueryProcessException {
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(new PartialPath(String.format(AUDIT_LOG_DEVICE, username)));
    insertStatement.setTime(DateTimeUtils.currentTime());
    insertStatement.setMeasurements(new String[] {LOG, USERNAME, ADDRESS});
    insertStatement.setAligned(false);
    insertStatement.setValues(
        new Object[] {new Binary(log), new Binary(username), new Binary(address)});
    insertStatement.setDataTypes(
        new TSDataType[] {TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT});
    return insertStatement;
  }

  public static void log(String log, Statement statement) {
    AuditLogOperation operation = judgeLogOperation(statement.getType());
    IClientSession currSession = SessionManager.getInstance().getCurrSession();
    String username = "";
    String address = "";
    if (currSession != null) {
      ClientSession clientSession = (ClientSession) currSession;
      String clientAddress = clientSession.getClientAddress();
      int clientPort = ((ClientSession) currSession).getClientPort();
      address = String.format("%s:%s", clientAddress, clientPort);
      username = currSession.getUsername();
    }

    if (auditLogOperationList.contains(operation)) {
      if (auditLogStorageList.contains(AuditLogStorage.IOTDB)) {
        try {
          COORDINATOR.execute(
              generateInsertStatement(log, address, username),
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              ClusterPartitionFetcher.getInstance(),
              SCHEMA_FETCHER);
        } catch (IllegalPathException | IoTDBConnectionException | QueryProcessException e) {
          logger.error("write audit log series error,", e);
        }
      }
      if (auditLogStorageList.contains(AuditLogStorage.LOGGER)) {
        AUDIT_LOGGER.info("user:{},address:{},log:{}", username, address, log);
      }
    }
  }

  public static void log(String log, Statement statement, boolean isNativeApi) {
    if (isNativeApi) {
      if (config.isEnableAuditLogForNativeInsertApi()) {
        log(log, statement);
      }
    } else {
      log(log, statement);
    }
  }

  private static AuditLogOperation judgeLogOperation(StatementType type) {
    switch (type) {
      case AUTHOR:
      case CREATE_USER:
      case DELETE_USER:
      case MODIFY_PASSWORD:
      case GRANT_USER_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
      case CREATE_ROLE:
      case DELETE_ROLE:
      case GRANT_ROLE_PRIVILEGE:
      case REVOKE_ROLE_PRIVILEGE:
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
      case STORAGE_GROUP_SCHEMA:
      case DELETE_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case CREATE_ALIGNED_TIMESERIES:
      case CREATE_MULTI_TIMESERIES:
      case DELETE_TIMESERIES:
      case ALTER_TIMESERIES:
      case CHANGE_ALIAS:
      case CHANGE_TAG_OFFSET:
      case CREATE_FUNCTION:
      case DROP_FUNCTION:
      case CREATE_INDEX:
      case DROP_INDEX:
      case QUERY_INDEX:
      case CREATE_TRIGGER:
      case DROP_TRIGGER:
      case CREATE_TEMPLATE:
      case SET_TEMPLATE:
      case MERGE:
      case FULL_MERGE:
      case MNODE:
      case MEASUREMENT_MNODE:
      case STORAGE_GROUP_MNODE:
      case AUTO_CREATE_DEVICE_MNODE:
      case TTL:
      case FLUSH:
      case CLEAR_CACHE:
      case DELETE_PARTITION:
      case LOAD_CONFIGURATION:
      case CREATE_SCHEMA_SNAPSHOT:
      case CREATE_CONTINUOUS_QUERY:
      case DROP_CONTINUOUS_QUERY:
      case SET_SYSTEM_MODE:
      case UNSET_TEMPLATE:
      case PRUNE_TEMPLATE:
      case APPEND_TEMPLATE:
      case DROP_TEMPLATE:
      case CREATE_PIPESINK:
      case DROP_PIPESINK:
      case CREATE_PIPE:
      case START_PIPE:
      case STOP_PIPE:
      case DROP_PIPE:
      case DEACTIVATE_TEMPLATE:
      case CREATE_PIPEPLUGIN:
      case DROP_PIPEPLUGIN:
        return AuditLogOperation.DDL;
      case LOAD_DATA:
      case INSERT:
      case BATCH_INSERT:
      case BATCH_INSERT_ROWS:
      case BATCH_INSERT_ONE_DEVICE:
      case MULTI_BATCH_INSERT:
      case DELETE:
      case SELECT_INTO:
      case LOAD_FILES:
      case REMOVE_FILE:
      case UNLOAD_FILE:
      case ACTIVATE_TEMPLATE:
      case SETTLE:
      case INTERNAL_CREATE_TIMESERIES:
        return AuditLogOperation.DML;
      case LIST_USER:
      case LIST_ROLE:
      case LIST_USER_PRIVILEGE:
      case LIST_ROLE_PRIVILEGE:
      case LIST_USER_ROLES:
      case LIST_ROLE_USERS:
      case QUERY:
      case LAST:
      case GROUP_BY_TIME:
      case GROUP_BY_FILL:
      case AGGREGATION:
      case FILL:
      case UDAF:
      case UDTF:
      case SHOW:
      case SHOW_PIPES:
      case SHOW_MERGE_STATUS:
      case KILL:
      case TRACING:
      case SHOW_CONTINUOUS_QUERIES:
      case SHOW_SCHEMA_TEMPLATE:
      case SHOW_NODES_IN_SCHEMA_TEMPLATE:
      case SHOW_PATH_SET_SCHEMA_TEMPLATE:
      case SHOW_PATH_USING_SCHEMA_TEMPLATE:
      case SHOW_QUERY_RESOURCE:
      case FETCH_SCHEMA:
      case COUNT:
      case SHOW_TRIGGERS:
      case SHOW_PIPEPLUGINS:
        return AuditLogOperation.QUERY;
      default:
        logger.error("Unrecognizable operator type ({}) for audit log", type);
        return AuditLogOperation.NULL;
    }
  }
}
