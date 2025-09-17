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

import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.time.ZoneId;

import static org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.ILoader.SCHEMA_FETCHER;

public class DNAuditLogger extends AbstractAuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(DNAuditLogger.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String LOG = "log";
  private static final String USERNAME = "username";
  private static final String CLI_HOSTNAME = "cli_hostname";
  private static final String RESULT = "result";
  private static final String AUDIT_EVENT_TYPE = "audit_event_type";
  private static final String OPERATION_TYPE = "operation_type";
  private static final String PRIVILEGE_TYPE = "privilege_type";
  private static final String PRIVILEGE_LEVEL = "privilege_level";
  private static final String DATABASE = "database";
  private static final String SQL_STRING = "sql_string";

  private static final String AUDIT_LOG_DEVICE = "root.__audit.log.%s.%s";
  private static final String AUDIT_LOGIN_LOG_DEVICE = "root.__audit.login.%s.%s";
  private static final String AUDIT_CN_LOG_DEVICE = "root.__audit.control.%s.%s";
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionInfo sessionInfo =
      new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault());

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final DataNodeDevicePathCache DEVICE_PATH_CACHE =
      DataNodeDevicePathCache.getInstance();
  private static boolean tableViewIsInitialized = false;

  private DNAuditLogger() {
    // Empty constructor
  }

  public static DNAuditLogger getInstance() {
    return DNAuditLoggerHolder.INSTANCE;
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      AuditLogFields auditLogFields, String log, String log_device) throws IllegalPathException {
    String username = auditLogFields.getUsername();
    String address = auditLogFields.getCliHostname();
    AuditEventType type = auditLogFields.getAuditType();
    AuditLogOperation operation = auditLogFields.getOperationType();
    PrivilegeType privilegeType = auditLogFields.getPrivilegeType();
    PrivilegeLevel privilegeLevel = judgePrivilegeLevel(privilegeType);
    String dataNodeId = String.valueOf(config.getDataNodeId());
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(
        DEVICE_PATH_CACHE.getPartialPath(String.format(log_device, dataNodeId, username)));
    insertStatement.setTime(CommonDateTimeUtils.currentTime());
    insertStatement.setMeasurements(
        new String[] {
          USERNAME,
          CLI_HOSTNAME,
          AUDIT_EVENT_TYPE,
          OPERATION_TYPE,
          PRIVILEGE_TYPE,
          PRIVILEGE_LEVEL,
          RESULT,
          DATABASE,
          SQL_STRING,
          LOG
        });
    insertStatement.setAligned(false);
    insertStatement.setValues(
        new Object[] {
          new Binary(username == null ? "null" : username, TSFileConfig.STRING_CHARSET),
          new Binary(address == null ? "null" : address, TSFileConfig.STRING_CHARSET),
          new Binary(type == null ? "null" : type.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              operation == null ? "null" : operation.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              privilegeType == null ? "null" : privilegeType.toString(),
              TSFileConfig.STRING_CHARSET),
          new Binary(
              privilegeLevel == null ? "null" : privilegeLevel.toString(),
              TSFileConfig.STRING_CHARSET),
          auditLogFields.isResult(),
          new Binary(
              auditLogFields.getDatabase() == null ? "null" : auditLogFields.getDatabase(),
              TSFileConfig.STRING_CHARSET),
          new Binary(
              auditLogFields.getSqlString() == null ? "null" : auditLogFields.getSqlString(),
              TSFileConfig.STRING_CHARSET),
          new Binary(log == null ? "null" : log, TSFileConfig.STRING_CHARSET)
        });
    insertStatement.setDataTypes(
        new TSDataType[] {
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.TEXT,
          TSDataType.TEXT
        });
    return insertStatement;
  }

  public static void createViewIfNecessary() {
    if (!tableViewIsInitialized) {
      Statement statement =
          StatementGenerator.createStatement(
              "CREATE DATABASE " + SystemConstant.AUDIT_DATABASE, ZoneId.systemDefault());
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              statement,
              SESSION_MANAGER.requestQueryId(),
              sessionInfo,
              "",
              ClusterPartitionFetcher.getInstance(),
              SCHEMA_FETCHER);
      if (result.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || result.status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        statement =
            StatementGenerator.createStatement(
                "CREATE VIEW __view_system.audit_log (\n"
                    + "    dn_id STRING TAG,\n"
                    + "    user_name STRING TAG,\n"
                    + "    cli_hostname STRING FIELD,\n"
                    + "    audit_event_type STRING FIELD,\n"
                    + "    operation_type STRING FIELD,\n"
                    + "    privilege_type STRING FIELD,\n"
                    + "    privilege_level STRING FIELD,\n"
                    + "    result BOOLEAN FIELD,\n"
                    + "    database STRING FIELD,\n"
                    + "    sql_string STRING FIELD,\n"
                    + "    log STRING FIELD\n"
                    + ") AS root.__audit.log",
                ZoneId.systemDefault());
        SqlParser relationSqlParser = new SqlParser();
        IClientSession session = SESSION_MANAGER.getCurrSession();
        Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
        COORDINATOR.executeForTableModel(
            statement,
            relationSqlParser,
            session,
            SESSION_MANAGER.requestQueryId(),
            SESSION_MANAGER.getSessionInfoOfTableModel(session),
            "",
            metadata,
            config.getQueryTimeoutThreshold());
        tableViewIsInitialized = true;
      } else {
        logger.error("Failed to create database {} for audit log", SystemConstant.AUDIT_DATABASE);
      }
    }
  }

  public void log(AuditLogFields auditLogFields, String log) throws IllegalPathException {
    createViewIfNecessary();
    checkBeforeLog(auditLogFields);
    InsertRowStatement statement = generateInsertStatement(auditLogFields, log, AUDIT_LOG_DEVICE);
    COORDINATOR.executeForTreeModel(
        statement,
        SESSION_MANAGER.requestQueryId(),
        sessionInfo,
        "",
        ClusterPartitionFetcher.getInstance(),
        SCHEMA_FETCHER);
    String username = auditLogFields.getUsername();
    AuditEventType type = auditLogFields.getAuditType();
    String dataNodeId = String.valueOf(config.getDataNodeId());
    if (isLoginEvent(type)) {
      statement.setDevicePath(
          DEVICE_PATH_CACHE.getPartialPath(
              String.format(AUDIT_LOGIN_LOG_DEVICE, dataNodeId, username)));
      COORDINATOR.executeForTreeModel(
          statement,
          SESSION_MANAGER.requestQueryId(),
          sessionInfo,
          "",
          ClusterPartitionFetcher.getInstance(),
          SCHEMA_FETCHER);
    }
  }

  public void logFromCN(AuditLogFields auditLogFields, String log) throws IllegalPathException {
    createViewIfNecessary();
    checkBeforeLog(auditLogFields);
    InsertRowStatement statement =
        generateInsertStatement(auditLogFields, log, AUDIT_CN_LOG_DEVICE);
    COORDINATOR.executeForTreeModel(
        statement,
        SESSION_MANAGER.requestQueryId(),
        sessionInfo,
        "",
        ClusterPartitionFetcher.getInstance(),
        SCHEMA_FETCHER);
  }

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
