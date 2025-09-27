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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.ILoader.SCHEMA_FETCHER;

public class DNAuditLogger extends AbstractAuditLogger {
  public static final String PREFIX_PASSWORD_HISTORY = "root.__audit.password_history";
  private static final Logger logger = LoggerFactory.getLogger(DNAuditLogger.class);

  // TODO: @zhujt20 Optimize the following stupid retry
  private static final int INSERT_RETRY_COUNT = 5;
  private static final int INSERT_RETRY_INTERVAL_MS = 2000;

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String AUDIT_LOG_DEVICE = "root.__audit.log.node_%s.u_%s";
  private static final String AUDIT_LOGIN_LOG_DEVICE = "root.__audit.login.node_%s.u_%s";
  private static final String AUDIT_CN_LOG_DEVICE = "root.__audit.log.node_%s.u_all";
  private static final SessionInfo sessionInfo =
      new SessionInfo(
          0,
          new UserEntity(
              AuthorityChecker.INTERNAL_AUDIT_USER_ID,
              AuthorityChecker.INTERNAL_AUDIT_USER,
              IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
          ZoneId.systemDefault());

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final DataNodeDevicePathCache DEVICE_PATH_CACHE =
      DataNodeDevicePathCache.getInstance();
  private static final AtomicBoolean tableViewIsInitialized = new AtomicBoolean(false);

  private Coordinator coordinator;

  private DNAuditLogger() {
    // Empty constructor
  }

  public static DNAuditLogger getInstance() {
    return DNAuditLoggerHolder.INSTANCE;
  }

  public void setCoordinator(Coordinator coordinator) {
    DNAuditLoggerHolder.INSTANCE.coordinator = coordinator;
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath log_device) {
    String username = auditLogFields.getUsername();
    String address = auditLogFields.getCliHostname();
    AuditEventType type = auditLogFields.getAuditEventType();
    AuditLogOperation operation = auditLogFields.getAuditLogOperation();
    PrivilegeLevel privilegeLevel = null;
    if (auditLogFields.getPrivilegeTypes() != null) {
      for (PrivilegeType privilegeType : auditLogFields.getPrivilegeTypes()) {
        privilegeLevel = judgePrivilegeLevel(privilegeType);
        if (privilegeLevel.equals(PrivilegeLevel.GLOBAL)) {
          break;
        }
      }
    }
    String dataNodeId = String.valueOf(config.getDataNodeId());
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(log_device);
    insertStatement.setTime(CommonDateTimeUtils.currentTime());
    insertStatement.setMeasurements(
        new String[] {
          AUDIT_LOG_USERNAME,
          AUDIT_LOG_CLI_HOSTNAME,
          AUDIT_LOG_AUDIT_EVENT_TYPE,
          AUDIT_LOG_OPERATION_TYPE,
          AUDIT_LOG_PRIVILEGE_TYPE,
          AUDIT_LOG_PRIVILEGE_LEVEL,
          AUDIT_LOG_RESULT,
          AUDIT_LOG_DATABASE,
          AUDIT_LOG_SQL_STRING,
          AUDIT_LOG_LOG
        });
    insertStatement.setAligned(false);
    String sqlString = auditLogFields.getSqlString();
    if (sqlString != null) {
      if (sqlString.toUpperCase().startsWith("CREATE USER")) {
        sqlString = String.join(" ", Arrays.asList(sqlString.split(" ")).subList(0, 3)) + " ...";
      }
      Pattern pattern = Pattern.compile("(?i)(values)\\([^)]*\\)");
      Matcher matcher = pattern.matcher(sqlString);
      StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        matcher.appendReplacement(sb, matcher.group(1) + "(...)");
      }
      matcher.appendTail(sb);
      sqlString = sb.toString();
    }
    insertStatement.setValues(
        new Object[] {
          new Binary(username == null ? "null" : username, TSFileConfig.STRING_CHARSET),
          new Binary(address == null ? "null" : address, TSFileConfig.STRING_CHARSET),
          new Binary(type == null ? "null" : type.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              operation == null ? "null" : operation.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              auditLogFields.getPrivilegeTypes() == null
                  ? "null"
                  : auditLogFields.getPrivilegeTypeString(),
              TSFileConfig.STRING_CHARSET),
          new Binary(
              privilegeLevel == null ? "null" : privilegeLevel.toString(),
              TSFileConfig.STRING_CHARSET),
          auditLogFields.getResult(),
          new Binary(
              auditLogFields.getDatabase() == null ? "null" : auditLogFields.getDatabase(),
              TSFileConfig.STRING_CHARSET),
          new Binary(sqlString == null ? "null" : sqlString, TSFileConfig.STRING_CHARSET),
          new Binary(log == null ? "null" : log, TSFileConfig.STRING_CHARSET)
        });
    insertStatement.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.BOOLEAN,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
        });
    return insertStatement;
  }

  public void createViewIfNecessary() {
    if (!tableViewIsInitialized.get()) {
      synchronized (this) {
        if (tableViewIsInitialized.get()) {
          return;
        }
        Statement statement =
            StatementGenerator.createStatement(
                "SHOW DATABASES " + SystemConstant.AUDIT_DATABASE, ZoneId.systemDefault());
        try (final ConfigNodeClient client =
            CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          ShowDatabaseStatement showStatement = (ShowDatabaseStatement) statement;
          final List<String> databasePathPattern =
              Arrays.asList(showStatement.getPathPattern().getNodes());
          final TGetDatabaseReq req =
              new TGetDatabaseReq(
                      databasePathPattern, showStatement.getAuthorityScope().serialize())
                  .setIsTableModel(false);
          final TShowDatabaseResp resp = client.showDatabase(req);
          if (resp.getDatabaseInfoMapSize() > 0) {
            tableViewIsInitialized.set(true);
            return;
          }
        } catch (ClientManagerException | TException | IOException e) {
          logger.error(
              "Failed to show database before creating database {} for audit log",
              SystemConstant.AUDIT_DATABASE);
        }

        statement =
            StatementGenerator.createStatement(
                "CREATE DATABASE "
                    + SystemConstant.AUDIT_DATABASE
                    + " WITH SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=1",
                ZoneId.systemDefault());
        ExecutionResult result =
            coordinator.executeForTreeModel(
                statement,
                SESSION_MANAGER.requestQueryId(),
                sessionInfo,
                "",
                ClusterPartitionFetcher.getInstance(),
                SCHEMA_FETCHER);
        if (result.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || result.status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {

          SqlParser relationSqlParser = new SqlParser();
          IClientSession session =
              new InternalClientSession(
                  String.format(
                      "%s_%s", DNAuditLogger.class.getSimpleName(), SystemConstant.AUDIT_DATABASE));
          session.setUsername(AuthorityChecker.INTERNAL_AUDIT_USER);
          session.setZoneId(ZoneId.systemDefault());
          session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
          session.setDatabaseName(SystemConstant.AUDIT_DATABASE);
          session.setSqlDialect(IClientSession.SqlDialect.TABLE);
          SESSION_MANAGER.registerSession(session);
          Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

          org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement stmt =
              relationSqlParser.createStatement(
                  "CREATE DATABASE " + SystemConstant.AUDIT_PREFIX_KEY,
                  ZoneId.systemDefault(),
                  session);
          TSStatus status =
              coordinator.executeForTableModel(
                      stmt,
                      relationSqlParser,
                      session,
                      SESSION_MANAGER.requestQueryId(),
                      SESSION_MANAGER.getSessionInfoOfTableModel(session),
                      "",
                      metadata,
                      config.getQueryTimeoutThreshold(),
                      false)
                  .status;
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
              && status.getCode() != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
            logger.error("Failed to create view for audit log, because {}", status.getMessage());
          }
          stmt =
              relationSqlParser.createStatement(
                  String.format(
                      "CREATE VIEW __audit.audit_log (\n"
                          + "    %s STRING TAG,\n"
                          + "    %s STRING TAG,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s BOOLEAN FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD,\n"
                          + "    %s STRING FIELD\n"
                          + ") AS root.__audit.log.**",
                      AUDIT_LOG_NODE_ID,
                      AUDIT_LOG_USER_ID,
                      AUDIT_LOG_USERNAME,
                      AUDIT_LOG_CLI_HOSTNAME,
                      AUDIT_LOG_AUDIT_EVENT_TYPE,
                      AUDIT_LOG_OPERATION_TYPE,
                      AUDIT_LOG_PRIVILEGE_TYPE,
                      AUDIT_LOG_PRIVILEGE_LEVEL,
                      AUDIT_LOG_RESULT,
                      AUDIT_LOG_DATABASE,
                      AUDIT_LOG_SQL_STRING,
                      AUDIT_LOG_LOG),
                  ZoneId.systemDefault(),
                  session);
          status =
              coordinator.executeForTableModel(
                      stmt,
                      relationSqlParser,
                      session,
                      SESSION_MANAGER.requestQueryId(),
                      SESSION_MANAGER.getSessionInfoOfTableModel(session),
                      "",
                      metadata,
                      config.getQueryTimeoutThreshold(),
                      false)
                  .status;
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
              && status.getCode()
                  != TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode()) {
            logger.error("Failed to create view for audit log, because {}", status.getMessage());
          } else {
            logger.info("Create view for audit log successfully");
            tableViewIsInitialized.set(true);
          }
        } else {
          logger.error("Failed to create database {} for audit log", SystemConstant.AUDIT_DATABASE);
        }
      }
    }
  }

  @Override
  public void log(IAuditEntity auditLogFields, Supplier<String> log) {
    if (!IS_AUDIT_LOG_ENABLED) {
      return;
    }
    createViewIfNecessary();
    if (noNeedInsertAuditLog(auditLogFields)) {
      return;
    }
    long userId = auditLogFields.getUserId();
    String user = String.valueOf(userId);
    if (userId == -1) {
      user = "none";
    }
    String dataNodeId = String.valueOf(config.getDataNodeId());
    InsertRowStatement statement;
    try {
      statement =
          generateInsertStatement(
              auditLogFields,
              log.get(),
              DEVICE_PATH_CACHE.getPartialPath(String.format(AUDIT_LOG_DEVICE, dataNodeId, user)));
    } catch (IllegalPathException e) {
      logger.error("Failed to log audit events because ", e);
      return;
    }
    for (int retry = 0; retry < INSERT_RETRY_COUNT; retry++) {
      ExecutionResult insertResult =
          coordinator.executeForTreeModel(
              statement,
              SESSION_MANAGER.requestQueryId(),
              sessionInfo,
              "",
              ClusterPartitionFetcher.getInstance(),
              SCHEMA_FETCHER);
      if (insertResult.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(INSERT_RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        logger.error("Audit log insertion retry sleep was interrupted", e);
      }
    }
    AuditEventType type = auditLogFields.getAuditEventType();
    if (isLoginEvent(type)) {
      // TODO: @wenyanshi-123 Reactivate the following codes in the future
      //      try {
      //        statement.setDevicePath(
      //            DEVICE_PATH_CACHE.getPartialPath(
      //                String.format(AUDIT_LOGIN_LOG_DEVICE, dataNodeId, user)));
      //      } catch (IllegalPathException e) {
      //        logger.error("Failed to log audit login events because ", e);
      //        return;
      //      }
      //      coordinator.executeForTreeModel(
      //          statement,
      //          SESSION_MANAGER.requestQueryId(),
      //          sessionInfo,
      //          "",
      //          ClusterPartitionFetcher.getInstance(),
      //          SCHEMA_FETCHER);
    }
  }

  public void logFromCN(AuditLogFields auditLogFields, String log, int nodeId)
      throws IllegalPathException {
    if (!IS_AUDIT_LOG_ENABLED) {
      return;
    }
    createViewIfNecessary();
    if (noNeedInsertAuditLog(auditLogFields)) {
      return;
    }
    InsertRowStatement statement =
        generateInsertStatement(
            auditLogFields,
            log,
            DEVICE_PATH_CACHE.getPartialPath(String.format(AUDIT_CN_LOG_DEVICE, nodeId)));
    for (int retry = 0; retry < INSERT_RETRY_COUNT; retry++) {
      ExecutionResult insertResult =
          coordinator.executeForTreeModel(
              statement,
              SESSION_MANAGER.requestQueryId(),
              sessionInfo,
              "",
              ClusterPartitionFetcher.getInstance(),
              SCHEMA_FETCHER);
      if (insertResult.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(INSERT_RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        logger.error("Audit log insertion retry sleep was interrupted", e);
      }
    }
  }

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
