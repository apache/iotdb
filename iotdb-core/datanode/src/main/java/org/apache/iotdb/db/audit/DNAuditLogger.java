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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
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
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.utils.AsyncBatchUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.pipe.receiver.protocol.legacy.loader.ILoader.SCHEMA_FETCHER;

public class DNAuditLogger extends AbstractAuditLogger {
  public static final String PREFIX_PASSWORD_HISTORY = "root.__audit.password_history";
  private static final Logger logger = LoggerFactory.getLogger(DNAuditLogger.class);

  public static final String PREFIX_LOGIN_HISTORY = "root.__audit.login.u_%d.node_%s";

  public static final String VISIT_HISTORY_PATH = "root.__audit.login.u_%d.**";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final String AUDIT_LOG_DEVICE = "root.__audit.log.node_%s.u_%s";

  private static final String AUDIT_LOG_DEVICE_PATH = "root.__audit.log.**";
  private static final String AUDIT_LOG_PREFIX = "AUDIT";
  private static final SessionInfo sessionInfo =
      new SessionInfo(
          0,
          new UserEntity(
              AuthorityChecker.INTERNAL_AUDIT_USER_ID,
              AuthorityChecker.INTERNAL_AUDIT_USER,
              IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
          ZoneId.systemDefault());

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final DataNodeDevicePathCache DEVICE_PATH_CACHE =
      DataNodeDevicePathCache.getInstance();
  private static final AtomicBoolean tableViewIsInitialized = new AtomicBoolean(false);
  private static final AtomicBoolean loginHistoryViewIsInitialized = new AtomicBoolean(false);

  private static final String LOGIN_HISTORY_TABLE_NAME = "login_history";
  private static final String LOGIN_HISTORY_IP = "ip";

  private Coordinator coordinator;

  private AsyncBatchUtils<InsertRowStatement> asyncBatchUtils;

  private final Object asyncBatchLock = new Object();

  private DNAuditLogger() {
    // Empty constructor
    asyncBatchUtils = null;
  }

  public void stop() {
    synchronized (asyncBatchLock) {
      if (asyncBatchUtils != null) {
        asyncBatchUtils.shutdown();
        asyncBatchUtils = null;
      }
    }
  }

  public void start() {
    synchronized (asyncBatchLock) {
      if (asyncBatchUtils == null
          && CommonDescriptor.getInstance().getConfig().isEnableAuditLog()) {

        asyncBatchUtils =
            new AsyncBatchUtils<>(
                "DNAuditLogger",
                CommonDescriptor.getInstance()
                    .getConfig()
                    .getAuditLogBatchIntervalInMs(), // flush interval ms
                CommonDescriptor.getInstance()
                    .getConfig()
                    .getAuditLogBatchMaxQueueBytes(), // max queue bytes
                batch -> {
                  InsertRowsStatement stmt = new InsertRowsStatement();
                  stmt.setInsertRowStatementList(batch);
                  return coordinator.executeForTreeModel(
                      stmt,
                      SessionManager.getInstance().requestQueryId(),
                      sessionInfo,
                      "",
                      ClusterPartitionFetcher.getInstance(),
                      SCHEMA_FETCHER);
                });
        asyncBatchUtils.start();
      }
    }
  }

  public static DNAuditLogger getInstance() {
    return DNAuditLoggerHolder.INSTANCE;
  }

  public void setCoordinator(Coordinator coordinator) {
    DNAuditLoggerHolder.INSTANCE.coordinator = coordinator;
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath logDevice) {
    return generateInsertStatement(
        auditLogFields, log, logDevice, CommonDateTimeUtils.currentTime());
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath logDevice, long logTimestamp) {
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
    } else {
      privilegeLevel = PrivilegeLevel.GLOBAL;
    }
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(logDevice);
    insertStatement.setTime(logTimestamp);
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
    insertStatement.setAligned(true);
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

  /** Result of checking audit database and table existence via ConfigNode. */
  private static class AuditDbCheckResult {
    final boolean dbInTreeModelExists;
    final boolean tableExists;

    AuditDbCheckResult(boolean dbInTreeModelExists, boolean tableExists) {
      this.dbInTreeModelExists = dbInTreeModelExists;
      this.tableExists = tableExists;
    }
  }

  /**
   * Check whether the audit database exists in the tree model and whether the specified table
   * already exists in the table model.
   *
   * <p>This method queries metadata from ConfigNode in two steps:
   *
   * <ol>
   *   <li>Check whether the audit database exists in the tree model.
   *   <li>If it exists, check whether the audit database is visible in the table model and whether
   *       the specified table already exists.
   * </ol>
   *
   * <p>The result is returned as an {@link AuditDbCheckResult} object containing the existence
   * status of both the database and the table.
   *
   * @param tableName the name of the audit table to check in the table model
   * @param logPrefix the prefix used in log messages to identify the caller context
   * @return an {@link AuditDbCheckResult} containing:
   *     <ul>
   *       <li>{@code dbInTreeModelExists} - whether the audit database exists in the tree model
   *       <li>{@code tableExists} - whether the specified table already exists in the table model
   *     </ul>
   *
   * @throws RuntimeException if unexpected errors occur while communicating with ConfigNode
   */
  private AuditDbCheckResult checkAuditDbAndTable(String tableName, String logPrefix) {
    boolean dbInTreeModelExists = false;
    boolean tableExists = false;
    Statement statement =
        StatementGenerator.createStatement(
            "SHOW DATABASES " + SystemConstant.AUDIT_DATABASE, ZoneId.systemDefault());
    try (final ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      ShowDatabaseStatement showStatement = (ShowDatabaseStatement) statement;
      final List<String> databasePathPattern =
          Arrays.asList(showStatement.getPathPattern().getNodes());
      final TGetDatabaseReq req =
          new TGetDatabaseReq(databasePathPattern, showStatement.getAuthorityScope().serialize())
              .setIsTableModel(false);
      final TShowDatabaseResp resp = client.showDatabase(req);
      if (resp.getDatabaseInfoMapSize() > 0) {
        dbInTreeModelExists = true;
        statement = StatementGenerator.createStatement("SHOW DATABASES ", ZoneId.systemDefault());
        showStatement = (ShowDatabaseStatement) statement;
        final List<String> allDatabasePathPattern =
            Arrays.asList(showStatement.getPathPattern().getNodes());
        final TGetDatabaseReq tableModelReq =
            new TGetDatabaseReq(
                    allDatabasePathPattern, showStatement.getAuthorityScope().serialize())
                .setIsTableModel(true)
                .setCanSeeAuditDB(true);
        final TShowDatabaseResp tableResp = client.showDatabase(tableModelReq);
        if (tableResp.getDatabaseInfoMapSize() > 0
            && tableResp.getDatabaseInfoMap().containsKey(SystemConstant.AUDIT_PREFIX_KEY)) {
          TShowTableResp showTableResp = client.showTables(SystemConstant.AUDIT_PREFIX_KEY, false);
          for (TTableInfo tableInfo : showTableResp.getTableInfoList()) {
            if (tableInfo.getTableName().equals(tableName)) {
              tableExists = true;
              break;
            }
          }
        }
      }
    } catch (ClientManagerException | TException e) {
      logger.warn(
          "[{}] Failed to show database before creating view: {}", logPrefix, e.getMessage());
    }
    return new AuditDbCheckResult(dbInTreeModelExists, tableExists);
  }

  private boolean ensureAuditDatabaseInTreeModel() {
    Statement statement =
        StatementGenerator.createStatement(
            "CREATE DATABASE "
                + SystemConstant.AUDIT_DATABASE
                + " WITH SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=1",
            ZoneId.systemDefault());
    ExecutionResult result =
        coordinator.executeForTreeModel(
            statement,
            SessionManager.getInstance().requestQueryId(),
            sessionInfo,
            "",
            ClusterPartitionFetcher.getInstance(),
            SCHEMA_FETCHER);
    return result.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || result.status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode();
  }

  private IClientSession createInternalAuditSession(String sessionSuffix) {
    IClientSession session =
        new InternalClientSession(
            String.format("%s_%s", sessionSuffix, SystemConstant.AUDIT_PREFIX_KEY));
    session.setUsername(AuthorityChecker.INTERNAL_AUDIT_USER);
    session.setZoneId(ZoneId.systemDefault());
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    session.setDatabaseName(SystemConstant.AUDIT_PREFIX_KEY);
    session.setSqlDialect(SqlDialect.TABLE);
    return session;
  }

  private boolean ensureAuditDatabaseInTableModel(IClientSession session, String logPrefix) {
    SqlParser relationSqlParser = new SqlParser();
    Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
    org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement stmt =
        relationSqlParser.createStatement(
            "CREATE DATABASE " + SystemConstant.AUDIT_PREFIX_KEY, ZoneId.systemDefault(), session);
    TSStatus status =
        coordinator.executeForTableModel(
                stmt,
                relationSqlParser,
                session,
                SessionManager.getInstance().requestQueryId(),
                SessionManager.getInstance().getSessionInfoOfTableModel(session),
                "",
                metadata,
                config.getQueryTimeoutThreshold(),
                false,
                false)
            .status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      logger.warn(
          "[{}] Failed to create database in table model: {}", logPrefix, status.getMessage());
      return true;
    }
    return false;
  }

  private boolean executeCreateView(
      IClientSession session,
      String createViewSql,
      String logPrefix,
      String successMsg,
      String errorMsg) {
    SqlParser relationSqlParser = new SqlParser();
    Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
    org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement stmt =
        relationSqlParser.createStatement(createViewSql, ZoneId.systemDefault(), session);
    TSStatus status =
        coordinator.executeForTableModel(
                stmt,
                relationSqlParser,
                session,
                SessionManager.getInstance().requestQueryId(),
                SessionManager.getInstance().getSessionInfoOfTableModel(session),
                "",
                metadata,
                config.getQueryTimeoutThreshold(),
                false,
                false)
            .status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode()
            != TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode()) {
      logger.warn("[{}] {}, {}", logPrefix, errorMsg, status.getMessage());
      return false;
    }
    logger.info("[{}] {}", logPrefix, successMsg);
    return true;
  }

  public void createViewIfNecessary() {
    if (!tableViewIsInitialized.get()) {
      synchronized (this) {
        if (tableViewIsInitialized.get()) {
          return;
        }
        AuditDbCheckResult checkResult = checkAuditDbAndTable("audit_log", AUDIT_LOG_PREFIX);
        if (checkResult.tableExists) {
          logger.info(
              "[{}}] Database {} already exists for audit log",
              AUDIT_LOG_PREFIX,
              SystemConstant.AUDIT_PREFIX_KEY);
          tableViewIsInitialized.set(true);
          return;
        }
        boolean dbInTreeModelExists = checkResult.dbInTreeModelExists;
        if (!dbInTreeModelExists && ensureAuditDatabaseInTreeModel()) {
          dbInTreeModelExists = true;
        }
        if (!dbInTreeModelExists) {
          logger.warn(
              "[{}}] Failed to create database {} for audit log",
              AUDIT_LOG_PREFIX,
              SystemConstant.AUDIT_DATABASE);
          return;
        }
        setTtl();
        IClientSession session = createInternalAuditSession(DNAuditLogger.class.getSimpleName());
        if (ensureAuditDatabaseInTableModel(session, AUDIT_LOG_PREFIX)) {
          return;
        }
        String createViewSql =
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
                AUDIT_LOG_LOG);
        if (executeCreateView(
            session,
            createViewSql,
            AUDIT_LOG_PREFIX,
            "Create view for audit log successfully",
            "Failed to create view for audit log, because")) {
          tableViewIsInitialized.set(true);
        }
      }
    }
  }

  /**
   * Create login history view for table model so root user can query visit history via SELECT. This
   * is independent of audit switch - login history is always recorded and should be queryable
   * regardless of whether audit log is enabled.
   */
  public void createLoginHistoryViewIfNecessary() {
    if (loginHistoryViewIsInitialized.get()) {
      return;
    }

    synchronized (this) {
      if (loginHistoryViewIsInitialized.get()) {
        return;
      }

      tryCreateLoginHistoryView();
    }
  }

  /**
   * Try to create the login history view.
   *
   * <p>This method performs several prerequisite checks before creating the view:
   *
   * <ul>
   *   <li>Coordinator must be available
   *   <li>The view must not already exist
   *   <li>The audit database must exist in the tree model
   * </ul>
   *
   * <p>If all conditions are satisfied, the login history view will be created.
   */
  private void tryCreateLoginHistoryView() {

    if (coordinator == null) {
      logger.warn(
          "[LOGIN_HISTORY] Coordinator is not set, skip creating login history view for table model");
      return;
    }

    AuditDbCheckResult checkResult =
        checkAuditDbAndTable(LOGIN_HISTORY_TABLE_NAME, "LOGIN_HISTORY");

    if (checkResult.tableExists) {
      logger.info(
          "[LOGIN_HISTORY] View {} already exists for visit history", LOGIN_HISTORY_TABLE_NAME);
      loginHistoryViewIsInitialized.set(true);
      return;
    }

    if (!ensureAuditDatabase(checkResult)) {
      return;
    }

    createLoginHistoryView();
  }

  /**
   * Ensure the audit database exists in the tree model.
   *
   * <p>If the database does not exist, this method will attempt to create it. If creation fails,
   * the login history view cannot be created.
   *
   * @param checkResult the result of the audit database/table existence check
   * @return true if the audit database exists or is successfully created, false otherwise
   */
  private boolean ensureAuditDatabase(AuditDbCheckResult checkResult) {

    boolean dbExists = checkResult.dbInTreeModelExists;

    if (!dbExists && ensureAuditDatabaseInTreeModel()) {
      dbExists = true;
    }

    if (!dbExists) {
      logger.warn("[LOGIN_HISTORY] Failed to create database {}", SystemConstant.AUDIT_DATABASE);
      return false;
    }

    return true;
  }

  /**
   * Create the login history view in the table model.
   *
   * <p>This method creates a SQL view on top of the underlying audit log series so that login
   * history can be queried using the table model.
   *
   * <p>The view maps audit log fields such as user id, node id, username, IP address, and login
   * result to a relational table structure.
   */
  private void createLoginHistoryView() {

    IClientSession session =
        createInternalAuditSession(DNAuditLogger.class.getSimpleName() + "LoginHistory");

    if (ensureAuditDatabaseInTableModel(session, "LOGIN_HISTORY")) {
      return;
    }

    String createViewSql = buildCreateViewSql();

    if (executeCreateView(
        session,
        createViewSql,
        "LOGIN_HISTORY",
        "Create view for login history successfully",
        "Failed to create view for login history:")) {

      loginHistoryViewIsInitialized.set(true);
    }
  }

  /**
   * Build the SQL statement used to create the login history view.
   *
   * <p>The view exposes login audit records stored in the time-series tree model
   * (root.__audit.login.**) as a relational table under the __audit database.
   *
   * @return the SQL string used to create the login history view
   */
  private String buildCreateViewSql() {
    return String.format(
        "CREATE VIEW __audit.%s (\n"
            + "    %s STRING TAG,\n"
            + "    %s STRING TAG,\n"
            + "    %s STRING FIELD,\n"
            + "    %s STRING FIELD,\n"
            + "    %s BOOLEAN FIELD\n"
            + ") AS root.__audit.login.**",
        LOGIN_HISTORY_TABLE_NAME,
        AUDIT_LOG_USER_ID,
        AUDIT_LOG_NODE_ID,
        AUDIT_LOG_USERNAME,
        LOGIN_HISTORY_IP,
        AUDIT_LOG_RESULT);
  }

  public void checkAndSetTtl() {
    if (tableViewIsInitialized.get()) {
      setTtl();
    }
  }

  public void setTtl() {
    double auditLogTtlInDays = CommonDescriptor.getInstance().getConfig().getAuditLogTtlInDays();
    if (auditLogTtlInDays > 0) {
      long auditLogTtlInMs;
      if (auditLogTtlInDays == Long.MAX_VALUE) {
        auditLogTtlInMs = Long.MAX_VALUE;
      } else {
        auditLogTtlInMs = (long) (auditLogTtlInDays * 24 * 3600 * 1000);
      }
      Statement statement =
          StatementGenerator.createStatement(
              "SET TTL TO " + AUDIT_LOG_DEVICE_PATH + " " + auditLogTtlInMs,
              ZoneId.systemDefault());
      coordinator.executeForTreeModel(
          statement,
          SessionManager.getInstance().requestQueryId(),
          sessionInfo,
          "",
          ClusterPartitionFetcher.getInstance(),
          SCHEMA_FETCHER);
    }
  }

  @Override
  public void log(IAuditEntity auditLogFields, Supplier<String> log) {
    if (!commonConfig.isEnableAuditLog()) {
      return;
    }
    try {
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
      InsertRowStatement statement =
          generateInsertStatement(
              auditLogFields,
              log.get(),
              DEVICE_PATH_CACHE.getPartialPath(String.format(AUDIT_LOG_DEVICE, dataNodeId, user)));
      asyncBatchUtils.push(statement);
    } catch (Exception e) {
      logger.warn("[{}}] Failed to log audit events because", AUDIT_LOG_PREFIX, e);
    }
  }

  public void logFromCN(AuditLogFields auditLogFields, String log, int nodeId, long logTimestamp)
      throws IllegalPathException {
    if (!commonConfig.isEnableAuditLog()) {
      return;
    }
    try {
      createViewIfNecessary();
      if (noNeedInsertAuditLog(auditLogFields)) {
        return;
      }
      long userId = auditLogFields.getUserId();
      String user = String.valueOf(userId);
      if (userId == -1) {
        user = "none";
      }
      InsertRowStatement statement =
          generateInsertStatement(
              auditLogFields,
              log,
              DEVICE_PATH_CACHE.getPartialPath(String.format(AUDIT_LOG_DEVICE, nodeId, user)),
              logTimestamp);
      asyncBatchUtils.push(statement);
    } catch (Exception e) {
      logger.warn("[{}}] Failed to log audit events because", AUDIT_LOG_PREFIX, e);
    }
  }

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
