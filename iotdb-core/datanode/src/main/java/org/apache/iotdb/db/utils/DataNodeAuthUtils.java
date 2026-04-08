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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSVisitHistoryResp;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class DataNodeAuthUtils {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private DataNodeAuthUtils() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeAuthUtils.class);

  /**
   * @return the timestamp when the password of the user is lastly changed from the given one to a
   *     new one, or -1 if the password has not been changed.
   */
  public static long getPasswordChangeTimeMillis(long userId, String password) {

    long queryId = -1;
    try {
      Statement statement =
          StatementGenerator.createStatement(
              "SELECT password from "
                  + DNAuditLogger.PREFIX_PASSWORD_HISTORY
                  + ".`_"
                  + userId
                  + "` where oldPassword='"
                  + AuthUtils.encryptPassword(password)
                  + "' order by time desc limit 1",
              ZoneId.systemDefault());

      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Fail to get password change time: {}", result.status);
        return -1;
      }

      IQueryExecution queryExecution = Coordinator.getInstance().getQueryExecution(queryId);
      TsBlock lastTsBlock;
      Optional<TsBlock> batchResult = queryExecution.getBatchResult();
      lastTsBlock = batchResult.orElse(null);
      if (lastTsBlock != null) {
        if (lastTsBlock.getPositionCount() <= 0) {
          // no password history, may have upgraded from an older version
          return -1;
        }
        long timeByIndex = lastTsBlock.getTimeByIndex(lastTsBlock.getPositionCount() - 1);
        return CommonDateTimeUtils.convertIoTDBTimeToMillis(timeByIndex);
      }
    } catch (IoTDBException e) {
      LOGGER.warn("Cannot generate query for checking password reuse interval", e);
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
    return -1;
  }

  public static void verifyPasswordReuse(long userId, String password) {
    long passwordReuseIntervalDays =
        CommonDescriptor.getInstance().getConfig().getPasswordReuseIntervalDays();
    if (password == null || passwordReuseIntervalDays <= 0) {
      return;
    }

    long passwordChangeTime = DataNodeAuthUtils.getPasswordChangeTimeMillis(userId, password);
    long currentTimeMillis = System.currentTimeMillis();
    long elapsedTime = currentTimeMillis - passwordChangeTime;
    long reuseIntervalMillis =
        passwordReuseIntervalDays * 1000 * 86400 > 0
            ? passwordReuseIntervalDays * 1000 * 86400
            : Long.MAX_VALUE;
    if (elapsedTime <= reuseIntervalMillis) {
      throw new SemanticException(
          String.format(
              "The password has been used recently, and it cannot be reused before %s",
              new Date(passwordChangeTime + reuseIntervalMillis)));
    }
    LOGGER.info(
        "It has been {}ms, since the password was changed {} -> {}",
        elapsedTime,
        passwordChangeTime,
        currentTimeMillis);
  }

  public static TSStatus recordPasswordHistory(
      long userId, String password, String oldEncryptedPassword, long timeToRecord) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    try {
      insertRowStatement.setDevicePath(
          new PartialPath(DNAuditLogger.PREFIX_PASSWORD_HISTORY + ".`_" + userId + "`"));
      insertRowStatement.setTime(timeToRecord);
      insertRowStatement.setMeasurements(new String[] {"password", "oldPassword"});
      insertRowStatement.setValues(
          new Object[] {
            new Binary(AuthUtils.encryptPassword(password), StandardCharsets.UTF_8),
            oldEncryptedPassword == null
                ? null
                : new Binary(oldEncryptedPassword, StandardCharsets.UTF_8)
          });
      insertRowStatement.setDataTypes(new TSDataType[] {TSDataType.STRING, TSDataType.STRING});
    } catch (IllegalPathException ignored) {
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "Cannot create password history for " + userId + " because the path will be illegal");
    }

    long queryId = -1;
    try {
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  insertRowStatement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      return result.status;
    } catch (Exception e) {
      if (CommonDescriptor.getInstance().getConfig().getPasswordExpirationDays() < 0
          && CommonDescriptor.getInstance().getConfig().getPasswordReuseIntervalDays() < 0) {
        // password history is not used in the current configuration, tolerate the failure
        return StatusUtils.OK;
      }
      if (CommonDescriptor.getInstance().getConfig().isMayBypassPasswordCheckInException()) {
        return StatusUtils.OK;
      }
      LOGGER.error("Cannot create password history for {}", userId, e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("The server is not ready for login, please check the server log for details");
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  public static TSStatus deletePasswordHistory(long userId) {
    DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
    deleteTimeSeriesStatement.setMayDeleteAudit(true);
    try {
      PartialPath devicePath =
          new PartialPath(DNAuditLogger.PREFIX_PASSWORD_HISTORY + ".`_" + userId + "`");
      deleteTimeSeriesStatement.setPathPatternList(
          Arrays.asList(
              devicePath.concatAsMeasurementPath("password"),
              devicePath.concatAsMeasurementPath("oldPassword")));
    } catch (IllegalPathException ignored) {
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "Cannot delete password history for " + userId + " because the path will be illegal");
    }

    long queryId = -1;
    try {
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  deleteTimeSeriesStatement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      return result.status;
    } catch (Exception e) {
      if (CommonDescriptor.getInstance().getConfig().isMayBypassPasswordCheckInException()) {
        return StatusUtils.OK;
      }
      LOGGER.error("Cannot delete password history for {}", userId, e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "The server is not ready for this operation, please check the server log for details");
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  /**
   * Clear login history before a given time for the specified user across all nodes.
   *
   * <p>Uses path pattern {@code root.__audit.login.u_{userId}.**} with a time filter so that a
   * single execution on one DataNode deletes matching rows on all nodes. Intended to be called
   * asynchronously after successful login (fire-and-forget). Exceptions are logged and not
   * rethrown.
   *
   * @param username the username (for logging)
   * @param userId the user id
   * @param beforeTimeIoTDB IoTDB timestamp; rows with time &lt; this value are deleted (use {@link
   *     org.apache.iotdb.commons.utils.CommonDateTimeUtils#currentTime()})
   */
  public static void clearLoginHistoryForLoginSuccess(
      String username, long userId, long beforeTimeIoTDB) {
    if (!commonConfig.isEnableAuditLog()) {
      return;
    }
    long queryId = -1;
    String sql =
        String.format(
            "DELETE FROM " + DNAuditLogger.VISIT_HISTORY_PATH + " WHERE time < %d",
            userId,
            beforeTimeIoTDB);
    try {
      Statement statement = StatementGenerator.createStatement(sql, ZoneId.systemDefault());
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());
      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Fail to clear login history before time for {}: {}", username, result.status);
      }
    } catch (Exception e) {
      LOGGER.warn("Fail to clear login history before time for {}", username, e);
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  /**
   * Delete all login history for the specified user across all nodes. Intended to be called
   * asynchronously after drop user. Exceptions are logged and not rethrown (fire-and-forget).
   *
   * <p>Uses {@link DeleteTimeSeriesStatement} with path pattern {@code
   * root.__audit.login.u_{userId}.**} so that a single execution on one DataNode removes all login
   * history on every node.
   *
   * @param username the username
   * @param userId the user id
   */
  public static void clearLoginHistoryForDropUser(String username, long userId) {
    DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
    deleteTimeSeriesStatement.setMayDeleteAudit(true);
    try {
      deleteTimeSeriesStatement.setPathPatternList(
          Collections.singletonList(
              new PartialPath(String.format(DNAuditLogger.VISIT_HISTORY_PATH, userId))));
    } catch (IllegalPathException ignored) {
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "Cannot delete login history for user " + username + " because the path is illegal");
      return;
    }

    long queryId = -1;
    try {
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      Coordinator.getInstance()
          .executeForTreeModel(
              deleteTimeSeriesStatement,
              queryId,
              sessionInfo,
              "",
              ClusterPartitionFetcher.getInstance(),
              ClusterSchemaFetcher.getInstance());
    } catch (Exception e) {
      LOGGER.warn("Fail to delete login history for user {}", username, e);
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Fail to delete login history for user " + username);
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  /**
   * Check if the password for the give user has expired.
   *
   * @return the first element is the timestamp when the password will expire, Long.MAX if the
   *     password never expires. the second element is the time when the password was changed to the
   *     current value Null if the password history cannot be found. Both timestamps are in
   *     millis-seconds.
   */
  public static Pair<Long, Long> checkPasswordExpiration(
      final long userId, final String password, final boolean useEncryptedPassword) {
    if (userId == -1) {
      return null;
    }

    // check password expiration
    long passwordExpirationDays =
        CommonDescriptor.getInstance().getConfig().getPasswordExpirationDays();
    boolean mayBypassPasswordCheckInException =
        CommonDescriptor.getInstance().getConfig().isMayBypassPasswordCheckInException();

    TSLastDataQueryReq lastDataQueryReq = new TSLastDataQueryReq();
    lastDataQueryReq.setSessionId(0);
    lastDataQueryReq.setPaths(
        Collections.singletonList(
            DNAuditLogger.PREFIX_PASSWORD_HISTORY + ".`_" + userId + "`.password"));

    long queryId = -1;
    try {
      Statement statement = StatementGenerator.createStatement(lastDataQueryReq);
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Fail to check password expiration: {}", result.status);
        throw new IoTDBRuntimeException(
            "Cannot query password history because: "
                + result
                + ", please log in later or disable password expiration.",
            result.status.getCode());
      }

      IQueryExecution queryExecution = Coordinator.getInstance().getQueryExecution(queryId);
      Optional<TsBlock> batchResult = queryExecution.getBatchResult();
      if (batchResult.isPresent()) {
        TsBlock tsBlock = batchResult.get();
        if (tsBlock.getPositionCount() <= 0) {
          // no password history, may have upgraded from an older version
          return null;
        }
        long lastPasswordTime =
            CommonDateTimeUtils.convertIoTDBTimeToMillis(tsBlock.getTimeByIndex(0));
        // columns of last query: [timeseriesName, value, dataType]
        String oldPassword = tsBlock.getColumn(1).getBinary(0).toString();
        if (oldPassword.equals(
            useEncryptedPassword ? password : AuthUtils.encryptPassword(password))) {
          if (lastPasswordTime + passwordExpirationDays * 1000 * 86400 <= lastPasswordTime) {
            // overflow or passwordExpirationDays <= 0
            return new Pair<>(Long.MAX_VALUE, lastPasswordTime);
          } else {
            return new Pair<>(
                lastPasswordTime + passwordExpirationDays * 1000 * 86400, lastPasswordTime);
          }
        } else {
          // 1. the password is incorrect, later logIn will fail
          // 2. the password history does not record correctly, use the current time to create one
          return null;
        }
      } else {
        return null;
      }
    } catch (Throwable e) {
      LOGGER.error("Fail to check password expiration", e);
      if (mayBypassPasswordCheckInException) {
        return new Pair<>(Long.MAX_VALUE, 0L);
      } else {
        throw new IoTDBRuntimeException(
            "Internal server error " + ", please log in later or disable password expiration.",
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  public static void recordLoginHistory(
      String username, long userId, String hostname, boolean loginResult, long timeToRecord) {
    if (!commonConfig.isEnableAuditLog()) {
      return;
    }
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    try {
      String dataNodeId = String.valueOf(config.getDataNodeId());
      insertRowStatement.setDevicePath(
          new PartialPath(String.format(DNAuditLogger.PREFIX_LOGIN_HISTORY, userId, dataNodeId)));
      insertRowStatement.setTime(timeToRecord);
      insertRowStatement.setMeasurements(
          new String[] {
            AbstractAuditLogger.AUDIT_LOG_USERNAME, "ip", AbstractAuditLogger.AUDIT_LOG_RESULT
          });
      insertRowStatement.setValues(
          new Object[] {
            new Binary(username == null ? "null" : username, TSFileConfig.STRING_CHARSET),
            new Binary(hostname == null ? "null" : hostname, TSFileConfig.STRING_CHARSET),
            loginResult
          });
      insertRowStatement.setDataTypes(
          new TSDataType[] {TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN});
      insertRowStatement.setAligned(true);
    } catch (IllegalPathException e) {
      LOGGER.warn(
          "Cannot create password history for {} ,  because the path will be illegal:{}",
          username,
          e.getMessage());
    }
    long queryId = -1;
    try {
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      Coordinator.getInstance()
          .executeForTreeModel(
              insertRowStatement,
              queryId,
              sessionInfo,
              "",
              ClusterPartitionFetcher.getInstance(),
              ClusterSchemaFetcher.getInstance());
    } catch (Exception e) {
      LOGGER.error("Cannot record login history for {}", username, e);
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }

  public static TSStatus getVisitHistory(BasicOpenSessionResp basicOpenSessionResp, long userId) {
    boolean enableAuditLog = commonConfig.isEnableAuditLog();
    if (!enableAuditLog) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    TSVisitHistoryResp tsVisitHistoryResp = new TSVisitHistoryResp();
    try {
      fillLastSuccessLoginInfo(userId, tsVisitHistoryResp);
      fillLastFailedLoginInfo(
          userId, tsVisitHistoryResp.getLastSuccessloginTime(), tsVisitHistoryResp);
      tsVisitHistoryResp.setFailedAttempts(
          computeFailedLoginAttempts(userId, tsVisitHistoryResp.getLastSuccessloginTime()));
      basicOpenSessionResp.setTsVisitHistoryResp(tsVisitHistoryResp);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      LOGGER.warn("Cannot generate query for visit history interval.", e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Meet errors when getting visit history.");
    }
  }

  private static Optional<TsBlock> findLastNonEmptyTsBlock(List<TsBlock> tsBlocks) {
    TsBlock last = null;
    for (TsBlock block : tsBlocks) {
      if (block != null && block.getPositionCount() > 0) {
        last = block;
      }
    }
    return Optional.ofNullable(last);
  }

  private static void fillLastSuccessLoginInfo(long userId, TSVisitHistoryResp resp) {
    String sql =
        String.format(
            "SELECT ip FROM "
                + DNAuditLogger.VISIT_HISTORY_PATH
                + " WHERE result = true ORDER BY time DESC LIMIT 1 ALIGN BY DEVICE",
            userId);
    List<TsBlock> tsBlocks = getTsBlockBySql(sql);
    Optional<TsBlock> lastTsBlock = findLastNonEmptyTsBlock(tsBlocks);
    resp.setLastSuccessIp(
        lastTsBlock
            .map(b -> new String(b.getColumn(1).getBinary(0).getValues(), StandardCharsets.UTF_8))
            .orElse("null"));
    resp.setLastSuccessloginTime(
        lastTsBlock.map(b -> b.getTimeByIndex(b.getPositionCount() - 1)).orElse(0L));
  }

  private static void fillLastFailedLoginInfo(
      long userId, long afterTime, TSVisitHistoryResp resp) {
    String sql =
        String.format(
            "SELECT ip FROM "
                + DNAuditLogger.VISIT_HISTORY_PATH
                + " WHERE result = false AND time > %d ORDER BY time DESC LIMIT 1 ALIGN BY DEVICE",
            userId,
            afterTime);
    List<TsBlock> tsBlocks = getTsBlockBySql(sql);
    Optional<TsBlock> lastTsBlock = findLastNonEmptyTsBlock(tsBlocks);
    resp.setLastFailedIp(
        lastTsBlock
            .map(b -> new String(b.getColumn(1).getBinary(0).getValues(), StandardCharsets.UTF_8))
            .orElse("null"));
    resp.setLastFailedLoginTime(
        lastTsBlock.map(b -> b.getTimeByIndex(b.getPositionCount() - 1)).orElse(0L));
  }

  private static int computeFailedLoginAttempts(long userId, long afterSuccessTime) {
    String sql =
        String.format(
            "SELECT count(ip) from "
                + DNAuditLogger.VISIT_HISTORY_PATH
                + " where result = false AND time > %d ALIGN BY DEVICE",
            userId,
            afterSuccessTime);
    List<TsBlock> tsBlocks = getTsBlockBySql(sql);
    int count = 0;
    for (TsBlock tsBlock : tsBlocks) {
      if (tsBlock != null && tsBlock.getPositionCount() > 0) {
        count += tsBlock.getColumn(1).getLong(0);
      }
    }
    return count;
  }

  private static List<TsBlock> getTsBlockBySql(String sql) {
    long queryId = -1;
    List<TsBlock> tsBlocks = new ArrayList<>();
    try {
      Statement statement = StatementGenerator.createStatement(sql, ZoneId.systemDefault());
      SessionInfo sessionInfo =
          new SessionInfo(
              0,
              new UserEntity(
                  AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                  AuthorityChecker.INTERNAL_AUDIT_USER,
                  IoTDBDescriptor.getInstance().getConfig().getInternalAddress()),
              ZoneId.systemDefault());

      queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  sql,
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                .setMessage("Meet errors when getting visit history."));
      }
      IQueryExecution queryExecution = Coordinator.getInstance().getQueryExecution(queryId);
      while (true) {
        Optional<TsBlock> batchResult = queryExecution.getBatchResult();
        if (!batchResult.isPresent()) {
          break;
        }
        TsBlock tsBlock = batchResult.get();
        if (tsBlock.getPositionCount() > 0) {
          tsBlocks.add(tsBlock);
        }
      }
      return tsBlocks;
    } catch (IoTDBException e) {
      LOGGER.warn("Cannot generate query for visit history interval", e);
      return tsBlocks;
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
  }
}
