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
import org.apache.iotdb.commons.audit.UserEntity;
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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

public class DataNodeAuthUtils {

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
   * Check if the password for the give user has expired.
   *
   * @return the timestamp when the password will expire. Long.MAX if the password never expires.
   *     Null if the password history cannot be found.
   */
  public static Long checkPasswordExpiration(long userId, String password) {
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
        if (oldPassword.equals(AuthUtils.encryptPassword(password))) {
          if (lastPasswordTime + passwordExpirationDays * 1000 * 86400 <= lastPasswordTime) {
            // overflow or passwordExpirationDays <= 0
            return Long.MAX_VALUE;
          } else {
            return lastPasswordTime + passwordExpirationDays * 1000 * 86400;
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
        return Long.MAX_VALUE;
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
}
