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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
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
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Date;
import java.util.Optional;

public class DataNodeAuthUtils {

  private DataNodeAuthUtils() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeAuthUtils.class);

  /**
   * @return the timestamp when the password of the user is lastly changed from the given one to a
   *     new one, or -1 if the password has not been changed.
   */
  public static long getPasswordChangeTimeMillis(String username, String password) {

    try {
      Statement statement =
          StatementGenerator.createStatement(
              "SELECT password from "
                  + SystemConstant.PREFIX_PASSWORD_HISTORY
                  + ".`_"
                  + username
                  + "` where oldPassword='"
                  + AuthUtils.encryptPassword(password)
                  + "' order by time desc limit 1",
              ZoneId.systemDefault());
      SessionInfo sessionInfo =
          new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault());

      long queryId = SessionManager.getInstance().requestQueryId();
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
      LOGGER.warn("Cannot generate query for checking password expiration", e);
    }
    return -1;
  }

  public static void verifyPasswordReuse(String username, String password) {
    long passwordReuseIntervalDays =
        CommonDescriptor.getInstance().getConfig().getPasswordReuseIntervalDays();
    if (password == null || passwordReuseIntervalDays <= 0) {
      return;
    }

    long passwordChangeTime = DataNodeAuthUtils.getPasswordChangeTimeMillis(username, password);
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

  public static TSStatus recordPassword(String username, String password, String oldPassword) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    try {
      insertRowStatement.setDevicePath(
          new PartialPath(SystemConstant.PREFIX_PASSWORD_HISTORY + ".`_" + username + "`"));
      insertRowStatement.setTime(CommonDateTimeUtils.currentTime());
      insertRowStatement.setMeasurements(new String[] {"password", "oldPassword"});
      insertRowStatement.setValues(
          new Object[] {
            new Binary(AuthUtils.encryptPassword(password), StandardCharsets.UTF_8),
            oldPassword == null ? null : new Binary(oldPassword, StandardCharsets.UTF_8)
          });
      insertRowStatement.setDataTypes(new TSDataType[] {TSDataType.STRING, TSDataType.STRING});
    } catch (IllegalPathException ignored) {
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "Cannot create password history for "
                  + username
                  + " because the path will be illegal");
    }

    try {
      SessionInfo sessionInfo =
          new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault());

      long queryId = SessionManager.getInstance().requestQueryId();
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
      LOGGER.error("Cannot create password history for {} because {}", username, e.getMessage());
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "Cannot create password history for " + username + " because " + e.getMessage());
    }
  }
}
