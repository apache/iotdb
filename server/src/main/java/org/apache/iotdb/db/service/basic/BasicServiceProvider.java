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
package org.apache.iotdb.db.service.basic;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.SessionTimeoutManager;
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.db.service.basic.dto.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.dto.BasicResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BasicServiceProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicServiceProvider.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  private static final String INFO_NOT_LOGIN = "{}: Not login. ";

  protected final QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();
  protected final SessionManager sessionManager = SessionManager.getInstance();
  protected final TracingManager tracingManager = TracingManager.getInstance();

  protected Planner processor;
  protected IPlanExecutor executor;

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  public BasicServiceProvider() throws QueryProcessException {
    processor = new Planner();
    executor = new PlanExecutor();
  }

  protected BasicOpenSessionResp openSession(
      String username, String password, String zoneId, TSProtocolVersion tsProtocolVersion)
      throws TException {
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    String loginMessage = null;
    try {
      status = authorizer.login(username, password);
    } catch (AuthException e) {
      LOGGER.info("meet error while logging in.", e);
      status = false;
      loginMessage = e.getMessage();
    }

    long sessionId = -1;
    if (status) {
      // check the version compatibility
      boolean compatible = checkCompatibility(tsProtocolVersion);
      if (!compatible) {
        openSessionResp
            .sessionId(sessionId)
            .message("The version is incompatible, please upgrade to " + IoTDBConstant.VERSION)
            .tsStatusCode(TSStatusCode.INCOMPATIBLE_VERSION);
        return openSessionResp;
      }

      openSessionResp.message("Login successfully").tsStatusCode(TSStatusCode.SUCCESS_STATUS);

      sessionId = sessionManager.requestSessionId(username, zoneId);
      AUDIT_LOGGER.info("User {} opens Session-{}", username, sessionId);
      LOGGER.info(
          "{}: Login status: {}. User : {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          openSessionResp.getMessage(),
          username);
    } else {

      openSessionResp
          .message(loginMessage != null ? loginMessage : "Authentication failed.")
          .tsStatusCode(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR);

      sessionId = sessionManager.requestSessionId(username, zoneId);
      AUDIT_LOGGER.info("User {} opens Session failed with an incorrect password", username);
    }

    SessionTimeoutManager.getInstance().register(sessionId);
    return openSessionResp.sessionId(sessionId);
  }

  protected boolean closeSession(long sessionId) {
    AUDIT_LOGGER.info("Session-{} is closing", sessionId);

    sessionManager.removeCurrSessionId();

    return SessionTimeoutManager.getInstance().unregister(sessionId);
  }

  protected BasicResp closeOperation(
      long sessionId,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId) {
    if (!checkLogin(sessionId)) {
      return new BasicResp(
          TSStatusCode.NOT_LOGIN_ERROR,
          "Log in failed. Either you are not authorized or the session has timed out.");
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "{}: receive close operation from Session {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          sessionManager.getCurrSessionId());
    }

    try {
      if (haveStatementId) {
        if (haveSetQueryId) {
          sessionManager.closeDataset(statementId, queryId);
        } else {
          sessionManager.closeStatement(sessionId, statementId);
        }
        return new BasicResp(TSStatusCode.SUCCESS_STATUS);
      } else {
        return new BasicResp(TSStatusCode.CLOSE_OPERATION_ERROR, "statement id not set by client.");
      }
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CLOSE_OPERATION, TSStatusCode.CLOSE_OPERATION_ERROR);
    }
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  protected boolean checkLogin(long sessionId) {
    boolean isLoggedIn = sessionManager.getUsername(sessionId) != null;
    if (!isLoggedIn) {
      LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
    } else {
      SessionTimeoutManager.getInstance().refresh(sessionId);
    }
    return isLoggedIn;
  }

  protected BasicResp checkAuthority(PhysicalPlan plan, long sessionId) {
    List<PartialPath> paths = plan.getPaths();
    try {
      if (!checkAuthorization(paths, plan, sessionManager.getUsername(sessionId))) {
        return new BasicResp(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      LOGGER.warn("meet error while checking authorization.", e);
      return new BasicResp(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CHECK_AUTHORITY, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return null;
  }

  private boolean checkAuthorization(List<PartialPath> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  protected boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    plan.checkIntegrity();
    if (!(plan instanceof SetSystemModePlan)
        && !(plan instanceof FlushPlan)
        && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(CURRENT_RPC_VERSION);
  }

  private BasicResp onNPEOrUnexpectedException(
      Exception e, String operation, TSStatusCode statusCode) {
    String message = String.format("[%s] Exception occurred: %s failed. ", statusCode, operation);
    if (e instanceof NullPointerException) {
      LOGGER.error("Status code: {}, operation: {} failed", statusCode, operation, e);
    } else {
      LOGGER.warn("Status code: {}, operation: {} failed", statusCode, operation, e);
    }
    return new BasicResp(statusCode, message + e.getMessage());
  }

  private BasicResp onNPEOrUnexpectedException(
      Exception e, OperationType operation, TSStatusCode statusCode) {
    return onNPEOrUnexpectedException(e, operation.getName(), statusCode);
  }
}
