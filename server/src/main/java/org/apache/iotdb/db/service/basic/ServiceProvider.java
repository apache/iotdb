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
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.SessionTimeoutManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.db.utils.AuditLogUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;

/**
 * There is only one ServiceProvider instance for each IoTDB instance. Both client-thread model
 * based services and message-thread model based services (e.g., mqtt) are using this service.
 */
public abstract class ServiceProvider {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ServiceProvider.class);

  public static final Logger SLOW_SQL_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SLOW_SQL_LOGGER_NAME);

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  public static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public static final QueryTimeManager QUERY_TIME_MANAGER = QueryTimeManager.getInstance();
  public static final TracingManager TRACING_MANAGER = TracingManager.getInstance();
  public static final QueryFrequencyRecorder QUERY_FREQUENCY_RECORDER =
      new QueryFrequencyRecorder(CONFIG);

  public static SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final Planner planner;
  protected final IPlanExecutor executor;

  public Planner getPlanner() {
    return planner;
  }

  public IPlanExecutor getExecutor() {
    return executor;
  }

  public ServiceProvider(PlanExecutor executor) throws QueryProcessException {
    planner = new Planner();
    this.executor = executor;
  }

  public abstract QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout);

  public abstract boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException;

  /**
   * Check whether current user has logged in. If login, the session's lifetime will be updated.
   *
   * @return true: If logged in; false: If not logged in
   */
  public boolean checkLogin(IClientSession session) {
    boolean isLoggedIn = session != null && session.isLogin();
    if (!isLoggedIn) {
      LOGGER.info("{}: Not login. ", IoTDBConstant.GLOBAL_DB_NAME);
      return false;
    } else {
      SessionTimeoutManager.getInstance().refresh(session);
    }
    return isLoggedIn;
  }

  /**
   * Check whether current session is timeout.
   *
   * @param session clientSession.
   * @return true: If session timeout; false: If not session timeout.
   */
  public boolean checkSessionTimeout(IClientSession session) {
    if (!SessionTimeoutManager.getInstance().isSessionAlive(session)) {
      return true;
    }
    return false;
  }

  public boolean checkAuthorization(PhysicalPlan plan, String username) throws AuthException {
    if (!plan.isAuthenticationRequired()) {
      return true;
    }

    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(
        username, plan.getAuthPaths(), plan.getOperatorType(), targetUser);
  }

  public TSStatus checkAuthority(PhysicalPlan plan, IClientSession session) {
    try {
      if (!checkAuthorization(plan, session.getUsername())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation, please add privilege "
                + PrivilegeType.values()[
                    AuthorityChecker.translateToPermissionId(plan.getOperatorType())]);
      }
    } catch (AuthException e) {
      LOGGER.warn("meet error while checking authorization.", e);
      return RpcUtils.getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CHECK_AUTHORITY, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return null;
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion,
      boolean enableAudit)
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

    if (status) {
      // check the version compatibility
      boolean compatible = checkCompatibility(tsProtocolVersion);
      if (!compatible) {
        openSessionResp.setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
        openSessionResp.setMessage(
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        return openSessionResp.sessionId(-1);
      }

      openSessionResp.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      openSessionResp.setMessage("Login successfully");

      SESSION_MANAGER.supplySession(session, username, zoneId, clientVersion, enableAudit);
      AuditLogUtils.writeAuditLog(
          String.format(
              "%s: Login status: %s. User : %s, opens Session-%s",
              IoTDBConstant.GLOBAL_DB_NAME, openSessionResp.getMessage(), username, session));

    } else {
      openSessionResp.setMessage(loginMessage != null ? loginMessage : "Authentication failed.");
      openSessionResp.setCode(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR.getStatusCode());
      session.setUsername(username);
      AuditLogUtils.writeAuditLog(
          String.format("User %s opens Session failed with an incorrect password", username), true);
      // TODO we should close this connection ASAP, otherwise there will be DDoS.
    }
    SessionTimeoutManager.getInstance().register(session);
    return openSessionResp.sessionId(session == null ? -1 : session.getId());
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion)
      throws TException {
    return login(
        session,
        username,
        password,
        zoneId,
        tsProtocolVersion,
        IoTDBConstant.ClientVersion.V_0_12,
        false);
  }

  public boolean closeSession(IClientSession session) {
    AuditLogUtils.writeAuditLog(String.format("Session-%s is closing", session));
    return SessionTimeoutManager.getInstance().unregister(session);
  }

  public TSStatus closeOperation(
      IClientSession session,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId) {
    if (!checkLogin(session)) {
      return RpcUtils.getStatus(
          TSStatusCode.NOT_LOGIN_ERROR,
          "Log in failed. Either you are not authorized or the session has timed out.");
    }
    if (checkSessionTimeout(session)) {
      return RpcUtils.getStatus(TSStatusCode.SESSION_TIMEOUT, "Session timeout");
    }
    AuditLogUtils.writeAuditLog(
        String.format(
            "%s: receive close operation from Session %s", IoTDBConstant.GLOBAL_DB_NAME, session));
    try {
      if (haveStatementId) {
        if (haveSetQueryId) {
          SESSION_MANAGER.closeDataset(statementId, queryId);
        } else {
          SESSION_MANAGER.closeStatement(session, statementId);
        }
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.CLOSE_OPERATION_ERROR, "statement id not set by client.");
      }
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CLOSE_OPERATION, TSStatusCode.CLOSE_OPERATION_ERROR);
    }
  }

  /** create QueryDataSet and buffer it for fetchResults */
  public QueryDataSet createQueryDataSet(
      QueryContext context, PhysicalPlan physicalPlan, int fetchSize)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
          IOException, MetadataException, SQLException, TException, InterruptedException {

    QueryDataSet queryDataSet = executor.processQuery(physicalPlan, context);
    queryDataSet.setFetchSize(fetchSize);
    SESSION_MANAGER.setDataset(context.getQueryId(), queryDataSet);
    return queryDataSet;
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(CURRENT_RPC_VERSION);
  }
}
