package org.apache.iotdb.db.service.basic;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.SessionTimeoutManager;
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.db.service.basic.dto.OpenSessionResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicServiceProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicServiceProvider.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

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

  protected OpenSessionResp openSession(
      String username, String password, String zoneId, TSProtocolVersion tsProtocolVersion)
      throws TException {
    OpenSessionResp openSessionResp = new OpenSessionResp();

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
        openSessionResp.setTsStatusCode(TSStatusCode.INCOMPATIBLE_VERSION);
        openSessionResp.setLoginMessage(
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        openSessionResp.setSessionId(sessionId);
        return openSessionResp;
      }

      openSessionResp.setLoginMessage("Login successfully");
      openSessionResp.setTsStatusCode(TSStatusCode.SUCCESS_STATUS);

      sessionId = sessionManager.requestSessionId(username, zoneId);
      AUDIT_LOGGER.info("User {} opens Session-{}", username, sessionId);
      LOGGER.info(
          "{}: Login status: {}. User : {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          openSessionResp.getLoginMessage(),
          username);
    } else {

      openSessionResp.setLoginMessage(
          loginMessage != null ? loginMessage : "Authentication failed.");
      openSessionResp.setTsStatusCode(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR);

      sessionId = sessionManager.requestSessionId(username, zoneId);
      AUDIT_LOGGER.info("User {} opens Session failed with an incorrect password", username);
    }

    SessionTimeoutManager.getInstance().register(sessionId);
    openSessionResp.setSessionId(sessionId);
    return openSessionResp;
  }

  protected boolean closeSession(long sessionId) {
    AUDIT_LOGGER.info("Session-{} is closing", sessionId);

    sessionManager.removeCurrSessionId();

    return SessionTimeoutManager.getInstance().unregister(sessionId);
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(CURRENT_RPC_VERSION);
  }
}
