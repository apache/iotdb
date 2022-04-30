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

package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.mpp.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.SessionTimeoutManager;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  private ClusterAuthorizer clusterAuthorizer = new ClusterAuthorizer();
  private SessionManager sessionManager = SessionManager.getInstance();
  private IAuthorizer iAuthorizer;

  public AuthorizerManager() {
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error("Authorizer uninitialized");
    }
  }

  /** SingleTone */
  private static class AuthorizerManagerHolder {
    private static final AuthorizerManager INSTANCE = new AuthorizerManager();

    private AuthorizerManagerHolder() {}
  }

  public static AuthorizerManager getInstance() {
    return AuthorizerManager.AuthorizerManagerHolder.INSTANCE;
  }

  @Override
  public boolean login(String username, String password) throws AuthException {
    return iAuthorizer.login(username, password);
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    iAuthorizer.createUser(username, password);
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    iAuthorizer.deleteUser(username);
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.grantPrivilegeToUser(username, path, privilegeId);
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.revokePrivilegeFromUser(username, path, privilegeId);
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    iAuthorizer.createRole(roleName);
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    iAuthorizer.deleteRole(roleName);
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.grantPrivilegeToRole(roleName, path, privilegeId);
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.revokePrivilegeFromRole(roleName, path, privilegeId);
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    iAuthorizer.grantRoleToUser(roleName, username);
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    iAuthorizer.revokeRoleFromUser(roleName, username);
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    return iAuthorizer.getPrivileges(username, path);
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    iAuthorizer.updateUserPassword(username, newPassword);
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    return iAuthorizer.checkUserPrivileges(username, path, privilegeId);
  }

  @Override
  public void reset() throws AuthException {}

  @Override
  public List<String> listAllUsers() {
    return iAuthorizer.listAllUsers();
  }

  @Override
  public List<String> listAllRoles() {
    return iAuthorizer.listAllRoles();
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    return iAuthorizer.getRole(roleName);
  }

  @Override
  public User getUser(String username) throws AuthException {
    return iAuthorizer.getUser(username);
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    return iAuthorizer.isUserUseWaterMark(userName);
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    iAuthorizer.setUserUseWaterMark(userName, useWaterMark);
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    return iAuthorizer.getAllUserWaterMarkStatus();
  }

  @Override
  public Map<String, User> getAllUsers() {
    return iAuthorizer.getAllUsers();
  }

  @Override
  public Map<String, Role> getAllRoles() {
    return iAuthorizer.getAllRoles();
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    iAuthorizer.replaceAllUsers(users);
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    iAuthorizer.replaceAllRoles(roles);
  }

  public SettableFuture<ConfigTaskResult> operatePermission(TAuthorizerReq authorizerReq) {
    return clusterAuthorizer.operatePermission(authorizerReq);
  }

  public SettableFuture<ConfigTaskResult> queryPermission(TAuthorizerReq authorizerReq) {
    return clusterAuthorizer.queryPermission(authorizerReq);
  }

  public BasicOpenSessionResp openSession(
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion)
      throws TException {
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();
    TSStatus status;
    status = clusterAuthorizer.login(new TLoginReq(username, password));
    long sessionId = -1;
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // check the version compatibility
      boolean compatible = tsProtocolVersion.equals(SessionManager.CURRENT_RPC_VERSION);
      if (!compatible) {
        openSessionResp.setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
        openSessionResp.setMessage(
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        return openSessionResp.sessionId(sessionId);
      }

      openSessionResp.setCode(status.getCode());
      openSessionResp.setMessage(status.getMessage());

      sessionId = sessionManager.requestSessionId(username, zoneId, clientVersion);

      logger.info(
          "{}: Login status: {}. User : {}, opens Session-{}",
          IoTDBConstant.GLOBAL_DB_NAME,
          openSessionResp.getMessage(),
          username,
          sessionId);
    } else {
      openSessionResp.setMessage(status.getMessage());
      openSessionResp.setCode(status.getCode());

      sessionId = sessionManager.requestSessionId(username, zoneId, clientVersion);
      SessionManager.AUDIT_LOGGER.info(
          "User {} opens Session failed with an incorrect password", username);
    }

    SessionTimeoutManager.getInstance().register(sessionId);
    return openSessionResp.sessionId(sessionId);
  }

  /** Check whether specific Session has the authorization to given plan. */
  public TSStatus checkAuthority(Statement statement, long sessionId) {
    try {
      if (!checkAuthorization(statement, sessionManager.getUsername(sessionId))) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation " + statement.getType());
      }
    } catch (AuthException e) {
      logger.warn("meet error while checking authorization.", e);
      return RpcUtils.getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CHECK_AUTHORITY, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /** Check whether specific user has the authorization to given plan. */
  public boolean checkAuthorization(Statement statement, String username) throws AuthException {
    if (!statement.isAuthenticationRequired()) {
      return true;
    }
    String targetUser = null;
    if (statement instanceof AuthorStatement) {
      targetUser = ((AuthorStatement) statement).getUserName();
    }
    return AuthorityChecker.checkPermission(
        username, statement.getPaths(), statement.getType(), targetUser);
  }

  public TSStatus checkPath(String username, List<String> allPath, int permission) {
    return clusterAuthorizer.checkUserPrivileges(
        new TCheckUserPrivilegesReq(username, allPath, permission));
  }
}
