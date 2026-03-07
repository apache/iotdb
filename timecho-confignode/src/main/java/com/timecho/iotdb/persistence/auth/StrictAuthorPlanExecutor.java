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

package com.timecho.iotdb.persistence.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.ModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.auth.AuthorPlanExecutor;
import org.apache.iotdb.confignode.persistence.auth.IAuthorPlanExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckMaxClientNumResp;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.persistence.auth.AuthorInfo.NO_USER_MSG;

public class StrictAuthorPlanExecutor implements IAuthorPlanExecutor {

  private final IAuthorizer authorizer;
  private final IAuthorPlanExecutor commonAuthorPlanExecutor;

  public StrictAuthorPlanExecutor(IAuthorizer authorizer, ConfigManager configManager) {
    this.authorizer = authorizer;
    this.commonAuthorPlanExecutor = new AuthorPlanExecutor(authorizer, configManager);
  }

  @Override
  public TPermissionInfoResp login(String username, String password, boolean useEncryptedPassword) {
    TPermissionInfoResp resp =
        commonAuthorPlanExecutor.login(username, password, useEncryptedPassword);
    if (resp.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && resp.userInfo.userId == 0) {
      resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(
          new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
              .setMessage(
                  "SUPER USER is not allowed to login when separation of admin powers is enabled."));
    }
    return resp;
  }

  @Override
  public TSStatus checkSessionNumOnAlter(
      String username, int newSessionPerUser, ConfigManager configManager) {
    return commonAuthorPlanExecutor.checkSessionNumOnAlter(
        username, newSessionPerUser, configManager);
  }

  @Override
  public TSStatus checkSessionNumOnConnect(
      Map<String, Integer> currentSessionInfo, int rpcMaxConcurrentClientNum) {
    return commonAuthorPlanExecutor.checkSessionNumOnConnect(
        currentSessionInfo, rpcMaxConcurrentClientNum);
  }

  @Override
  public TCheckMaxClientNumResp checkMaxClientNumValid(int maxConcurrentClientNum)
      throws AuthException {
    return commonAuthorPlanExecutor.checkMaxClientNumValid(maxConcurrentClientNum);
  }

  @Override
  public String login4Pipe(String username, String password) {
    return commonAuthorPlanExecutor.login4Pipe(username, password);
  }

  @Override
  public TSStatus executeAuthorNonQuery(AuthorTreePlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    String userName = authorPlan.getUserName();
    // other checks have been done in datanode
    switch (authorType) {
      case CreateUser:
      case UpdateUserMaxSession:
      case UpdateUserMinSession:
      case CreateUserWithRawPassword:
      case CreateRole:
      case DropRole:
      case GrantRole:
      case GrantRoleToUser:
      case RevokeUser:
      case RevokeRole:
      case RevokeRoleFromUser:
        return commonAuthorPlanExecutor.executeAuthorNonQuery(authorPlan);

      case GrantUser:
        try {
          Set<Integer> permissions = authorPlan.getPermissions();
          User user = authorizer.getUser(userName);
          if (user == null) {
            return RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + userName);
          }
          // check user other admin permissions
          if (isGrantingDifferentAdminPowerToAdmin(user, permissions)) {
            return RpcUtils.getStatus(
                TSStatusCode.NO_PERMISSION,
                "When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.");
          }
        } catch (AuthException e) {
          return RpcUtils.getStatus(e.getCode(), e.getMessage());
        }
        return commonAuthorPlanExecutor.executeAuthorNonQuery(authorPlan);

      case DropUserV2:
        return dropUser(authorPlan);

      case UpdateUserV2:
        return updateUser(authorPlan);

      case RenameUser:
        return renameUser(authorPlan);

      default:
        return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR, "Unsupported author type.");
    }
  }

  @Override
  public TSStatus executeRelationalAuthorNonQuery(AuthorRelationalPlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    switch (authorType) {
      case RCreateUser:
      case RUpdateUserMaxSession:
      case RUpdateUserMinSession:
      case RCreateRole:
      case RDropRole:
      case RGrantUserRole:
      case RRevokeUserRole:
      case RGrantUserAny:
      case RGrantRoleAny:
      case RGrantUserAll:
      case RGrantRoleAll:
      case RGrantUserDBPriv:
      case RGrantUserTBPriv:
      case RGrantRoleDBPriv:
      case RGrantRoleTBPriv:
      case RRevokeUserAny:
      case RRevokeRoleAny:
      case RRevokeUserAll:
      case RRevokeRoleAll:
      case RRevokeUserDBPriv:
      case RRevokeUserTBPriv:
      case RRevokeRoleDBPriv:
      case RRevokeRoleTBPriv:
      case RGrantRoleSysPri:
      case RRevokeUserSysPri:
      case RRevokeRoleSysPri:
      case RAccountUnlock:
        return commonAuthorPlanExecutor.executeRelationalAuthorNonQuery(authorPlan);
      case RGrantUserSysPri:
        try {
          Set<Integer> permissions = authorPlan.getPermissions();
          String userName = authorPlan.getUserName();
          User user = authorizer.getUser(userName);
          if (user == null) {
            return RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + userName);
          }
          // check user other admin permissions
          if (isGrantingDifferentAdminPowerToAdmin(user, permissions)) {
            return RpcUtils.getStatus(
                TSStatusCode.NO_PERMISSION,
                "When enabling separation of admin powers, an admin is not allowed to be granted other admin permissions.");
          }
          return commonAuthorPlanExecutor.executeRelationalAuthorNonQuery(authorPlan);
        } catch (AuthException e) {
          return RpcUtils.getStatus(e.getCode(), e.getMessage());
        }
      case RUpdateUserV2:
        return updateUser(authorPlan);

      case RDropUserV2:
        return dropUser(authorPlan);

      case RRenameUser:
        return renameUser(authorPlan);

      default:
        return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR, "Unsupported author type.");
    }
  }

  private TSStatus dropUser(AuthorPlan authorPlan) {
    String userName = authorPlan.getUserName();
    try {
      User executedByUser = authorizer.getUser(authorPlan.getExecutedByUserId());
      User droppedUser = authorizer.getUser(userName);
      if (droppedUser == null) {
        return RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + userName);
      }
      if (isBuiltinUser(droppedUser.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "Builtin user is not allowed to be dropped.");
      }
      if (isSystemAdmin(droppedUser.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to drop system admin.");
      }
      if (isAuditAdmin(droppedUser.getUserId())) {
        return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION, "No permission to drop audit admin.");
      }
      if (isSecurityAdmin(droppedUser.getUserId())
          && !isBuiltinSecurityAdmin(executedByUser.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to drop security admin.");
      }
      authorizer.deleteUser(userName);
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private TSStatus updateUser(AuthorPlan authorPlan) {
    String userName = authorPlan.getUserName();
    String password =
        authorPlan instanceof AuthorTreePlan
            ? authorPlan.getNewPassword()
            : authorPlan.getPassword();
    try {
      User executedByUser = authorizer.getUser(authorPlan.getExecutedByUserId());
      User userToUpdate = authorizer.getUser(userName);
      if (userToUpdate == null) {
        return RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + userName);
      }
      if (userToUpdate.getUserId() == executedByUser.getUserId()) {
        authorizer.updateUserPassword(userName, password);
        return RpcUtils.SUCCESS_STATUS;
      }

      if (AuthorityChecker.SUPER_USER_ID == userToUpdate.getUserId()) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update original super user.");
      }

      if (isSystemAdmin(userToUpdate.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update system admin.");
      }

      if (isAuditAdmin(userToUpdate.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update audit admin.");
      }

      if (isSecurityAdmin(userToUpdate.getUserId())) {
        if (!isBuiltinSecurityAdmin(executedByUser.getUserId())) {
          return RpcUtils.getStatus(
              TSStatusCode.NO_PERMISSION, "No permission to update security admin.");
        }
        authorizer.updateUserPassword(userName, password);
        return RpcUtils.SUCCESS_STATUS;
      }

      if (!isSecurityAdmin(executedByUser.getUserId())) {
        return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION, "No permission to update user.");
      }
      authorizer.updateUserPassword(userName, password);
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private TSStatus renameUser(AuthorPlan authorPlan) {
    String userName = authorPlan.getUserName();
    String newUserName = authorPlan.getNewUsername();
    try {
      User executedByUser = authorizer.getUser(authorPlan.getExecutedByUserId());
      User userToUpdate = authorizer.getUser(userName);
      if (userToUpdate == null) {
        return RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + userName);
      }

      if (AuthorityChecker.SUPER_USER_ID == userToUpdate.getUserId()) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update original super user.");
      }

      if (userToUpdate.getUserId() == executedByUser.getUserId()) {
        authorizer.renameUser(userName, newUserName);
        return RpcUtils.SUCCESS_STATUS;
      }

      if (isSystemAdmin(userToUpdate.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update system admin.");
      }

      if (isAuditAdmin(userToUpdate.getUserId())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to update audit admin.");
      }

      if (isSecurityAdmin(userToUpdate.getUserId())) {
        if (!isBuiltinSecurityAdmin(executedByUser.getUserId())) {
          return RpcUtils.getStatus(
              TSStatusCode.NO_PERMISSION, "No permission to update security admin.");
        }
        authorizer.renameUser(userName, newUserName);
        return RpcUtils.SUCCESS_STATUS;
      }

      if (!isSecurityAdmin(executedByUser.getUserId())) {
        return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION, "No permission to update user.");
      }
      authorizer.renameUser(userName, newUserName);
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public PermissionInfoResp executeListUsers(AuthorPlan plan) throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    long executedByUserId = plan.getExecutedByUserId();
    final List<TListUserInfo> userInfoList;
    User user = authorizer.getUser(executedByUserId);
    boolean hasPermissionToListAllUsers = user.checkSysPrivilege(PrivilegeType.SECURITY);
    if (hasPermissionToListAllUsers) {
      // any user with SECURITY permission
      userInfoList = authorizer.listAllUsersInfo();
    } else if (isBuiltinSystemAdmin(executedByUserId)) {
      // builtin system admin
      userInfoList =
          authorizer.listAllUsersInfo().stream()
              .filter(userInfo -> isSystemAdmin(userInfo.getUserId()))
              .collect(Collectors.toList());
    } else if (isBuiltinAuditAdmin(executedByUserId)) {
      // builtin audit admin
      userInfoList =
          authorizer.listAllUsersInfo().stream()
              .filter(userInfo -> isAuditAdmin(userInfo.getUserId()))
              .collect(Collectors.toList());
    } else {
      // common user/admin without SECURITY permission
      userInfoList = new ArrayList<>(1);
      userInfoList.add(user.convertToListUserInfo());
    }

    if (!plan.getRoleName().isEmpty()) {
      // list user of role
      final Role role = authorizer.getRole(plan.getRoleName());
      if (role == null) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.ROLE_NOT_EXIST, "No such role : " + plan.getRoleName()));
        return result;
      }
      final Iterator<TListUserInfo> itr = userInfoList.iterator();
      while (itr.hasNext()) {
        TListUserInfo userInfo = itr.next();
        User userObj = authorizer.getUser(userInfo.getUserId());
        if (userObj == null || !userObj.hasRole(plan.getRoleName())) {
          itr.remove();
        }
      }
    }
    result.setTag(ColumnHeaderConstant.USER);
    result.setMemberInfo(
        userInfoList.stream().map(TListUserInfo::getUsername).collect(Collectors.toList()));
    result.setUsersInfo(userInfoList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  @Override
  public PermissionInfoResp executeListRoles(AuthorPlan plan) throws AuthException {
    long executedByUserId = plan.getExecutedByUserId();
    // checked before
    // 1. non builtin users query themselves
    // 2. SECURITY admin
    // 3. execute 'list role of user'
    if (!isBuiltinUser(executedByUserId)
        || isSecurityAdmin(executedByUserId)
        || !plan.getUserName().isEmpty()) {
      return commonAuthorPlanExecutor.executeListRoles(plan);
    }
    final PermissionInfoResp result = new PermissionInfoResp();

    result.setTag(ColumnHeaderConstant.ROLE);
    result.setMemberInfo(new ArrayList<>(getVisitableRolesForBuiltinAdmin(executedByUserId)));
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  @Override
  public PermissionInfoResp executeListRolePrivileges(AuthorPlan plan) throws AuthException {
    // checked before
    // 1. non builtin users query themselves
    // 2. SECURITY admin
    if (!isBuiltinUser(plan.getExecutedByUserId()) || isSecurityAdmin(plan.getExecutedByUserId())) {
      return commonAuthorPlanExecutor.executeListRolePrivileges(plan);
    }
    Set<String> visitableRolesForBuiltinAdmin =
        getVisitableRolesForBuiltinAdmin(plan.getExecutedByUserId());
    if (!visitableRolesForBuiltinAdmin.contains(plan.getRoleName())) {
      PermissionInfoResp resp = new PermissionInfoResp();
      resp.setStatus(AuthorityChecker.getTSStatus(false, PrivilegeType.SECURITY));
      return resp;
    }
    return commonAuthorPlanExecutor.executeListRolePrivileges(plan);
  }

  private Set<String> getVisitableRolesForBuiltinAdmin(long executedByUserId) throws AuthException {
    List<String> userList = authorizer.listAllUsers();
    Set<String> visitableRoles = new HashSet<>();
    if (isBuiltinSystemAdmin(executedByUserId)) {
      // builtin system admin
      for (String username : userList) {
        User user = authorizer.getUser(username);
        if (isSystemAdmin(user.getUserId())) {
          visitableRoles.addAll(user.getRoleSet());
        }
      }
    } else if (isBuiltinAuditAdmin(executedByUserId)) {
      // builtin audit admin
      for (String username : userList) {
        User user = authorizer.getUser(username);
        if (isAuditAdmin(user.getUserId())) {
          visitableRoles.addAll(user.getRoleSet());
        }
      }
    }
    return visitableRoles;
  }

  @Override
  public PermissionInfoResp executeListUserPrivileges(AuthorPlan plan) throws AuthException {
    TPermissionInfoResp user = getUser(plan.getUserName());
    PermissionInfoResp permissionInfoResp =
        commonAuthorPlanExecutor.executeListUserPrivileges(plan);
    if (permissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && AuthorityChecker.SUPER_USER_ID == user.getUserInfo().getUserId()) {
      TRoleResp permissionInfo =
          permissionInfoResp.getPermissionInfoResp().getUserInfo().getPermissionInfo();
      permissionInfo.setPrivilegeList(Collections.emptyList());
      permissionInfo.setSysPriSet(Collections.emptySet());
      permissionInfo.setSysPriSetGrantOpt(Collections.emptySet());
      permissionInfo.setAnyScopeSet(Collections.emptySet());
      permissionInfo.setAnyScopeGrantSet(Collections.emptySet());
      permissionInfo.setDbPrivilegeMap(Collections.emptyMap());
    }
    return permissionInfoResp;
  }

  @Override
  public TPermissionInfoResp getUserPermissionInfo(String username, ModelType type)
      throws AuthException {
    return commonAuthorPlanExecutor.getUserPermissionInfo(username, type);
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(String username, PrivilegeUnion union) {
    return commonAuthorPlanExecutor.checkUserPrivileges(username, union);
  }

  @Override
  public TAuthizedPatternTreeResp generateAuthorizedPTree(String username, int permission)
      throws AuthException {
    return commonAuthorPlanExecutor.generateAuthorizedPTree(username, permission);
  }

  @Override
  public PathPatternTree generateRawAuthorizedPTree(String username, PrivilegeType type)
      throws AuthException {
    return commonAuthorPlanExecutor.generateRawAuthorizedPTree(username, type);
  }

  @Override
  public TPermissionInfoResp checkRoleOfUser(String username, String roleName)
      throws AuthException {
    return commonAuthorPlanExecutor.checkRoleOfUser(username, roleName);
  }

  @Override
  public TPermissionInfoResp getUser(String username) throws AuthException {
    return commonAuthorPlanExecutor.getUser(username);
  }

  @Override
  public String getUserName(long userId) throws AuthException {
    return commonAuthorPlanExecutor.getUserName(userId);
  }

  private boolean isBuiltinUser(long userId) {
    return userId <= User.INTERNAL_USER_END_ID;
  }

  private boolean isSystemAdmin(long userId) {
    if (userId == AuthorityChecker.SUPER_USER_ID) {
      return false;
    }
    if (isBuiltinSystemAdmin(userId)) {
      return true;
    }
    try {
      User user = authorizer.getUser(userId);
      if (user == null) {
        return false;
      }
      return user.checkSysPrivilege(PrivilegeType.SYSTEM);
    } catch (AuthException ignored) {
      return false;
    }
  }

  private boolean isBuiltinSystemAdmin(long userId) {
    return userId == User.INTERNAL_SYSTEM_ADMIN;
  }

  private boolean isSecurityAdmin(long userId) {
    if (userId == AuthorityChecker.SUPER_USER_ID) {
      return false;
    }
    if (isBuiltinSecurityAdmin(userId)) {
      return true;
    }
    try {
      User user = authorizer.getUser(userId);
      if (user == null) {
        return false;
      }
      return user.checkSysPrivilege(PrivilegeType.SECURITY);
    } catch (AuthException ignored) {
      return false;
    }
  }

  private boolean isBuiltinSecurityAdmin(long userId) {
    return userId == User.INTERNAL_SECURITY_ADMIN;
  }

  private boolean isAuditAdmin(long userId) {
    if (userId == AuthorityChecker.SUPER_USER_ID) {
      return false;
    }
    if (isBuiltinAuditAdmin(userId)) {
      return true;
    }
    try {
      User user = authorizer.getUser(userId);
      if (user == null) {
        return false;
      }
      return user.checkSysPrivilege(PrivilegeType.AUDIT);
    } catch (AuthException ignored) {
      return false;
    }
  }

  private boolean isBuiltinAuditAdmin(long userId) {
    return userId == User.INTERNAL_AUDIT_ADMIN;
  }

  private boolean isGrantingDifferentAdminPowerToAdmin(User user, Set<Integer> privileges) {
    int adminPowerNumAfterGrant = 0;
    if (user.checkSysPrivilege(PrivilegeType.SYSTEM)) {
      adminPowerNumAfterGrant++;
    }
    if (user.checkSysPrivilege(PrivilegeType.SECURITY)) {
      adminPowerNumAfterGrant++;
    }
    if (user.checkSysPrivilege(PrivilegeType.AUDIT)) {
      adminPowerNumAfterGrant++;
    }
    if (adminPowerNumAfterGrant == 3) {
      return false;
    }
    for (int privilegeNum : privileges) {
      PrivilegeType privilege = PrivilegeType.values()[privilegeNum];
      if (!privilege.isAdminPrivilege()) {
        continue;
      }
      if (!user.checkSysPrivilege(privilege)) {
        adminPowerNumAfterGrant++;
      }
      if (adminPowerNumAfterGrant > 1) {
        return true;
      }
    }
    return false;
  }
}
