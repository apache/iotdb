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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.OpenIdAuthorizer;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorInfo implements SnapshotProcessor {

  private static final Logger logger = LoggerFactory.getLogger(AuthorInfo.class);
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private IAuthorizer authorizer;

  public AuthorInfo() {
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error("get user or role permissionInfo failed because ", e);
    }
  }

  public TPermissionInfoResp login(String username, String password) {
    boolean status;
    String loginMessage = null;
    TSStatus tsStatus = new TSStatus();
    TPermissionInfoResp result = new TPermissionInfoResp();
    try {
      status = authorizer.login(username, password);
      if (status) {
        // Bring this user's permission information back to the datanode for caching
        if (authorizer instanceof OpenIdAuthorizer) {
          username = ((OpenIdAuthorizer) authorizer).getIoTDBUserName(username);
          result = getUserPermissionInfo(username);
          result.getUserInfo().setIsOpenIdUser(true);
        } else {
          result = getUserPermissionInfo(username);
        }

        result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Login successfully"));
      } else {
        result = AuthUtils.generateEmptyPermissionInfoResp();
      }
    } catch (AuthException e) {
      logger.error("meet error while logging in.", e);
      status = false;
      loginMessage = e.getMessage();
    }
    if (!status) {
      tsStatus.setMessage(loginMessage != null ? loginMessage : "Authentication failed.");
      tsStatus.setCode(TSStatusCode.WRONG_LOGIN_PASSWORD.getStatusCode());
      result.setStatus(tsStatus);
    }
    return result;
  }

  public TPermissionInfoResp checkUserPrivileges(
      String username, List<String> paths, int permission) {
    boolean status = true;
    TPermissionInfoResp result = new TPermissionInfoResp();
    try {
      for (String path : paths) {
        if (!checkOnePath(username, path, permission)) {
          status = false;
          break;
        }
      }
    } catch (AuthException e) {
      status = false;
    }
    if (status) {
      try {
        // Bring this user's permission information back to the datanode for caching
        result = getUserPermissionInfo(username);
        result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      } catch (AuthException e) {
        result.setStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
      }
    } else {
      result = AuthUtils.generateEmptyPermissionInfoResp();
      result.setStatus(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));
    }
    return result;
  }

  private boolean checkOnePath(String username, String path, int permission) throws AuthException {
    try {
      if (authorizer.checkUserPrivileges(username, path, permission)) {
        return true;
      }
    } catch (AuthException e) {
      logger.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
      throw new AuthException(e.getCode(), e);
    }
    return false;
  }

  public TSStatus authorNonQuery(AuthorPlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    String userName = authorPlan.getUserName();
    String roleName = authorPlan.getRoleName();
    String password = authorPlan.getPassword();
    String newPassword = authorPlan.getNewPassword();
    Set<Integer> permissions = authorPlan.getPermissions();
    List<String> nodeNameList = authorPlan.getNodeNameList();
    try {
      switch (authorType) {
        case UpdateUser:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CreateUser:
          authorizer.createUser(userName, password);
          break;
        case CreateRole:
          authorizer.createRole(roleName);
          break;
        case DropUser:
          authorizer.deleteUser(userName);
          break;
        case DropRole:
          authorizer.deleteRole(roleName);
          break;
        case GrantRole:
          for (int i : permissions) {
            for (String path : nodeNameList) {
              authorizer.grantPrivilegeToRole(roleName, path, i);
            }
          }
          break;
        case GrantUser:
          for (int i : permissions) {
            for (String path : nodeNameList) {
              authorizer.grantPrivilegeToUser(userName, path, i);
            }
          }
          break;
        case GrantRoleToUser:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case RevokeUser:
          for (int i : permissions) {
            for (String path : nodeNameList) {
              authorizer.revokePrivilegeFromUser(userName, path, i);
            }
          }
          break;
        case RevokeRole:
          for (int i : permissions) {
            for (String path : nodeNameList) {
              authorizer.revokePrivilegeFromRole(roleName, path, i);
            }
          }
          break;
        case RevokeRoleFromUser:
          authorizer.revokeRoleFromUser(roleName, userName);
          break;
        default:
          throw new AuthException(
              TSStatusCode.UNSUPPORTED_AUTH_OPERATION,
              "unknown type: " + authorPlan.getAuthorType());
      }
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public PermissionInfoResp executeListUsers(AuthorPlan plan) throws AuthException {
    PermissionInfoResp result = new PermissionInfoResp();
    Map<String, List<String>> permissionInfo = new HashMap<>();
    List<String> userList = authorizer.listAllUsers();
    if (!plan.getRoleName().isEmpty()) {
      Role role = authorizer.getRole(plan.getRoleName());
      if (role == null) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.ROLE_NOT_EXIST, "No such role : " + plan.getRoleName()));
        result.setPermissionInfo(permissionInfo);
        return result;
      }
      Iterator<String> itr = userList.iterator();
      while (itr.hasNext()) {
        User userObj = authorizer.getUser(itr.next());
        if (userObj == null || !userObj.hasRole(plan.getRoleName())) {
          itr.remove();
        }
      }
    }

    permissionInfo.put(IoTDBConstant.COLUMN_USER, userList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setPermissionInfo(permissionInfo);
    return result;
  }

  public PermissionInfoResp executeListRoles(AuthorPlan plan) throws AuthException {
    PermissionInfoResp result = new PermissionInfoResp();
    Map<String, List<String>> permissionInfo = new HashMap<>();
    List<String> roleList = new ArrayList<>();
    if (plan.getUserName().isEmpty()) {
      roleList.addAll(authorizer.listAllRoles());
    } else {
      User user = authorizer.getUser(plan.getUserName());
      if (user == null) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.USER_NOT_EXIST, "No such user : " + plan.getUserName()));
        result.setPermissionInfo(permissionInfo);
        return result;
      }
      roleList.addAll(user.getRoleList());
    }

    permissionInfo.put(IoTDBConstant.COLUMN_ROLE, roleList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setPermissionInfo(permissionInfo);
    return result;
  }

  public PermissionInfoResp executeListRolePrivileges(AuthorPlan plan) throws AuthException {
    PermissionInfoResp result = new PermissionInfoResp();
    Map<String, List<String>> permissionInfo = new HashMap<>();
    Role role = authorizer.getRole(plan.getRoleName());
    if (role == null) {
      result.setStatus(
          RpcUtils.getStatus(TSStatusCode.ROLE_NOT_EXIST, "No such role : " + plan.getRoleName()));
      result.setPermissionInfo(permissionInfo);
      return result;
    }
    Set<String> rolePrivilegesSet = new HashSet<>();
    for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
      if (plan.getNodeNameList().isEmpty()) {
        rolePrivilegesSet.add(pathPrivilege.toString());
        continue;
      }
      for (String path : plan.getNodeNameList()) {
        if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path)) {
          rolePrivilegesSet.add(pathPrivilege.toString());
        }
      }
    }

    permissionInfo.put(IoTDBConstant.COLUMN_PRIVILEGE, new ArrayList<>(rolePrivilegesSet));
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setPermissionInfo(permissionInfo);
    return result;
  }

  public PermissionInfoResp executeListUserPrivileges(AuthorPlan plan) throws AuthException {
    PermissionInfoResp result = new PermissionInfoResp();
    Map<String, List<String>> permissionInfo = new HashMap<>();
    User user = authorizer.getUser(plan.getUserName());
    if (user == null) {
      result.setStatus(
          RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, "No such user : " + plan.getUserName()));
      result.setPermissionInfo(permissionInfo);
      return result;
    }
    List<String> userPrivilegesList = new ArrayList<>();

    if (IoTDBConstant.PATH_ROOT.equals(plan.getUserName())) {
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        userPrivilegesList.add(privilegeType.toString());
      }
    } else {
      List<String> rolePrivileges = new ArrayList<>();
      Set<String> userPrivilegeSet = new HashSet<>();
      for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
        if (plan.getNodeNameList().isEmpty()
            && !userPrivilegeSet.contains(pathPrivilege.toString())) {
          rolePrivileges.add("");
          userPrivilegeSet.add(pathPrivilege.toString());
          continue;
        }
        for (String path : plan.getNodeNameList()) {
          if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path)
              && !userPrivilegeSet.contains(pathPrivilege.toString())) {
            rolePrivileges.add("");
            userPrivilegeSet.add(pathPrivilege.toString());
          }
        }
      }
      userPrivilegesList.addAll(userPrivilegeSet);
      for (String roleN : user.getRoleList()) {
        Role role = authorizer.getRole(roleN);
        if (roleN == null) {
          continue;
        }
        Set<String> rolePrivilegeSet = new HashSet<>();
        for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
          if (plan.getNodeNameList().isEmpty()
              && !rolePrivilegeSet.contains(pathPrivilege.toString())) {
            rolePrivileges.add(roleN);
            rolePrivilegeSet.add(pathPrivilege.toString());
            continue;
          }
          for (String path : plan.getNodeNameList()) {
            if (AuthUtils.pathBelongsTo(pathPrivilege.getPath(), path)
                && !rolePrivilegeSet.contains(pathPrivilege.toString())) {
              rolePrivileges.add(roleN);
              rolePrivilegeSet.add(pathPrivilege.toString());
            }
          }
        }
        userPrivilegesList.addAll(rolePrivilegeSet);
      }
      permissionInfo.put(IoTDBConstant.COLUMN_ROLE, rolePrivileges);
    }
    permissionInfo.put(IoTDBConstant.COLUMN_PRIVILEGE, userPrivilegesList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setPermissionInfo(permissionInfo);
    return result;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return authorizer.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    authorizer.processLoadSnapshot(snapshotDir);
  }

  @TestOnly
  public void clear() throws AuthException {
    File userFolder = new File(commonConfig.getUserFolder());
    if (userFolder.exists()) {
      FileUtils.deleteDirectory(userFolder);
    }
    File roleFolder = new File(commonConfig.getRoleFolder());
    if (roleFolder.exists()) {
      FileUtils.deleteDirectory(roleFolder);
    }
    authorizer.reset();
  }

  /**
   * Save the user's permission information,Bring back the DataNode for caching
   *
   * @param username The username of the user that needs to be cached
   */
  public TPermissionInfoResp getUserPermissionInfo(String username) throws AuthException {
    TPermissionInfoResp result = new TPermissionInfoResp();
    TUserResp tUserResp = new TUserResp();
    Map<String, TRoleResp> tRoleRespMap = new HashMap();
    List<String> userPrivilegeList = new ArrayList<>();

    // User permission information
    User user = authorizer.getUser(username);
    if (user.getPrivilegeList() != null) {
      for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
        userPrivilegeList.add(pathPrivilege.getPath());
        String privilegeIdList = pathPrivilege.getPrivileges().toString();
        userPrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
      }
      tUserResp.setUsername(user.getName());
      tUserResp.setPassword(user.getPassword());
      tUserResp.setPrivilegeList(userPrivilegeList);
      tUserResp.setRoleList(user.getRoleList());
    }

    // Permission information for roles owned by users
    if (user.getRoleList() != null) {
      for (String roleName : user.getRoleList()) {
        Role role = authorizer.getRole(roleName);
        List<String> rolePrivilegeList = new ArrayList<>();
        for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
          rolePrivilegeList.add(pathPrivilege.getPath());
          String privilegeIdList = pathPrivilege.getPrivileges().toString();
          rolePrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
        }
        tRoleRespMap.put(roleName, new TRoleResp(roleName, rolePrivilegeList));
      }
    }
    result.setUserInfo(tUserResp);
    result.setRoleInfo(tRoleRespMap);
    return result;
  }
}
