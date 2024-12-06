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
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorReadPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.auth.entity.PrivilegeType.isPathRelevant;

public class AuthorInfo implements SnapshotProcessor {

  // Works at config node.
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorInfo.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final String NO_USER_MSG = "No such user : ";

  private IAuthorizer authorizer;

  private boolean hasPrePriv = true;

  public AuthorInfo() {
    try {
      authorizer = BasicAuthorizer.getInstance();

    } catch (AuthException e) {
      LOGGER.error("get user or role permissionInfo failed because ", e);
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
      LOGGER.error("meet error while logging in.", e);
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

  // if All paths fail, return No permission
  // if some paths fail, return SUCCESS and failed index list
  // if all path success, return success and empty index list
  public TPermissionInfoResp checkUserPrivileges(
      String username, List<PartialPath> paths, int permission) {
    boolean status = true;
    TPermissionInfoResp result = new TPermissionInfoResp();
    List<Integer> failedList = new ArrayList<>();
    try {
      if (paths.isEmpty()) {
        status = authorizer.checkUserPrivileges(username, null, permission);
      } else {
        int pos = 0;
        for (PartialPath path : paths) {
          if (!checkOnePath(username, path, permission)) {
            failedList.add(pos);
          }
          pos++;
        }
        if (failedList.size() == paths.size()) {
          status = false;
        }
      }
    } catch (AuthException e) {
      status = false;
    }
    if (status) {
      try {
        // Bring this user's permission information back to the datanode for caching
        result = getUserPermissionInfo(username);
        result.setFailPos(failedList);
        result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      } catch (AuthException e) {
        result.setStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
      }
    } else {
      result = AuthUtils.generateEmptyPermissionInfoResp();
      result.setFailPos(failedList);
      result.setStatus(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));
    }
    return result;
  }

  private boolean checkOnePath(String username, PartialPath path, int permission)
      throws AuthException {
    try {
      if (authorizer.checkUserPrivileges(username, path, permission)) {
        return true;
      }
    } catch (AuthException e) {
      LOGGER.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
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
    boolean grantOpt = authorPlan.getGrantOpt();
    List<PartialPath> nodeNameList = authorPlan.getNodeNameList();
    // We will process the new version permissions after handling all the old version permissions.
    // We assume that:
    // 1. the permission logs generated by new version will always come after the old permissions.
    // 2. two types of permission logs will not be mixed.
    // When we begin to handle the new version's permissions, we need to check whether the old
    // permissions have
    // been processed before. The encoding and meaning of these old permissions have changed
    // significantly.
    if (authorType.ordinal() >= ConfigPhysicalPlanType.CreateUserDep.ordinal()
        && authorType.ordinal() <= ConfigPhysicalPlanType.UpdateUserDep.ordinal()) {
      // if meet old version's permissions, we will set pre version tag.
      authorizer.setUserForPreVersion(true);
      authorizer.setRoleForPreVersion(true);
    } else {
      if (hasPrePriv) {
        // when we refresh our preversion's information？
        // 1. before raftlog redoing finish.（ALL author plans in raftlog are pre version)
        // 2. refresh during raftlog. (pre version mixed with new version)
        authorizer.checkUserPathPrivilege();
        hasPrePriv = false;
      }
    }
    try {
      switch (authorType) {
        case UpdateUserDep:
        case UpdateUser:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CreateUserDep:
          AuthUtils.validatePasswordPre(password);
          AuthUtils.validateUsernamePre(userName);
          authorizer.createUserWithoutCheck(userName, password);
          break;
        case CreateUser:
          authorizer.createUser(userName, password);
          break;
        case CreateUserWithRawPassword:
          authorizer.createUserWithRawPassword(userName, password);
          break;
        case CreateRoleDep:
          AuthUtils.validateRolenamePre(roleName);
          authorizer.createRole(roleName);
          break;
        case CreateRole:
          AuthUtils.validateRolename(roleName);
          authorizer.createRole(roleName);
          break;
        case DropUserDep:
        case DropUser:
          authorizer.deleteUser(userName);
          break;
        case DropRoleDep:
        case DropRole:
          authorizer.deleteRole(roleName);
          break;
        case GrantRoleDep:
          grantPrivilegeForPreVersion(false, roleName, permissions, nodeNameList);
          break;
        case GrantRole:
          for (int permission : permissions) {
            if (!isPathRelevant(permission)) {
              authorizer.grantPrivilegeToRole(roleName, null, permission, grantOpt);
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.grantPrivilegeToRole(roleName, path, permission, grantOpt);
            }
          }
          break;
        case GrantUserDep:
          grantPrivilegeForPreVersion(true, userName, permissions, nodeNameList);
          break;
        case GrantUser:
          for (int permission : permissions) {
            if (!isPathRelevant(permission)) {
              authorizer.grantPrivilegeToUser(userName, null, permission, grantOpt);
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.grantPrivilegeToUser(userName, path, permission, grantOpt);
            }
          }
          break;
        case GrantRoleToUserDep:
        case GrantRoleToUser:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case RevokeUserDep:
          revokePrivilegeForPreVersion(true, userName, permissions, nodeNameList);
          break;
        case RevokeUser:
          for (int permission : permissions) {
            if (!isPathRelevant(permission)) {
              authorizer.revokePrivilegeFromUser(userName, null, permission);
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromUser(userName, path, permission);
            }
          }
          break;
        case RevokeRoleDep:
          revokePrivilegeForPreVersion(false, roleName, permissions, nodeNameList);
          break;
        case RevokeRole:
          for (int permission : permissions) {
            if (!isPathRelevant(permission)) {
              authorizer.revokePrivilegeFromRole(roleName, null, permission);
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromRole(roleName, path, permission);
            }
          }
          break;
        case RevokeRoleFromUserDep:
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
    } finally {
      authorizer.setUserForPreVersion(false);
      authorizer.setRoleForPreVersion(false);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public PermissionInfoResp executeListUsers(final AuthorReadPlan plan) throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    final List<String> userList = authorizer.listAllUsers();
    if (!plan.getRoleName().isEmpty()) {
      final Role role = authorizer.getRole(plan.getRoleName());
      if (role == null) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.ROLE_NOT_EXIST, "No such role : " + plan.getRoleName()));
        return result;
      }
      final Iterator<String> itr = userList.iterator();
      while (itr.hasNext()) {
        User userObj = authorizer.getUser(itr.next());
        if (userObj == null || !userObj.hasRole(plan.getRoleName())) {
          itr.remove();
        }
      }
    }
    result.setTag(ColumnHeaderConstant.USER);
    result.setMemberInfo(userList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  public PermissionInfoResp executeListRoles(final AuthorReadPlan plan) throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    final List<String> permissionInfo = new ArrayList<>();
    final List<String> roleList = new ArrayList<>();
    if (plan.getUserName().isEmpty()) {
      roleList.addAll(authorizer.listAllRoles());
    } else {
      final User user = authorizer.getUser(plan.getUserName());
      if (user == null) {
        result.setStatus(
            RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + plan.getUserName()));
        result.setMemberInfo(permissionInfo);
        return result;
      }
      roleList.addAll(user.getRoleList());
    }
    result.setTag(ColumnHeaderConstant.ROLE);
    result.setMemberInfo(roleList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  public PermissionInfoResp executeListRolePrivileges(final AuthorReadPlan plan)
      throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    final List<String> permissionInfo = new ArrayList<>();
    final Role role = authorizer.getRole(plan.getRoleName());
    if (role == null) {
      result.setStatus(
          RpcUtils.getStatus(TSStatusCode.ROLE_NOT_EXIST, "No such role : " + plan.getRoleName()));
      result.setMemberInfo(permissionInfo);
      return result;
    }
    final TPermissionInfoResp resp = new TPermissionInfoResp();
    final TRoleResp roleResp = new TRoleResp();
    roleResp.setRoleName(role.getName());
    final List<TPathPrivilege> pathList = new ArrayList<>();
    for (final PathPrivilege path : role.getPathPrivilegeList()) {
      final TPathPrivilege pathPri = new TPathPrivilege();
      pathPri.setPriGrantOpt(path.getGrantOpt());
      pathPri.setPriSet(path.getPrivileges());
      pathPri.setPath(path.getPath().toString());
      pathList.add(pathPri);
    }
    roleResp.setPrivilegeList(pathList);
    roleResp.setSysPriSet(role.getSysPrivilege());
    roleResp.setSysPriSetGrantOpt(role.getSysPriGrantOpt());
    final Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put(role.getName(), roleResp);
    resp.setRoleInfo(roleInfo);
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setTag(ColumnHeaderConstant.PRIVILEGES);
    result.setPermissionInfoResp(resp);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setMemberInfo(permissionInfo);
    return result;
  }

  public PermissionInfoResp executeListUserPrivileges(final AuthorReadPlan plan)
      throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    final User user = authorizer.getUser(plan.getUserName());
    if (user == null) {
      result.setStatus(
          RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + plan.getUserName()));
      return result;
    }
    final TPermissionInfoResp resp = getUserPermissionInfo(plan.getUserName());
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setTag(ColumnHeaderConstant.PRIVILEGES);
    result.setPermissionInfoResp(resp);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  public TAuthizedPatternTreeResp generateAuthizedPTree(String username, int permission)
      throws AuthException {
    TAuthizedPatternTreeResp resp = new TAuthizedPatternTreeResp();
    User user = authorizer.getUser(username);
    PathPatternTree pPtree = new PathPatternTree();
    if (user == null) {
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + username));
      resp.setUsername(username);
      resp.setPrivilegeId(permission);
      return resp;
    }
    for (PathPrivilege path : user.getPathPrivilegeList()) {
      if (path.checkPrivilege(permission)) {
        pPtree.appendPathPattern(path.getPath());
      }
    }
    for (String rolename : user.getRoleList()) {
      Role role = authorizer.getRole(rolename);
      if (role != null) {
        for (PathPrivilege path : role.getPathPrivilegeList()) {
          if (path.checkPrivilege(permission)) {
            pPtree.appendPathPattern(path.getPath());
          }
        }
      }
    }
    pPtree.constructTree();
    resp.setUsername(username);
    resp.setPrivilegeId(permission);
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      pPtree.serialize(dataOutputStream);
    } catch (IOException e) {
      resp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Get error when serialize pattern tree."));
      return resp;
    }
    resp.setPathPatternTree(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    resp.setPermissionInfo(getUserPermissionInfo(username));
    return resp;
  }

  public TPermissionInfoResp checkUserPrivilegeGrantOpt(
      String username, List<PartialPath> paths, int permission) throws AuthException {
    User user = authorizer.getUser(username);
    TPermissionInfoResp resp = new TPermissionInfoResp();
    boolean status = false;
    if (user == null) {
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + username));
      return resp;
    }
    try {
      if (isPathRelevant(permission)) {
        for (PartialPath path : paths) {
          if (user.checkPathPrivilegeGrantOpt(path, permission)) {
            status = true;
            continue;
          }
          if (!status) {
            for (String roleName : user.getRoleList()) {
              Role role = authorizer.getRole(roleName);
              if (role.checkPathPrivilegeGrantOpt(path, permission)) {
                status = true;
                break;
              }
            }
          }
          if (!status) {
            break;
          }
        }
      } else {
        if (user.checkSysPriGrantOpt(permission)) {
          status = true;
        }
        if (!status) {
          for (String roleName : user.getRoleList()) {
            Role role = authorizer.getRole(roleName);
            if (role.checkSysPriGrantOpt(permission)) {
              status = true;
              break;
            }
          }
        }
      }
    } catch (AuthException e) {
      status = false;
    }
    if (status) {
      try {
        // Bring this user's permission information back to the datanode for caching
        resp = getUserPermissionInfo(username);
        resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      } catch (AuthException e) {
        resp.setStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
      }
    } else {
      resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));
    }

    return resp;
  }

  public TPermissionInfoResp checkRoleOfUser(String username, String rolename)
      throws AuthException {
    TPermissionInfoResp result;
    User user = authorizer.getUser(username);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format("No such user : %s", username));
    }
    result = getUserPermissionInfo(username);
    if (user.getRoleList().contains(rolename)) {
      result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    } else {
      result.setStatus(RpcUtils.getStatus(TSStatusCode.USER_NOT_HAS_ROLE));
    }
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
    File userFolder = new File(COMMON_CONFIG.getUserFolder());
    if (userFolder.exists()) {
      FileUtils.deleteFileOrDirectory(userFolder);
    }
    File roleFolder = new File(COMMON_CONFIG.getRoleFolder());
    if (roleFolder.exists()) {
      FileUtils.deleteFileOrDirectory(roleFolder);
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
    List<TPathPrivilege> userPrivilegeList = new ArrayList<>();

    // User permission information
    User user = authorizer.getUser(username);
    if (user.getPathPrivilegeList() != null) {
      for (PathPrivilege pathPrivilege : user.getPathPrivilegeList()) {
        TPathPrivilege path = new TPathPrivilege();
        path.setPath(pathPrivilege.getPath().getFullPath());
        path.setPriSet(pathPrivilege.getPrivileges());
        path.setPriGrantOpt(pathPrivilege.getGrantOpt());
        userPrivilegeList.add(path);
      }
    }
    tUserResp.setUsername(user.getName());
    tUserResp.setPassword(user.getPassword());
    tUserResp.setPrivilegeList(userPrivilegeList);
    tUserResp.setRoleList(user.getRoleList());
    tUserResp.setSysPriSet(user.getSysPrivilege());
    tUserResp.setSysPriSetGrantOpt(user.getSysPriGrantOpt());

    // Permission information for roles owned by users
    if (user.getRoleList() != null) {
      for (String roleName : user.getRoleList()) {
        Role role = authorizer.getRole(roleName);
        List<TPathPrivilege> rolePrivilegeList = new ArrayList<>();
        for (PathPrivilege pathPrivilege : role.getPathPrivilegeList()) {
          TPathPrivilege path = new TPathPrivilege();
          path.setPath(pathPrivilege.getPath().getFullPath());
          path.setPriSet(pathPrivilege.getPrivileges());
          path.setPriGrantOpt(pathPrivilege.getGrantOpt());
          rolePrivilegeList.add(path);
        }
        tRoleRespMap.put(
            roleName,
            new TRoleResp(
                roleName, rolePrivilegeList, role.getSysPrivilege(), role.getSysPriGrantOpt()));
      }
    }
    result.setUserInfo(tUserResp);
    result.setRoleInfo(tRoleRespMap);
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return result;
  }

  public void checkUserPathPrivilege() {
    authorizer.checkUserPathPrivilege();
  }

  private void grantPrivilegeForPreVersion(
      boolean isUser, String name, Set<Integer> permissions, List<PartialPath> nodeNameList)
      throws AuthException {
    for (int permission : permissions) {
      PriPrivilegeType type = PriPrivilegeType.values()[permission];
      if (type.isAccept()) {
        if (isUser) {
          for (PartialPath path : nodeNameList) {
            authorizer.grantPrivilegeToUser(name, path, permission, false);
          }
        } else {
          for (PartialPath path : nodeNameList) {
            authorizer.grantPrivilegeToRole(name, path, permission, false);
          }
        }
      }
    }
  }

  private void revokePrivilegeForPreVersion(
      boolean isUser, String name, Set<Integer> permissions, List<PartialPath> nodeNameList)
      throws AuthException {
    for (int permission : permissions) {
      PriPrivilegeType type = PriPrivilegeType.values()[permission];
      if (type.isAccept()) {
        if (!type.isAccept()) {
          if (isUser) {
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromUser(name, path, permission);
            }
          } else {
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromRole(name, path, permission);
            }
          }
        }
      }
    }
  }
}
