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
import org.apache.iotdb.commons.auth.entity.ModelType;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
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
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorInfo implements SnapshotProcessor {

  // Works at config node.
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorInfo.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final String NO_USER_MSG = "No such user : ";

  private IAuthorizer authorizer;

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
          result = getUserPermissionInfo(username, ModelType.ALL);
          result.getUserInfo().setIsOpenIdUser(true);
        } else {
          result = getUserPermissionInfo(username, ModelType.ALL);
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

  public TPermissionInfoResp checkUserPrivileges(String username, PrivilegeUnion union) {
    boolean status;
    TPermissionInfoResp result = new TPermissionInfoResp();
    List<Integer> failedList = new ArrayList<>();
    try {
      if (union.getModelType() == PrivilegeModelType.TREE) {
        List<? extends PartialPath> list = union.getPaths();
        int pos = 0;
        for (PartialPath path : list) {
          if (!authorizer.checkUserPrivileges(
              username,
              new PrivilegeUnion(path, union.getPrivilegeType(), union.isGrantOption()))) {
            failedList.add(pos);
          }
          pos++;
        }
        if (union.isGrantOption()) {
          // all path should have grant option.
          status = failedList.isEmpty();
        } else {
          status = failedList.size() != list.size();
        }
      } else {
        status = authorizer.checkUserPrivileges(username, union);
      }
    } catch (AuthException e) {
      status = false;
    }

    try {
      result = getUserPermissionInfo(username, ModelType.ALL);
      result.setFailPos(failedList);
      if (status) {
        result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      } else {
        result.setStatus(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));
      }
    } catch (AuthException e) {
      result.setStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
    }
    return result;
  }

  public TSStatus authorNonQuery(AuthorPlan authorPlan) {
    if (authorPlan instanceof AuthorTreePlan) {
      return authorNonQuery((AuthorTreePlan) authorPlan);
    } else {
      return authorNonQuery((AuthorRelationalPlan) authorPlan);
    }
  }

  public TSStatus authorNonQuery(AuthorTreePlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    String userName = authorPlan.getUserName();
    String roleName = authorPlan.getRoleName();
    String password = authorPlan.getPassword();
    String newPassword = authorPlan.getNewPassword();
    Set<Integer> permissions = authorPlan.getPermissions();
    boolean grantOpt = authorPlan.getGrantOpt();
    List<PartialPath> nodeNameList = authorPlan.getNodeNameList();
    try {
      switch (authorType) {
        case UpdateUser:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CreateUser:
          authorizer.createUser(userName, password);
          break;
        case CreateUserWithRawPassword:
          authorizer.createUserWithRawPassword(userName, password);
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
          for (int permission : permissions) {
            PrivilegeType priv = PrivilegeType.values()[permission];
            if (priv.isSystemPrivilege()) {
              authorizer.grantPrivilegeToRole(roleName, new PrivilegeUnion(priv, grantOpt));
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.grantPrivilegeToRole(roleName, new PrivilegeUnion(path, priv, grantOpt));
            }
          }
          break;
        case GrantUser:
          for (int permission : permissions) {
            PrivilegeType priv = PrivilegeType.values()[permission];
            if (priv.isSystemPrivilege()) {
              authorizer.grantPrivilegeToUser(userName, new PrivilegeUnion(priv, grantOpt));
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.grantPrivilegeToUser(userName, new PrivilegeUnion(path, priv, grantOpt));
            }
          }
          break;
        case GrantRoleToUser:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case RevokeUser:
          for (int permission : permissions) {
            PrivilegeType priv = PrivilegeType.values()[permission];
            if (priv.isSystemPrivilege()) {
              authorizer.revokePrivilegeFromUser(userName, new PrivilegeUnion(priv, grantOpt));
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromUser(
                  userName, new PrivilegeUnion(path, priv, grantOpt));
            }
          }
          break;
        case RevokeRole:
          for (int permission : permissions) {
            PrivilegeType priv = PrivilegeType.values()[permission];
            if (priv.isSystemPrivilege()) {
              authorizer.revokePrivilegeFromRole(roleName, new PrivilegeUnion(priv, grantOpt));
              continue;
            }
            for (PartialPath path : nodeNameList) {
              authorizer.revokePrivilegeFromRole(
                  roleName, new PrivilegeUnion(path, priv, grantOpt));
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

  public TSStatus authorNonQuery(AuthorRelationalPlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    String userName = authorPlan.getUserName();
    String roleName = authorPlan.getRoleName();
    String database = authorPlan.getDatabaseName();
    String table = authorPlan.getTableName();
    boolean grantOpt = authorPlan.getGrantOpt();
    Set<Integer> permissions = authorPlan.getPermissions();
    Set<PrivilegeType> privileges = new HashSet<>();
    if (authorType.ordinal() >= ConfigPhysicalPlanType.RGrantUserAny.ordinal()
        && authorType.ordinal() <= ConfigPhysicalPlanType.RRevokeRoleSysPri.ordinal()) {
      for (int permission : permissions) {
        privileges.add(PrivilegeType.values()[permission]);
      }
    }

    try {
      switch (authorType) {
        case RCreateUser:
          authorizer.createUser(userName, authorPlan.getPassword());
          break;
        case RCreateRole:
          authorizer.createRole(roleName);
          break;
        case RUpdateUser:
          authorizer.updateUserPassword(userName, authorPlan.getPassword());
          break;
        case RDropRole:
          authorizer.deleteRole(roleName);
          break;
        case RDropUser:
          authorizer.deleteUser(userName);
          break;
        case RGrantUserRole:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case RRevokeUserRole:
          authorizer.revokeRoleFromUser(roleName, userName);
          break;
        case RGrantUserAny:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToUser(
                userName, new PrivilegeUnion(privilege, grantOpt, true));
          }
          break;
        case RGrantRoleAny:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToRole(
                roleName, new PrivilegeUnion(privilege, grantOpt, true));
          }
          break;
        case RGrantUserAll:
          // database scope and table scope all
          if (!database.isEmpty()) {
            for (PrivilegeType privilege : privileges) {
              if (privilege.isRelationalPrivilege()) {
                if (table.isEmpty()) {
                  authorizer.grantPrivilegeToUser(
                      userName, new PrivilegeUnion(database, privilege, grantOpt));
                } else {
                  authorizer.grantPrivilegeToUser(
                      userName, new PrivilegeUnion(database, table, privilege, grantOpt));
                }
              }
            }
            break;
          }
          for (PrivilegeType privilege : PrivilegeType.values()) {
            if (privilege.forRelationalSys()) {
              authorizer.grantPrivilegeToUser(userName, new PrivilegeUnion(privilege, grantOpt));
            }
            if (privilege.isRelationalPrivilege()) {
              authorizer.grantPrivilegeToUser(
                  userName, new PrivilegeUnion(privilege, grantOpt, true));
            }
          }
          break;
        case RGrantRoleAll:
          // database scope and table scope all
          if (!database.isEmpty()) {
            for (PrivilegeType privilege : privileges) {
              if (privilege.isRelationalPrivilege()) {
                if (table.isEmpty()) {
                  authorizer.grantPrivilegeToRole(
                      roleName, new PrivilegeUnion(database, privilege, grantOpt));
                } else {
                  authorizer.grantPrivilegeToRole(
                      roleName, new PrivilegeUnion(database, table, privilege, grantOpt));
                }
              }
            }
            break;
          }
          for (PrivilegeType privilege : PrivilegeType.values()) {
            if (privilege.forRelationalSys()) {
              authorizer.grantPrivilegeToRole(roleName, new PrivilegeUnion(privilege, grantOpt));
            }
            if (privilege.isRelationalPrivilege()) {
              authorizer.grantPrivilegeToRole(
                  roleName, new PrivilegeUnion(privilege, grantOpt, true));
            }
          }
          break;
        case RGrantUserDBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToUser(
                userName, new PrivilegeUnion(database, privilege, grantOpt));
          }
          break;
        case RGrantUserTBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToUser(
                userName, new PrivilegeUnion(database, table, privilege, grantOpt));
          }
          break;
        case RGrantRoleDBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToRole(
                roleName, new PrivilegeUnion(database, privilege, grantOpt));
          }
          break;
        case RGrantRoleTBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToRole(
                roleName, new PrivilegeUnion(database, table, privilege, grantOpt));
          }
          break;
        case RRevokeUserAny:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromUser(
                userName, new PrivilegeUnion(privilege, grantOpt, true));
          }
          break;
        case RRevokeRoleAny:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromRole(
                roleName, new PrivilegeUnion(privilege, grantOpt, true));
          }
          break;
        case RRevokeUserAll:
          if (!database.isEmpty()) {
            for (PrivilegeType privilege : PrivilegeType.values()) {
              if (privilege.isRelationalPrivilege()) {
                if (table.isEmpty()) {
                  authorizer.revokePrivilegeFromUser(
                      userName, new PrivilegeUnion(database, privilege, grantOpt));
                } else {
                  authorizer.revokePrivilegeFromUser(
                      userName, new PrivilegeUnion(database, table, privilege, grantOpt));
                }
              }
            }
            break;
          }
          authorizer.revokeAllPrivilegeFromUser(userName);
          break;
        case RRevokeRoleAll:
          if (!database.isEmpty()) {
            for (PrivilegeType privilege : PrivilegeType.values()) {
              if (privilege.isRelationalPrivilege()) {
                if (table.isEmpty()) {
                  authorizer.revokePrivilegeFromRole(
                      roleName, new PrivilegeUnion(database, privilege, grantOpt));
                } else {
                  authorizer.revokePrivilegeFromRole(
                      roleName, new PrivilegeUnion(database, table, privilege, grantOpt));
                }
              }
            }
            break;
          }
          authorizer.revokeAllPrivilegeFromRole(roleName);
          break;
        case RRevokeUserDBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromUser(
                userName, new PrivilegeUnion(database, privilege, grantOpt));
          }
          break;
        case RRevokeUserTBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromUser(
                userName, new PrivilegeUnion(database, table, privilege, grantOpt));
          }
          break;
        case RRevokeRoleDBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromRole(
                roleName, new PrivilegeUnion(database, privilege, grantOpt));
          }
          break;
        case RRevokeRoleTBPriv:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromRole(
                roleName, new PrivilegeUnion(database, table, privilege, grantOpt));
          }
          break;
        case RGrantUserSysPri:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToUser(userName, new PrivilegeUnion(privilege, grantOpt));
          }
          break;
        case RGrantRoleSysPri:
          for (PrivilegeType privilege : privileges) {
            authorizer.grantPrivilegeToRole(roleName, new PrivilegeUnion(privilege, grantOpt));
          }
          break;
        case RRevokeUserSysPri:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromUser(userName, new PrivilegeUnion(privilege, grantOpt));
          }
          break;
        case RRevokeRoleSysPri:
          for (PrivilegeType privilege : privileges) {
            authorizer.revokePrivilegeFromRole(roleName, new PrivilegeUnion(privilege, grantOpt));
          }
          break;
        default:
          throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, "not support");
      }
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public PermissionInfoResp executeListUsers(final AuthorPlan plan) throws AuthException {
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

  public PermissionInfoResp executeListRoles(final AuthorPlan plan) throws AuthException {
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
      roleList.addAll(user.getRoleSet());
    }
    result.setTag(ColumnHeaderConstant.ROLE);
    result.setMemberInfo(roleList);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  public PermissionInfoResp executeListRolePrivileges(final AuthorPlan plan) throws AuthException {
    boolean isTreePlan = plan instanceof AuthorTreePlan;
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
    final TRoleResp roleResp = role.getRoleInfo(isTreePlan ? ModelType.TREE : ModelType.RELATIONAL);
    Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put(role.getName(), roleResp);
    resp.setRoleInfo(roleInfo);
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setTag(ColumnHeaderConstant.PRIVILEGES);
    result.setPermissionInfoResp(resp);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setMemberInfo(permissionInfo);
    return result;
  }

  public PermissionInfoResp executeListUserPrivileges(final AuthorPlan plan) throws AuthException {
    final PermissionInfoResp result = new PermissionInfoResp();
    boolean isTreePlan = plan instanceof AuthorTreePlan;
    final User user = authorizer.getUser(plan.getUserName());
    if (user == null) {
      result.setStatus(
          RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + plan.getUserName()));
      return result;
    }
    final TPermissionInfoResp resp =
        getUserPermissionInfo(
            plan.getUserName(), isTreePlan ? ModelType.TREE : ModelType.RELATIONAL);
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    result.setTag(ColumnHeaderConstant.PRIVILEGES);
    result.setPermissionInfoResp(resp);
    result.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return result;
  }

  public TAuthizedPatternTreeResp generateAuthorizedPTree(String username, int permission)
      throws AuthException {
    TAuthizedPatternTreeResp resp = new TAuthizedPatternTreeResp();
    User user = authorizer.getUser(username);
    PrivilegeType type = PrivilegeType.values()[permission];
    PathPatternTree pPtree = new PathPatternTree();
    if (user == null) {
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.USER_NOT_EXIST, NO_USER_MSG + username));
      resp.setUsername(username);
      resp.setPrivilegeId(permission);
      return resp;
    }
    for (PathPrivilege path : user.getPathPrivilegeList()) {
      if (path.checkPrivilege(type)) {
        pPtree.appendPathPattern(path.getPath());
      }
    }
    for (String rolename : user.getRoleSet()) {
      Role role = authorizer.getRole(rolename);
      if (role != null) {
        for (PathPrivilege path : role.getPathPrivilegeList()) {
          if (path.checkPrivilege(type)) {
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
    resp.setPermissionInfo(getUserPermissionInfo(username, ModelType.ALL));
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
    result = getUserPermissionInfo(username, ModelType.ALL);
    if (user.getRoleSet().contains(rolename)) {
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
  public TPermissionInfoResp getUserPermissionInfo(String username, ModelType type)
      throws AuthException {
    TPermissionInfoResp result = new TPermissionInfoResp();
    User user = authorizer.getUser(username);
    if (user == null) {
      return AuthUtils.generateEmptyPermissionInfoResp();
    }
    TUserResp tUserResp = user.getUserInfo(type);
    // Permission information for roles owned by users
    if (!user.getRoleSet().isEmpty()) {
      for (String roleName : user.getRoleSet()) {
        Role role = authorizer.getRole(roleName);
        TRoleResp roleResp = role.getRoleInfo(type);
        result.putToRoleInfo(roleName, roleResp);
      }
    } else {
      result.setRoleInfo(new HashMap<>());
    }
    result.setUserInfo(tUserResp);
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return result;
  }
}
