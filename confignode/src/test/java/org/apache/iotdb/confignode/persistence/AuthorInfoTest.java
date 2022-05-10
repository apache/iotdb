/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorInfoTest {

  private static AuthorInfo authorInfo;

  @BeforeClass
  public static void setup() {
    authorInfo = AuthorInfo.getInstance();
  }

  @Test
  public void permissionTest() throws TException, AuthException {
    TSStatus status;

    List<String> userList = new ArrayList<>();
    userList.add("root");
    userList.add("tempuser0");
    userList.add("tempuser1");

    List<String> roleList = new ArrayList<>();
    roleList.add("temprole0");
    roleList.add("temprole1");

    AuthorReq authorReq;
    TCheckUserPrivilegesReq checkUserPrivilegesReq;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.DELETE_USER.ordinal());
    privilegeList.add(PrivilegeType.CREATE_USER.ordinal());

    Set<Integer> revokePrivilege = new HashSet<>();
    revokePrivilege.add(PrivilegeType.DELETE_USER.ordinal());

    Map<String, List<String>> permissionInfo;
    List<String> privilege = new ArrayList<>();
    privilege.add("root : CREATE_USER");
    privilege.add("root : CREATE_USER");

    List<String> paths = new ArrayList<>();
    paths.add("root.ln");

    cleanUserAndRole();

    // create user
    authorReq =
        new AuthorReq(
            ConfigRequestType.CreateUser, "tempuser0", "", "passwd", "", new HashSet<>(), "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorReq.setUserName("tempuser1");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo.checkUserPrivileges("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
    Assert.assertEquals(TSStatusCode.NO_PERMISSION_ERROR.getStatusCode(), status.getCode());

    // drop user
    authorReq =
        new AuthorReq(ConfigRequestType.DropUser, "tempuser1", "", "", "", new HashSet<>(), "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list user
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUser();
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("tempuser1");
    Assert.assertEquals(
        userList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER));

    // create role
    authorReq =
        new AuthorReq(ConfigRequestType.CreateRole, "", "temprole0", "", "", new HashSet<>(), "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorReq.setRoleName("temprole1");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // drop role
    authorReq =
        new AuthorReq(ConfigRequestType.DropRole, "", "temprole1", "", "", new HashSet<>(), "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list role
    permissionInfoResp = authorInfo.executeListRole();
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("temprole1");
    Assert.assertEquals(
        roleList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE));

    // alter user
    authorReq =
        new AuthorReq(
            ConfigRequestType.UpdateUser, "tempuser0", "", "", "newpwd", new HashSet<>(), "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant user
    authorReq =
        new AuthorReq(
            ConfigRequestType.GrantUser, "tempuser0", "", "", "", privilegeList, "root.ln");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo.checkUserPrivileges("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorReq =
        new AuthorReq(
            ConfigRequestType.GrantRole, "", "temprole0", "", "", privilegeList, "root.ln");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role to user
    authorReq =
        new AuthorReq(
            ConfigRequestType.GrantRoleToUser,
            "tempuser0",
            "temprole0",
            "",
            "",
            new HashSet<>(),
            "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke user
    authorReq =
        new AuthorReq(
            ConfigRequestType.RevokeUser, "tempuser0", "", "", "", revokePrivilege, "root.ln");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke role
    authorReq =
        new AuthorReq(
            ConfigRequestType.RevokeRole, "", "temprole0", "", "", revokePrivilege, "root.ln");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges user
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListUserPrivilege,
            "tempuser0",
            "",
            "",
            "",
            new HashSet<>(),
            "root.ln");
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list user privileges
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListUserPrivilege, "tempuser0", "", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list privileges role
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListRolePrivilege,
            "",
            "temprole0",
            "",
            "",
            new HashSet<>(),
            "root.ln");
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    privilege.remove(0);
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list role privileges
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListRolePrivilege, "", "temprole0", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list all role of user
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListUserRoles, "tempuser0", "", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListUserRoles(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("temprole1");
    Assert.assertEquals(
        roleList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE));

    // list all user of role
    authorReq =
        new AuthorReq(
            ConfigRequestType.ListRoleUsers, "", "temprole0", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListRoleUsers(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("tempuser1");
    userList.remove("root");
    Assert.assertEquals(
        userList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER));

    // revoke role from user
    authorReq =
        new AuthorReq(
            ConfigRequestType.RevokeRoleFromUser,
            "tempuser0",
            "temprole0",
            "",
            "",
            new HashSet<>(),
            "");
    status = authorInfo.authorNonQuery(authorReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list root privileges
    authorReq =
        new AuthorReq(ConfigRequestType.ListUserPrivilege, "root", "", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorReq);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    for (int i = 0; i < PrivilegeType.values().length; i++) {
      Assert.assertEquals(
          PrivilegeType.values()[i].toString(),
          permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).get(i));
    }
  }

  private void cleanUserAndRole() throws TException, AuthException {
    TSStatus status;

    // clean user
    AuthorReq authorReq =
        new AuthorReq(ConfigRequestType.ListUser, "", "", "", "", new HashSet<>(), "");
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUser();
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER);
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorReq =
            new AuthorReq(ConfigRequestType.DropUser, user, "", "", "", new HashSet<>(), "");
        status = authorInfo.authorNonQuery(authorReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }

    // clean role
    authorReq = new AuthorReq(ConfigRequestType.ListUser, "", "", "", "", new HashSet<>(), "");
    permissionInfoResp = authorInfo.executeListRole();
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> roleList = permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE);
    for (String roleN : roleList) {
      authorReq = new AuthorReq(ConfigRequestType.DropRole, "", roleN, "", "", new HashSet<>(), "");
      status = authorInfo.authorNonQuery(authorReq);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }
}
