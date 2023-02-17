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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class AuthorInfoTest {

  private static AuthorInfo authorInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "authorInfo-snapshot");

  @BeforeClass
  public static void setup() {
    authorInfo = new AuthorInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException, AuthException {
    authorInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void permissionTest() throws TException, AuthException {

    TSStatus status;

    List<String> userList = new ArrayList<>();
    userList.add("root");
    userList.add("user0");
    userList.add("user1");

    List<String> roleList = new ArrayList<>();
    roleList.add("role0");
    roleList.add("role1");

    AuthorPlan authorPlan;
    TCheckUserPrivilegesReq checkUserPrivilegesReq;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.DELETE_USER.ordinal());
    privilegeList.add(PrivilegeType.CREATE_USER.ordinal());

    Set<Integer> revokePrivilege = new HashSet<>();
    revokePrivilege.add(PrivilegeType.DELETE_USER.ordinal());

    Map<String, List<String>> permissionInfo;
    List<String> privilege = new ArrayList<>();
    privilege.add("root.** : CREATE_USER");
    privilege.add("root.** : CREATE_USER");

    List<String> paths = new ArrayList<>();
    paths.add("root.ln");

    cleanUserAndRole();

    // create user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUser,
            "user0",
            "",
            "passwd",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertNull(status.getMessage());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorPlan.setUserName("user1");
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", paths, PrivilegeType.DELETE_USER.ordinal())
            .getStatus();
    Assert.assertEquals(TSStatusCode.NO_PERMISSION.getStatusCode(), status.getCode());

    // drop user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropUser,
            "user1",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUser, "", "", "", "", new HashSet<>(), new ArrayList<>());
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("user1");
    Assert.assertEquals(
        userList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER));

    // create role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRole,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorPlan.setRoleName("role1");
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // drop role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropRole,
            "",
            "role1",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRole, "", "", "", "", new HashSet<>(), new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("role1");
    Assert.assertEquals(
        roleList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE));

    // alter user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.UpdateUser,
            "user0",
            "",
            "",
            "newpwd",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant user
    List<String> nodeNameList = new ArrayList<>();
    nodeNameList.add("root.ln.**");
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", privilegeList, nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", paths, PrivilegeType.DELETE_USER.ordinal())
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRole, "", "role0", "", "", privilegeList, nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role to user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleToUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeUser, "user0", "", "", "", revokePrivilege, nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRole, "", "role0", "", "", revokePrivilege, nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges user on root.ln.**
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            nodeNameList);
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).size());

    // list privileges user on root.**
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            Collections.singletonList("root.**"));
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list user privileges
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list privileges role on root.ln.**
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            nodeNameList);
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    privilege.remove(0);
    Assert.assertEquals(
        0, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).size());

    // list privileges role on root.**
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            Collections.singletonList("root.**"));
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list role privileges
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        privilege, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

    // list all role of user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRole,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("role1");
    Assert.assertEquals(
        roleList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE));

    // list all user of role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("user1");
    userList.remove("root");
    Assert.assertEquals(
        userList, permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER));

    // revoke role from user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRoleFromUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list root privileges
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "root",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
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
    AuthorPlan authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUser, "", "", "", "", new HashSet<>(), new ArrayList<>());
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_USER);
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorPlan =
            new AuthorPlan(
                ConfigPhysicalPlanType.DropUser,
                user,
                "",
                "",
                "",
                new HashSet<>(),
                new ArrayList<>());
        status = authorInfo.authorNonQuery(authorPlan);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }

    // clean role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRole, "", "", "", "", new HashSet<>(), new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> roleList = permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_ROLE);
    for (String roleN : roleList) {
      authorPlan =
          new AuthorPlan(
              ConfigPhysicalPlanType.DropRole,
              "",
              roleN,
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = authorInfo.authorNonQuery(authorPlan);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  @Test
  public void takeSnapshot() throws TException, IOException, AuthException {
    cleanUserAndRole();
    // create role
    AuthorPlan createRoleReq = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
    createRoleReq.setRoleName("testRole");
    TSStatus status = authorInfo.authorNonQuery(createRoleReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    AuthorPlan createUserReq = new AuthorPlan(ConfigPhysicalPlanType.CreateUser);
    createUserReq.setUserName("testUser");
    createUserReq.setPassword("testPassword");
    status = authorInfo.authorNonQuery(createUserReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    AuthorPlan listUserPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUser, "", "", "", "", new HashSet<>(), new ArrayList<>());
    AuthorPlan listRolePlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRole, "", "", "", "", new HashSet<>(), new ArrayList<>());
    Assert.assertEquals(
        1, authorInfo.executeListRoles(listRolePlan).getPermissionInfo().get("role").size());
    Assert.assertEquals(
        2, authorInfo.executeListUsers(listUserPlan).getPermissionInfo().get("user").size());
    Assert.assertTrue(authorInfo.processTakeSnapshot(snapshotDir));
    authorInfo.clear();
    authorInfo.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(
        1, authorInfo.executeListRoles(listRolePlan).getPermissionInfo().get("role").size());
    Assert.assertEquals(
        2, authorInfo.executeListUsers(listUserPlan).getPermissionInfo().get("user").size());
  }

  @Test
  public void testMultPathsPermission() throws TException, AuthException {
    TSStatus status;

    AuthorPlan authorPlan;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.INSERT_TIMESERIES.ordinal());
    privilegeList.add(PrivilegeType.READ_TIMESERIES.ordinal());

    Map<String, List<String>> permissionInfo;
    List<String> userPrivilege = new ArrayList<>();
    userPrivilege.add("root.sg.** : INSERT_TIMESERIES READ_TIMESERIES");
    userPrivilege.add("root.ln.** : INSERT_TIMESERIES READ_TIMESERIES");
    Collections.sort(userPrivilege);

    List<String> rolePrivilege = new ArrayList<>();
    rolePrivilege.add("root.abc.** : INSERT_TIMESERIES READ_TIMESERIES");
    rolePrivilege.add("root.role_1.** : INSERT_TIMESERIES READ_TIMESERIES");
    Collections.sort(rolePrivilege);

    List<String> allPrivilege = new ArrayList<>();
    allPrivilege.addAll(userPrivilege);
    allPrivilege.addAll(rolePrivilege);
    Collections.sort(allPrivilege);

    List<String> userPaths = new ArrayList<>();
    userPaths.add("root.ln.**");
    userPaths.add("root.sg.**");

    List<String> rolePaths = new ArrayList<>();
    rolePaths.add("root.role_1.**");
    rolePaths.add("root.abc.**");

    cleanUserAndRole();

    // create user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUser,
            "user0",
            "",
            "passwd",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertNull(status.getMessage());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // create role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRole,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", privilegeList, userPaths);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", userPaths, PrivilegeType.INSERT_TIMESERIES.ordinal())
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRole, "", "role0", "", "", privilegeList, rolePaths);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role to user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleToUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges user
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            userPaths);
    PermissionInfoResp permissionInfoResp;
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        userPrivilege,
        permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).stream()
            .sorted()
            .collect(Collectors.toList()));

    // list all user privileges
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        allPrivilege,
        permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).stream()
            .sorted()
            .collect(Collectors.toList()));

    // list privileges role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            rolePaths);
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        rolePrivilege,
        permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).stream()
            .sorted()
            .collect(Collectors.toList()));

    // list all role privileges
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        rolePrivilege,
        permissionInfoResp.getPermissionInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).stream()
            .sorted()
            .collect(Collectors.toList()));
  }
}
