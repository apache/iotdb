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
import org.apache.iotdb.commons.auth.entity.ModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
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

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class AuthorInfoTest {

  private static AuthorInfo authorInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "authorInfo-snapshot");

  private static final File userFolder =
      new File(CommonDescriptor.getInstance().getConfig().getUserFolder());
  private static final File roleFolder =
      new File(CommonDescriptor.getInstance().getConfig().getRoleFolder());

  @BeforeClass
  public static void setup() {
    authorInfo = new AuthorInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
    if (!userFolder.exists()) {
      userFolder.mkdirs();
    }
    if (!roleFolder.exists()) {
      roleFolder.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException, AuthException {
    authorInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
    if (userFolder.exists()) {
      FileUtils.deleteDirectory(userFolder);
    }
    if (roleFolder.exists()) {
      FileUtils.deleteDirectory(roleFolder);
    }
  }

  @Test
  public void permissionTest() throws TException, AuthException, IllegalPathException {

    TSStatus status;

    List<String> userList = new ArrayList<>();
    userList.add("root");
    userList.add("user0");
    userList.add("user1");

    List<String> roleList = new ArrayList<>();
    roleList.add("role0");
    roleList.add("role1");

    AuthorPlan authorPlan;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.READ_DATA.ordinal());

    Set<Integer> sysPriList = new HashSet<>();
    sysPriList.add(PrivilegeType.MANAGE_ROLE.ordinal());

    Set<Integer> revokePrivilege = new HashSet<>();
    revokePrivilege.add(PrivilegeType.READ_DATA.ordinal());

    List<String> privilege = new ArrayList<>();
    privilege.add("root.** : MANAGE_USER");

    List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath("root.ln"));

    cleanUserAndRole();

    // create user
    {
      authorPlan =
          new AuthorTreePlan(
              ConfigPhysicalPlanType.CreateUser,
              "user0",
              "",
              "passwd",
              "",
              new HashSet<>(),
              false,
              new ArrayList<>());
      status = authorInfo.authorNonQuery(authorPlan);
      Assert.assertNull(status.getMessage());
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorPlan.setUserName("user1");
      status = authorInfo.authorNonQuery(authorPlan);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", new PrivilegeUnion(PrivilegeType.MANAGE_USER))
            .getStatus();
    Assert.assertEquals(TSStatusCode.NO_PERMISSION.getStatusCode(), status.getCode());

    // drop user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.DropUser,
            "user1",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("user1");
    Assert.assertEquals(userList, permissionInfoResp.getMemberList());

    // create role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.CreateRole,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorPlan.setRoleName("role1");
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // drop role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.DropRole,
            "",
            "role1",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRole,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("role1");
    Assert.assertEquals(roleList, permissionInfoResp.getMemberList());

    // alter user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.UpdateUser,
            "user0",
            "",
            "",
            "newpwd",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant user path privilege
    List<PartialPath> nodeNameList = new ArrayList<>();
    nodeNameList.add(new PartialPath("root.ln.**"));
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantUser,
            "user0",
            "",
            "",
            "",
            privilegeList,
            false,
            nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        authorInfo
            .checkUserPrivileges("user0", new PrivilegeUnion(nodeNameList, PrivilegeType.READ_DATA))
            .getStatus()
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    // grant user system privilege
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", sysPriList, false, null);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        authorInfo
            .checkUserPrivileges("user0", new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
            .getStatus()
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantRole,
            "",
            "role0",
            "",
            "",
            privilegeList,
            false,
            nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role to user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantRoleToUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.RevokeUser,
            "user0",
            "",
            "",
            "",
            revokePrivilege,
            false,
            nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // revoke role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.RevokeRole,
            "",
            "role0",
            "",
            "",
            revokePrivilege,
            false,
            nodeNameList);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        authorInfo.getUserPermissionInfo("user0", ModelType.ALL),
        permissionInfoResp.getPermissionInfoResp());

    // list privileges role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list all role of user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRole,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    roleList.remove("role1");
    Assert.assertEquals(roleList, permissionInfoResp.getMemberList());

    // list all user of role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    userList.remove("user1");
    userList.remove("root");
    Assert.assertEquals(userList, permissionInfoResp.getMemberList());

    // revoke role from user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.RevokeRoleFromUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  private void cleanUserAndRole() throws TException, AuthException {
    TSStatus status;

    // clean user
    AuthorPlan authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUsers(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = permissionInfoResp.getMemberList();
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorPlan =
            new AuthorTreePlan(
                ConfigPhysicalPlanType.DropUser,
                user,
                "",
                "",
                "",
                new HashSet<>(),
                false,
                new ArrayList<>());
        status = authorInfo.authorNonQuery(authorPlan);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }

    // clean role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRole,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> roleList = permissionInfoResp.getMemberList();
    for (String roleN : roleList) {
      authorPlan =
          new AuthorTreePlan(
              ConfigPhysicalPlanType.DropRole,
              "",
              roleN,
              "",
              "",
              new HashSet<>(),
              false,
              new ArrayList<>());
      status = authorInfo.authorNonQuery(authorPlan);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  @Test
  public void takeSnapshot() throws TException, IOException, AuthException {
    cleanUserAndRole();
    // create role
    AuthorPlan createRoleReq = new AuthorTreePlan(ConfigPhysicalPlanType.CreateRole);
    createRoleReq.setRoleName("testRole");
    TSStatus status = authorInfo.authorNonQuery(createRoleReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    AuthorPlan createUserReq = new AuthorTreePlan(ConfigPhysicalPlanType.CreateUser);
    createUserReq.setUserName("testUser");
    createUserReq.setPassword("testPassword");
    status = authorInfo.authorNonQuery(createUserReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    AuthorPlan listUserPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    AuthorPlan listRolePlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRole,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    Assert.assertEquals(1, authorInfo.executeListRoles(listRolePlan).getMemberList().size());
    Assert.assertEquals(2, authorInfo.executeListUsers(listUserPlan).getMemberList().size());
    Assert.assertTrue(authorInfo.processTakeSnapshot(snapshotDir));
    authorInfo.clear();
    authorInfo.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(1, authorInfo.executeListRoles(listRolePlan).getMemberList().size());
    Assert.assertEquals(2, authorInfo.executeListUsers(listUserPlan).getMemberList().size());
  }

  @Test
  public void testMultPathsPermission() throws TException, AuthException, IllegalPathException {
    TSStatus status;

    AuthorPlan authorPlan;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.WRITE_DATA.ordinal());
    privilegeList.add(PrivilegeType.READ_DATA.ordinal());

    Map<String, List<String>> permissionInfo;
    List<String> userPrivilege = new ArrayList<>();
    userPrivilege.add("root.sg.** : READ_DATA WRITE_DATA");
    userPrivilege.add("root.ln.** : READ_DATA WRITE_DATA");
    Collections.sort(userPrivilege);

    List<String> rolePrivilege = new ArrayList<>();
    rolePrivilege.add("root.abc.** : READ_DATA WRITE_DATA");
    rolePrivilege.add("root.role_1.** : READ_DATA WRITE_DATA");
    Collections.sort(rolePrivilege);

    List<String> allPrivilege = new ArrayList<>();
    allPrivilege.addAll(userPrivilege);
    allPrivilege.addAll(rolePrivilege);
    Collections.sort(allPrivilege);

    List<PartialPath> userPaths = new ArrayList<>();
    userPaths.add(new PartialPath("root.ln.**"));
    userPaths.add(new PartialPath("root.sg.**"));

    List<PartialPath> rolePaths = new ArrayList<>();
    rolePaths.add(new PartialPath("root.role_1.**"));
    rolePaths.add(new PartialPath("root.abc.**"));

    cleanUserAndRole();

    // create user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.CreateUser,
            "user0",
            "",
            "passwd",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertNull(status.getMessage());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // create role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.CreateRole,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", privilegeList, false, userPaths);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", new PrivilegeUnion(userPaths, PrivilegeType.WRITE_DATA))
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantRole, "", "role0", "", "", privilegeList, false, rolePaths);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role to user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.GrantRoleToUser,
            "user0",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges user
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "user0",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    PermissionInfoResp permissionInfoResp;
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges role
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "role0",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRolePrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void createUserWithRawPassword() throws AuthException {
    TSStatus status;
    AuthorPlan authorPlan;
    authorPlan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.CreateUserWithRawPassword,
            "testuser",
            "",
            AuthUtils.encryptPassword("password"),
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TPermissionInfoResp result = authorInfo.login("testuser", "password");
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getStatus().getCode());
  }

  private void checkAuthorNonQueryReturn(AuthorPlan plan) {
    TSStatus status = authorInfo.authorNonQuery(plan);
    Assert.assertNull(status.getMessage());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void relationalPermissionTest() throws TException, AuthException {
    cleanUserAndRole();
    TSStatus status;

    // create user
    AuthorPlan plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RCreateUser, "user", "", "", "", false, -1, "password");

    checkAuthorNonQueryReturn(plan);

    // check user permission
    status =
        authorInfo
            .checkUserPrivileges("user", new PrivilegeUnion(PrivilegeType.MANAGE_USER))
            .getStatus();
    Assert.assertEquals(TSStatusCode.NO_PERMISSION.getStatusCode(), status.getCode());

    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RDropUser, "user", "", "", "", false, -1, "");
    checkAuthorNonQueryReturn(plan);

    // list user
    plan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    PermissionInfoResp permissionInfoResp = authorInfo.executeListUsers(plan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(1, permissionInfoResp.getMemberList().size()); // Only root

    // create role
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RCreateRole, "", "role", "", "", false, -1, "");

    checkAuthorNonQueryReturn(plan);

    // create user
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RCreateUser, "user", "", "", "", false, -1, "password");

    checkAuthorNonQueryReturn(plan);
    // grant privileges

    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserSysPri,
            "user",
            "",
            "",
            "",
            false,
            PrivilegeType.MANAGE_USER.ordinal(),
            "");
    checkAuthorNonQueryReturn(plan);
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserDBPriv,
            "user",
            "",
            "testdb",
            "",
            true,
            PrivilegeType.DELETE.ordinal(),
            "");
    checkAuthorNonQueryReturn(plan);
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserTBPriv,
            "user",
            "",
            "testdb",
            "testtb",
            true,
            PrivilegeType.SELECT.ordinal(),
            "");
    checkAuthorNonQueryReturn(plan);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges("user", new PrivilegeUnion(PrivilegeType.MANAGE_USER))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges("user", new PrivilegeUnion("testdb", PrivilegeType.DELETE))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges(
                "user", new PrivilegeUnion("testdb", "testtb", PrivilegeType.DELETE))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges(
                "user", new PrivilegeUnion("testdb", "testtb", PrivilegeType.SELECT))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.NO_PERMISSION.getStatusCode(),
        authorInfo
            .checkUserPrivileges("user", new PrivilegeUnion("testdb", PrivilegeType.SELECT))
            .getStatus()
            .getCode());

    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantRoleSysPri,
            "",
            "role",
            "",
            "",
            false,
            PrivilegeType.MANAGE_DATABASE.ordinal(),
            "");
    checkAuthorNonQueryReturn(plan);
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantRoleTBPriv,
            "",
            "role",
            "database2",
            "testtb",
            true,
            PrivilegeType.ALTER.ordinal(),
            "");
    checkAuthorNonQueryReturn(plan);

    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserRole, "user", "role", "", "", false, 0, "");
    checkAuthorNonQueryReturn(plan);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges("user", new PrivilegeUnion(PrivilegeType.MANAGE_DATABASE))
            .getStatus()
            .getCode());

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorInfo
            .checkUserPrivileges(
                "user", new PrivilegeUnion("database2", "testtb", PrivilegeType.ALTER))
            .getStatus()
            .getCode());

    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RDropUser, "user", "", "", "", false, 0, "");
    checkAuthorNonQueryReturn(plan);
    plan =
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RDropRole, "", "role", "", "", false, 0, "");
    checkAuthorNonQueryReturn(plan);
    plan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListUsers(plan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(1, permissionInfoResp.getMemberList().size()); // Only root

    plan =
        new AuthorTreePlan(
            ConfigPhysicalPlanType.ListRole,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    permissionInfoResp = authorInfo.executeListRoles(plan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(0, permissionInfoResp.getMemberList().size()); // Only root
  }
}
