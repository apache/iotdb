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
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
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
    TCheckUserPrivilegesReq checkUserPrivilegesReq;

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
          new AuthorPlan(
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
            .checkUserPrivileges("user0", paths, PrivilegeType.MANAGE_USER.ordinal())
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
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list user
    authorPlan =
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
            .checkUserPrivileges("user0", nodeNameList, PrivilegeType.READ_DATA.ordinal())
            .getStatus()
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    // grant user system privilege
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", sysPriList, false, null);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        authorInfo
            .checkUserPrivileges("user0", new ArrayList<>(), PrivilegeType.MANAGE_ROLE.ordinal())
            .getStatus()
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", new ArrayList<>(), PrivilegeType.MANAGE_ROLE.ordinal())
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        authorInfo.getUserPermissionInfo("user0"), permissionInfoResp.getPermissionInfoResp());

    // list privileges role
    authorPlan =
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
            new AuthorPlan(
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
        new AuthorPlan(
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
          new AuthorPlan(
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
            ConfigPhysicalPlanType.ListUser,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    AuthorPlan listRolePlan =
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
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
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUser, "user0", "", "", "", privilegeList, false, userPaths);
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // check user privileges
    status =
        authorInfo
            .checkUserPrivileges("user0", userPaths, PrivilegeType.WRITE_DATA.ordinal())
            .getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // grant role
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRole, "", "role0", "", "", privilegeList, false, rolePaths);
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
            false,
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
            false,
            new ArrayList<>());
    PermissionInfoResp permissionInfoResp;
    permissionInfoResp = authorInfo.executeListUserPrivileges(authorPlan);
    status = permissionInfoResp.getStatus();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // list privileges role
    authorPlan =
        new AuthorPlan(
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
  public void testDepAuthorPlan() throws TException, AuthException, IllegalPathException {

    AuthorPlan authorPlan;
    TSStatus status;
    cleanUserAndRole();
    // After authNonQuery, preVersion tag must be false;

    /*--TEST FOR USER CREATE 、UPDATE AND DROP -*/
    // this operation will success for pre version.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUserDep,
            "user1",
            "",
            "password1",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // this operation will success for pre version. --length~(32,64)
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUserDep,
            "user1234567user1234567user1234567user1234567",
            "",
            "password1",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // this operation will fail for pre version. --length > 64
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUserDep,
            "user1234567user1234567user1234567user1234567user1234567user1234567user1234567user1234567",
            "",
            "password1",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // this operation will fail for pre version. -- contain &%*@
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUserDep,
            "user1*&%",
            "",
            "password1",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // root, user1, user1234567user1234567user1234567user1234567
    Assert.assertEquals(
        3,
        authorInfo
            .executeListUsers(
                new AuthorPlan(
                    ConfigPhysicalPlanType.ListUser,
                    "",
                    "",
                    "",
                    "",
                    new HashSet<>(),
                    false,
                    new ArrayList<>()))
            .getMemberList()
            .size());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropUserDep,
            "user1234567user1234567user1234567user1234567",
            "",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    Assert.assertEquals(
        2,
        authorInfo
            .executeListUsers(
                new AuthorPlan(
                    ConfigPhysicalPlanType.ListUserDep,
                    "",
                    "",
                    "",
                    "",
                    new HashSet<>(),
                    false,
                    new ArrayList<>()))
            .getMemberList()
            .size());

    // for pre version, password with &% will meet error.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.UpdateUserDep,
            "user1",
            "",
            "password*&S",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    /*--TEST FOR ROLE CREATE AND DROP -*/
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRoleDep,
            "",
            "role1",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    // name longer than 32, It's ok.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRoleDep,
            "",
            "role1234567role1234567role1234567role1234567",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    // contain wrong character, error.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRoleDep,
            "",
            "role1234567role1%%234567role1234567role1234567",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropRoleDep,
            "",
            "role1234567role1234567role1234567role1234567",
            "",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    Assert.assertEquals(
        1,
        authorInfo
            .executeListRoles(
                new AuthorPlan(
                    ConfigPhysicalPlanType.ListRoleDep,
                    "",
                    "",
                    "",
                    "",
                    new HashSet<>(),
                    false,
                    new ArrayList<>()))
            .getMemberList()
            .size());

    // NOW WE HAVE USER：user1， root; ROLE: role1
    // 1. ALTER_PASSWORD WILL BE IGNORE
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.ALTER_PASSWORD.ordinal()),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, BasicAuthorizer.getInstance().getUser("user1").getPathPrivilegeList().size());
    Assert.assertEquals(0, BasicAuthorizer.getInstance().getUser("user1").getSysPrivilege().size());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.ALTER_PASSWORD.ordinal()),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, BasicAuthorizer.getInstance().getRole("role1").getPathPrivilegeList().size());
    Assert.assertEquals(0, BasicAuthorizer.getInstance().getRole("role1").getSysPrivilege().size());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    // 2. GRANT_PRIVILEGE WILL BE IGNORE
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.GRANT_PRIVILEGE.ordinal()),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, BasicAuthorizer.getInstance().getUser("user1").getPathPrivilegeList().size());
    Assert.assertEquals(0, BasicAuthorizer.getInstance().getUser("user1").getSysPrivilege().size());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.GRANT_PRIVILEGE.ordinal()),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, BasicAuthorizer.getInstance().getRole("role1").getPathPrivilegeList().size());
    Assert.assertEquals(0, BasicAuthorizer.getInstance().getRole("role1").getSysPrivilege().size());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    // 3. check path and complex

    // READ -> READ_SCHEMA, READ_DATA
    // root.t1.*.t2 NO ERROR
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.READ.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.*.t2")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().getUser("user1").getServiceReady());
    Assert.assertEquals(
        2,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.*.t2"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.*.t2"))
            .contains(PrivilegeType.READ_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.*.t2"))
            .contains(PrivilegeType.READ_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    /** user1 : root.t1.*.t2 : read_data, read_schema */

    // READ -> READ_SCHEMA, READ_DATA
    // root.t1.t2.* NO ERROR
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.*")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().getUser("user1").getServiceReady());
    Assert.assertEquals(
        2,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.*"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.*"))
            .contains(PrivilegeType.WRITE_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.*"))
            .contains(PrivilegeType.WRITE_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    /** user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_data, write_schema */

    // READ -> READ_SCHEMA, READ_DATA
    // root.t1.t2 NO ERROR
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.READ.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertTrue(BasicAuthorizer.getInstance().getRole("role1").getServiceReady());
    Assert.assertEquals(
        2,
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2"))
            .contains(PrivilegeType.READ_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2"))
            .contains(PrivilegeType.READ_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_data, write_schema role1
     * : root.t1.t2 : read_data, read_schema
     */

    // READ -> READ_SCHEMA, READ_DATA
    // root.t1.t2.* NO ERROR
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.**")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertTrue(BasicAuthorizer.getInstance().getRole("role1").getServiceReady());
    Assert.assertEquals(
        2,
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2.**"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2.**"))
            .contains(PrivilegeType.WRITE_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2.**"))
            .contains(PrivilegeType.WRITE_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_data, write_schema role1
     * : root.t1.t2 : read_data, read_schema : root.t1.t2.** :write_data, write_schema
     */

    // duplicate grant will not raise an error instead. But it's acceptable. --skip
    // duplicate revoke will get an error.

    // REVOKE TEST
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.READ.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.t3")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().getUser("user1").getServiceReady());
    Assert.assertEquals(
        4,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .contains(PrivilegeType.READ_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .contains(PrivilegeType.READ_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_data, write_schema :
     * root.t1.t2.t3 : read_data, read_schema role1 : root.t1.t2 : read_data, read_schema :
     * root.t1.t2.** :write_data, write_schema
     */

    // this revoke operation will not match other path privilege.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE_DATA.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.*")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        3,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .contains(PrivilegeType.READ_DATA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .contains(PrivilegeType.READ_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_schema : root.t1.t2.t3 :
     * read_data, read_schema role1 : root.t1.t2 : read_data, read_schema : root.t1.t2.**
     * :write_data, write_schema
     */
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE_DATA.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.**")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        1,
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .size());
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2.**"))
            .contains(PrivilegeType.WRITE_SCHEMA.ordinal()));
    Assert.assertTrue(
        BasicAuthorizer.getInstance()
            .getRole("role1")
            .getPathPrivileges(new PartialPath("root.t1.t2"))
            .contains(PrivilegeType.READ_SCHEMA.ordinal()));
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_schema : root.t1.t2.t3 :
     * read_data, read_schema role1 : root.t1.t2 : read_data, read_schema : root.t1.t2.**
     * :write_schema
     */

    // duplicate revoke will raise an error.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeUserDep,
            "user1",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE_DATA.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.*")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // duplicate revoke will raise an error.
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRoleDep,
            "",
            "role1",
            "",
            "",
            Collections.singleton(PriPrivilegeType.WRITE_DATA.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.t1.t2.**")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    HashSet<Integer> priSet = new HashSet<>();
    priSet.add(PriPrivilegeType.READ.ordinal());
    priSet.add(PriPrivilegeType.WRITE_SCHEMA.ordinal());
    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "user1",
            "",
            "",
            "",
            priSet,
            false,
            Collections.singletonList(new PartialPath("root.t1.*a.**")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    // now check user's path and role's path.
    /**
     * user1 : root.t1.*.t2 : read_data, read_schema : root.t1.t2.* : write_schema : root.t1.t2.t3 :
     * read_data, read_schema : root.t1.*a.** : read_data, read_schema, write_schema role1 :
     * root.t1.t2 : read_data, read_schema : root.t1.t2.** :write_schema
     */

    // will turn to :
    /**
     * user1 : root.t1.** : read_data, read_schema, write_schema : root.t1.t2.** : write_schema :
     * root.t1.t2.t3 : read_data, read_schema role1 : root.t1.t2 : read_data, read_schema :
     * root.t1.t2.** :write_schema
     */
    authorInfo.checkUserPathPrivilege();
    Assert.assertEquals(
        3,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.**"))
            .size());
    Assert.assertTrue(
        AuthUtils.hasPrivilege(
            new PartialPath("root.t1.t2.**"),
            PrivilegeType.WRITE_SCHEMA.ordinal(),
            BasicAuthorizer.getInstance().getUser("user1").getPathPrivilegeList()));
    Assert.assertEquals(
        3,
        BasicAuthorizer.getInstance()
            .getUser("user1")
            .getPathPrivileges(new PartialPath("root.t1.t2.t3"))
            .size());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());
    Assert.assertFalse(BasicAuthorizer.getInstance().forRolePreVersion());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUserDep,
            "userfull",
            "",
            "password1",
            "",
            new HashSet<>(),
            false,
            new ArrayList<>());
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertFalse(BasicAuthorizer.getInstance().forUserPreVersion());

    authorPlan =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUserDep,
            "userfull",
            "",
            "",
            "",
            Collections.singleton(PriPrivilegeType.ALL.ordinal()),
            false,
            Collections.singletonList(new PartialPath("root.**")));
    status = authorInfo.authorNonQuery(authorPlan);
    Assert.assertEquals(
        PrivilegeType.getPathPriCount(),
        BasicAuthorizer.getInstance()
            .getUser("userfull")
            .getPathPrivileges(new PartialPath("root.**"))
            .size());
    Assert.assertEquals(
        PrivilegeType.getSysPriCount(),
        BasicAuthorizer.getInstance().getUser("userfull").getSysPrivilege().size());
  }
}
