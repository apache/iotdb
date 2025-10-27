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

package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class, TableClusterIT.class})
public class IoTDBClusterAuthorityRelationalIT {
  @Before
  public void setUp() throws Exception {
    // Init 1C1D environment
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void runAndCheck(SyncConfigNodeIServiceClient client, TAuthorizerRelationalReq req)
      throws TException {
    TSStatus status;
    status = client.operateRPermission(req);
    assertEquals(status.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private void cleanUserAndRole(SyncConfigNodeIServiceClient client) throws TException {
    TSStatus status;

    // clean user
    TAuthorizerRelationalReq authorizerReq =
        new TAuthorizerRelationalReq(
            AuthorRType.LIST_USER.ordinal(),
            "",
            "",
            "",
            "",
            "",
            Collections.emptySet(),
            false,
            0,
            "");

    TAuthorizerResp authorizerResp = client.queryRPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = authorizerResp.getMemberInfo();
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorizerReq =
            new TAuthorizerRelationalReq(
                AuthorRType.DROP_USER.ordinal(),
                user,
                "",
                "",
                "",
                "",
                Collections.emptySet(),
                false,
                0,
                "");
        status = client.operateRPermission(authorizerReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }

    // clean role
    authorizerReq =
        new TAuthorizerRelationalReq(
            AuthorRType.LIST_ROLE.ordinal(),
            "",
            "",
            "",
            "",
            "",
            Collections.emptySet(),
            false,
            0,
            "");

    authorizerResp = client.queryRPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allRoles = authorizerResp.getMemberInfo();
    for (String role : allRoles) {
      authorizerReq =
          new TAuthorizerRelationalReq(
              AuthorRType.DROP_ROLE.ordinal(),
              role,
              "",
              "",
              "",
              "",
              Collections.emptySet(),
              false,
              0,
              "");
      status = client.operateRPermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  private void createUserORRoleAndCheck(
      SyncConfigNodeIServiceClient client, String name, boolean isUser, String password)
      throws TException {
    TSStatus status;
    TAuthorizerRelationalReq authorizerReq =
        new TAuthorizerRelationalReq(
            isUser ? AuthorRType.CREATE_USER.ordinal() : AuthorRType.CREATE_ROLE.ordinal(),
            isUser ? name : "",
            isUser ? "" : name,
            password,
            "",
            "",
            Collections.emptySet(),
            false,
            0,
            "");
    status = client.operateRPermission(authorizerReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    authorizerReq =
        new TAuthorizerRelationalReq(
            isUser ? AuthorRType.LIST_USER.ordinal() : AuthorRType.LIST_ROLE.ordinal(),
            "",
            "",
            "",
            "",
            "",
            Collections.emptySet(),
            false,
            0,
            "");
    TAuthorizerResp resp = client.queryRPermission(authorizerReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
    assertTrue(resp.getMemberInfo().contains(name));
  }

  private void grantSysPrivilegeAndCheck(
      SyncConfigNodeIServiceClient client,
      String userName,
      String roleName,
      boolean toUser,
      PrivilegeType sysPriv,
      boolean grantOpt)
      throws TException {
    TSStatus status;
    TAuthorizerRelationalReq authorizerRelationalReq =
        new TAuthorizerRelationalReq(
            toUser ? AuthorRType.GRANT_USER_SYS.ordinal() : AuthorRType.GRANT_ROLE_SYS.ordinal(),
            userName,
            roleName,
            "",
            "",
            "",
            Collections.singleton(sysPriv.ordinal()),
            grantOpt,
            0,
            "");

    status = client.operateRPermission(authorizerRelationalReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TCheckUserPrivilegesReq checkUserPrivilegesReq =
        new TCheckUserPrivilegesReq(
            userName, PrivilegeModelType.SYSTEM.ordinal(), sysPriv.ordinal(), grantOpt);
    TPermissionInfoResp resp = client.checkUserPrivileges(checkUserPrivilegesReq);
    assertEquals(resp.status.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertTrue(
        toUser
            ? resp.getUserInfo().getPermissionInfo().getSysPriSet().contains(sysPriv.ordinal())
            : resp.getRoleInfo().containsKey(roleName)
                && resp.getRoleInfo().get(roleName).getSysPriSet().contains(sysPriv.ordinal()));
    if (grantOpt) {
      assertTrue(
          toUser
              ? resp.getUserInfo()
                  .getPermissionInfo()
                  .getSysPriSetGrantOpt()
                  .contains(sysPriv.ordinal())
              : resp.getRoleInfo().containsKey(roleName)
                  && resp.getRoleInfo()
                      .get(roleName)
                      .getSysPriSetGrantOpt()
                      .contains(sysPriv.ordinal()));
    }
  }

  private void grantPrivilegeAndCheck(
      SyncConfigNodeIServiceClient client,
      String username,
      String rolename,
      boolean toUser,
      PrivilegeUnion union)
      throws TException {
    TSStatus status;
    AuthorRType type;
    if (union.isForAny()) {
      type = toUser ? AuthorRType.GRANT_USER_ANY : AuthorRType.GRANT_ROLE_ANY;
    } else if (union.getTbName() == null) {
      type = toUser ? AuthorRType.GRANT_USER_DB : AuthorRType.GRANT_ROLE_DB;
    } else {
      type = toUser ? AuthorRType.GRANT_USER_TB : AuthorRType.GRANT_ROLE_TB;
    }
    TAuthorizerRelationalReq authorizerRelationalReq =
        new TAuthorizerRelationalReq(
            type.ordinal(),
            toUser ? username : "",
            toUser ? "" : rolename,
            "",
            union.getDBName() == null ? "" : union.getDBName(),
            union.getTbName() == null ? "" : union.getTbName(),
            Collections.singleton(union.getPrivilegeType().ordinal()),
            union.isGrantOption(),
            0,
            "");
    status = client.operateRPermission(authorizerRelationalReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    int reqtype = -1;
    if (union.getPrivilegeType().isRelationalPrivilege()) {
      reqtype = PrivilegeModelType.RELATIONAL.ordinal();
    } else if (union.getPrivilegeType().isSystemPrivilege()) {
      reqtype = PrivilegeModelType.SYSTEM.ordinal();
    }
    TCheckUserPrivilegesReq checkUserPrivilegesReq =
        new TCheckUserPrivilegesReq(
            username, reqtype, union.getPrivilegeType().ordinal(), union.isGrantOption());
    if (union.getDBName() != null) {
      checkUserPrivilegesReq.setDatabase(union.getDBName());
    }
    if (union.getTbName() != null) {
      checkUserPrivilegesReq.setTable(union.getTbName());
    }

    TPermissionInfoResp resp = client.checkUserPrivileges(checkUserPrivilegesReq);
    assertEquals(resp.status.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (toUser) {
      TUserResp userInfo = resp.getUserInfo();
      if (union.isForAny()) {
        assertTrue(
            userInfo
                .getPermissionInfo()
                .getAnyScopeGrantSet()
                .contains(union.getPrivilegeType().ordinal()));
      } else if (union.getTbName() == null) {
        assertTrue(userInfo.getPermissionInfo().getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            userInfo
                .getPermissionInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      } else {
        assertTrue(userInfo.getPermissionInfo().getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            userInfo
                .getPermissionInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTablePrivilegeMap()
                .containsKey(union.getTbName()));
        assertTrue(
            userInfo
                .getPermissionInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTablePrivilegeMap()
                .get(union.getTbName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      }
    } else {
      assertTrue(resp.getRoleInfo().containsKey(rolename));
      TRoleResp roleResp = resp.getRoleInfo().get(rolename);
      if (union.isForAny()) {
        assertTrue(roleResp.getAnyScopeGrantSet().contains(union.getPrivilegeType().ordinal()));
      }
      if (union.getTbName() == null) {
        assertTrue(roleResp.getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            roleResp
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      } else {
        assertTrue(roleResp.getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            roleResp
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTablePrivilegeMap()
                .containsKey(union.getTbName()));
        assertTrue(
            roleResp
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTablePrivilegeMap()
                .get(union.getTbName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      }
    }
  }

  private void expectSuccess(TPermissionInfoResp resp) {
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
  }

  private void expectFailed(TPermissionInfoResp resp) {
    assertEquals(TSStatusCode.NO_PERMISSION.getStatusCode(), resp.getStatus().getCode());
  }

  @Test
  public void permissionTest()
      throws TException, ClientManagerException, IOException, InterruptedException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      cleanUserAndRole(client);
      createUserORRoleAndCheck(client, "user1", true, "password123456");
      createUserORRoleAndCheck(client, "role1", false, "");
      runAndCheck(
          client,
          new TAuthorizerRelationalReq(
              AuthorRType.GRANT_USER_ROLE.ordinal(),
              "user1",
              "role1",
              "",
              "",
              "",
              Collections.emptySet(),
              false,
              0,
              ""));
      grantSysPrivilegeAndCheck(client, "user1", "role1", true, PrivilegeType.MANAGE_USER, false);
      grantSysPrivilegeAndCheck(client, "user1", "role1", true, PrivilegeType.MANAGE_ROLE, true);
      grantPrivilegeAndCheck(
          client, "user1", "", true, new PrivilegeUnion("database", "table", PrivilegeType.SELECT));
      grantPrivilegeAndCheck(
          client, "user1", "", true, new PrivilegeUnion("database2", PrivilegeType.ALTER));
      grantPrivilegeAndCheck(
          client, "user1", "", true, new PrivilegeUnion(PrivilegeType.INSERT, true, true));
      grantPrivilegeAndCheck(
          client,
          "user1",
          "role1",
          false,
          new PrivilegeUnion("database", "table2", PrivilegeType.DELETE));
      grantPrivilegeAndCheck(
          client,
          "user1",
          "role1",
          false,
          new PrivilegeUnion("database2", PrivilegeType.CREATE, true));

      // privileges status
      // user1 <-- role1
      // user1 : MANAGE_USER, MANAGE_ROLE(with grant option)
      //         "database"."table" select;
      //         "database2".*      alter;
      //          any               insert(with grant option);
      // role1: MAINTAIN(with grant option)
      //        "database"."table2" delete,
      //        "database2".*       create(with grant option);

      // check login
      TLoginReq req = new TLoginReq("user1", "password123456");
      expectSuccess(client.login(req));

      // check user has role.
      TAuthorizerReq user_role_req =
          new TAuthorizerReq(
              0,
              "user1",
              "role1",
              "",
              "",
              Collections.emptySet(),
              false,
              AuthUtils.serializePartialPathList(Collections.emptyList()),
              0,
              "");
      expectSuccess(client.checkRoleOfUser(user_role_req));

      // check db is visible
      TCheckUserPrivilegesReq check_req =
          new TCheckUserPrivilegesReq("user1", PrivilegeModelType.RELATIONAL.ordinal(), -1, false)
              .setDatabase("database");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check db is visible, because any privileges, so database3 is visible.
      check_req =
          new TCheckUserPrivilegesReq("user1", PrivilegeModelType.RELATIONAL.ordinal(), -1, false)
              .setDatabase("database3");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check table is visible.
      check_req =
          new TCheckUserPrivilegesReq("user1", PrivilegeModelType.RELATIONAL.ordinal(), -1, false)
              .setDatabase("database")
              .setTable("table");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check db privileges
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.ALTER.ordinal(),
                  false)
              .setDatabase("database2");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check db privileges, success for any
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.INSERT.ordinal(),
                  false)
              .setDatabase("database10");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check db privileges, success for role
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.CREATE.ordinal(),
                  false)
              .setDatabase("database2");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check db privileges grant option,
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.CREATE.ordinal(),
                  true)
              .setDatabase("database2");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check tb privilege
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.SELECT.ordinal(),
                  false)
              .setDatabase("database")
              .setTable("table");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check tb privilege
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.ALTER.ordinal(),
                  false)
              .setDatabase("database2")
              .setTable("table");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check tb privilege
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.CREATE.ordinal(),
                  false)
              .setDatabase("database2")
              .setTable("table");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check tb privilege
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.DELETE.ordinal(),
                  false)
              .setDatabase("database")
              .setTable("table2");
      expectSuccess(client.checkUserPrivileges(check_req));

      // check tb privilege
      check_req =
          new TCheckUserPrivilegesReq(
                  "user1",
                  PrivilegeModelType.RELATIONAL.ordinal(),
                  PrivilegeType.SELECT.ordinal(),
                  false)
              .setDatabase("database")
              .setTable("table2");
      expectFailed(client.checkUserPrivileges(check_req));
    }
  }
}
