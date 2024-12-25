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
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.db.auth.BasicAuthorityCache;
import org.apache.iotdb.db.auth.ClusterAuthorityFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterAuthorityRelationalIT {
  @Before
  public void setUp() throws Exception {
    // Init 1C0D environment
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
            AuthorRType.LIST_USER.ordinal(), "", "", "", "", "", -1, false);

    TAuthorizerResp authorizerResp = client.queryRPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = authorizerResp.getMemberInfo();
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorizerReq =
            new TAuthorizerRelationalReq(
                AuthorRType.DROP_USER.ordinal(), user, "", "", "", "", -1, false);
        status = client.operateRPermission(authorizerReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
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
            -1,
            false);
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
            -1,
            false);
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
            sysPriv.ordinal(),
            grantOpt);

    status = client.operateRPermission(authorizerRelationalReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TCheckUserPrivilegesReq checkUserPrivilegesReq =
        new TCheckUserPrivilegesReq(
            userName, PrivilegeModelType.SYSTEM.ordinal(), sysPriv.ordinal(), grantOpt);
    TPermissionInfoResp resp = client.checkUserPrivileges(checkUserPrivilegesReq);
    assertEquals(resp.status.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertTrue(
        toUser
            ? resp.getUserInfo().getBasicInfo().getSysPriSet().contains(sysPriv.ordinal())
            : resp.getRoleInfo().containsKey(roleName)
                && resp.getRoleInfo().get(roleName).getSysPriSet().contains(sysPriv.ordinal()));
    if (grantOpt) {
      assertTrue(
          toUser
              ? resp.getUserInfo().getBasicInfo().getSysPriSetGrantOpt().contains(sysPriv.ordinal())
              : resp.getRoleInfo().containsKey(roleName)
                  && resp.getRoleInfo()
                      .get(roleName)
                      .getSysPriSetGrantOpt()
                      .contains(sysPriv.ordinal()));
    }
  }

  private void grantPrivilegeAndCheck(
      SyncConfigNodeIServiceClient client, String name, boolean toUser, PrivilegeUnion union)
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
            toUser ? name : "",
            toUser ? "" : name,
            "",
            union.getDBName() == null ? "" : union.getDBName(),
            union.getTbName() == null ? "" : union.getTbName(),
            union.getPrivilegeType().ordinal(),
            union.isGrantOption());
    status = client.operateRPermission(authorizerRelationalReq);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TCheckUserPrivilegesReq checkUserPrivilegesReq =
        new TCheckUserPrivilegesReq(
                name,
                PrivilegeModelType.RELATIONAL.ordinal(),
                union.getPrivilegeType().ordinal(),
                union.isGrantOption())
            .setDatabase(union.getDBName());
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
                .getBasicInfo()
                .getAnyScopeGrantSet()
                .contains(union.getPrivilegeType().ordinal()));
      } else if (union.getTbName() == null) {
        assertTrue(userInfo.getBasicInfo().getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            userInfo
                .getBasicInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      } else {
        assertTrue(userInfo.getBasicInfo().getDbPrivilegeMap().containsKey(union.getDBName()));
        assertTrue(
            userInfo
                .getBasicInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTableinfo()
                .containsKey(union.getTbName()));
        assertTrue(
            userInfo
                .getBasicInfo()
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTableinfo()
                .get(union.getTbName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      }
    } else {
      assertTrue(resp.getRoleInfo().containsKey(name));
      TRoleResp roleResp = resp.getRoleInfo().get(name);
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
                .getTableinfo()
                .containsKey(union.getTbName()));
        assertTrue(
            roleResp
                .getDbPrivilegeMap()
                .get(union.getDBName())
                .getTableinfo()
                .get(union.getTbName())
                .getPrivileges()
                .contains(union.getPrivilegeType().ordinal()));
      }
    }
  }

  @Test
  public void permissionTest()
      throws TException, ClientManagerException, IOException, InterruptedException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      cleanUserAndRole(client);
      createUserORRoleAndCheck(client, "user1", true, "password");
      createUserORRoleAndCheck(client, "role1", false, "");
      runAndCheck(
          client,
          new TAuthorizerRelationalReq(
              AuthorRType.GRANT_USER_ROLE.ordinal(), "user1", "role1", "", "", "", -1, false));
      grantSysPrivilegeAndCheck(client, "user1", "role1", true, PrivilegeType.MANAGE_USER, false);
      grantSysPrivilegeAndCheck(client, "user1", "role1", true, PrivilegeType.MANAGE_ROLE, true);
      grantSysPrivilegeAndCheck(client, "user1", "role1", false, PrivilegeType.MAINTAIN, true);
      grantPrivilegeAndCheck(
          client, "user1", true, new PrivilegeUnion("database", "table", PrivilegeType.SELECT));
      grantPrivilegeAndCheck(
          client, "user1", true, new PrivilegeUnion("database2", PrivilegeType.ALTER));
      grantPrivilegeAndCheck(
          client, "user1", true, new PrivilegeUnion(PrivilegeType.INSERT, true, true));
      grantPrivilegeAndCheck(
          client, "role1", false, new PrivilegeUnion("database", "table2", PrivilegeType.DELETE));
      grantPrivilegeAndCheck(
          client, "role1", false, new PrivilegeUnion("database2", PrivilegeType.CREATE, true));
      // privileges status
      // user1 <-- role1
      // user1 : MANAGE_USER, MANAGE_ROLE(with grant option)
      //         "database"."table" select;
      //         "database2".*      alter;
      //          any               insert(with grant option);
      // role1: MAINTAIN(with grant option)
      //        "database"."table2" delete,
      //        "database2".*       create(with grant option);
      testClusterAuthorFetcher(false);
      testClusterAuthorFetcher(true);
    }
  }

  private void testClusterAuthorFetcher(boolean acceptCache) {
    ClusterAuthorityFetcher authorityFetcher =
        new ClusterAuthorityFetcher(new BasicAuthorityCache());
    authorityFetcher.setAcceptCache(acceptCache);
    assertEquals(
        authorityFetcher.checkUser("user1", "password").getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertTrue(authorityFetcher.checkRole("user1", "password"));
    assertEquals(
        authorityFetcher.checkDBVisible("user1", "database").getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertEquals(
        authorityFetcher.checkDBVisible("user1", "database3").getCode(),
        TSStatusCode.NO_PERMISSION.getStatusCode());
    assertEquals(
        authorityFetcher.checkTBVisible("user1", "database", "table").getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertEquals(
        authorityFetcher.checkTBVisible("user1", "database2", "table").getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // Any privilege
    assertEquals(
        authorityFetcher.checkTBVisible("user1", "database", "table2").getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher.checkUserDBPrivileges("user1", "database", PrivilegeType.SELECT).getCode(),
        TSStatusCode.NO_PERMISSION.getStatusCode());
    assertEquals(
        authorityFetcher.checkUserDBPrivileges("user1", "database2", PrivilegeType.ALTER).getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // from role
    assertEquals(
        authorityFetcher
            .checkUserDBPrivileges("user1", "database2", PrivilegeType.CREATE)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher
            .checkUserTBPrivileges("user1", "database", "table", PrivilegeType.SELECT)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // for database.*
    assertEquals(
        authorityFetcher
            .checkUserTBPrivileges("user1", "database2", "table_no", PrivilegeType.ALTER)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // for any privilege
    assertEquals(
        authorityFetcher
            .checkUserTBPrivileges("user1", "database_no", "table_no", PrivilegeType.INSERT)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    // for any insert (with grant option)
    assertEquals(
        authorityFetcher
            .checkUserDBPrivilegesGrantOpt("user1", "database_no", PrivilegeType.INSERT)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertEquals(
        authorityFetcher
            .checkUserDBPrivilegesGrantOpt("user1", "database2", PrivilegeType.DELETE)
            .getCode(),
        TSStatusCode.NO_PERMISSION.getStatusCode());

    assertEquals(
        authorityFetcher
            .checkUserDBPrivilegesGrantOpt("user1", "database2", PrivilegeType.CREATE)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher
            .checkUserTBPrivilegesGrantOpt("user1", "database", "table", PrivilegeType.SELECT)
            .getCode(),
        TSStatusCode.NO_PERMISSION.getStatusCode());

    // for any privilege
    assertEquals(
        authorityFetcher
            .checkUserTBPrivilegesGrantOpt("user1", "database", "table", PrivilegeType.INSERT)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher
            .checkUserTBPrivilegesGrantOpt("user1", "database2", "table_no", PrivilegeType.CREATE)
            .getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher.checkUserSysPrivilegesGrantOpt("user1", PrivilegeType.MAINTAIN).getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    assertEquals(
        authorityFetcher
            .checkUserSysPrivilegesGrantOpt("user1", PrivilegeType.MANAGE_USER)
            .getCode(),
        TSStatusCode.NO_PERMISSION.getStatusCode());
  }
}
