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

package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.db.mpp.plan.statement.AuthorType;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterAuthorityIT {

  @Before
  public void setUp() throws Exception {
    // Init 1C0D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 0);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void cleanUserAndRole(IConfigNodeRPCService.Iface client) throws TException {
    TSStatus status;

    // clean user
    TAuthorizerReq authorizerReq =
        new TAuthorizerReq(
            AuthorType.LIST_USER.ordinal(),
            "",
            "",
            "",
            "",
            new HashSet<>(),
            Collections.singletonList(""));
    TAuthorizerResp authorizerResp = client.queryPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER);
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorizerReq =
            new TAuthorizerReq(
                AuthorType.DROP_USER.ordinal(),
                user,
                "",
                "",
                "",
                new HashSet<>(),
                Collections.singletonList(""));
        status = client.operatePermission(authorizerReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @Test
  public void permissionTest() {
    TSStatus status;
    List<String> userList = new ArrayList<>();
    userList.add("root");
    userList.add("tempuser0");
    userList.add("tempuser1");

    List<String> roleList = new ArrayList<>();
    roleList.add("temprole0");
    roleList.add("temprole1");

    TAuthorizerReq authorizerReq;
    TAuthorizerResp authorizerResp;
    TCheckUserPrivilegesReq checkUserPrivilegesReq;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.DELETE_USER.ordinal());
    privilegeList.add(PrivilegeType.CREATE_USER.ordinal());

    Set<Integer> revokePrivilege = new HashSet<>();
    revokePrivilege.add(PrivilegeType.DELETE_USER.ordinal());

    List<String> privilege = new ArrayList<>();
    privilege.add("root.** : CREATE_USER");
    privilege.add("root.** : CREATE_USER");

    List<String> paths = new ArrayList<>();
    paths.add("root.ln.**");

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      cleanUserAndRole(client);

      // create user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.CREATE_USER.ordinal(),
              "tempuser0",
              "",
              "passwd",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setUserName("tempuser1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.NO_PERMISSION.getStatusCode(), status.getCode());

      // drop user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.DROP_USER.ordinal(),
              "tempuser1",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER.ordinal(), "", "", "", "", new HashSet<>(), new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      assertEquals(userList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER));

      // create role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.CREATE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setRoleName("temprole1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // drop role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.DROP_ROLE.ordinal(),
              "",
              "temprole1",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE.ordinal(), "", "", "", "", new HashSet<>(), new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_ROLE));

      // alter user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.UPDATE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "newpwd",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant user
      List<String> nodeNameList = new ArrayList<>();
      nodeNameList.add("root.ln.**");
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              privilegeList,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              privilegeList,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant role to user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // revoke user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              revokePrivilege,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // revoke role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              revokePrivilege,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list privileges user on root.ln.**
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          0, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).size());

      // list privileges user on root.**
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              Collections.singletonList("root.**"));
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list user privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list privileges role on root.ln.**
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      privilege.remove(0);
      assertEquals(
          0, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).size());

      // list privileges role on root.**
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              Collections.singletonList("root.**"));
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list role privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list all role of user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_ROLE));

      // list all user of role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      userList.remove("root");
      assertEquals(userList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER));

      // revoke role from user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list root privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "root",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      for (int i = 0; i < PrivilegeType.values().length; i++) {
        assertEquals(
            PrivilegeType.values()[i].toString(),
            authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
