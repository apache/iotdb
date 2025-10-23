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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterAuthorityIT {

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
            false,
            AuthUtils.serializePartialPathList(new ArrayList<>()),
            0,
            "");
    TAuthorizerResp authorizerResp = client.queryPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = authorizerResp.getMemberInfo();
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
                false,
                AuthUtils.serializePartialPathList(new ArrayList<>()),
                0,
                "");
        status = client.operatePermission(authorizerReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @Test
  public void permissionTest() throws IllegalPathException {
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

    Set<Integer> pathPrivilegeList = new HashSet<>();
    pathPrivilegeList.add(PrivilegeType.READ_DATA.ordinal());

    Set<Integer> revokePathPrivilege = new HashSet<>();
    revokePathPrivilege.add(PrivilegeType.READ_DATA.ordinal());

    List<String> privilege = new ArrayList<>();
    privilege.add("root.** : READ_DATA");

    List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath("root.ln.**"));

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      cleanUserAndRole(client);

      // create user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.CREATE_USER.ordinal(),
              "tempuser0",
              "",
              "passwd123456",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setUserName("tempuser1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0, tempuser1

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq(
                  "tempuser0",
                  PrivilegeModelType.TREE.ordinal(),
                  PrivilegeType.MANAGE_USER.ordinal(),
                  false)
              .setPaths(AuthUtils.serializePartialPathList(paths));
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
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0

      // list user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER.ordinal(),
              "",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      assertEquals(userList, authorizerResp.getMemberInfo());

      // create role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.CREATE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setRoleName("temprole1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0,temprole0,temprole1

      // drop role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.DROP_ROLE.ordinal(),
              "",
              "temprole1",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0,temprole0

      // list role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE.ordinal(),
              "",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getMemberInfo());

      // alter user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.UPDATE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "newpwd123456",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant user
      List<PartialPath> nodeNameList = new ArrayList<>();
      nodeNameList.add(new PartialPath("root.ln.**"));
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              pathPrivilegeList,
              false,
              AuthUtils.serializePartialPathList(nodeNameList),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: root.ln.** ,read_data
      //                temprole0

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq(
                  "tempuser0",
                  PrivilegeModelType.TREE.ordinal(),
                  PrivilegeType.READ_DATA.ordinal(),
                  false)
              .setPaths(AuthUtils.serializePartialPathList(nodeNameList));
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
              pathPrivilegeList,
              false,
              AuthUtils.serializePartialPathList(nodeNameList),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: root.ln.** ,read_data
      //                temprole0: root.ln.** , read_data

      // grant role to user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(nodeNameList),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: root.ln.** ,read_data; [temprole0]
      //                temprole0: root.ln.** , read_data

      // revoke user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              revokePathPrivilege,
              false,
              AuthUtils.serializePartialPathList(nodeNameList),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: ; [temprole0]
      //                temprole0: root.ln.** , read_data

      // revoke role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              revokePathPrivilege,
              false,
              AuthUtils.serializePartialPathList(nodeNameList),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: ; [temprole0]
      //                temprole0: ;

      // list privileges of user.
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertEquals(ColumnHeaderConstant.PRIVILEGES, authorizerResp.getTag());
      assertEquals(
          "tempuser0",
          authorizerResp.getPermissionInfo().getUserInfo().getPermissionInfo().getName());
      assertEquals(
          new ArrayList<>(),
          authorizerResp.getPermissionInfo().getUserInfo().getPermissionInfo().getPrivilegeList());
      assertEquals(1, authorizerResp.getPermissionInfo().getUserInfo().getRoleSet().size());

      // list privileges role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertNull(authorizerResp.getPermissionInfo().getUserInfo());
      assertEquals(1, authorizerResp.getPermissionInfo().getRoleInfoSize());
      assertEquals(
          0,
          authorizerResp.getPermissionInfo().getRoleInfo().get("temprole0").getPrivilegeListSize());

      // list all role of user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getMemberInfo());

      // list all user of role
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      userList.remove("root");
      assertEquals(userList, authorizerResp.getMemberInfo());

      // list root privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "root",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertNull(authorizerResp.getMemberInfo());
      assertEquals(new HashMap<>(), authorizerResp.getPermissionInfo().getRoleInfo());
      assertEquals(new HashSet<>(), authorizerResp.getPermissionInfo().getUserInfo().getRoleSet());
      assertEquals(
          PrivilegeType.getValidPrivilegeCount(PrivilegeModelType.TREE),
          authorizerResp
              .getPermissionInfo()
              .getUserInfo()
              .getPermissionInfo()
              .getPrivilegeList()
              .get(0)
              .priSet
              .size());
      assertEquals(
          PrivilegeType.getValidPrivilegeCount(PrivilegeModelType.SYSTEM),
          authorizerResp
              .getPermissionInfo()
              .getUserInfo()
              .getPermissionInfo()
              .getSysPriSet()
              .size());
      assertEquals(
          PrivilegeType.getValidPrivilegeCount(PrivilegeModelType.SYSTEM),
          authorizerResp
              .getPermissionInfo()
              .getUserInfo()
              .getPermissionInfo()
              .getSysPriSetGrantOptSize());

      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              Collections.singleton(PrivilegeType.MANAGE_USER.ordinal()),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: MANAGE_USER

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq(
              "tempuser0",
              PrivilegeModelType.SYSTEM.ordinal(),
              PrivilegeType.MANAGE_USER.ordinal(),
              false);
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      authorizerReq =
          new TAuthorizerReq(
              AuthorType.GRANT_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              Collections.singleton(PrivilegeType.MANAGE_DATABASE.ordinal()),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // in confignode: tempuser0: MANAGE_USER

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq(
              "tempuser0",
              PrivilegeModelType.SYSTEM.ordinal(),
              PrivilegeType.MANAGE_DATABASE.ordinal(),
              false);
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // revoke role from user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.REVOKE_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list all role of user
      authorizerReq =
          new TAuthorizerReq(
              AuthorType.LIST_ROLE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              false,
              AuthUtils.serializePartialPathList(new ArrayList<>()),
              0,
              "");
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertEquals(new ArrayList<>(), authorizerResp.getMemberInfo());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
