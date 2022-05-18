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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorizerManagerTest {

  AuthorizerManager authorizerManager;
  TPermissionInfoResp tPermissionInfoResp;

  @Before
  public void setUp() throws Exception {
    authorizerManager = AuthorizerManager.getInstance();
    tPermissionInfoResp = new TPermissionInfoResp();
  }

  @Test
  public void permissionCacheTest() {
    User user = new User();
    Role role = new Role();
    List<String> roleList = new ArrayList<>();
    Set<Integer> privilegesIds = new HashSet<>();
    PathPrivilege privilege = new PathPrivilege();
    List<PathPrivilege> privilegeList = new ArrayList<>();

    privilegesIds.add(PrivilegeType.CREATE_ROLE.ordinal());
    privilegesIds.add(PrivilegeType.REVOKE_USER_ROLE.ordinal());

    privilege.setPath("root.ln");
    privilege.setPrivileges(privilegesIds);

    privilegeList.add(privilege);

    role.setName("role");
    role.setPrivilegeList(privilegeList);

    roleList.add("role");
    user.setName("user");
    user.setPassword("password");
    user.setPrivilegeList(privilegeList);
    user.setRoleList(roleList);

    TPermissionInfoResp result = new TPermissionInfoResp();
    TUserResp tUserResp = new TUserResp();
    TRoleResp tRoleResp = new TRoleResp();
    Map<String, TRoleResp> tRoleRespMap = new HashMap();
    List<String> userPrivilegeList = new ArrayList<>();
    List<String> rolePrivilegeList = new ArrayList<>();

    // User permission information
    for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
      userPrivilegeList.add(pathPrivilege.getPath());
      String privilegeIdList = pathPrivilege.getPrivileges().toString();
      userPrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
    }
    tUserResp.setUsername(user.getName());
    tUserResp.setPassword(user.getPassword());
    tUserResp.setPrivilegeList(userPrivilegeList);
    tUserResp.setRoleList(user.getRoleList());

    // Permission information for roles owned by users
    for (String roleName : user.getRoleList()) {
      tRoleResp.setRoleName(roleName);
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        rolePrivilegeList.add(pathPrivilege.getPath());
        String privilegeIdList = pathPrivilege.getPrivileges().toString();
        rolePrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
      }
      tRoleResp.setPrivilegeList(rolePrivilegeList);
      tRoleRespMap.put(roleName, tRoleResp);
    }
    result.setUserInfo(tUserResp);
    result.setRoleInfo(tRoleRespMap);

    authorizerManager.settPermissionInfoResp(result);
    authorizerManager.getUserCache().get(user.getName());

    User user1 = authorizerManager.getUserCache().getIfPresent(user.getName());
    Role role1 = authorizerManager.getRoleCache().getIfPresent(role.getName());
    Assert.assertEquals(user.getName(), user1.getName());
    Assert.assertEquals(user.getPassword(), user1.getPassword());
    Assert.assertEquals(user.getPrivilegeList(), user1.getPrivilegeList());
    Assert.assertEquals(user.getRoleList(), user1.getRoleList());

    Assert.assertEquals(role.getName(), role1.getName());
    Assert.assertEquals(role.getPrivilegeList(), role1.getPrivilegeList());

    authorizerManager.invalidateCache(user.getName(), "");

    user1 = authorizerManager.getUserCache().getIfPresent(user.getName());
    role1 = authorizerManager.getRoleCache().getIfPresent(role.getName());

    Assert.assertEquals(user1, null);
    Assert.assertEquals(role1, null);
  }
}
