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
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorizerManagerTest {

  ClusterAuthorityFetcher authorityFetcher = new ClusterAuthorityFetcher(new BasicAuthorityCache());

  @Test
  public void permissionCacheTest() {
    User user = new User();
    Role role1 = new Role();
    Role role2 = new Role();
    List<String> roleList = new ArrayList<>();
    Set<Integer> privilegesIds = new HashSet<>();
    PathPrivilege privilege = new PathPrivilege();
    List<PathPrivilege> privilegeList = new ArrayList<>();
    privilegesIds.add(PrivilegeType.CREATE_ROLE.ordinal());
    privilegesIds.add(PrivilegeType.REVOKE_USER_ROLE.ordinal());
    privilege.setPath("root.ln");
    privilege.setPrivileges(privilegesIds);
    privilegeList.add(privilege);
    role1.setName("role1");
    role1.setPrivilegeList(privilegeList);
    role2.setName("role2");
    role2.setPrivilegeList(new ArrayList<>());
    roleList.add("role1");
    roleList.add("role2");
    user.setName("user");
    user.setPassword("password");
    user.setPrivilegeList(privilegeList);
    user.setRoleList(roleList);
    TPermissionInfoResp result = new TPermissionInfoResp();
    TUserResp tUserResp = new TUserResp();
    Map<String, TRoleResp> tRoleRespMap = new HashMap();
    List<String> userPrivilegeList = new ArrayList<>();
    List<String> rolePrivilegeList = new ArrayList<>();
    List<Role> roleList1 = new ArrayList<>();
    roleList1.add(role1);
    roleList1.add(role2);

    // User permission information
    for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
      userPrivilegeList.add(pathPrivilege.getPath());
      String privilegeIdList = pathPrivilege.getPrivileges().toString();
      userPrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
    }
    tUserResp.setUsername(user.getName());
    tUserResp.setPassword(user.getPassword());
    tUserResp.setPrivilegeList(userPrivilegeList);
    tUserResp.setRoleList(new ArrayList<>());
    result.setUserInfo(tUserResp);
    result.setRoleInfo(new HashMap<>());

    // User authentication permission without role
    authorityFetcher
        .getAuthorCache()
        .putUserCache(user.getName(), authorityFetcher.cacheUser(result));
    User user1 = authorityFetcher.getAuthorCache().getUserCache(user.getName());
    assert user1 != null;
    Assert.assertEquals(user.getName(), user1.getName());
    Assert.assertEquals(user.getPassword(), user1.getPassword());
    Assert.assertEquals(user.getPrivilegeList(), user1.getPrivilegeList());

    // User has permission
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorityFetcher
            .checkUserPrivileges(
                "user", Collections.singletonList("root.ln"), PrivilegeType.CREATE_ROLE.ordinal())
            .getCode());
    // User does not have permission
    Assert.assertEquals(
        TSStatusCode.NO_PERMISSION.getStatusCode(),
        authorityFetcher
            .checkUserPrivileges(
                "user", Collections.singletonList("root.ln"), PrivilegeType.CREATE_USER.ordinal())
            .getCode());

    // Authenticate users with roles
    authorityFetcher.getAuthorCache().invalidateCache(user.getName(), "");
    tUserResp.setPrivilegeList(new ArrayList<>());
    tUserResp.setRoleList(user.getRoleList());

    // Permission information for roles owned by users
    for (Role role : roleList1) {
      TRoleResp tRoleResp = new TRoleResp();
      rolePrivilegeList = new ArrayList<>();
      tRoleResp.setRoleName(role.getName());
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        rolePrivilegeList.add(pathPrivilege.getPath());
        String privilegeIdList = pathPrivilege.getPrivileges().toString();
        rolePrivilegeList.add(privilegeIdList.substring(1, privilegeIdList.length() - 1));
      }
      tRoleResp.setPrivilegeList(rolePrivilegeList);
      tRoleRespMap.put(role.getName(), tRoleResp);
    }
    result.setRoleInfo(tRoleRespMap);
    authorityFetcher
        .getAuthorCache()
        .putUserCache(user.getName(), authorityFetcher.cacheUser(result));
    Role role3 = authorityFetcher.getAuthorCache().getRoleCache(role1.getName());
    Assert.assertEquals(role1.getName(), role3.getName());
    Assert.assertEquals(role1.getPrivilegeList(), role3.getPrivilegeList());

    // role has permission
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        authorityFetcher
            .checkUserPrivileges(
                "user", Collections.singletonList("root.ln"), PrivilegeType.CREATE_ROLE.ordinal())
            .getCode());
    // role does not have permission
    Assert.assertEquals(
        TSStatusCode.NO_PERMISSION.getStatusCode(),
        authorityFetcher
            .checkUserPrivileges(
                "user", Collections.singletonList("root.ln"), PrivilegeType.CREATE_USER.ordinal())
            .getCode());

    authorityFetcher.getAuthorCache().invalidateCache(user.getName(), "");

    user1 = authorityFetcher.getAuthorCache().getUserCache(user.getName());
    role1 = authorityFetcher.getAuthorCache().getRoleCache(role1.getName());

    Assert.assertNull(user1);
    Assert.assertNull(role1);
  }
}
