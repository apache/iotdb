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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;

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
  public void permissionCacheTest() throws IllegalPathException {
    User user = new User();
    Role role1 = new Role();
    Role role2 = new Role();
    List<String> roleList = new ArrayList<>();
    Set<Integer> privilegesIds = new HashSet<>();
    Set<Integer> sysPriIds = new HashSet<>();
    PathPrivilege privilege = new PathPrivilege();
    List<PathPrivilege> privilegeList = new ArrayList<>();
    privilegesIds.add(PrivilegeType.READ_DATA.ordinal());
    sysPriIds.add(PrivilegeType.MAINTAIN.ordinal());

    privilege.setPath(new PartialPath("root.ln"));
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
    user.setSysPrivilegeSet(sysPriIds);
    user.setRoleList(roleList);
    TPermissionInfoResp result = new TPermissionInfoResp();
    TUserResp tUserResp = new TUserResp();
    Map<String, TRoleResp> tRoleRespMap = new HashMap();
    List<TPathPrivilege> userPrivilegeList = new ArrayList<>();
    List<TPathPrivilege> rolePrivilegeList = new ArrayList<>();
    List<Role> roleList1 = new ArrayList<>();
    roleList1.add(role1);
    roleList1.add(role2);

    // User permission information
    for (PathPrivilege pathPrivilege : user.getPathPrivilegeList()) {
      TPathPrivilege pathPri = new TPathPrivilege();
      pathPri.setPath(pathPrivilege.getPath().getFullPath());
      pathPri.setPriSet(pathPrivilege.getPrivileges());
      pathPri.setPriGrantOpt(pathPrivilege.getGrantOpt());
      userPrivilegeList.add(pathPri);
    }
    tUserResp.setUsername(user.getName());
    tUserResp.setPassword(user.getPassword());
    tUserResp.setPrivilegeList(userPrivilegeList);
    tUserResp.setSysPriSet(user.getSysPrivilege());

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
    Assert.assertEquals(user.getPathPrivilegeList(), user1.getPathPrivilegeList());
    Assert.assertEquals(user.getSysPrivilege(), user1.getSysPrivilege());

    // User has permission
    Assert.assertEquals(
        authorityFetcher
            .checkUserPathPrivileges(
                "user",
                Collections.singletonList(new PartialPath("root.ln")),
                PrivilegeType.READ_DATA.ordinal())
            .size(),
        0);
    // User does not have permission
    Assert.assertEquals(
        authorityFetcher
            .checkUserPathPrivileges(
                "user",
                Collections.singletonList(new PartialPath("root.ln")),
                PrivilegeType.WRITE_DATA.ordinal())
            .size(),
        1);

    // Authenticate users with roles
    authorityFetcher.getAuthorCache().invalidateCache(user.getName(), "");
    tUserResp.setPrivilegeList(new ArrayList<>());
    tUserResp.setRoleList(user.getRoleList());

    // Permission information for roles owned by users
    for (Role role : roleList1) {
      TRoleResp tRoleResp = new TRoleResp();
      rolePrivilegeList = new ArrayList<>();
      tRoleResp.setRoleName(role.getName());
      for (PathPrivilege pathPrivilege : role.getPathPrivilegeList()) {
        TPathPrivilege pathPri = new TPathPrivilege();
        pathPri.setPath(pathPrivilege.getPath().getFullPath());
        pathPri.setPriSet(pathPrivilege.getPrivileges());
        pathPri.setPriGrantOpt(pathPrivilege.getGrantOpt());
        rolePrivilegeList.add(pathPri);
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
    Assert.assertEquals(role1.getPathPrivilegeList(), role3.getPathPrivilegeList());

    // role has permission
    Assert.assertEquals(
        authorityFetcher
            .checkUserPathPrivileges(
                "user",
                Collections.singletonList(new PartialPath("root.ln")),
                PrivilegeType.READ_DATA.ordinal())
            .size(),
        0);
    // role does not have permission
    Assert.assertEquals(
        authorityFetcher
            .checkUserPathPrivileges(
                "user",
                Collections.singletonList(new PartialPath("root.ln")),
                PrivilegeType.MANAGE_USER.ordinal())
            .size(),
        1);

    authorityFetcher.getAuthorCache().invalidateCache(user.getName(), "");

    user1 = authorityFetcher.getAuthorCache().getUserCache(user.getName());
    role1 = authorityFetcher.getAuthorCache().getRoleCache(role1.getName());

    Assert.assertNull(user1);
    Assert.assertNull(role1);
  }

  @Test
  public void grantOptTest() throws IllegalPathException {
    User user = new User();
    Role role = new Role();

    Set<Integer> sysPri = new HashSet<>();
    sysPri.add(PrivilegeType.MANAGE_DATABASE.ordinal());
    sysPri.add(PrivilegeType.USE_PIPE.ordinal());
    user.setSysPrivilegeSet(sysPri);

    Set<Integer> sysGrantOpt = new HashSet<>();
    sysGrantOpt.add(PrivilegeType.USE_PIPE.ordinal());
    user.setSysPriGrantOpt(sysGrantOpt);

    List<PathPrivilege> pathList = new ArrayList<>();
    PartialPath pathRoot = new PartialPath("root.**");
    PartialPath path1 = new PartialPath("root.d1.**");
    PathPrivilege priv1 = new PathPrivilege(path1);
    priv1.grantPrivilege(PrivilegeType.READ_DATA.ordinal(), false);
    priv1.grantPrivilege(PrivilegeType.WRITE_SCHEMA.ordinal(), true);
    pathList.add(priv1);

    user.setPrivilegeList(pathList);

    user.setName("user1");
    user.setPassword("123456");

    // user's priv:
    // 1. MANAGE_DATABASE
    // 2. USE_PIPE with grant option
    // 3. READ_DATA root.d1.**
    // 4. WRITE_SCHEMA root.d1.** with grant option

    // role's priv:
    // 1. USE_UDF
    // 2. USE_CQ with grant option
    // 3. READ_DATA root.t9.** with grant option

    role.setName("role1");
    Set<Integer> sysPriRole = new HashSet<>();
    sysPriRole.add(PrivilegeType.USE_UDF.ordinal());
    sysPriRole.add(PrivilegeType.USE_CQ.ordinal());
    role.setSysPrivilegeSet(sysPriRole);

    Set<Integer> sysGrantOptRole = new HashSet<>();
    sysGrantOptRole.add(PrivilegeType.USE_CQ.ordinal());
    role.setSysPriGrantOpt(sysGrantOptRole);

    PathPrivilege privRole = new PathPrivilege(new PartialPath("root.t9.**"));
    privRole.grantPrivilege(PrivilegeType.READ_DATA.ordinal(), true);
    role.setPrivilegeList(Collections.singletonList(privRole));
    user.setRoleList(Collections.singletonList("role1"));

    TPermissionInfoResp result = new TPermissionInfoResp();
    TUserResp tUserResp = new TUserResp();
    List<TPathPrivilege> userPrivilegeList = new ArrayList<>();
    Map<String, TRoleResp> tRoleRespMap = new HashMap();
    // User permission information
    for (PathPrivilege pathPrivilege : user.getPathPrivilegeList()) {
      TPathPrivilege pathPri = new TPathPrivilege();
      pathPri.setPath(pathPrivilege.getPath().getFullPath());
      pathPri.setPriSet(pathPrivilege.getPrivileges());
      pathPri.setPriGrantOpt(pathPrivilege.getGrantOpt());
      userPrivilegeList.add(pathPri);
    }
    tUserResp.setUsername(user.getName());
    tUserResp.setPassword(user.getPassword());
    tUserResp.setPrivilegeList(userPrivilegeList);
    tUserResp.setSysPriSet(user.getSysPrivilege());
    tUserResp.setSysPriSetGrantOpt(user.getSysPriGrantOpt());
    tUserResp.setRoleList(user.getRoleList());
    result.setUserInfo(tUserResp);
    result.setRoleInfo(new HashMap<>());

    TRoleResp tRoleResp = new TRoleResp();
    tRoleResp.setRoleName(role.getName());
    List<TPathPrivilege> rolePrivilegeList = new ArrayList<>();
    for (PathPrivilege pathPrivilege : role.getPathPrivilegeList()) {
      TPathPrivilege pathPri = new TPathPrivilege();
      pathPri.setPath(pathPrivilege.getPath().getFullPath());
      pathPri.setPriSet(pathPrivilege.getPrivileges());
      pathPri.setPriGrantOpt(pathPrivilege.getGrantOpt());
      rolePrivilegeList.add(pathPri);
    }
    tRoleResp.setPrivilegeList(rolePrivilegeList);
    tRoleResp.setSysPriSetGrantOpt(role.getSysPriGrantOpt());
    tRoleResp.setSysPriSet(role.getSysPrivilege());
    tRoleRespMap.put(role.getName(), tRoleResp);
    result.setRoleInfo(tRoleRespMap);

    authorityFetcher.getAuthorCache().putUserCache("user1", authorityFetcher.cacheUser(result));

    // for system priv. we have USE_PIPE grant option.
    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(pathRoot), PrivilegeType.USE_PIPE.ordinal()));
    Assert.assertFalse(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(pathRoot), PrivilegeType.MANAGE_USER.ordinal()));

    // for path priv. we have write_schema on root.d1.** with grant option.
    // require root.d1.** with write_schema, return true
    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(path1), PrivilegeType.WRITE_SCHEMA.ordinal()));
    // require root.** with write_schema, return false
    Assert.assertFalse(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(pathRoot), PrivilegeType.WRITE_SCHEMA.ordinal()));
    // reuqire root.d1.d2 with write_schema, return true
    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1",
            Collections.singletonList(new PartialPath(new String("root.d1.d2"))),
            PrivilegeType.WRITE_SCHEMA.ordinal()));

    // require root.d1.d2 with read_schema, return false
    Assert.assertFalse(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1",
            Collections.singletonList(new PartialPath(new String("root.d1.d2"))),
            PrivilegeType.READ_SCHEMA.ordinal()));

    // role test
    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1",
            Collections.singletonList(new PartialPath(new String("root.t9.**"))),
            PrivilegeType.READ_DATA.ordinal()));

    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1",
            Collections.singletonList(new PartialPath(new String("root.t9.t10"))),
            PrivilegeType.READ_DATA.ordinal()));

    Assert.assertFalse(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1",
            Collections.singletonList(new PartialPath(new String("root.t9.**"))),
            PrivilegeType.WRITE_DATA.ordinal()));
    Assert.assertFalse(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(pathRoot), PrivilegeType.USE_TRIGGER.ordinal()));
    Assert.assertTrue(
        authorityFetcher.checkUserPrivilegeGrantOpt(
            "user1", Collections.singletonList(pathRoot), PrivilegeType.USE_CQ.ordinal()));
  }
}
