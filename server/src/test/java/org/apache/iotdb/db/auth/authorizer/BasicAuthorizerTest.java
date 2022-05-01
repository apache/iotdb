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
package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.role.BasicRoleManager;
import org.apache.iotdb.db.auth.role.IRoleManager;
import org.apache.iotdb.db.auth.user.BasicUserManager;
import org.apache.iotdb.db.auth.user.IUserManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicAuthorizerTest {
  protected static final int DEFAULT_BUFFER_SIZE = 4096;

  IAuthorizer authorizer;
  String userName = "userName";
  String password = "userPwd";
  String roleName = "roleName";
  String nodeName = "root.ln";
  List<PathPrivilege> privilegeList = new ArrayList<>();
  List<String> roleList = new ArrayList<>();
  Set<Integer> privileges = new HashSet<>();
  int privilege = PrivilegeType.GRANT_ROLE_PRIVILEGE.ordinal();
  User user;
  Role role;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    authorizer = BasicAuthorizer.getInstance();
    privileges.add(privilege);
    privilegeList.add(new PathPrivilege(privileges, nodeName));
    role = new Role(roleName, privilegeList);
    roleList.add(roleName);
    user = new User(userName, password, privilegeList, roleList);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void BasicAuthorSerDeTest() throws AuthException {

    ByteBuffer bf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    IUserManager userManager = ((BasicAuthorizer) authorizer).getUserManager();
    IRoleManager roleManager = ((BasicAuthorizer) authorizer).getRoleManager();
    Map<String, User> userMap = ((BasicUserManager) userManager).getUserMap();
    Map<String, Role> roleMap = ((BasicRoleManager) roleManager).getRoleMap();
    userMap.put(userName, user);
    roleMap.put(roleName, role);

    ByteBuffer b = ((BasicAuthorizer) authorizer).serialize(bf);
    b.flip();
    ((BasicUserManager) userManager).setUserMap(new HashMap<>());
    ((BasicRoleManager) roleManager).setRoleMap(new HashMap<>());

    IAuthorizer iAuthorizer = ((BasicAuthorizer) authorizer).deserialize(b);
    IUserManager userManager1 = ((BasicAuthorizer) iAuthorizer).getUserManager();
    IRoleManager roleManager1 = ((BasicAuthorizer) iAuthorizer).getRoleManager();

    Assert.assertEquals(userManager, userManager1);
    Assert.assertEquals(roleManager, roleManager1);
  }
}
