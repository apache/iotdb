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
package org.apache.iotdb.db.auth.entity;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RoleTest {

  @Test
  public void testRole() {
    Role role = new Role("role");
    PathPrivilege pathPrivilege = new PathPrivilege("root.ln");
    role.setPrivilegeList(Collections.singletonList(pathPrivilege));
    role.setPrivileges("root.ln", Collections.singleton(1));
    Assert.assertEquals(
        "Role{name='role', privilegeList=[root.ln : INSERT_TIMESERIES]}", role.toString());
    Role role1 = new Role("role1");
    role1.deserialize(role.serialize());
    Assert.assertEquals(
        "Role{name='role', privilegeList=[root.ln : INSERT_TIMESERIES]}", role1.toString());
  }
}
