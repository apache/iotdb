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

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.User;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class UserTest {

  @Test
  public void testUser() {
    User user = new User("user", "password");
    PathPrivilege pathPrivilege = new PathPrivilege("root.ln");
    user.setPrivilegeList(Collections.singletonList(pathPrivilege));
    user.setPrivileges("root.ln", Collections.singleton(1));
    Assert.assertEquals(
        "User{name='user', password='password', privilegeList=[root.ln : INSERT_TIMESERIES], roleList=[], isOpenIdUser=false, useWaterMark=false, lastActiveTime=0}",
        user.toString());
    User user1 = new User("user1", "password1");
    user1.deserialize(user.serialize());
    Assert.assertEquals(
        "User{name='user', password='password', privilegeList=[root.ln : INSERT_TIMESERIES], roleList=[], isOpenIdUser=false, useWaterMark=false, lastActiveTime=0}",
        user1.toString());
  }
}
