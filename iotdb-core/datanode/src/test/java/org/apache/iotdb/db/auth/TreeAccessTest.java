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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TreeAccessTest {

  @Before
  public void setup() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @After
  public void teardown() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @Test
  public void test1() {
    User mockUser = Mockito.mock(User.class);
    Mockito.when(mockUser.getName()).thenReturn("user1");
    Mockito.when(mockUser.getUserId()).thenReturn(10000L);
    Mockito.when(mockUser.checkSysPriGrantOpt(PrivilegeType.SYSTEM)).thenReturn(false);
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(mockUser.getName(), mockUser);
    User mockUser2 = Mockito.mock(User.class);
    Mockito.when(mockUser2.getName()).thenReturn("user2");
    Mockito.when(mockUser2.getUserId()).thenReturn(10001L);
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(mockUser.getName(), mockUser);
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(mockUser2.getName(), mockUser2);
    TreeAccessCheckVisitor treeAccessCheckVisitor = new TreeAccessCheckVisitor();

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_USER);
    authorStatement.setPrivilegeList(new String[] {"SYSTEM"});
    authorStatement.setUserName("user2");
    authorStatement.setGrantOpt(true);
    Assert.assertEquals(
        TSStatusCode.NO_PERMISSION.getStatusCode(),
        treeAccessCheckVisitor
            .visitAuthor(authorStatement, new TreeAccessCheckContext(10000L, "user1", ""))
            .getCode());
    Mockito.when(mockUser.checkSysPriGrantOpt(PrivilegeType.SYSTEM)).thenReturn(true);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        treeAccessCheckVisitor
            .visitAuthor(authorStatement, new TreeAccessCheckContext(10000L, "user1", ""))
            .getCode());
  }
}
