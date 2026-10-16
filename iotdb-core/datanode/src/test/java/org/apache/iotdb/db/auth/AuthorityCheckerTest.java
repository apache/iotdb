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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class AuthorityCheckerTest {

  @Before
  public void setup() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @After
  public void teardown() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @Test
  public void testLogReduce() throws IllegalPathException {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int oldSize = config.getPathLogMaxSize();
    config.setPathLogMaxSize(1);
    Assert.assertEquals(
        "No permissions for this operation, please add privilege WRITE_DATA on [root.db.device.s1, ...]",
        AuthorityChecker.getTSStatus(
                Arrays.asList(0, 1),
                Arrays.asList(
                    new MeasurementPath("root.db.device.s1"),
                    new MeasurementPath("root.db.device.s2")),
                PrivilegeType.WRITE_DATA)
            .getMessage());
    config.setPathLogMaxSize(oldSize);
  }

  @Test
  public void testCountTimeSeriesExplicitSystemDatabasePermission() throws Exception {
    User user = new User("user1", "password");
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(user.getName(), user);

    CountTimeSeriesStatement systemStatement =
        new CountTimeSeriesStatement(new PartialPath("root.__system.**"));
    Assert.assertEquals(
        TSStatusCode.NO_PERMISSION.getStatusCode(),
        systemStatement.checkPermissionBeforeProcess(user.getName()).getCode());

    user.addSysPrivilege(PrivilegeType.MAINTAIN.ordinal());
    systemStatement = new CountTimeSeriesStatement(new PartialPath("root.__system.**"));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        systemStatement.checkPermissionBeforeProcess(user.getName()).getCode());
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.__system.**")),
        systemStatement.getAuthorityScope().getAllPathPatterns());
    Assert.assertTrue(systemStatement.isCanSeeSystemDB());

    CountLevelTimeSeriesStatement systemLevelStatement =
        new CountLevelTimeSeriesStatement(new PartialPath("root.__system.**"), 1);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        systemLevelStatement.checkPermissionBeforeProcess(user.getName()).getCode());
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.__system.**")),
        systemLevelStatement.getAuthorityScope().getAllPathPatterns());
    Assert.assertTrue(systemLevelStatement.isCanSeeSystemDB());
  }

  @Test
  public void testCountTimeSeriesImplicitSystemDatabasePermission() throws Exception {
    User user = new User("user2", "password");
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(user.getName(), user);

    CountTimeSeriesStatement statement = new CountTimeSeriesStatement(new PartialPath("root.**"));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        statement.checkPermissionBeforeProcess(user.getName()).getCode());
    Assert.assertFalse(statement.isCanSeeSystemDB());

    user.addSysPrivilege(PrivilegeType.MAINTAIN.ordinal());
    statement = new CountTimeSeriesStatement(new PartialPath("root.**"));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        statement.checkPermissionBeforeProcess(user.getName()).getCode());
    Assert.assertTrue(statement.isCanSeeSystemDB());
    Assert.assertTrue(
        statement
            .getAuthorityScope()
            .getAllPathPatterns()
            .contains(new PartialPath("root.__system.**")));
  }
}
