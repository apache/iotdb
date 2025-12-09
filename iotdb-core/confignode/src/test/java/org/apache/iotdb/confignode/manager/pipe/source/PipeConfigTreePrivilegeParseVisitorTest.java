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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.function.BiFunction;

public class PipeConfigTreePrivilegeParseVisitorTest {

  private final PipeConfigTreePrivilegeParseVisitor skipVisitor =
      new PipeConfigTreePrivilegeParseVisitor(true);
  private final PipeConfigTreePrivilegeParseVisitor throwVisitor =
      new PipeConfigTreePrivilegeParseVisitor(false);

  private ConfigNode oldInstance;
  private final TestPermissionManager permissionManager = new TestPermissionManager();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    oldInstance = ConfigNode.getInstance();
    ConfigNode.setInstance(new ConfigNode());
    final ConfigManager configManager = new ConfigManager();
    configManager.setPermissionManager(permissionManager);
    ConfigNode.getInstance().setConfigManager(configManager);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    ConfigNode.setInstance(oldInstance);
  }

  @Test
  public void testCanReadSysSchema() {
    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) -> privilegeUnion.getPrivilegeType() == PrivilegeType.SYSTEM);
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", null, true));

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.READ_SCHEMA
                && privilegeUnion.getPaths().stream().allMatch(path -> path.equals("root.db")));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", null, true));
    Assert.assertFalse(skipVisitor.canReadSysSchema("root.db", null, false));
    Assert.assertFalse(
        throwVisitor
            .visitCreateDatabase(
                new DatabaseSchemaPlan(
                    ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1")),
                null)
            .isPresent());

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.READ_SCHEMA
                && privilegeUnion.getPaths().stream().allMatch(path -> path.equals("root.db.**")));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", null, true));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", null, false));
  }

  @Test
  public void testAuthPrivilege() {
    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.MANAGE_USER);
    Assert.assertTrue(
        skipVisitor
            .visitGrantUser(new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser), null)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitRevokeUser(new AuthorTreePlan(ConfigPhysicalPlanType.RevokeUser), null)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitGrantRole(new AuthorTreePlan(ConfigPhysicalPlanType.GrantRole), null)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitRevokeRole(new AuthorTreePlan(ConfigPhysicalPlanType.RevokeRole), null)
            .isPresent());

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.MANAGE_ROLE);
    Assert.assertFalse(
        skipVisitor
            .visitGrantUser(new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser), null)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitRevokeUser(new AuthorTreePlan(ConfigPhysicalPlanType.RevokeUser), null)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitGrantRole(new AuthorTreePlan(ConfigPhysicalPlanType.GrantRole), null)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitRevokeRole(new AuthorTreePlan(ConfigPhysicalPlanType.RevokeRole), null)
            .isPresent());
  }

  private static class TestPermissionManager extends PermissionManager {

    private BiFunction<String, PrivilegeUnion, Boolean> checkUserPrivileges =
        (userName, privilegeUnion) -> true;

    public TestPermissionManager() {
      super(null, null);
    }

    @Override
    public TPermissionInfoResp checkUserPrivileges(
        final String username, final PrivilegeUnion union) {
      return checkUserPrivileges.apply(username, union)
          ? new TPermissionInfoResp(StatusUtils.OK)
          : new TPermissionInfoResp(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()));
    }

    void setUserPrivilege(final BiFunction<String, PrivilegeUnion, Boolean> checkUserPrivileges) {
      this.checkUserPrivileges = checkUserPrivileges;
    }
  }
}
