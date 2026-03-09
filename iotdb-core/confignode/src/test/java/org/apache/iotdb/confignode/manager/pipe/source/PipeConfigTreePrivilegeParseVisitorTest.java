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
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterEncodingCompressorPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

public class PipeConfigTreePrivilegeParseVisitorTest {
  private static final UserEntity FAKE_USER_ENTITY = new UserEntity(0L, "", "");
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
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", FAKE_USER_ENTITY, true));

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.READ_SCHEMA
                && privilegeUnion.getPaths().stream().allMatch(path -> path.equals("root.db")));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", FAKE_USER_ENTITY, true));
    Assert.assertFalse(skipVisitor.canReadSysSchema("root.db", FAKE_USER_ENTITY, false));
    Assert.assertFalse(
        throwVisitor
            .visitCreateDatabase(
                new DatabaseSchemaPlan(
                    ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1")),
                FAKE_USER_ENTITY)
            .isPresent());

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.READ_SCHEMA
                && privilegeUnion.getPaths().stream().allMatch(path -> path.equals("root.db.**")));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", FAKE_USER_ENTITY, true));
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", FAKE_USER_ENTITY, false));
  }

  @Test
  public void testAuthPrivilege() {
    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.MANAGE_USER);
    Assert.assertTrue(
        skipVisitor
            .visitGrantUser(new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitRevokeUser(
                new AuthorTreePlan(ConfigPhysicalPlanType.RevokeUser), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitGrantRole(new AuthorTreePlan(ConfigPhysicalPlanType.GrantRole), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitRevokeRole(
                new AuthorTreePlan(ConfigPhysicalPlanType.RevokeRole), FAKE_USER_ENTITY)
            .isPresent());

    permissionManager.setUserPrivilege((userName, privilegeUnion) -> false);
    AuthorTreePlan plan = new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser);
    plan.setUserName("");
    Assert.assertTrue(skipVisitor.visitGrantUser(plan, FAKE_USER_ENTITY).isPresent());
    Assert.assertTrue(skipVisitor.visitRevokeUser(plan, FAKE_USER_ENTITY).isPresent());
    plan.setUserName("another");
    Assert.assertFalse(skipVisitor.visitGrantUser(plan, FAKE_USER_ENTITY).isPresent());
    Assert.assertFalse(skipVisitor.visitRevokeUser(plan, FAKE_USER_ENTITY).isPresent());

    permissionManager.setUserPrivilege(
        (userName, privilegeUnion) ->
            privilegeUnion.getPrivilegeType() == PrivilegeType.MANAGE_ROLE);
    Assert.assertFalse(
        skipVisitor
            .visitGrantUser(new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertFalse(
        skipVisitor
            .visitRevokeUser(
                new AuthorTreePlan(ConfigPhysicalPlanType.RevokeUser), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitGrantRole(new AuthorTreePlan(ConfigPhysicalPlanType.GrantRole), FAKE_USER_ENTITY)
            .isPresent());
    Assert.assertTrue(
        skipVisitor
            .visitRevokeRole(
                new AuthorTreePlan(ConfigPhysicalPlanType.RevokeRole), FAKE_USER_ENTITY)
            .isPresent());

    permissionManager.setUserPrivilege((userName, privilegeUnion) -> false);
    plan = new AuthorTreePlan(ConfigPhysicalPlanType.GrantUser);
    plan.setRoleName("");
    Assert.assertTrue(skipVisitor.visitGrantRole(plan, FAKE_USER_ENTITY).isPresent());
    Assert.assertTrue(skipVisitor.visitRevokeRole(plan, FAKE_USER_ENTITY).isPresent());
    plan.setRoleName("another");
    Assert.assertFalse(skipVisitor.visitGrantRole(plan, FAKE_USER_ENTITY).isPresent());
    Assert.assertFalse(skipVisitor.visitRevokeRole(plan, FAKE_USER_ENTITY).isPresent());
  }

  @Test
  public void testPatternRelatedPrivilege() throws IOException {
    final PartialPath matchedPath =
        new PartialPath(new String[] {"root", "db", "device", "measurement"});
    final PartialPath unmatchedPath =
        new PartialPath(new String[] {"root", "db", "device2", "measurement"});
    final PartialPath intersectPath =
        new PartialPath(new String[] {"root", "*", "device", "measurement"});

    final PathPatternTree originalTree = new PathPatternTree();
    originalTree.appendPathPattern(matchedPath);
    originalTree.appendPathPattern(unmatchedPath);
    originalTree.constructTree();
    final ByteBuffer buffer = originalTree.serialize();

    Assert.assertEquals(
        Collections.singletonList(matchedPath),
        PathPatternTree.deserialize(
                ((PipeDeleteTimeSeriesPlan)
                        skipVisitor
                            .visitPipeDeleteTimeSeries(
                                new PipeDeleteTimeSeriesPlan(buffer), FAKE_USER_ENTITY)
                            .get())
                    .getPatternTreeBytes())
            .getAllPathPatterns());
    Assert.assertEquals(
        Collections.singletonList(matchedPath),
        PathPatternTree.deserialize(
                ((PipeDeleteLogicalViewPlan)
                        skipVisitor
                            .visitPipeDeleteLogicalView(
                                new PipeDeleteLogicalViewPlan(buffer), FAKE_USER_ENTITY)
                            .get())
                    .getPatternTreeBytes())
            .getAllPathPatterns());
    Assert.assertEquals(
        Collections.singletonList(matchedPath),
        PathPatternTree.deserialize(
                ((PipeAlterEncodingCompressorPlan)
                        skipVisitor
                            .visitPipeAlterEncodingCompressor(
                                new PipeAlterEncodingCompressorPlan(
                                    buffer, (byte) 0, (byte) 0, false),
                                FAKE_USER_ENTITY)
                            .get())
                    .getPatternTreeBytes())
            .getAllPathPatterns());
    Assert.assertThrows(
        AccessDeniedException.class,
        () ->
            throwVisitor.visitPipeDeleteTimeSeries(
                new PipeDeleteTimeSeriesPlan(buffer), FAKE_USER_ENTITY));
    Assert.assertThrows(
        AccessDeniedException.class,
        () ->
            throwVisitor.visitPipeDeleteLogicalView(
                new PipeDeleteLogicalViewPlan(buffer), FAKE_USER_ENTITY));
    Assert.assertThrows(
        AccessDeniedException.class,
        () ->
            throwVisitor.visitPipeAlterEncodingCompressor(
                new PipeAlterEncodingCompressorPlan(buffer, (byte) 0, (byte) 0, false),
                FAKE_USER_ENTITY));

    Assert.assertEquals(
        Collections.singleton(matchedPath),
        ((PipeDeactivateTemplatePlan)
                skipVisitor
                    .visitPipeDeactivateTemplate(
                        new PipeDeactivateTemplatePlan(
                            new HashMap<PartialPath, List<Template>>() {
                              {
                                put(intersectPath, Collections.singletonList(new Template()));
                                put(unmatchedPath, Collections.singletonList(new Template()));
                              }
                            }),
                        FAKE_USER_ENTITY)
                    .get())
            .getTemplateSetInfo()
            .keySet());

    Assert.assertArrayEquals(
        new String[] {"root", "db", "device", "measurement"},
        ((SetTTLPlan)
                skipVisitor
                    .visitTTL(
                        new SetTTLPlan(new String[] {"root", "*", "device", "measurement"}, 100),
                        FAKE_USER_ENTITY)
                    .get())
            .getPathPattern());
    Assert.assertFalse(
        skipVisitor
            .visitTTL(
                new SetTTLPlan(new String[] {"root", "db2", "device", "measurement"}, 100),
                FAKE_USER_ENTITY)
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

    private void setUserPrivilege(
        final BiFunction<String, PrivilegeUnion, Boolean> checkUserPrivileges) {
      this.checkUserPrivileges = checkUserPrivileges;
    }

    @Override
    public PathPatternTree fetchRawAuthorizedPTree(
        final String userName, final PrivilegeType type) {
      final PathPatternTree tree = new PathPatternTree();
      tree.appendPathPattern(new PartialPath(new String[] {"root", "db", "device", "**"}));
      tree.constructTree();
      return tree;
    }

    @Override
    public TPermissionInfoResp checkRoleOfUser(String username, String rolename) {
      return Objects.equals(username, rolename)
          ? new TPermissionInfoResp(StatusUtils.OK)
          : new TPermissionInfoResp(new TSStatus(TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode()));
    }
  }
}
