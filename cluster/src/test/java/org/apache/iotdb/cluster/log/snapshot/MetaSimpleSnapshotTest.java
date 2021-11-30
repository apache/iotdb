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
package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.CreateTemplatePlanUtil;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetaSimpleSnapshotTest extends IoTDBTest {

  private MetaGroupMember metaGroupMember;
  private boolean subServerInitialized;

  @Override
  @Before
  public void setUp()
      throws org.apache.iotdb.db.exception.StartupException,
          org.apache.iotdb.db.exception.query.QueryProcessException, IllegalPathException {
    super.setUp();
    subServerInitialized = false;
    metaGroupMember =
        new TestMetaGroupMember() {
          @Override
          protected void rebuildDataGroups() {
            subServerInitialized = true;
          }
        };
    metaGroupMember.setCoordinator(new Coordinator());
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    metaGroupMember.stop();
    metaGroupMember.closeLogManager();
    super.tearDown();
  }

  @Test
  public void testSerialize() {
    try {
      Map<PartialPath, Long> storageGroupTTLMap = new HashMap<>();
      Map<String, User> userMap = new HashMap<>();
      Map<String, Role> roleMap = new HashMap<>();
      Map<String, Template> templateMap = new HashMap<>();
      PartitionTable partitionTable = TestUtils.getPartitionTable(10);
      long lastLogIndex = 10;
      long lastLogTerm = 5;

      for (int i = 0; i < 10; i++) {
        PartialPath partialPath = new PartialPath("root.ln.sg1");
        storageGroupTTLMap.put(partialPath, (long) i);
      }

      for (int i = 0; i < 5; i++) {
        String userName = "user_" + i;
        User user = new User(userName, "password_" + i);
        userMap.put(userName, user);
      }

      for (int i = 0; i < 10; i++) {
        String roleName = "role_" + i;
        Role role = new Role(roleName);
        roleMap.put(roleName, role);
      }

      CreateTemplatePlan createTemplatePlan = CreateTemplatePlanUtil.getCreateTemplatePlan();

      for (int i = 0; i < 10; i++) {
        String templateName = "template_" + i;
        Template template = new Template(createTemplatePlan);
        templateMap.put(templateName, template);
      }

      MetaSimpleSnapshot metaSimpleSnapshot =
          new MetaSimpleSnapshot(
              storageGroupTTLMap, userMap, roleMap, templateMap, partitionTable.serialize());

      metaSimpleSnapshot.setLastLogIndex(lastLogIndex);
      metaSimpleSnapshot.setLastLogTerm(lastLogTerm);

      ByteBuffer buffer = metaSimpleSnapshot.serialize();

      MetaSimpleSnapshot newSnapshot = new MetaSimpleSnapshot();
      newSnapshot.deserialize(buffer);

      assertEquals(storageGroupTTLMap, newSnapshot.getStorageGroupTTLMap());
      assertEquals(userMap, newSnapshot.getUserMap());
      assertEquals(roleMap, newSnapshot.getRoleMap());
      assertEquals(templateMap, newSnapshot.getTemplateMap());

      assertEquals(partitionTable.serialize(), newSnapshot.getPartitionTableBuffer());
      assertEquals(lastLogIndex, newSnapshot.getLastLogIndex());
      assertEquals(lastLogTerm, newSnapshot.getLastLogTerm());

      assertEquals(metaSimpleSnapshot, newSnapshot);

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testInstall()
      throws IllegalPathException, SnapshotInstallationException, AuthException {
    Map<PartialPath, Long> storageGroupTTLMap = new HashMap<>();
    Map<String, User> userMap = new HashMap<>();
    Map<String, Role> roleMap = new HashMap<>();
    Map<String, Template> templateMap = new HashMap<>();
    PartitionTable partitionTable = TestUtils.getPartitionTable(10);
    long lastLogIndex = 10;
    long lastLogTerm = 5;

    for (int i = 0; i < 10; i++) {
      PartialPath partialPath = new PartialPath("root.ln.sg" + i);
      storageGroupTTLMap.put(partialPath, (long) i);
    }

    for (int i = 0; i < 5; i++) {
      String userName = "user_" + i;
      User user = new User(userName, "password_" + i);
      userMap.put(userName, user);
    }

    for (int i = 0; i < 10; i++) {
      String roleName = "role_" + i;
      Role role = new Role(roleName);
      roleMap.put(roleName, role);
    }

    CreateTemplatePlan createTemplatePlan = CreateTemplatePlanUtil.getCreateTemplatePlan();

    for (int i = 0; i < 10; i++) {
      String templateName = "template_" + i;
      createTemplatePlan.setName(templateName);
      Template template = new Template(createTemplatePlan);
      templateMap.put(templateName, template);
    }

    MetaSimpleSnapshot metaSimpleSnapshot =
        new MetaSimpleSnapshot(
            storageGroupTTLMap, userMap, roleMap, templateMap, partitionTable.serialize());
    metaSimpleSnapshot.setLastLogIndex(lastLogIndex);
    metaSimpleSnapshot.setLastLogTerm(lastLogTerm);

    SnapshotInstaller defaultInstaller = metaSimpleSnapshot.getDefaultInstaller(metaGroupMember);
    defaultInstaller.install(metaSimpleSnapshot, -1, false);

    Map<PartialPath, Long> storageGroupsTTL = IoTDB.metaManager.getStorageGroupsTTL();
    for (int i = 0; i < 10; i++) {
      PartialPath partialPath = new PartialPath("root.ln.sg" + i);
      assertEquals(i, (long) storageGroupsTTL.get(partialPath));
    }

    for (int i = 0; i < 5; i++) {
      String userName = "user_" + i;
      User user = BasicAuthorizer.getInstance().getUser(userName);
      assertEquals(userMap.get(userName), user);
    }

    for (int i = 0; i < 10; i++) {
      String roleName = "role_" + i;
      Role role = BasicAuthorizer.getInstance().getRole(roleName);
      assertEquals(roleMap.get(roleName), role);
    }

    for (int i = 0; i < 10; i++) {
      String templateName = "template_" + i;
      try {
        Template template = TemplateManager.getInstance().getTemplate(templateName);
        assertEquals(templateMap.get(templateName), template);
      } catch (UndefinedTemplateException e) {
        fail();
      }
    }

    assertEquals(partitionTable, metaGroupMember.getPartitionTable());
    assertEquals(lastLogIndex, metaGroupMember.getLogManager().getLastLogIndex());
    assertEquals(lastLogTerm, metaGroupMember.getLogManager().getLastLogTerm());
    assertTrue(subServerInitialized);
  }
}
