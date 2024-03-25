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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.CNPhysicalPlanGenerator;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class CNPhysicalPlanGeneratorTest {
  private static AuthorInfo authorInfo;
  private static ClusterSchemaInfo clusterSchemaInfo;

  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "authorInfo-snapshot");
  private static final String USER_SNAPSHOT_FILE_NAME = "system" + File.separator + "users";
  private static final String ROLE_SNAPSHOT_FILE_NAME = "system" + File.separator + "roles";

  private static final String SCHEMA_INFO_FILE_NAME = "cluster_schema.bin";
  private static final String TEMPLATE_INFO_FILE_NAME = "template_info.bin";

  private static void setupAuthorInfo() {
    authorInfo = new AuthorInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdir();
    }
  }

  private static void setupClusterSchemaInfo() throws IOException {
    clusterSchemaInfo = new ClusterSchemaInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdir();
    }
  }

  @After
  public void cleanUpInfo() throws AuthException {
    if (authorInfo != null) {
      authorInfo.clear();
    }
    if (clusterSchemaInfo != null) {
      clusterSchemaInfo.clear();
    }
    FileUtils.deleteDirectory(snapshotDir);
  }

  @Test
  public void roleGeneratorTest() throws Exception {
    HashSet<Integer> answerSet = new HashSet<>();
    String roleName = "test1";
    setupAuthorInfo();
    AuthorPlan plan = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
    plan.setRoleName(roleName);
    answerSet.add(plan.hashCode());
    // step 1: create role - plan1
    authorInfo.authorNonQuery(plan);

    // step 2: grant role path privileges - plan2
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRole);
    plan.setRoleName(roleName);
    plan.setNodeNameList(Collections.singletonList(new PartialPath("root.db.t1")));
    Set<Integer> pathPris = new HashSet<>();
    pathPris.add(PrivilegeType.WRITE_DATA.ordinal());
    pathPris.add(PrivilegeType.WRITE_SCHEMA.ordinal());
    plan.setPermissions(pathPris);
    authorInfo.authorNonQuery(plan);

    // answer set
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.WRITE_DATA.ordinal());
    answerSet.add(plan.hashCode());
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.WRITE_SCHEMA.ordinal());
    answerSet.add(plan.hashCode());

    // step 3: grant role sys privileges - plan3
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRole);
    plan.setRoleName(roleName);
    plan.setNodeNameList(Collections.emptyList());
    Set<Integer> sysPris = new HashSet<>();
    sysPris.add(PrivilegeType.MANAGE_DATABASE.ordinal());
    sysPris.add(PrivilegeType.MANAGE_ROLE.ordinal());
    plan.setPermissions(sysPris);
    plan.setGrantOpt(true);
    authorInfo.authorNonQuery(plan);

    // answer set
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.MANAGE_ROLE.ordinal());
    answerSet.add(plan.hashCode());
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.MANAGE_DATABASE.ordinal());
    answerSet.add(plan.hashCode());

    // PhysicalPlan gnerator will return five plans:
    // 1. create role plan
    // 2. grant path privileges plan * 2
    // 3. grant system privileges plan * 2
    boolean success = authorInfo.processTakeSnapshot(snapshotDir);

    File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir
                + File.separator
                + ROLE_SNAPSHOT_FILE_NAME
                + File.separator
                + roleName
                + ".profile");

    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(roleProfile.toPath(), CNSnapshotFileType.ROLE);
    int count = 0;
    for (ConfigPhysicalPlan authPlan : planGenerator) {
      Assert.assertTrue(answerSet.contains(authPlan.hashCode()));
      count++;
    }
    Assert.assertEquals(5, count);
  }

  @Test
  public void userGeneratorTest() throws Exception {
    String userName = "test1";
    Set<Integer> answerSet = new HashSet<>();
    setupAuthorInfo();
    AuthorPlan plan = new AuthorPlan(ConfigPhysicalPlanType.CreateUser);
    plan.setPassword("password");
    plan.setUserName(userName);
    // create user plan 1
    authorInfo.authorNonQuery(plan);
    answerSet.add(plan.hashCode());

    plan = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
    plan.setRoleName("role1");
    authorInfo.authorNonQuery(plan);

    // grant path privileges, plan 2 , plan 3
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantUser);
    plan.setUserName(userName);
    plan.setNodeNameList(Collections.singletonList(new PartialPath("root.db1.t2")));
    Set<Integer> priSet = new HashSet<>();
    priSet.add(PrivilegeType.WRITE_SCHEMA.ordinal());
    priSet.add(PrivilegeType.READ_DATA.ordinal());
    plan.setPermissions(priSet);
    plan.setGrantOpt(true);
    authorInfo.authorNonQuery(plan);

    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.WRITE_SCHEMA.ordinal());
    answerSet.add(plan.hashCode());

    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.READ_DATA.ordinal());
    answerSet.add(plan.hashCode());

    // grant system privileges, plan 4
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantUser);
    plan.setUserName(userName);
    plan.setNodeNameList(Collections.emptyList());
    plan.setPermissions(Collections.singleton(PrivilegeType.MANAGE_DATABASE.ordinal()));
    plan.setGrantOpt(false);
    authorInfo.authorNonQuery(plan);
    answerSet.add(plan.hashCode());

    // grant role to user, plan 5
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRoleToUser);
    plan.setRoleName("role1");
    plan.setUserName(userName);
    authorInfo.authorNonQuery(plan);
    answerSet.add(plan.hashCode());

    boolean success = authorInfo.processTakeSnapshot(snapshotDir);

    File userProfile =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir
                + File.separator
                + USER_SNAPSHOT_FILE_NAME
                + File.separator
                + userName
                + ".profile");

    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(userProfile.toPath(), CNSnapshotFileType.USER);
    int count = 0;
    // plan 1-4
    for (ConfigPhysicalPlan authPlan : planGenerator) {
      Assert.assertTrue(answerSet.contains(authPlan.hashCode()));
      count++;
    }
    Assert.assertEquals(4, count);
    File roleListProfile =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir
                + File.separator
                + USER_SNAPSHOT_FILE_NAME
                + File.separator
                + userName
                + "_role.profile");
    planGenerator =
        new CNPhysicalPlanGenerator(roleListProfile.toPath(), CNSnapshotFileType.USER_ROLE);
    count = 0;
    // plan 5
    for (ConfigPhysicalPlan authPlan : planGenerator) {
      Assert.assertTrue(answerSet.contains(authPlan.hashCode()));
      count++;
    }
    Assert.assertEquals(1, count);
  }

  @Test
  public void databaseWithoutTemplateGeneratorTest() throws Exception {
    setupClusterSchemaInfo();
    Set<Integer> answerSet = new HashSet<>();
    Set<String> storageGroupPathList = new TreeSet<>();
    storageGroupPathList.add("root.sg");
    storageGroupPathList.add("root.a.sg");
    storageGroupPathList.add("root.a.b.sg");
    storageGroupPathList.add("root.a.a.a.b.sg");

    int i = 0;
    for (String path : storageGroupPathList) {
      TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
      tDatabaseSchema.setName(path);
      tDatabaseSchema.setTTL(i);
      tDatabaseSchema.setDataReplicationFactor(i);
      tDatabaseSchema.setSchemaReplicationFactor(i);
      tDatabaseSchema.setTimePartitionInterval(i);
      clusterSchemaInfo.createDatabase(
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchema));
      SetTTLPlan plan = new SetTTLPlan(Collections.singletonList(path), tDatabaseSchema.getTTL());
      answerSet.add(plan.hashCode());
      TDatabaseSchema tDatabaseSchemaBak = new TDatabaseSchema(tDatabaseSchema);
      tDatabaseSchemaBak.unsetTTL();
      DatabaseSchemaPlan databaseSchemaPlan =
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchemaBak);
      answerSet.add(databaseSchemaPlan.hashCode());
      i++;
    }

    boolean success = clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);

    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.CreateDatabase) {
        Assert.assertTrue(answerSet.contains(((DatabaseSchemaPlan) plan).hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.SetTTL) {
        Assert.assertTrue(answerSet.contains(((SetTTLPlan) plan).hashCode()));
      }
      count++;
    }
    planGenerator.checkException();
    Assert.assertEquals(8, count);
  }

  @Test
  public void templateGneratorTest() throws Exception {
    setupClusterSchemaInfo();
    Template t1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY));
    Template t2 =
        new Template(
            "t2",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN, TSEncoding.DIFF),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY, CompressionType.LZ4));
    Map<String, CreateSchemaTemplatePlan> answerPlan = new HashMap<>();

    CreateSchemaTemplatePlan plan1 = new CreateSchemaTemplatePlan(t1.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan1);
    answerPlan.put(t1.getName(), plan1);

    CreateSchemaTemplatePlan plan2 = new CreateSchemaTemplatePlan(t2.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan2);
    answerPlan.put(t2.getName(), plan2);

    boolean success = clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);
    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      CreateSchemaTemplatePlan templatePlan = (CreateSchemaTemplatePlan) plan;
      Assert.assertTrue(answerPlan.get(templatePlan.getTemplate().getName()).equals(templatePlan));
      count++;
    }
    Assert.assertEquals(2, count);
  }

  @Test
  public void templateAndDatabaseComplatedTest() throws Exception {
    setupClusterSchemaInfo();
    Set<Integer> answerSet = new HashSet<>();
    Set<String> storageGroupPathList = new TreeSet<>();
    storageGroupPathList.add("root.sg");
    storageGroupPathList.add("root.a.sg");
    storageGroupPathList.add("root.a.b.sg");
    storageGroupPathList.add("root.a.a.a.b.sg");

    int i = 0;
    for (String path : storageGroupPathList) {
      TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
      tDatabaseSchema.setName(path);
      tDatabaseSchema.setTTL(i);
      tDatabaseSchema.setDataReplicationFactor(i);
      tDatabaseSchema.setSchemaReplicationFactor(i);
      tDatabaseSchema.setTimePartitionInterval(i);
      clusterSchemaInfo.createDatabase(
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchema));
      SetTTLPlan plan = new SetTTLPlan(Collections.singletonList(path), tDatabaseSchema.getTTL());
      answerSet.add(plan.hashCode());
      TDatabaseSchema tDatabaseSchemaBak = new TDatabaseSchema(tDatabaseSchema);
      tDatabaseSchemaBak.unsetTTL();
      DatabaseSchemaPlan databaseSchemaPlan =
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchemaBak);
      answerSet.add(databaseSchemaPlan.hashCode());
      i++;
    }
    Template t1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY));
    Template t2 =
        new Template(
            "t2",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN, TSEncoding.DIFF),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY, CompressionType.LZ4));
    CreateSchemaTemplatePlan plan1 = new CreateSchemaTemplatePlan(t1.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan1);
    answerSet.add(plan1.hashCode());

    CreateSchemaTemplatePlan plan2 = new CreateSchemaTemplatePlan(t2.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan2);
    answerSet.add(plan2.hashCode());

    PreSetSchemaTemplatePlan preSetSchemaTemplatePlan1 =
        new PreSetSchemaTemplatePlan("t1", "root.sg.t1");
    PreSetSchemaTemplatePlan preSetSchemaTemplatePlan2 =
        new PreSetSchemaTemplatePlan("t2", "root.a.sg.t1");
    CommitSetSchemaTemplatePlan setSchemaTemplatePlan1 =
        new CommitSetSchemaTemplatePlan("t1", "root.sg.t1");
    CommitSetSchemaTemplatePlan setSchemaTemplatePlan2 =
        new CommitSetSchemaTemplatePlan("t2", "root.a.sg.t1");
    clusterSchemaInfo.preSetSchemaTemplate(preSetSchemaTemplatePlan1);
    clusterSchemaInfo.preSetSchemaTemplate(preSetSchemaTemplatePlan2);
    clusterSchemaInfo.commitSetSchemaTemplate(setSchemaTemplatePlan1);
    clusterSchemaInfo.commitSetSchemaTemplate(setSchemaTemplatePlan2);
    answerSet.add(setSchemaTemplatePlan1.hashCode());
    answerSet.add(setSchemaTemplatePlan1.hashCode());

    boolean success = clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);
    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.CreateDatabase) {
        Assert.assertTrue(answerSet.contains(((DatabaseSchemaPlan) plan).hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.SetTTL) {
        Assert.assertTrue(answerSet.contains(((SetTTLPlan) plan).hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.CreateSchemaTemplate) {
        Assert.assertTrue(answerSet.contains(((CreateSchemaTemplatePlan) plan).hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.PreSetSchemaTemplate) {
        Assert.assertTrue(answerSet.contains(((PreSetSchemaTemplatePlan) plan).hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.CommitSetSchemaTemplate) {
        Assert.assertTrue(answerSet.contains(((CommitSetSchemaTemplatePlan) plan).hashCode()));
      }
      count++;
    }
    Assert.assertEquals(12, count);
  }
}
