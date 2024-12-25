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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
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
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATER_NO_REGEX;

public class CNPhysicalPlanGeneratorTest {
  private static AuthorInfo authorInfo;
  private static ClusterSchemaInfo clusterSchemaInfo;
  private static TTLInfo ttlInfo;

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

  private static void setupTTLInfo() throws IOException {
    ttlInfo = new TTLInfo();
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
    if (ttlInfo != null) {
      ttlInfo.clear();
    }
    FileUtils.deleteFileOrDirectory(snapshotDir);
  }

  @Test
  public void roleGeneratorTest() throws Exception {
    final HashSet<Integer> answerSet = new HashSet<>();
    String roleName = "test1";
    setupAuthorInfo();
    AuthorPlan plan = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
    plan.setRoleName(roleName);
    plan.setPermissions(new HashSet<>());
    plan.setNodeNameList(new ArrayList<>());
    answerSet.add(plan.hashCode());
    // Step 1: create role - plan1
    authorInfo.authorNonQuery(plan);

    // Step 2: grant role path privileges - plan2
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRole);
    plan.setRoleName(roleName);
    plan.setUserName("");
    plan.setNodeNameList(Collections.singletonList(new PartialPath("root.db.t1")));
    final Set<Integer> pathPris = new HashSet<>();
    pathPris.add(PrivilegeType.WRITE_DATA.ordinal());
    pathPris.add(PrivilegeType.WRITE_SCHEMA.ordinal());
    plan.setPermissions(pathPris);
    authorInfo.authorNonQuery(plan);

    // Answer set
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.WRITE_DATA.ordinal());
    answerSet.add(plan.hashCode());
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.WRITE_SCHEMA.ordinal());
    answerSet.add(plan.hashCode());

    // Step 3: grant role sys privileges - plan3
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRole);
    plan.setRoleName(roleName);
    plan.setUserName("");
    plan.setNodeNameList(Collections.emptyList());
    final Set<Integer> sysPris = new HashSet<>();
    sysPris.add(PrivilegeType.MANAGE_DATABASE.ordinal());
    sysPris.add(PrivilegeType.MANAGE_ROLE.ordinal());
    plan.setPermissions(sysPris);
    plan.setGrantOpt(true);
    authorInfo.authorNonQuery(plan);

    // Answer set
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.MANAGE_ROLE.ordinal());
    answerSet.add(plan.hashCode());
    plan.getPermissions().clear();
    plan.getPermissions().add(PrivilegeType.MANAGE_DATABASE.ordinal());
    answerSet.add(plan.hashCode());

    // PhysicalPlan generator will return five plans:
    // 1. create role plan
    // 2. grant path privileges plan * 2
    // 3. grant system privileges plan * 2
    Assert.assertTrue(authorInfo.processTakeSnapshot(snapshotDir));

    final File roleProfile =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir
                + File.separator
                + ROLE_SNAPSHOT_FILE_NAME
                + File.separator
                + roleName
                + ".profile");

    final CNPhysicalPlanGenerator planGenerator =
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
    final String userName = "test1";
    final Set<Integer> answerSet = new HashSet<>();
    setupAuthorInfo();
    AuthorPlan plan = new AuthorPlan(ConfigPhysicalPlanType.CreateUser);
    plan.setPassword("password");
    plan.setUserName(userName);
    plan.setPermissions(new HashSet<>());
    plan.setNodeNameList(new ArrayList<>());
    // Create user plan 1
    authorInfo.authorNonQuery(plan);
    plan.setAuthorType(ConfigPhysicalPlanType.CreateUserWithRawPassword);
    plan.setPassword(AuthUtils.encryptPassword("password"));
    answerSet.add(plan.hashCode());

    plan = new AuthorPlan(ConfigPhysicalPlanType.CreateRole);
    plan.setRoleName("role1");
    plan.setPermissions(new HashSet<>());
    plan.setNodeNameList(new ArrayList<>());
    authorInfo.authorNonQuery(plan);

    // Grant path privileges, plan 2 , plan 3
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantUser);
    plan.setUserName(userName);
    plan.setRoleName("");
    plan.setNodeNameList(Collections.singletonList(new PartialPath("root.db1.t2")));
    final Set<Integer> priSet = new HashSet<>();
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

    // Grant system privileges, plan 4
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantUser);
    plan.setUserName(userName);
    plan.setRoleName("");
    plan.setNodeNameList(Collections.emptyList());
    plan.setPermissions(Collections.singleton(PrivilegeType.MANAGE_DATABASE.ordinal()));
    plan.setGrantOpt(false);
    authorInfo.authorNonQuery(plan);
    answerSet.add(plan.hashCode());

    // Grant role to user, plan 5
    plan = new AuthorPlan(ConfigPhysicalPlanType.GrantRoleToUser);
    plan.setRoleName("role1");
    plan.setUserName("");
    plan.setUserName(userName);
    plan.setPermissions(new HashSet<>());
    plan.setNodeNameList(new ArrayList<>());
    authorInfo.authorNonQuery(plan);
    answerSet.add(plan.hashCode());

    Assert.assertTrue(authorInfo.processTakeSnapshot(snapshotDir));

    final File userProfile =
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
    final File roleListProfile =
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
    setupTTLInfo();
    final Set<Integer> answerSet = new HashSet<>();
    final Set<String> storageGroupPathList = new TreeSet<>();
    storageGroupPathList.add("root.sg");
    storageGroupPathList.add("root.ln");
    storageGroupPathList.add("root.a.sg");
    storageGroupPathList.add("root.a.b.sg");
    storageGroupPathList.add("root.a.a.a.b.sg");

    int i = 0;
    for (String path : storageGroupPathList) {
      final TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
      tDatabaseSchema.setName(path);
      tDatabaseSchema.setTTL(i + 1);
      tDatabaseSchema.setDataReplicationFactor(i);
      tDatabaseSchema.setSchemaReplicationFactor(i);
      tDatabaseSchema.setTimePartitionInterval(i);
      DatabaseSchemaPlan databaseSchemaPlan =
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchema);
      clusterSchemaInfo.createDatabase(databaseSchemaPlan);
      answerSet.add(databaseSchemaPlan.hashCode());
      SetTTLPlan setTTLPlan =
          new SetTTLPlan(path.split(PATH_SEPARATER_NO_REGEX), tDatabaseSchema.getTTL());
      ttlInfo.setTTL(setTTLPlan);
      answerSet.add(setTTLPlan.hashCode());
      i++;
    }

    final boolean success =
        clusterSchemaInfo.processTakeSnapshot(snapshotDir)
            && ttlInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    final File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    final File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);
    final File ttlInfo =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir + File.separator + TTLInfo.SNAPSHOT_FILENAME);

    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.CreateDatabase) {
        Assert.assertTrue(answerSet.contains(plan.hashCode()));
        count++;
      }
    }
    planGenerator.checkException();
    Assert.assertEquals(5, count);
    planGenerator = new CNPhysicalPlanGenerator(ttlInfo.toPath(), CNSnapshotFileType.TTL);
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.SetTTL) {
        if (!new PartialPath(((SetTTLPlan) plan).getPathPattern())
            .getFullPath()
            .equals(
                PATH_ROOT
                    + IoTDBConstant.PATH_SEPARATOR
                    + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
          Assert.assertTrue(answerSet.contains(plan.hashCode()));
        }
        Assert.assertEquals(
            CNPhysicalPlanGeneratorTest.ttlInfo.setTTL((SetTTLPlan) plan).code,
            TSStatusCode.SUCCESS_STATUS.getStatusCode());
        count++;
      }
    }
    planGenerator.checkException();
    Assert.assertEquals(11, count);
  }

  @Test
  public void templateGeneratorTest() throws Exception {
    setupClusterSchemaInfo();
    final Template t1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY));
    final Template t2 =
        new Template(
            "t2",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN, TSEncoding.DIFF),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY, CompressionType.LZ4));
    final Map<String, CreateSchemaTemplatePlan> answerPlan = new HashMap<>();

    final CreateSchemaTemplatePlan plan1 = new CreateSchemaTemplatePlan(t1.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan1);
    answerPlan.put(t1.getName(), plan1);

    final CreateSchemaTemplatePlan plan2 = new CreateSchemaTemplatePlan(t2.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan2);
    answerPlan.put(t2.getName(), plan2);

    final boolean success = clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    final File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    final File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);
    final CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      final CreateSchemaTemplatePlan templatePlan = (CreateSchemaTemplatePlan) plan;
      Assert.assertEquals(answerPlan.get(templatePlan.getTemplate().getName()), templatePlan);
      count++;
    }
    Assert.assertEquals(2, count);
  }

  @Test
  public void templateAndDatabaseCompletedTest() throws Exception {
    setupClusterSchemaInfo();
    setupTTLInfo();
    final Set<Integer> answerSet = new HashSet<>();
    final Set<String> storageGroupPathList = new TreeSet<>();
    storageGroupPathList.add("root.sg");
    storageGroupPathList.add("root.a.sg");
    storageGroupPathList.add("root.a.b.sg");
    storageGroupPathList.add("root.a.a.a.b.sg");

    int i = 0;
    for (String path : storageGroupPathList) {
      final TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
      tDatabaseSchema.setName(path);
      tDatabaseSchema.setTTL(i + 1);
      tDatabaseSchema.setDataReplicationFactor(i);
      tDatabaseSchema.setSchemaReplicationFactor(i);
      tDatabaseSchema.setTimePartitionInterval(i);
      final DatabaseSchemaPlan databaseSchemaPlan =
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchema);
      clusterSchemaInfo.createDatabase(databaseSchemaPlan);
      answerSet.add(databaseSchemaPlan.hashCode());
      final SetTTLPlan plan =
          new SetTTLPlan(Arrays.asList(path.split("\\.")), tDatabaseSchema.getTTL());
      ttlInfo.setTTL(plan);
      answerSet.add(plan.hashCode());
      i++;
    }
    final Template t1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY));
    final Template t2 =
        new Template(
            "t2",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT),
            Arrays.asList(TSEncoding.GORILLA, TSEncoding.PLAIN, TSEncoding.DIFF),
            Arrays.asList(CompressionType.GZIP, CompressionType.SNAPPY, CompressionType.LZ4));
    final CreateSchemaTemplatePlan plan1 = new CreateSchemaTemplatePlan(t1.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan1);
    answerSet.add(plan1.hashCode());

    final CreateSchemaTemplatePlan plan2 = new CreateSchemaTemplatePlan(t2.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(plan2);
    answerSet.add(plan2.hashCode());

    final PreSetSchemaTemplatePlan preSetSchemaTemplatePlan1 =
        new PreSetSchemaTemplatePlan("t1", "root.sg");
    final PreSetSchemaTemplatePlan preSetSchemaTemplatePlan2 =
        new PreSetSchemaTemplatePlan("t2", "root.a.sg.t1");
    final CommitSetSchemaTemplatePlan setSchemaTemplatePlan1 =
        new CommitSetSchemaTemplatePlan("t1", "root.sg");
    final CommitSetSchemaTemplatePlan setSchemaTemplatePlan2 =
        new CommitSetSchemaTemplatePlan("t2", "root.a.sg.t1");
    clusterSchemaInfo.preSetSchemaTemplate(preSetSchemaTemplatePlan1);
    clusterSchemaInfo.preSetSchemaTemplate(preSetSchemaTemplatePlan2);
    clusterSchemaInfo.commitSetSchemaTemplate(setSchemaTemplatePlan1);
    clusterSchemaInfo.commitSetSchemaTemplate(setSchemaTemplatePlan2);
    answerSet.add(setSchemaTemplatePlan1.hashCode());
    answerSet.add(setSchemaTemplatePlan1.hashCode());

    final boolean success =
        clusterSchemaInfo.processTakeSnapshot(snapshotDir)
            && ttlInfo.processTakeSnapshot(snapshotDir);
    Assert.assertTrue(success);
    final File schemaInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + SCHEMA_INFO_FILE_NAME);
    final File templateInfo =
        SystemFileFactory.INSTANCE.getFile(snapshotDir + File.separator + TEMPLATE_INFO_FILE_NAME);
    final File ttlInfo =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir + File.separator + TTLInfo.SNAPSHOT_FILENAME);
    CNPhysicalPlanGenerator planGenerator =
        new CNPhysicalPlanGenerator(schemaInfo.toPath(), templateInfo.toPath());
    int count = 0;
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.CreateDatabase) {
        Assert.assertTrue(answerSet.contains(plan.hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.CreateSchemaTemplate) {
        Assert.assertTrue(answerSet.contains(plan.hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.PreSetSchemaTemplate) {
        Assert.assertTrue(answerSet.contains(plan.hashCode()));
      } else if (plan.getType() == ConfigPhysicalPlanType.CommitSetSchemaTemplate) {
        final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan =
            (CommitSetSchemaTemplatePlan) plan;
        if (commitSetSchemaTemplatePlan.getName().equals("t1")) {
          Assert.assertEquals(commitSetSchemaTemplatePlan, setSchemaTemplatePlan1);
        } else {
          Assert.assertEquals(commitSetSchemaTemplatePlan, setSchemaTemplatePlan1);
        }
      }
      count++;
    }
    Assert.assertEquals(8, count);

    planGenerator = new CNPhysicalPlanGenerator(ttlInfo.toPath(), CNSnapshotFileType.TTL);
    for (ConfigPhysicalPlan plan : planGenerator) {
      if (plan.getType() == ConfigPhysicalPlanType.SetTTL) {
        if (!new PartialPath(((SetTTLPlan) plan).getPathPattern())
            .getFullPath()
            .equals(
                PATH_ROOT
                    + IoTDBConstant.PATH_SEPARATOR
                    + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
          Assert.assertTrue(answerSet.contains(plan.hashCode()));
        }
        Assert.assertEquals(
            CNPhysicalPlanGeneratorTest.ttlInfo.setTTL((SetTTLPlan) plan).code,
            TSStatusCode.SUCCESS_STATUS.getStatusCode());
        count++;
      }
    }
    planGenerator.checkException();
    Assert.assertEquals(13, count);
  }
}
