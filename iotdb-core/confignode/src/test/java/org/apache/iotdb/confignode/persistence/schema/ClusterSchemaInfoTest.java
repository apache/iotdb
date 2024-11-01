/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.template.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ClusterSchemaInfoTest {

  private static ClusterSchemaInfo clusterSchemaInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Before
  public void setup() throws IOException {
    clusterSchemaInfo = new ClusterSchemaInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @After
  public void cleanup() throws IOException {
    clusterSchemaInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws IOException, IllegalPathException {
    Set<String> storageGroupPathList = new TreeSet<>();
    storageGroupPathList.add("root.sg");
    storageGroupPathList.add("root.a.sg");
    storageGroupPathList.add("root.a.b.sg");
    storageGroupPathList.add("root.a.a.a.b.sg");

    Map<String, TDatabaseSchema> testMap = new TreeMap<>();
    int i = 0;
    for (String path : storageGroupPathList) {
      TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
      tDatabaseSchema.setName(path);
      tDatabaseSchema.setDataReplicationFactor(i);
      tDatabaseSchema.setSchemaReplicationFactor(i);
      tDatabaseSchema.setTimePartitionInterval(i);
      testMap.put(path, tDatabaseSchema);
      clusterSchemaInfo.createDatabase(
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, tDatabaseSchema));
      i++;
    }
    clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    clusterSchemaInfo.clear();
    clusterSchemaInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(
        storageGroupPathList.size(), clusterSchemaInfo.getDatabaseNames(null).size());

    GetDatabasePlan getStorageGroupReq =
        new GetDatabasePlan(
            Arrays.asList(PathUtils.splitPathToDetachedNodes("root.**")), ALL_MATCH_SCOPE);
    Map<String, TDatabaseSchema> reloadResult =
        clusterSchemaInfo.getMatchedDatabaseSchemas(getStorageGroupReq).getSchemaMap();
    Assert.assertEquals(testMap, reloadResult);
  }

  @Test
  public void testSetTemplate() throws IllegalPathException {
    String templateName = "template_name";
    Template template = newSchemaTemplate(templateName);
    CreateSchemaTemplatePlan createSchemaTemplatePlan =
        new CreateSchemaTemplatePlan(template.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(createSchemaTemplatePlan);

    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test1")));
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test2")));
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test3")));

    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test1.template"));
    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test2.template"));
    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test3.template"));

    List<String> pathList =
        clusterSchemaInfo
            .getPathsSetTemplate(new GetPathsSetTemplatePlan(templateName, ALL_MATCH_SCOPE))
            .getPathList();
    Assert.assertEquals(3, pathList.size());
    Assert.assertTrue(pathList.contains("root.test1.template"));
    Assert.assertTrue(pathList.contains("root.test2.template"));
    Assert.assertTrue(pathList.contains("root.test3.template"));
  }

  private Template newSchemaTemplate(String name) throws IllegalPathException {
    List<String> measurements = Arrays.asList(name + "_" + "temperature", name + "_" + "status");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    return new Template(name, measurements, dataTypes, encodings, compressors);
  }

  @Test
  public void testTemplateSetAndRead() throws Exception {
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

    clusterSchemaInfo.createSchemaTemplate(new CreateSchemaTemplatePlan(t1.serialize().array()));
    clusterSchemaInfo.createSchemaTemplate(new CreateSchemaTemplatePlan(t2.serialize().array()));

    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1")));
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db2")));

    clusterSchemaInfo.setSchemaTemplate(new SetSchemaTemplatePlan("t1", "root.db1"));
    clusterSchemaInfo.preSetSchemaTemplate(new PreSetSchemaTemplatePlan("t2", "root.db2"));

    TemplateInfoResp templateInfoResp = clusterSchemaInfo.getAllTemplates();
    List<Template> templateList = templateInfoResp.getTemplateList();
    templateList.sort(Comparator.comparing(Template::getName));
    Assert.assertEquals(2, templateList.size());
    Assert.assertEquals(t1, templateList.get(0));
    Assert.assertEquals(t2, templateList.get(1));

    AllTemplateSetInfoResp allTemplateSetInfoResp = clusterSchemaInfo.getAllTemplateSetInfo();
    Map<Template, List<Pair<String, Boolean>>> map =
        TemplateInternalRPCUtil.parseAddAllTemplateSetInfoBytes(
            ByteBuffer.wrap(allTemplateSetInfoResp.getTemplateInfo()));
    Assert.assertEquals(2, map.size());
    for (Template template : map.keySet()) {
      Assert.assertEquals(1, map.get(template).size());
      if (template.getName().equals("t1")) {
        Assert.assertEquals("root.db1", map.get(template).get(0).left);
        Assert.assertFalse("root.db1", map.get(template).get(0).right);
      } else if (template.getName().equals("t2")) {
        Assert.assertEquals("root.db2", map.get(template).get(0).left);
        Assert.assertTrue(map.get(template).get(0).right);
      }
    }

    List<PartialPath> pathList =
        Arrays.asList(
            new PartialPath("root.db1"),
            new PartialPath("root.db1.**"),
            new PartialPath("root.db2"),
            new PartialPath("root.db2.**"));
    Template[] templates = new Template[] {t1, t1, t2, t2};
    TemplateSetInfoResp templateSetInfoResp =
        clusterSchemaInfo.getTemplateSetInfo(new GetTemplateSetInfoPlan(pathList));
    Map<PartialPath, List<Template>> templateSetMap = templateSetInfoResp.getPatternTemplateMap();
    for (int i = 0; i < pathList.size(); i++) {
      Assert.assertEquals(templates[i], templateSetMap.get(pathList.get(i)).get(0));
    }
  }
}
