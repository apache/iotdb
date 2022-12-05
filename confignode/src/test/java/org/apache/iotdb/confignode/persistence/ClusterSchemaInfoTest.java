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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.consensus.request.read.storagegroup.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

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

    Map<String, TStorageGroupSchema> testMap = new TreeMap<>();
    int i = 0;
    for (String path : storageGroupPathList) {
      TStorageGroupSchema tStorageGroupSchema = new TStorageGroupSchema();
      tStorageGroupSchema.setName(path);
      tStorageGroupSchema.setTTL(i);
      tStorageGroupSchema.setDataReplicationFactor(i);
      tStorageGroupSchema.setSchemaReplicationFactor(i);
      tStorageGroupSchema.setTimePartitionInterval(i);
      testMap.put(path, tStorageGroupSchema);
      clusterSchemaInfo.setStorageGroup(new SetStorageGroupPlan(tStorageGroupSchema));
      i++;
    }
    clusterSchemaInfo.processTakeSnapshot(snapshotDir);
    clusterSchemaInfo.clear();
    clusterSchemaInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(
        storageGroupPathList.size(), clusterSchemaInfo.getStorageGroupNames().size());

    GetStorageGroupPlan getStorageGroupReq =
        new GetStorageGroupPlan(Arrays.asList(PathUtils.splitPathToDetachedNodes("root.**")));
    Map<String, TStorageGroupSchema> reloadResult =
        clusterSchemaInfo.getMatchedStorageGroupSchemas(getStorageGroupReq).getSchemaMap();
    Assert.assertEquals(testMap, reloadResult);
  }

  @Test
  public void testSetTemplate() throws IllegalPathException, IOException {
    String templateName = "template_name";
    Template template = new Template(newCreateSchemaTemplateStatement(templateName));
    CreateSchemaTemplatePlan createSchemaTemplatePlan =
        new CreateSchemaTemplatePlan(template.serialize().array());
    clusterSchemaInfo.createSchemaTemplate(createSchemaTemplatePlan);

    clusterSchemaInfo.setStorageGroup(
        new SetStorageGroupPlan(new TStorageGroupSchema("root.test1")));
    clusterSchemaInfo.setStorageGroup(
        new SetStorageGroupPlan(new TStorageGroupSchema("root.test2")));
    clusterSchemaInfo.setStorageGroup(
        new SetStorageGroupPlan(new TStorageGroupSchema("root.test3")));

    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test1.template"));
    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test2.template"));
    clusterSchemaInfo.setSchemaTemplate(
        new SetSchemaTemplatePlan(templateName, "root.test3.template"));

    List<String> pathList =
        clusterSchemaInfo
            .getPathsSetTemplate(new GetPathsSetTemplatePlan(templateName))
            .getPathList();
    Assert.assertEquals(3, pathList.size());
    Assert.assertTrue(pathList.contains("root.test1.template"));
    Assert.assertTrue(pathList.contains("root.test2.template"));
    Assert.assertTrue(pathList.contains("root.test3.template"));
  }

  private CreateSchemaTemplateStatement newCreateSchemaTemplateStatement(String name) {
    List<List<String>> measurements =
        Arrays.asList(
            Arrays.asList(name + "_" + "temperature"), Arrays.asList(name + "_" + "status"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(Arrays.asList(TSDataType.FLOAT), Arrays.asList(TSDataType.BOOLEAN));
    List<List<TSEncoding>> encodings =
        Arrays.asList(Arrays.asList(TSEncoding.RLE), Arrays.asList(TSEncoding.PLAIN));
    List<List<CompressionType>> compressors =
        Arrays.asList(Arrays.asList(CompressionType.SNAPPY), Arrays.asList(CompressionType.SNAPPY));
    CreateSchemaTemplateStatement createSchemaTemplateStatement =
        new CreateSchemaTemplateStatement(name, measurements, dataTypes, encodings, compressors);
    return createSchemaTemplateStatement;
  }
}
