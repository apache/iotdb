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
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class ClusterSchemaInfoTest {

  private static ClusterSchemaInfo clusterSchemaInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    clusterSchemaInfo = new ClusterSchemaInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
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
}
