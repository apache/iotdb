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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeConsensusGroupTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;
import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class PipeInfoTest {

  private static PipeInfo pipeInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Before
  public void setup() throws IOException {
    pipeInfo = new PipeInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @After
  public void cleanup() throws IOException {
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {
    Map<String, String> collectorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    collectorAttributes.put("collector", "org.apache.iotdb.pipe.collector.DefaultCollector");
    processorAttributes.put("processor", "org.apache.iotdb.pipe.processor.SDTFilterProcessor");
    connectorAttributes.put("connector", "org.apache.iotdb.pipe.protocal.ThriftTransporter");
    PipeConsensusGroupTaskMeta pipeConsensusGroupTaskMeta = new PipeConsensusGroupTaskMeta(0, 1);
    Map<TConsensusGroupId, PipeConsensusGroupTaskMeta> pipeTasks = new HashMap<>();
    pipeTasks.put(new TConsensusGroupId(DataRegion, 1), pipeConsensusGroupTaskMeta);
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "testPipe", 121, collectorAttributes, processorAttributes, connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
    pipeInfo.getPipeTaskInfo().createPipe(createPipePlanV2);

    CreatePipePluginPlan createPipePluginPlan =
        new CreatePipePluginPlan(
            new PipePluginMeta("testPlugin", "org.apache.iotdb.testJar", "testJar", "???"),
            new Binary("123"));
    pipeInfo.getPipePluginInfo().createPipePlugin(createPipePluginPlan);

    pipeInfo.processTakeSnapshot(snapshotDir);

    PipeInfo pipeInfo1 = new PipeInfo();
    pipeInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(pipeInfo, pipeInfo1);
  }
}
