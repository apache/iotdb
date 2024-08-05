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

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class PipeInfoTest {

  private static PipeInfo pipeInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  private final String pipeName = "testPipe";
  private final String pluginName = "testPlugin";

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
    // Create pipe test pipe
    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("extractor", "iotdb-extractor");
    processorAttributes.put("processor", "do-nothing-processor");
    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("host", "127.0.0.1");
    connectorAttributes.put("port", "6667");

    PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(1, pipeTaskMeta);
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName, 121, extractorAttributes, processorAttributes, connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
    pipeInfo.getPipeTaskInfo().createPipe(createPipePlanV2);

    // Create pipe plugin test plugin
    CreatePipePluginPlan createPipePluginPlan =
        new CreatePipePluginPlan(
            new PipePluginMeta(pluginName, "org.apache.iotdb.TestJar", false, "test.jar", "???"),
            new Binary("123", TSFileConfig.STRING_CHARSET));
    pipeInfo.getPipePluginInfo().createPipePlugin(createPipePluginPlan);

    pipeInfo.processTakeSnapshot(snapshotDir);

    PipeInfo pipeInfo1 = new PipeInfo();
    pipeInfo1.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(pipeInfo.toString(), pipeInfo1.toString());
    Assert.assertEquals(pipeInfo, pipeInfo1);
  }

  @Test
  public void testManagement() {
    // Create pipe test pipe
    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "org.apache.iotdb.pipe.extractor.DefaultExtractor");
    processorAttributes.put("processor", "org.apache.iotdb.pipe.processor.SDTFilterProcessor");
    connectorAttributes.put("connector", "org.apache.iotdb.pipe.protocol.ThriftTransporter");
    PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    ConcurrentMap<Integer, PipeTaskMeta> pipeTasks = new ConcurrentHashMap<>();
    pipeTasks.put(1, pipeTaskMeta);
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            pipeName, 121, extractorAttributes, processorAttributes, connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
    pipeInfo.getPipeTaskInfo().createPipe(createPipePlanV2);

    Assert.assertTrue(pipeInfo.getPipeTaskInfo().isPipeExisted("testPipe"));

    // Start pipe test pipe
    SetPipeStatusPlanV2 setPipeStatusPlanV2 = new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING);
    pipeInfo.getPipeTaskInfo().setPipeStatus(setPipeStatusPlanV2);

    // Drop pipe test pipe
    DropPipePlanV2 dropPipePlanV2 = new DropPipePlanV2(pipeName);
    pipeInfo.getPipeTaskInfo().dropPipe(dropPipePlanV2);

    Assert.assertFalse(pipeInfo.getPipeTaskInfo().isPipeExisted("testPipe"));
    Assert.assertTrue(pipeInfo.getPipeTaskInfo().isEmpty());

    // Create pipe plugin test plugin
    CreatePipePluginPlan createPipePluginPlan =
        new CreatePipePluginPlan(
            new PipePluginMeta(pluginName, "org.apache.iotdb.TestJar", false, "test.jar", "???"),
            new Binary("123", TSFileConfig.STRING_CHARSET));
    pipeInfo.getPipePluginInfo().createPipePlugin(createPipePluginPlan);

    // Drop pipe plugin test plugin
    pipeInfo.getPipePluginInfo().validateBeforeDroppingPipePlugin(pluginName, false);
    DropPipePluginPlan dropPipePluginPlan = new DropPipePluginPlan(pluginName);
    pipeInfo.getPipePluginInfo().dropPipePlugin(dropPipePluginPlan);
  }
}
