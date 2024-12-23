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
package org.apache.iotdb.confignode.consensus.response.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeTableRespTest {

  public PipeTableResp constructPipeTableResp() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PipeMeta> pipeMetaList = new ArrayList<>();

    // PipeMeta 1
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
            "testPipe", 121, extractorAttributes, processorAttributes, connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    pipeMetaList.add(new PipeMeta(pipeStaticMeta, pipeRuntimeMeta));

    // PipeMeta 2
    Map<String, String> extractorAttributes1 = new HashMap<>();
    Map<String, String> processorAttributes1 = new HashMap<>();
    Map<String, String> connectorAttributes1 = new HashMap<>();

    extractorAttributes1.put("extractor", "iotdb-extractor");
    processorAttributes1.put("processor", "do-nothing-processor");
    connectorAttributes1.put("connector", "iotdb-thrift-connector");
    connectorAttributes1.put("host", "127.0.0.1");
    connectorAttributes1.put("port", "6667");

    PipeTaskMeta pipeTaskMeta1 = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    ConcurrentMap<Integer, PipeTaskMeta> pipeTasks1 = new ConcurrentHashMap<>();
    pipeTasks1.put(1, pipeTaskMeta1);
    PipeStaticMeta pipeStaticMeta1 =
        new PipeStaticMeta(
            "testPipe1", 122, extractorAttributes1, processorAttributes1, connectorAttributes1);
    PipeRuntimeMeta pipeRuntimeMeta1 = new PipeRuntimeMeta(pipeTasks1);
    pipeMetaList.add(new PipeMeta(pipeStaticMeta1, pipeRuntimeMeta1));

    // PipeMeta 3
    Map<String, String> extractorAttributes2 = new HashMap<>();
    Map<String, String> processorAttributes2 = new HashMap<>();
    Map<String, String> connectorAttributes2 = new HashMap<>();

    extractorAttributes2.put("extractor", "iotdb-extractor");
    processorAttributes2.put("processor", "do-nothing-processor");
    connectorAttributes2.put("connector", "iotdb-thrift-connector");
    connectorAttributes2.put("host", "172.30.30.30");
    connectorAttributes2.put("port", "6667");

    PipeTaskMeta pipeTaskMeta2 = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    ConcurrentMap<Integer, PipeTaskMeta> pipeTasks2 = new ConcurrentHashMap<>();
    pipeTasks2.put(1, pipeTaskMeta2);
    PipeStaticMeta pipeStaticMeta2 =
        new PipeStaticMeta(
            "testPipe2", 123, extractorAttributes2, processorAttributes2, connectorAttributes2);
    PipeRuntimeMeta pipeRuntimeMeta2 = new PipeRuntimeMeta(pipeTasks2);
    pipeMetaList.add(new PipeMeta(pipeStaticMeta2, pipeRuntimeMeta2));

    return new PipeTableResp(status, pipeMetaList);
  }

  @Test
  public void testFilter() {
    PipeTableResp pipeTableResp = constructPipeTableResp();
    PipeTableResp filteredPipeTableRespByConnector = pipeTableResp.filter(true, "testPipe");
    Assert.assertEquals(2, filteredPipeTableRespByConnector.getAllPipeMeta().size());

    PipeTableResp filteredPipeTableRespByName = pipeTableResp.filter(false, "testPipe");
    Assert.assertEquals(1, filteredPipeTableRespByName.getAllPipeMeta().size());

    PipeTableResp allPipeTableResp = pipeTableResp.filter(true, null);
    Assert.assertEquals(3, allPipeTableResp.getAllPipeMeta().size());
  }
}
