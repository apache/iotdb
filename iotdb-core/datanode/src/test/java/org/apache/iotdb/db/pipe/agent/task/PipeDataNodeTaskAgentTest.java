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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class PipeDataNodeTaskAgentTest {

  @Test
  public void testCreateMemoryCheckStillRunsWhenNoPipeTasksNeedToBeCreated() throws Exception {
    final boolean originalPipeEnableMemoryCheck =
        CommonDescriptor.getInstance().getConfig().isPipeEnableMemoryChecked();
    final long originalPipeInsertNodeQueueMemory =
        CommonDescriptor.getInstance().getConfig().getPipeInsertNodeQueueMemory();
    final double originalPipeTotalFloatingMemoryProportion =
        CommonDescriptor.getInstance().getConfig().getPipeTotalFloatingMemoryProportion();

    try {
      CommonDescriptor.getInstance().getConfig().setIsPipeEnableMemoryChecked(true);
      CommonDescriptor.getInstance().getConfig().setPipeInsertNodeQueueMemory(1);
      CommonDescriptor.getInstance().getConfig().setPipeTotalFloatingMemoryProportion(0);

      Assert.assertThrows(
          PipeException.class,
          () ->
              PipeDataNodeAgent.task()
                  .calculateMemoryUsage(
                      new PipeMeta(
                          new PipeStaticMeta(
                              "p", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
                          new PipeRuntimeMeta())));
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setIsPipeEnableMemoryChecked(originalPipeEnableMemoryCheck);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeInsertNodeQueueMemory(originalPipeInsertNodeQueueMemory);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeTotalFloatingMemoryProportion(originalPipeTotalFloatingMemoryProportion);
    }
  }

  @Test
  public void testPlainBatchMemoryIncludesLeaderCacheEndpointShards() throws Exception {
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_FORMAT_KEY, PipeSinkConstant.CONNECTOR_FORMAT_TABLET_VALUE);
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY, "1024");
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6667, 127.0.0.2:6667");
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY, "127.0.0.3");
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY, "6667");

    Assert.assertEquals(
        4 * 1024L, invokeCalculateSinkBatchMemory(new PipeParameters(sinkAttributes)));

    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY, Boolean.FALSE.toString());
    Assert.assertEquals(1024L, invokeCalculateSinkBatchMemory(new PipeParameters(sinkAttributes)));
  }

  @Test
  public void testTsFileBatchMemoryIgnoresLeaderCacheEndpointShards() throws Exception {
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_FORMAT_KEY, PipeSinkConstant.CONNECTOR_FORMAT_TS_FILE_VALUE);
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY, "2048");
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6667,127.0.0.2:6667");

    Assert.assertEquals(2048L, invokeCalculateSinkBatchMemory(new PipeParameters(sinkAttributes)));
  }

  @Test
  public void testPlainBatchMemoryReturnsZeroWhenBatchModeIsDisabled() throws Exception {
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_FORMAT_KEY, PipeSinkConstant.CONNECTOR_FORMAT_TABLET_VALUE);
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY, Boolean.FALSE.toString());
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY, "1024");

    Assert.assertEquals(0L, invokeCalculateSinkBatchMemory(new PipeParameters(sinkAttributes)));
  }

  @Test
  public void testSendTsFileReadBufferMemoryUsesSinkReadFileBufferSize() throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY, Boolean.FALSE.toString());

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_FORMAT_KEY, PipeSinkConstant.CONNECTOR_FORMAT_TABLET_VALUE);
    Assert.assertEquals(
        0L,
        invokeCalculateSendTsFileReadBufferMemory(
            new PipeParameters(sourceAttributes), new PipeParameters(sinkAttributes)));

    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_FORMAT_KEY, PipeSinkConstant.CONNECTOR_FORMAT_HYBRID_VALUE);
    Assert.assertEquals(
        PipeConfig.getInstance().getPipeSinkReadFileBufferSize(),
        invokeCalculateSendTsFileReadBufferMemory(
            new PipeParameters(sourceAttributes), new PipeParameters(sinkAttributes)));
  }

  private long invokeCalculateSinkBatchMemory(final PipeParameters sinkParameters)
      throws Exception {
    final Method method =
        PipeDataNodeTaskAgent.class.getDeclaredMethod(
            "calculateSinkBatchMemory", PipeParameters.class);
    method.setAccessible(true);
    return (long) method.invoke(null, sinkParameters);
  }

  private long invokeCalculateSendTsFileReadBufferMemory(
      final PipeParameters sourceParameters, final PipeParameters sinkParameters) throws Exception {
    final Method method =
        PipeDataNodeTaskAgent.class.getDeclaredMethod(
            "calculateSendTsFileReadBufferMemory", PipeParameters.class, PipeParameters.class);
    method.setAccessible(true);
    return (long) method.invoke(null, sourceParameters, sinkParameters);
  }
}
