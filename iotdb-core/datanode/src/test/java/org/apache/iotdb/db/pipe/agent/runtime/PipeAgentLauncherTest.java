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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.plugin.PipeDataNodePluginAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ConfigNodeClientManager.class,
  PipePluginExecutableManager.class,
  PipeDataNodeAgent.class
})
public class PipeAgentLauncherTest {

  private IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager;
  private ConfigNodeClient configNodeClient;
  private PipePluginExecutableManager pipePluginExecutableManager;
  private PipeDataNodePluginAgent pipeDataNodePluginAgent;

  @Before
  public void setUp() throws Exception {
    configNodeClientManager = Mockito.mock(IClientManager.class);
    configNodeClient = Mockito.mock(ConfigNodeClient.class);
    pipePluginExecutableManager = Mockito.mock(PipePluginExecutableManager.class);
    pipeDataNodePluginAgent = Mockito.mock(PipeDataNodePluginAgent.class);

    PowerMockito.mockStatic(ConfigNodeClientManager.class);
    PowerMockito.mockStatic(PipePluginExecutableManager.class);
    PowerMockito.mockStatic(PipeDataNodeAgent.class);
    PowerMockito.when(ConfigNodeClientManager.getInstance()).thenReturn(configNodeClientManager);
    PowerMockito.when(PipePluginExecutableManager.getInstance())
        .thenReturn(pipePluginExecutableManager);
    PowerMockito.when(PipeDataNodeAgent.plugin()).thenReturn(pipeDataNodePluginAgent);
    Mockito.when(configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID))
        .thenReturn(configNodeClient);
  }

  @Test
  public void testBatchFailureRetriesIndividuallyAndOnlyMarksFailedPluginUnavailable()
      throws Exception {
    final PipePluginMeta healthyPlugin = pipePluginMeta("healthy", "healthy.jar");
    final PipePluginMeta missingPlugin = pipePluginMeta("missing", "missing.jar");
    final TGetJarInListResp failedResponse = failureResponse();

    Mockito.when(configNodeClient.getPipePluginJar(Mockito.any(TGetJarInListReq.class)))
        .thenAnswer(
            invocation -> {
              final List<String> jarNames =
                  invocation.getArgument(0, TGetJarInListReq.class).getJarNameList();
              if (jarNames.size() == 2 || "missing.jar".equals(jarNames.get(0))) {
                return failedResponse;
              }
              return successResponse(Collections.singletonList(ByteBuffer.wrap(new byte[] {1})));
            });

    final Set<String> unavailablePlugins =
        PipeAgentLauncher.fetchAndSavePipePluginJars(Arrays.asList(healthyPlugin, missingPlugin));

    Assert.assertEquals(Collections.singleton("MISSING"), unavailablePlugins);
    Mockito.verify(pipePluginExecutableManager)
        .savePluginToInstallDir(
            Mockito.any(ByteBuffer.class), Mockito.eq("HEALTHY"), Mockito.eq("healthy.jar"));
    Mockito.verify(pipeDataNodePluginAgent)
        .markPluginLoadFailure(Mockito.eq(missingPlugin), Mockito.any());
    Mockito.verify(pipeDataNodePluginAgent, Mockito.never())
        .markPluginLoadFailure(Mockito.eq(healthyPlugin), Mockito.any());
  }

  @Test
  public void testBatchJarCountMismatchRetriesEachPluginIndividually() throws Exception {
    final PipePluginMeta firstPlugin = pipePluginMeta("first", "first.jar");
    final PipePluginMeta secondPlugin = pipePluginMeta("second", "second.jar");

    Mockito.when(configNodeClient.getPipePluginJar(Mockito.any(TGetJarInListReq.class)))
        .thenReturn(successResponse(Collections.singletonList(ByteBuffer.wrap(new byte[] {1}))));

    final Set<String> unavailablePlugins =
        PipeAgentLauncher.fetchAndSavePipePluginJars(Arrays.asList(firstPlugin, secondPlugin));

    Assert.assertTrue(unavailablePlugins.isEmpty());
    final ArgumentCaptor<TGetJarInListReq> requestCaptor =
        ArgumentCaptor.forClass(TGetJarInListReq.class);
    Mockito.verify(configNodeClient, Mockito.times(3)).getPipePluginJar(requestCaptor.capture());
    Assert.assertEquals(
        Arrays.asList("first.jar", "second.jar"),
        requestCaptor.getAllValues().get(0).getJarNameList());
    Assert.assertEquals(
        Collections.singletonList("first.jar"),
        requestCaptor.getAllValues().get(1).getJarNameList());
    Assert.assertEquals(
        Collections.singletonList("second.jar"),
        requestCaptor.getAllValues().get(2).getJarNameList());
    Mockito.verify(pipePluginExecutableManager, Mockito.times(2))
        .savePluginToInstallDir(
            Mockito.any(ByteBuffer.class), Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testSaveFailureDoesNotBlockSubsequentPlugin() throws Exception {
    final PipePluginMeta failedPlugin = pipePluginMeta("failed", "failed.jar");
    final PipePluginMeta healthyPlugin = pipePluginMeta("healthy", "healthy.jar");
    Mockito.when(configNodeClient.getPipePluginJar(Mockito.any(TGetJarInListReq.class)))
        .thenReturn(
            successResponse(
                Arrays.asList(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}))));
    Mockito.doThrow(new IOException("injected save failure"))
        .when(pipePluginExecutableManager)
        .savePluginToInstallDir(
            Mockito.any(ByteBuffer.class), Mockito.eq("FAILED"), Mockito.eq("failed.jar"));

    final Set<String> unavailablePlugins =
        PipeAgentLauncher.fetchAndSavePipePluginJars(Arrays.asList(failedPlugin, healthyPlugin));

    Assert.assertEquals(Collections.singleton("FAILED"), unavailablePlugins);
    Mockito.verify(pipePluginExecutableManager)
        .savePluginToInstallDir(
            Mockito.any(ByteBuffer.class), Mockito.eq("HEALTHY"), Mockito.eq("healthy.jar"));
    Mockito.verify(pipeDataNodePluginAgent)
        .markPluginLoadFailure(Mockito.eq(failedPlugin), Mockito.any(IOException.class));
    Mockito.verify(pipeDataNodePluginAgent, Mockito.never())
        .markPluginLoadFailure(Mockito.eq(healthyPlugin), Mockito.any());
  }

  @Test
  public void testPluginLoadFailureIsRecordedForShowPipePlugins() throws Exception {
    final PipeDataNodePluginAgent agent = new PipeDataNodePluginAgent();
    final PipePluginMeta plugin = pipePluginMeta("failed", "failed.jar");

    agent.markPluginLoadFailure(plugin, new IOException("missing jar"));

    final Field metaKeeperField =
        PipeDataNodePluginAgent.class.getDeclaredField("pipePluginMetaKeeper");
    metaKeeperField.setAccessible(true);
    final DataNodePipePluginMetaKeeper metaKeeper =
        (DataNodePipePluginMetaKeeper) metaKeeperField.get(agent);
    final PipePluginMeta recordedPlugin = metaKeeper.getPipePluginMeta("FAILED");

    Assert.assertEquals(
        "IOException: missing jar", recordedPlugin.getPluginLoadingExceptionMessage());
    Assert.assertEquals(
        Visibility.BOTH, metaKeeper.getPipePluginNameToVisibilityMap().get("FAILED"));
  }

  private static PipePluginMeta pipePluginMeta(final String pluginName, final String jarName) {
    return new PipePluginMeta(pluginName, "test.class", false, jarName, "test-md5");
  }

  private static TGetJarInListResp successResponse(final List<ByteBuffer> jarList) {
    return new TGetJarInListResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  private static TGetJarInListResp failureResponse() {
    return new TGetJarInListResp(
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()),
        Collections.emptyList());
  }
}
