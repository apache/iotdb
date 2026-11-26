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

package org.apache.iotdb.db.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.agent.plugin.PipePluginAgent;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.db.pipe.processor.iotconsensusv2.IoTConsensusV2Processor;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.IoTConsensusV2AsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreatePipeTask;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class PipeDataNodePluginAgentTest {
  private static final String TMP_LIB_ROOT_DIR = "PipePluginAgentTest_libroot";
  private static final String TMP_TEMP_LIB_ROOT_DIR = "PipePluginAgentTest_temporarylibroot";
  private static final PipePluginMeta PIPE_PLUGIN_META =
      new PipePluginMeta(
          "PLUGIN-NAME",
          "org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource",
          false,
          "IoTDBDataRegionESource.jar",
          "md5");

  @Before
  public void before() {
    try {
      PipePluginExecutableManager.setupAndGetInstance(TMP_LIB_ROOT_DIR, TMP_TEMP_LIB_ROOT_DIR);
      PipePluginClassLoaderManager.setupAndGetInstance(TMP_LIB_ROOT_DIR);
      String pluginPath =
          PipePluginExecutableManager.getInstance()
              .getPluginsDirPath(PIPE_PLUGIN_META.getPluginName());
      Files.createDirectories(Paths.get(pluginPath));
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @After
  public void after() {
    try {
      String pluginPath =
          PipePluginExecutableManager.getInstance()
              .getPluginsDirPath(PIPE_PLUGIN_META.getPluginName());
      EnvironmentUtils.cleanDir(pluginPath);
      EnvironmentUtils.cleanDir(PipePluginExecutableManager.getInstance().getInstallDir());
      EnvironmentUtils.cleanDir(TMP_TEMP_LIB_ROOT_DIR);
      EnvironmentUtils.cleanDir(TMP_LIB_ROOT_DIR);
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipePluginAgent() {
    PipeDataNodePluginAgent agent = new PipeDataNodePluginAgent();

    try {
      agent.register(PIPE_PLUGIN_META, null);
      agent.deregister(PIPE_PLUGIN_META.getPluginName(), true);
    } catch (Exception e) {
      Assert.fail();
    }
    Assert.assertEquals(
        IoTDBDataRegionSource.class,
        agent
            .dataRegion()
            .reflectSource(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeSourceConstant.EXTRACTOR_KEY,
                            BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName());
                      }
                    }))
            .getClass());
    Assert.assertEquals(
        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginClass(),
        agent
            .dataRegion()
            .reflectProcessor(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeProcessorConstant.PROCESSOR_KEY,
                            BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName());
                      }
                    }))
            .getClass());
    Assert.assertEquals(
        IoTConsensusV2Processor.class,
        agent
            .dataRegion()
            .reflectProcessor(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeProcessorConstant.PROCESSOR_KEY,
                            BuiltinPipePlugin.PIPE_CONSENSUS_PROCESSOR.getPipePluginName());
                      }
                    }))
            .getClass());
    Assert.assertEquals(
        IoTDBDataRegionAsyncSink.class,
        agent
            .dataRegion()
            .reflectSink(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeSinkConstant.CONNECTOR_KEY,
                            BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName());
                      }
                    }))
            .getClass());
    Assert.assertEquals(
        IoTDBDataRegionAsyncSink.class,
        agent.dataRegion().reflectSink(new PipeParameters(new HashMap<>())).getClass());
    Assert.assertEquals(
        IoTDBDataRegionSyncSink.class,
        agent
            .dataRegion()
            .reflectSink(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(PipeSinkConstant.CONNECTOR_SERIALIZE_BY_REGION_KEY, "true");
                      }
                    }))
            .getClass());
    Assert.assertEquals(
        IoTConsensusV2AsyncSink.class,
        agent
            .dataRegion()
            .reflectSink(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeSinkConstant.CONNECTOR_KEY,
                            BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName());
                      }
                    }))
            .getClass());
  }

  @Test
  public void testBuiltInAndCustomParametersAreNormalized() {
    final PipeDataNodePluginAgent agent = new PipeDataNodePluginAgent();
    final PipeParameters builtInParameters =
        new PipeParameters(
            Map.of(
                " SOURCE ", " IOTDB-EXTRACTOR ",
                " Source.Path ", " ROOT.SG.** ",
                " Source.Mode ", " QUERY ",
                " Source.Custom.Option ", " Mixed Value "));
    final PipeParameters normalizedParameters =
        PipePluginAgent.normalizeSourceParameters(builtInParameters);

    Assert.assertNotSame(builtInParameters, normalizedParameters);
    Assert.assertEquals("iotdb-extractor", normalizedParameters.getString("source"));
    Assert.assertEquals(" ROOT.SG.** ", normalizedParameters.getString("source.path"));
    Assert.assertEquals("query", normalizedParameters.getString("source.mode"));
    Assert.assertEquals("mixed value", normalizedParameters.getString("source.custom.option"));
    Assert.assertEquals("iotdb-extractor", normalizedParameters.getAttribute().get("source"));

    final PipeParameters customParameters =
        new PipeParameters(
            Map.of(" SOURCE ", " CUSTOM-PLUGIN ", " Custom.Option ", " Mixed Value "));
    final PipeParameters normalizedCustomParameters =
        PipePluginAgent.normalizeSourceParameters(customParameters);
    Assert.assertEquals(" CUSTOM-PLUGIN ", normalizedCustomParameters.getString("source"));
    Assert.assertEquals(
        " Mixed Value ", normalizedCustomParameters.getAttribute().get(" Custom.Option "));
    final PipeParameters existingBuiltInSourceParameters =
        new PipeParameters(Map.of("source", "iotdb-extractor"));
    final PipeParameters updatedBuiltInSourceParameters =
        PipePluginAgent.normalizeSourceParametersForUpdate(
            new PipeParameters(Map.of(" SOURCE.PATH ", " root.sg.** ", " SOURCE.MODE ", " QUERY ")),
            existingBuiltInSourceParameters);
    Assert.assertEquals(" root.sg.** ", updatedBuiltInSourceParameters.getString("source.path"));
    Assert.assertEquals("query", updatedBuiltInSourceParameters.getString("source.mode"));
    final PipeParameters existingCustomSourceParameters =
        new PipeParameters(Map.of("source", "custom-plugin"));
    final PipeParameters updatedCustomSourceParameters =
        new PipeParameters(Map.of(" Custom.Option ", " Mixed Value "));
    final PipeParameters normalizedUpdatedCustomSourceParameters =
        PipePluginAgent.normalizeSourceParametersForUpdate(
            updatedCustomSourceParameters, existingCustomSourceParameters);
    Assert.assertNotSame(updatedCustomSourceParameters, normalizedUpdatedCustomSourceParameters);
    Assert.assertEquals(
        " Mixed Value ",
        normalizedUpdatedCustomSourceParameters.getAttribute().get(" Custom.Option "));
    Assert.assertEquals(
        "IOTDB-THRIFT-CONNECTOR",
        PipeSinkConstant.getConnectorOrSinkNameWithDefault(
            new PipeParameters(Map.of(" CONNECTOR ", " IOTDB-THRIFT-CONNECTOR "))));
    final PipeParameters replacementSourceParameters =
        PipePluginAgent.normalizeSourceParametersForUpdate(
            new PipeParameters(Map.of(" SOURCE.MODE ", " QUERY ")),
            existingCustomSourceParameters,
            true);
    Assert.assertEquals("query", replacementSourceParameters.getString("source.mode"));
    final PipeParameters modifiedCustomSourceParameters =
        PipePluginAgent.normalizeSourceParametersForUpdate(
            new PipeParameters(Map.of(" SOURCE.MODE ", " QUERY ")), existingCustomSourceParameters);
    Assert.assertEquals(
        " QUERY ", modifiedCustomSourceParameters.getAttribute().get(" SOURCE.MODE "));
    Assert.assertTrue(
        PipeSinkConstant.isSerializeByRegionEnabled(
            new PipeParameters(Map.of(" CONNECTOR.SERIALIZE-BY-REGION ", " TRUE "))));
    Assert.assertTrue(
        PipeSinkConstant.isSerializeByRegionEnabled(
            new PipeParameters(Map.of(" SERIALIZE-BY-REGION ", " TRUE "))));
    final PipeParameters normalizedProcessorParameters =
        PipePluginAgent.normalizeProcessorParameters(
            new PipeParameters(
                Map.of(
                    " PROCESSOR ",
                    BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName().toUpperCase(),
                    " Processor.Mode ",
                    " ON ")));
    Assert.assertEquals(
        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName(),
        normalizedProcessorParameters.getString("processor"));
    Assert.assertEquals("on", normalizedProcessorParameters.getString("processor.mode"));
    final PipeParameters normalizedSinkParameters =
        PipePluginAgent.normalizeSinkParameters(
            new PipeParameters(
                Map.of(
                    " CONNECTOR ",
                    " IOTDB-THRIFT-CONNECTOR ",
                    " Connector.User ",
                    " ROOT ",
                    " Connector.Node-URLs ",
                    " https://NODEA:6667,NodeB:6667 ",
                    " Connector.Host ",
                    " HOST.EXAMPLE.COM ",
                    " Connector.IP ",
                    " 2001:DB8::1 ",
                    " Connector.Password ",
                    " P@SSword ",
                    " Connector.Ssl.Trust-Store-Path ",
                    " C:\\CERTS\\STORE.JKS ",
                    " Connector.Ssl.Enable ",
                    " TRUE ")));
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
        normalizedSinkParameters.getString("connector"));
    Assert.assertEquals(" ROOT ", normalizedSinkParameters.getString("connector.user"));
    Assert.assertEquals(
        " https://NODEA:6667,NodeB:6667 ",
        normalizedSinkParameters.getString(PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY));
    Assert.assertEquals(" HOST.EXAMPLE.COM ", normalizedSinkParameters.getString("connector.host"));
    Assert.assertEquals(" 2001:DB8::1 ", normalizedSinkParameters.getString("connector.ip"));
    Assert.assertEquals(" P@SSword ", normalizedSinkParameters.getString("connector.password"));
    Assert.assertEquals(
        " C:\\CERTS\\STORE.JKS ",
        normalizedSinkParameters.getString(
            PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY));
    Assert.assertEquals("true", normalizedSinkParameters.getString("connector.ssl.enable"));

    final PipeParameters normalizedSubscriptionSinkParameters =
        PipePluginAgent.normalizeSinkParameters(
            new PipeParameters(
                Map.of(
                    " CONNECTOR ", " SUBSCRIPTION-SINK ",
                    " Sink.Topic ", " MixedTopic ",
                    " Sink.Consumer-Group ", " MixedGroup ")));
    Assert.assertEquals(
        " MixedTopic ", normalizedSubscriptionSinkParameters.getString("sink.topic"));
    Assert.assertEquals(
        " MixedGroup ", normalizedSubscriptionSinkParameters.getString("sink.consumer-group"));
  }

  @Test
  public void testShowCreatePipeDisplaysNormalizedParameters() {
    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "normalizedPipe",
                1L,
                Map.of(
                    " SOURCE ", " IOTDB-EXTRACTOR ",
                    " Source.Path ", " ROOT.SG.** ",
                    " Source.Mode ", " QUERY ",
                    " Source.Custom.Option ", " Mixed Value "),
                Map.of(
                    " PROCESSOR ", " DO-NOTHING-PROCESSOR ",
                    " Processor.Mode ", " ON "),
                Map.of(
                    " CONNECTOR ", " IOTDB-THRIFT-CONNECTOR ",
                    " Connector.Custom.Option ", " Mixed Value ",
                    " Connector.Node-URLs ", " https://NODEA:6667,NodeB:6667 ")),
            new PipeRuntimeMeta());

    final String showCreate = ShowCreatePipeTask.getShowCreatePipeSQL(pipeMeta);

    Assert.assertTrue(showCreate.contains("'source.path'=' ROOT.SG.** '"));
    Assert.assertTrue(showCreate.contains("'source.mode'='query'"));
    Assert.assertTrue(showCreate.contains("'source.custom.option'='mixed value'"));
    Assert.assertTrue(showCreate.contains("'processor.mode'='on'"));
    Assert.assertTrue(showCreate.contains("'connector.custom.option'='mixed value'"));
    Assert.assertTrue(
        showCreate.contains("'connector.node-urls'=' https://NODEA:6667,NodeB:6667 '"));
  }

  @Test
  public void testPluginLoadFailureIsRecordedForShowPipePlugins() throws Exception {
    final PipeDataNodePluginAgent agent = new PipeDataNodePluginAgent();
    final PipePluginMeta plugin =
        new PipePluginMeta("failed", "test.class", false, "failed.jar", "test-md5");

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
}
