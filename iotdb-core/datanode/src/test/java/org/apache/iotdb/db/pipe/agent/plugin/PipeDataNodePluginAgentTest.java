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

import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class PipeDataNodePluginAgentTest {
  private static final String TMP_LIB_ROOT_DIR = "PipePluginAgentTest_libroot";
  private static final String TMP_TEMP_LIB_ROOT_DIR = "PipePluginAgentTest_temporarylibroot";
  private static final PipePluginMeta PIPE_PLUGIN_META =
      new PipePluginMeta(
          "PLUGIN-NAME",
          "org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor",
          false,
          "IoTDBDataRegionExtractor.jar",
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
      Files.deleteIfExists(Paths.get(pluginPath));
      Files.deleteIfExists(Paths.get(PipePluginExecutableManager.getInstance().getInstallDir()));
      Files.deleteIfExists(Paths.get(TMP_TEMP_LIB_ROOT_DIR));
      Files.deleteIfExists(Paths.get(TMP_LIB_ROOT_DIR));
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
        IoTDBDataRegionExtractor.class,
        agent
            .dataRegion()
            .reflectExtractor(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeExtractorConstant.EXTRACTOR_KEY,
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
        IoTDBDataRegionAsyncConnector.class,
        agent
            .dataRegion()
            .reflectConnector(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(
                            PipeConnectorConstant.CONNECTOR_KEY,
                            BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName());
                      }
                    }))
            .getClass());
  }
}
