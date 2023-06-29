package org.apache.iotdb.db.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class PipePluginAgentTest {
  private static final String TMP_DIR = "PipePluginAgentTest_libroot";

  @Before
  public void before() {
    try {
      Files.createDirectory(Paths.get(TMP_DIR));
      PipePluginClassLoaderManager.setupAndGetInstance(TMP_DIR);
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @After
  public void after() {
    try {
      Files.deleteIfExists(Paths.get(TMP_DIR));
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipePluginAgent() {
    PipePluginAgent agent = new PipePluginAgent();
    try {
      agent.register(
          new PipePluginMeta(
              "plugin-name",
              "org.apache.iotdb.db.pipe.extractor.IoTDBDataRegionExtractor",
              false,
              "jar",
              "md5"),
          null);
      agent.deregister("plugin-name", false);
    } catch (Exception e) {
      Assert.fail();
    }
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginClass(),
        agent
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
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginClass(),
        agent
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
