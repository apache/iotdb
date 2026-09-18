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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

abstract class AbstractPipeSingleIT {

  private static final String OPC_UA_SINK_CLASS_NAME =
      "org.apache.iotdb.pipe.plugin.sink.opcua.OpcUaSink";
  private static final String OPC_UA_SINK_JAR_PATH_PROPERTY = "OpcUaSinkPluginJar";
  private static final String OPC_UA_SINK_JAR_PREFIX = "opc-ua-sink-";
  private static final String OPC_UA_SINK_JAR_SUFFIX = "-jar-with-dependencies.jar";

  protected BaseEnv env;

  @Before
  public void setUp() throws Exception {
    MultiEnvFactory.createEnv(1);
    env = MultiEnvFactory.getEnv(0);
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    env.initClusterEnvironment();
  }

  protected final void registerOpcUaSinkPlugin() throws IOException {
    final String pluginUri = locateOpcUaSinkPluginJar().toUri().toString();
    registerOpcUaSinkPlugin(PipeSinkConstant.OPC_UA_SINK_NAME, pluginUri);
  }

  private void registerOpcUaSinkPlugin(final String pluginName, final String pluginUri) {
    TestUtils.executeNonQuery(
        env,
        String.format(
            "CREATE PIPEPLUGIN IF NOT EXISTS `%s` AS '%s' USING URI '%s'",
            pluginName, OPC_UA_SINK_CLASS_NAME, pluginUri));
  }

  private static Path locateOpcUaSinkPluginJar() throws IOException {
    final String configuredPluginJar = System.getProperty(OPC_UA_SINK_JAR_PATH_PROPERTY);
    if (configuredPluginJar != null && !configuredPluginJar.isEmpty()) {
      final Path pluginJar = Paths.get(configuredPluginJar).toAbsolutePath().normalize();
      if (Files.isRegularFile(pluginJar)) {
        return pluginJar;
      }
      throw new IOException("Cannot locate the OPC UA sink plugin jar at " + pluginJar + ".");
    }

    Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    while (current != null) {
      final Path targetDir =
          current.resolve("library-pipe").resolve("opc-ua-sink").resolve("target");
      if (Files.isDirectory(targetDir)) {
        try (final Stream<Path> stream = Files.list(targetDir)) {
          final List<Path> pluginJars =
              stream
                  .filter(Files::isRegularFile)
                  .filter(path -> path.getFileName().toString().startsWith(OPC_UA_SINK_JAR_PREFIX))
                  .filter(path -> path.getFileName().toString().endsWith(OPC_UA_SINK_JAR_SUFFIX))
                  .toList();
          if (pluginJars.size() == 1) {
            return pluginJars.get(0);
          }
          if (pluginJars.size() > 1) {
            throw new IOException(
                "Multiple OPC UA sink plugin jars found in "
                    + targetDir
                    + ". Set -D"
                    + OPC_UA_SINK_JAR_PATH_PROPERTY
                    + " to the expected jar.");
          }
        }
      }
      current = current.getParent();
    }
    throw new IOException("Cannot locate the OPC UA sink plugin jar.");
  }

  @After
  public final void tearDown() {
    env.cleanClusterEnvironment();
  }
}
