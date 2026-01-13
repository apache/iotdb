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

package org.apache.iotdb.it.env.cluster.node;

import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.apache.tsfile.external.commons.io.file.PathUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.AI_NODE_NAME;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getTimeForLogDirectory;

public class AINodeWrapper extends AbstractNodeWrapper {

  private static final Logger logger = IoTDBTestLogger.logger;
  private final long startTime;
  private final String seedConfigNode;
  private final int clusterIngressPort;

  private static final String SCRIPT_FILE = "start-ainode.sh";

  private static final String SHELL_COMMAND = "bash";

  private static final String PROPERTIES_FILE = "iotdb-ainode.properties";
  public static final String CONFIG_PATH = "conf";
  public static final String SCRIPT_PATH = "sbin";
  public static final String BUILT_IN_MODEL_PATH = "data/ainode/models/builtin";
  public static final String CACHE_BUILT_IN_MODEL_PATH = "/data/ainode/models";

  private void replaceAttribute(String[] keys, String[] values, String filePath) {
    Properties props = new Properties();
    try (FileInputStream in = new FileInputStream(filePath)) {
      props.load(in);
    } catch (IOException e) {
      logger.warn("Failed to load existing AINode properties from {}, because: ", filePath, e);
    }
    for (int i = 0; i < keys.length; i++) {
      props.setProperty(keys[i], values[i]);
    }
    try (FileOutputStream out = new FileOutputStream(filePath)) {
      props.store(out, "Updated by AINode integration-test env");
    } catch (IOException e) {
      logger.error("Failed to save properties to {}, because:", filePath, e);
    }
  }

  public AINodeWrapper(
      String seedConfigNode,
      int clusterIngressPort,
      String testClassName,
      String testMethodName,
      int clusterIndex,
      int[] port,
      long startTime) {
    super(testClassName, testMethodName, port, clusterIndex, false, startTime);
    this.seedConfigNode = seedConfigNode;
    this.clusterIngressPort = clusterIngressPort;
    this.startTime = startTime;
  }

  @Override
  public String getLogDirPath() {
    return System.getProperty(USER_DIR)
        + File.separator
        + TARGET
        + File.separator
        + "ainode-logs"
        + File.separator
        + getTestLogDirName()
        + File.separator
        + getTimeForLogDirectory(startTime);
  }

  @Override
  String getNodeType() {
    return "ainode";
  }

  @Override
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      String filePrefix = getNodePath();
      String propertiesFile =
          filePrefix + File.separator + CONFIG_PATH + File.separator + PROPERTIES_FILE;

      // set attribute
      replaceAttribute(
          new String[] {"ain_seed_config_node", "ain_rpc_port", "ain_cluster_ingress_port"},
          new String[] {
            this.seedConfigNode,
            Integer.toString(getPort()),
            Integer.toString(this.clusterIngressPort)
          },
          propertiesFile);

      // copy built-in LTSM
      String builtInModelPath = filePrefix + File.separator + BUILT_IN_MODEL_PATH;
      new File(builtInModelPath).mkdirs();
      try {
        if (new File(builtInModelPath).exists()) {
          PathUtils.deleteDirectory(Paths.get(builtInModelPath));
        }
      } catch (NoSuchFileException e) {
        // ignored
      }
      try (Stream<Path> s = Files.walk(Paths.get(CACHE_BUILT_IN_MODEL_PATH))) {
        s.forEach(
            source -> {
              Path destination =
                  Paths.get(
                      builtInModelPath,
                      source.toString().substring(CACHE_BUILT_IN_MODEL_PATH.length()));
              logger.info("AINode copying model weights from {} to {}", source, destination);
              try {
                Files.copy(
                    source,
                    destination,
                    LinkOption.NOFOLLOW_LINKS,
                    StandardCopyOption.COPY_ATTRIBUTES);
              } catch (IOException e) {
                logger.error("AINode got error copying model weights", e);
                throw new RuntimeException(e);
              }
            });
      } catch (Exception e) {
        logger.error("AINode got error copying model weights", e);
      }

      // start AINode
      List<String> startCommand = new ArrayList<>();
      startCommand.add(SHELL_COMMAND);
      startCommand.add(filePrefix + File.separator + SCRIPT_PATH + File.separator + SCRIPT_FILE);
      startCommand.add("-r");

      ProcessBuilder processBuilder =
          new ProcessBuilder(startCommand)
              .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
              .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
      this.instance = processBuilder.start();
      logger.info("In test {} {} started.", getTestLogDirName(), getId());
    } catch (Exception e) {
      throw new AssertionError("Start AI Node failed. " + e + Paths.get(""));
    }
  }

  @Override
  public int getMetricPort() {
    // no metric currently
    return -1;
  }

  @Override
  public String getId() {
    return AI_NODE_NAME + getPort();
  }

  /* Abstract methods, which must be implemented in ConfigNode and DataNode. */
  public void reloadMutableFields() {}
  ;

  public void renameFile() {}
  ;

  public String getSystemConfigPath() {
    return "";
  }
  ;

  /** Return the node config file path specified through system variable */
  public String getDefaultNodeConfigPath() {
    return "";
  }
  ;

  /** Return the common config file path specified through system variable */
  public String getDefaultCommonConfigPath() {
    return "";
  }
  ;

  public void addStartCmdParams(List<String> params) {}
  ;

  public String getSystemPropertiesPath() {
    return "";
  }
  ;

  public MppJVMConfig initVMConfig() {
    return null;
  }
  ;
}
