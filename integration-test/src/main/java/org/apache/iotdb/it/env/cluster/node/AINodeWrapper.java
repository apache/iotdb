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

import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.AI_NODE_NAME;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PYTHON_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getTimeForLogDirectory;

public class AINodeWrapper extends AbstractNodeWrapper {

  private static final Logger logger = IoTDBTestLogger.logger;
  private final long startTime;
  private final String seedConfigNode;

  private static final String SCRIPT_FILE = "start-ainode.sh";

  private static final String SHELL_COMMAND = "bash";

  private static final String PROPERTIES_FILE = "iotdb-ainode.properties";
  public static final String CONFIG_PATH = "conf";
  public static final String SCRIPT_PATH = "sbin";

  private void replaceAttribute(String[] keys, String[] values, String filePath) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      for (int i = 0; i < keys.length; i++) {
        String line = keys[i] + "=" + values[i];
        writer.newLine();
        writer.write(line);
      }
    } catch (IOException e) {
      logger.error(
          "Failed to set attribute for AINode in file: {} because {}", filePath, e.getMessage());
    }
  }

  public AINodeWrapper(
      String seedConfigNode,
      String testClassName,
      String testMethodName,
      int clusterIndex,
      int[] port,
      long startTime) {
    super(testClassName, testMethodName, port, clusterIndex, false, startTime);
    this.seedConfigNode = seedConfigNode;
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
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      String filePrefix =
          System.getProperty(USER_DIR)
              + File.separator
              + TARGET
              + File.separator
              + AI_NODE_NAME
              + getPort();
      String propertiesFile =
          filePrefix + File.separator + CONFIG_PATH + File.separator + PROPERTIES_FILE;

      // set attribute
      replaceAttribute(
          new String[] {"ain_seed_config_node", "ain_inference_rpc_port"},
          new String[] {this.seedConfigNode, Integer.toString(getPort())},
          propertiesFile);

      // start AINode
      List<String> startCommand = new ArrayList<>();
      startCommand.add(SHELL_COMMAND);
      startCommand.add(filePrefix + File.separator + SCRIPT_PATH + File.separator + SCRIPT_FILE);
      startCommand.add("-i");
      startCommand.add(filePrefix + File.separator + PYTHON_PATH);
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
