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

import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.*;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.ML_NODE_NAME;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getTimeForLogDirectory;

public class AINodeWrapper implements BaseNodeWrapper {

  private static final Logger logger = IoTDBTestLogger.logger;
  private final String testClassName;
  private final String testMethodName;
  private final String nodeAddress;
  private Process instance;
  private final int nodePort;
  private final long startTime;
  private final String seedConfigNode;

  private static final String SCRIPT_FILE = "start-ainode.sh";

  private static final String SHELL_COMMAND = "bash";

  private static final String PROPERTIES_FILE = "iotdb-ainode.properties";
  public static final String CONFIG_PATH = "conf";
  public static final String SCRIPT_PATH = "sbin";
  private static final String INTERPRETER_PATH = "/root/.venv/bin/python3";

  private String[] getReplaceCmd(String key, String value, String filePath) {
    String s = String.format("sed -i s/^%s=.*/%s=%s/ %s", key, key, value, filePath);
    return s.split("\\s+");
  }

  private void replaceAttribute(String key, String value, String filePath, File stdoutFile)
      throws IOException, InterruptedException {
    ProcessBuilder prepareProcessBuilder =
        new ProcessBuilder(getReplaceCmd(key, value, filePath))
            .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
            .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
    Process prepareProcess = prepareProcessBuilder.start();
    prepareProcess.waitFor();
  }

  public MLNodeWrapper(
      String seedConfigNode,
      String testClassName,
      String testMethodName,
      int[] port,
      long startTime) {
    this.seedConfigNode = seedConfigNode;
    this.testClassName = testClassName;
    this.testMethodName = testMethodName;
    this.nodeAddress = "127.0.0.1";
    this.nodePort = port[0];
    this.startTime = startTime;
  }

  @Override
  public void createNodeDir() {
    // Copy templateNodePath to nodePath
    String destPath = getNodePath();
    try {
      try {
        if (new File(destPath).exists()) {
          PathUtils.deleteDirectory(Paths.get(destPath));
        }
      } catch (NoSuchFileException e) {
        // ignored
      }
      // Here we need to copy without follow symbolic links, so we can't use FileUtils directly.
      try (Stream<Path> s = Files.walk(Paths.get(TEMPLATE_NODE_PATH))) {
        s.forEach(
            source -> {
              Path destination =
                  Paths.get(destPath, source.toString().substring(TEMPLATE_NODE_PATH.length()));
              try {
                Files.copy(
                    source,
                    destination,
                    LinkOption.NOFOLLOW_LINKS,
                    StandardCopyOption.COPY_ATTRIBUTES);
              } catch (IOException e) {
                logger.error("Got error copying files to node dest dir", e);
                throw new RuntimeException(e);
              }
            });
      }
    } catch (IOException ex) {
      logger.error("Copy node dir failed", ex);
      throw new AssertionError();
    }
  }

  @Override
  public void createLogDir() {
    try {
      // Make sure the log dir exist, as the first file is output by starting script directly.
      FileUtils.createParentDirectories(new File(getLogPath()));
    } catch (IOException ex) {
      logger.error("Copy node dir failed", ex);
      throw new AssertionError();
    }
  }

  private String getLogPath() {
    return getLogDirPath() + File.separator + getId() + ".log";
  }

  private String getLogDirPath() {
    return System.getProperty(USER_DIR)
        + File.separator
        + TARGET
        + File.separator
        + "mlnode-logs"
        + File.separator
        + getTestLogDirName()
        + File.separator
        + getTimeForLogDirectory(startTime);
  }

  private String getTestLogDirName() {
    if (testMethodName == null) {
      return testClassName;
    }
    return testClassName + "_" + testMethodName;
  }

  @Override
  public void destroyDir() {
    Exception lastException = null;
    for (int i = 0; i < 10; i++) {
      try {
        PathUtils.deleteDirectory(Paths.get(getNodePath()));
        return;
      } catch (IOException ex) {
        lastException = ex;
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Delete node dir failed. " + e);
        }
      }
    }
    logger.error(lastException.getMessage());
    throw new AssertionError("Delete node dir failed.");
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
              + ML_NODE_NAME
              + getPort();
      String propertiesFile =
          filePrefix + File.separator + CONFIG_PATH + File.separator + PROPERTIES_FILE;

      // set attribute
      replaceAttribute(
          "mln_target_config_node_list", this.seedConfigNode, propertiesFile, stdoutFile);
      replaceAttribute(
          "mln_inference_rpc_port", Integer.toString(getPort()), propertiesFile, stdoutFile);

      // start MLNode
      List<String> start_command = new ArrayList<>();
      start_command.add(SHELL_COMMAND);
      start_command.add(filePrefix + File.separator + SCRIPT_PATH + File.separator + SCRIPT_FILE);
      start_command.add("-i");
      start_command.add(INTERPRETER_PATH);
      start_command.add("-r");
      start_command.add("-n");
      ProcessBuilder processBuilder =
          new ProcessBuilder(start_command)
              .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
              .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
      this.instance = processBuilder.start();
      logger.info("In test {} {} started.", getTestLogDirName(), getId());
    } catch (IOException | InterruptedException e) {
      throw new AssertionError("Start ML Node failed. " + e + Paths.get(""));
    }
  }

  @Override
  public void stop() {
    if (this.instance == null) {
      return;
    }
    this.instance.destroy();
    try {
      if (!this.instance.waitFor(20, TimeUnit.SECONDS)) {
        this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Waiting ML Node to shutdown error. %s", e);
    }
  }

  @Override
  public String getIp() {
    return this.nodeAddress;
  }

  @Override
  public int getPort() {
    return this.nodePort;
  }

  @Override
  public int getMetricPort() {
    // no metric currently
    return -1;
  }

  @Override
  public String getId() {
    return ML_NODE_NAME + getPort();
  }

  @Override
  public String getIpAndPortString() {
    return this.getIp() + ":" + this.getPort();
  }

  @Override
  public void dumpJVMSnapshot(String testCaseName) {
    // there is no JVM to dump for MLNode
  }

  private String getNodePath() {
    return System.getProperty(USER_DIR) + File.separator + TARGET + File.separator + getId();
  }
}
