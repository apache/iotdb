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
package org.apache.iotdb.it.env;

import org.apache.iotdb.itbase.env.BaseNodeWrapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {
  private static final Logger logger = LoggerFactory.getLogger(AbstractNodeWrapper.class);
  private final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";
  private final File NULL_FILE =
      SystemUtils.IS_OS_WINDOWS ? new File("nul") : new File("/dev/null");

  private Process instance;
  protected final String testName;

  public AbstractNodeWrapper(String testName) {
    this.testName = testName;
  }

  protected final String getId() {
    return "node" + getPort();
  }

  protected final int[] searchAvailablePorts() {
    do {
      int randomPortStart = 1000 + (int) (Math.random() * (1999 - 1000));
      randomPortStart = randomPortStart * 10 + 1;
      String path =
          System.getProperty("user.dir")
              + File.separator
              + "target"
              + File.separator
              + "node"
              + randomPortStart;
      if (new File(path).exists()) {
        continue;
      }

      List<Integer> requiredPorts =
          IntStream.rangeClosed(randomPortStart, randomPortStart + 9)
              .boxed()
              .collect(Collectors.toList());
      String cmd = getSearchAvailablePortCmd(requiredPorts);

      try {
        Process proc = Runtime.getRuntime().exec(cmd);
        BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = br.readLine()) != null) {
          logger.debug(line);
        }
        if (proc.waitFor() == 1) {
          return requiredPorts.stream().mapToInt(Integer::intValue).toArray();
        }
      } catch (IOException | InterruptedException ex) {
        // ignore
      }
    } while (true);
  }

  private String getSearchAvailablePortCmd(List<Integer> ports) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return getWindowsSearchPortCmd(ports);
    }
    return getUnixSearchPortCmd(ports);
  }

  private String getWindowsSearchPortCmd(List<Integer> ports) {
    String cmd = "netstat -aon -p tcp | findStr ";
    return cmd
        + ports.stream().map(v -> "/C:'127.0.0.1:" + v + "'").collect(Collectors.joining(" "));
  }

  private String getUnixSearchPortCmd(List<Integer> ports) {
    String cmd = "lsof -iTCP -sTCP:LISTEN -P -n | awk '{print $9}' | grep -E ";
    return cmd + ports.stream().map(String::valueOf).collect(Collectors.joining("|")) + "\"";
  }

  @Override
  public void createDir() {
    // Copy templateNodePath to nodePath
    try {
      FileUtils.copyDirectoryToDirectory(new File(this.templateNodePath), new File(getNodePath()));
      String startScriptPath = getStartScriptPath();
      String stopScriptPath = getStopScriptPath();
      if (!new File(startScriptPath).setExecutable(true)) {
        logger.error("Change {} to executable failed.", startScriptPath);
      }
      if (!new File(stopScriptPath).setExecutable(true)) {
        logger.error("Change {} to executable failed.", stopScriptPath);
      }
      // Make sure the log dir exist, as the first file is output by starting script directly.
      FileUtils.createParentDirectories(new File(getLogPath()));
    } catch (IOException ex) {
      fail("Copy node dir failed. " + ex);
    }
  }

  @Override
  public void destroyDir() {
    for (int i = 0; i < 3; i++) {
      try {
        FileUtils.forceDelete(new File(getNodePath()));
        return;
      } catch (IOException ex) {
        logger.error("Delete node dir failed. RetryTimes={}", i + 1, ex);
        try {
          TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
          fail("Delete node dir failed. " + e);
        }
      }
    }
    fail("Delete node dir failed.");
  }

  @Override
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      ProcessBuilder processBuilder =
          new ProcessBuilder(getStartScriptPath())
              .redirectOutput(stdoutFile)
              .redirectError(stdoutFile);
      this.instance = processBuilder.start();
    } catch (IOException ex) {
      fail("Start node failed. " + ex);
    }
  }

  @Override
  public void stop() {
    if (this.instance == null) {
      return;
    }
    this.instance.destroy();
    // In Windows, the IoTDB process is started as a subprocess of the original batch script with a
    // new pid, so we need to kill the new subprocess as well.
    if (SystemUtils.IS_OS_WINDOWS) {
      ProcessBuilder processBuilder =
          new ProcessBuilder(getStopScriptPath())
              .redirectOutput(NULL_FILE)
              .redirectError(NULL_FILE);
      processBuilder.environment().put("CONSOLE_LOG_LEVEL", "DEBUG");
      Process p = null;
      try {
        p = processBuilder.start();
        p.waitFor(5, TimeUnit.SECONDS);
      } catch (IOException | InterruptedException e) {
        logger.error("Stop instance in Windows failed", e);
        if (p != null) {
          p.destroyForcibly();
        }
      }
    }
  }

  @Override
  public void waitingToShutDown() {
    while (this.instance.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.error("Waiting node to shutdown." + e);
      }
    }
  }

  @Override
  public void changeConfig(Properties properties) {
    try {
      String configPath = getConfigPath();
      Properties configProperties = new Properties();
      try (InputStream confInput = Files.newInputStream(Paths.get(configPath))) {
        configProperties.load(confInput);
      }
      updateConfig(configProperties);
      if (properties != null && !properties.isEmpty()) {
        configProperties.putAll(properties);
      }
      try (FileWriter confOutput = new FileWriter(configPath)) {
        configProperties.store(confOutput, null);
      }
    } catch (IOException ex) {
      fail("Change the config of data node failed. " + ex);
    }
  }

  @Override
  public final String getIp() {
    return "127.0.0.1";
  }

  @Override
  public final String getIpAndPortString() {
    return this.getIp() + ":" + this.getPort();
  }

  protected String workDirFilePath(String dirName, String fileName) {
    return getNodePath()
        + File.separator
        + "template-node"
        + File.separator
        + dirName
        + File.separator
        + fileName;
  }

  protected abstract String getConfigPath();

  protected abstract void updateConfig(Properties properties);

  protected abstract String getStartScriptPath();

  protected abstract String getStopScriptPath();

  protected abstract String getLogPath();

  protected String getNodePath() {
    return System.getProperty("user.dir") + File.separator + "target" + File.separator + getId();
  }
}
