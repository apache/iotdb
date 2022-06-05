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

import org.apache.iotdb.itbase.env.BaseNode;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;

public abstract class ClusterNodeBase implements BaseNode {
  private static final Logger logger = LoggerFactory.getLogger(ClusterNodeBase.class);

  private final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";

  private String id;
  private final String ip = "127.0.0.1";
  private int rpcPort;

  private String nodePath;
  private String scriptPath;
  private String logPath;

  private Process instance;

  protected void setId(String id) {
    this.id = id;
  }

  protected String getId() {
    return this.id;
  }

  protected void setPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  protected String getNodePath() {
    return this.nodePath;
  }

  protected void setNodePath(String nodePath) {
    this.nodePath = nodePath;
  }

  protected void setScriptPath(String scriptPath) {
    this.scriptPath = scriptPath;
  }

  protected void setLogPath(String logPath) {
    this.logPath = logPath;
  }

  protected int[] searchAvailablePorts() {
    String cmd = "lsof -iTCP -sTCP:LISTEN -P -n | awk '{print $9}' | grep -E ";
    boolean flag = true;
    int portStart = 10001;

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

      StringBuilder port = new StringBuilder(randomPortStart);
      for (int i = 0; i < 9; i++) {
        port.append("|");
        randomPortStart++;
        port.append(randomPortStart);
      }

      try {
        Process proc = Runtime.getRuntime().exec(cmd + "\"" + port + "\"");
        BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = br.readLine()) != null) {
          logger.debug(line);
        }
        if (proc.waitFor() == 1) {
          flag = false;
          portStart = randomPortStart - 9;
        }
      } catch (IOException | InterruptedException ex) {
        // ignore
      }
    } while (flag);

    return IntStream.rangeClosed(portStart, portStart + 9).toArray();
  }

  @Override
  public void createDir() {
    // Copy templateNodePath to nodePath
    try {
      FileUtils.copyDirectoryToDirectory(new File(this.templateNodePath), new File(this.nodePath));
      new File(this.scriptPath).setExecutable(true);
    } catch (IOException ex) {
      fail("Copy node dir failed. " + ex);
    }
  }

  @Override
  public void destroyDir() {
    // rm this.path
    try {
      FileUtils.forceDelete(new File(this.nodePath));
    } catch (IOException ex) {
      fail("Delete node dir failed. " + ex);
    }
  }

  @Override
  public void start() {
    try {
      ProcessBuilder processBuilder =
          new ProcessBuilder(this.scriptPath)
              .redirectOutput(new File("/dev/null"))
              .redirectError(new File("/dev/null"));
      processBuilder.environment().put("IT_LOG_PATH", this.logPath);
      processBuilder.environment().put("IT_LOG_LEVEL", "DEBUG");
      this.instance = processBuilder.start();
    } catch (IOException ex) {
      fail("Start node failed. " + ex);
    }
  }

  @Override
  public void stop() {
    this.instance.destroy();
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
  public String getIp() {
    return ip;
  }

  @Override
  public int getPort() {
    return rpcPort;
  }

  @Override
  public String getIpAndPortString() {
    return this.getIp() + ":" + this.rpcPort;
  }
}
