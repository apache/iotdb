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
package org.apache.iotdb.integration.env;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class ClusterNode {
  private final String id;
  private final String path;
  private final String ip;
  private final int rpcPort;
  private final int metaPort;
  private final int dataPort;
  private final int[] metaPortArray;

  private Process instance;

  public ClusterNode(
      String id, String ip, int rpcPort, int metaPort, int dataPort, int[] metaPortArray) {
    this.id = id;
    this.path =
        System.getProperty("user.dir") + File.separator + "target" + File.separator + this.id;
    this.ip = ip;
    this.rpcPort = rpcPort;
    this.metaPort = metaPort;
    this.dataPort = dataPort;
    this.metaPortArray = metaPortArray;
  }

  public void createDir() {
    // cp template-node to this.path

    String srcPath =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "template-node";
    try {
      FileUtils.copyDirectoryToDirectory(new File(srcPath), new File(this.path));
      new File(workDirFilePath("sbin", "start-node.sh")).setExecutable(true);
      new File(workDirFilePath("sbin", "stop-node.sh")).setExecutable(true);
    } catch (IOException ex) {
      // If copying failed, kill the process and retry once again.
      ex.printStackTrace();
      try {
        stop();
        destroyDir();
        FileUtils.copyDirectoryToDirectory(new File(srcPath), new File(this.path));
        new File(workDirFilePath("sbin", "start-node.sh")).setExecutable(true);
        new File(workDirFilePath("sbin", "stop-node.sh")).setExecutable(true);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Copy cluster node dir failed. ");
      }
    }
  }

  public void destroyDir() {
    // rm this.path
    try {
      FileUtils.forceDelete(new File(this.path));
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public void changeConfig(Properties engineProperties, Properties clusterProperties) {
    String clusterConfigPath = workDirFilePath("conf", "iotdb-cluster.properties");
    String engineConfigPath = workDirFilePath("conf", "iotdb-engine.properties");
    try {
      // iotdb-cluster.properties part
      Properties clusterConfig = new Properties();
      FileInputStream clusterConfInput = new FileInputStream(clusterConfigPath);
      clusterConfig.load(clusterConfInput);
      clusterConfInput.close();
      StringBuilder objString = new StringBuilder("127.0.0.1:" + metaPortArray[0]);
      for (int i = 1; i < metaPortArray.length; i++) {
        objString.append(",127.0.0.1:").append(metaPortArray[i]);
      }
      clusterConfig.setProperty("seed_nodes", objString.toString());
      clusterConfig.setProperty("enable_auto_create_schema", "true");
      clusterConfig.setProperty("internal_meta_port", String.valueOf(this.metaPort));
      clusterConfig.setProperty("internal_data_port", String.valueOf(this.dataPort));
      clusterConfig.setProperty("consistency_level", "strong");
      clusterConfig.setProperty("cluster_info_public_port", String.valueOf(this.rpcPort - 100));
      clusterConfig.putAll(clusterProperties);
      FileWriter clusterConfOutput = new FileWriter(clusterConfigPath);
      clusterConfig.store(clusterConfOutput, null);
      clusterConfOutput.close();

      // iotdb-engine.properties part
      Properties engineConfig = new Properties();
      FileInputStream engineConfInput = new FileInputStream(engineConfigPath);
      engineConfig.load(engineConfInput);
      engineConfInput.close();
      engineConfig.setProperty("rpc_port", String.valueOf(this.rpcPort));
      engineConfig.setProperty("enable_influxdb_rpc_service", Boolean.toString(false));
      engineConfig.putAll(engineProperties);
      FileWriter engineConfOutput = new FileWriter(engineConfigPath);
      engineConfig.store(engineConfOutput, null);
      engineConfOutput.close();

    } catch (IOException ex) {
      fail("Change cluster config failed. " + ex.getMessage());
    }
  }

  public void start() throws IOException {
    ProcessBuilder processBuilder;
    if (SystemUtils.IS_OS_WINDOWS) {
      processBuilder =
          new ProcessBuilder(workDirFilePath("sbin", "start-node.bat"))
              .redirectOutput(new File("nul"))
              .redirectError(new File("nul"));
    } else {
      processBuilder =
          new ProcessBuilder(workDirFilePath("sbin", "start-node.sh"))
              .redirectOutput(new File("/dev/null"))
              .redirectError(new File("/dev/null"));
    }
    this.instance = processBuilder.start();
  }

  public void stop() throws IOException {
    if (this.instance != null) {
      this.instance.destroy();
    }
    // In Windows, the IoTDB process is started as a subprocess of start-node.bat with a new pid. So
    // We need to kill the new subprocess as well.
    if (SystemUtils.IS_OS_WINDOWS) {
      ProcessBuilder processBuilder =
          new ProcessBuilder(workDirFilePath("sbin", "stop-node.bat"))
              .redirectOutput(new File("nul"))
              .redirectError(new File("nul"));
      Process p = processBuilder.start();
      try {
        p.waitFor(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        p.destroyForcibly();
      }
    }
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return rpcPort;
  }

  private String workDirFilePath(String dirName, String fileName) {
    return this.path
        + File.separator
        + "template-node"
        + File.separator
        + dirName
        + File.separator
        + fileName;
  }
}
