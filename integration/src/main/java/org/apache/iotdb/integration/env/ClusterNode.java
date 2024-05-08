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

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

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
    String scriptPath =
        this.path
            + File.separator
            + "template-node"
            + File.separator
            + "sbin"
            + File.separator
            + "start-node"
            + ".sh";
    try {
      FileUtils.copyDirectoryToDirectory(new File(srcPath), new File(this.path));
      new File(scriptPath).setExecutable(true);
    } catch (IOException ex) {
      fail("Copy cluster node dir failed. " + ex.getMessage());
    }
  }

  public void destroyDir() {
    // rm this.path
    try {
      FileUtils.forceDelete(new File(this.path));
    } catch (IOException ex) {
      // ignore
    }
  }

  public void changeConfig(Properties engineProperties, Properties clusterProperties) {
    try {
      // iotdb-cluster.properties part
      String clusterConfigPath =
          this.path
              + File.separator
              + "template-node"
              + File.separator
              + "conf"
              + File.separator
              + "iotdb-cluster.properties";

      Properties clusterConfig = new Properties();
      clusterConfig.load(new FileInputStream(clusterConfigPath));
      StringBuilder objString = new StringBuilder("127.0.0.1:" + metaPortArray[0]);
      for (int i = 1; i < metaPortArray.length; i++) {
        objString.append(",127.0.0.1:").append(metaPortArray[i]);
      }
      clusterConfig.setProperty("seed_nodes", objString.toString());
      clusterConfig.setProperty("enable_auto_create_schema", "true");
      clusterConfig.setProperty("internal_meta_port", String.valueOf(this.metaPort));
      clusterConfig.setProperty("internal_data_port", String.valueOf(this.dataPort));
      clusterConfig.setProperty("consistency_level", "strong");

      // Temporary settings
      clusterConfig.setProperty("cluster_info_public_port", String.valueOf(this.rpcPort - 100));
      clusterConfig.putAll(clusterProperties);
      clusterConfig.store(new FileWriter(clusterConfigPath), null);

      // iotdb-common.properties part
      String engineConfigPath =
          this.path
              + File.separator
              + "template-node"
              + File.separator
              + "conf"
              + File.separator
              + "iotdb-datanode.properties";

      Properties engineConfig = new Properties();
      engineConfig.load(new FileInputStream(engineConfigPath));
      engineConfig.setProperty(IoTDBConstant.DN_RPC_PORT, String.valueOf(this.rpcPort));
      engineConfig.setProperty("enable_influxdb_rpc_service", Boolean.toString(false));
      engineConfig.putAll(engineProperties);
      engineConfig.store(new FileWriter(engineConfigPath), null);

    } catch (IOException ex) {
      fail("Change cluster config failed. " + ex.getMessage());
    }
  }

  public void start() throws IOException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(
                this.path
                    + File.separator
                    + "template-node"
                    + File.separator
                    + "sbin"
                    + File.separator
                    + "start-node"
                    + ".sh")
            .redirectOutput(new File("/dev/null"))
            .redirectError(new File("/dev/null"));
    this.instance = processBuilder.start();
  }

  public void stop() {
    this.instance.destroy();
  }

  public void waitingToShutDown() {
    while (this.instance.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return rpcPort;
  }
}
