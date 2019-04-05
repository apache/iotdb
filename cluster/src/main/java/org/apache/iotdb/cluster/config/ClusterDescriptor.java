/**
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
package org.apache.iotdb.cluster.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.iotdb.cluster.rpc.service.TSServiceClusterImpl;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterDescriptor.class);

  private IoTDBConfig ioTDBConf = IoTDBDescriptor.getInstance().getConfig();

  private ClusterConfig conf = new ClusterConfig();

  private ClusterDescriptor() {
    loadProps();
  }

  public static ClusterDescriptor getInstance() {
    return ClusterDescriptorHolder.INSTANCE;
  }

  public ClusterConfig getConfig() {
    return conf;
  }

  /**
   * Load an property file and set ClusterConfig variables.
   * Change this method to public only for test.
   * In most case, you should invoke this method.
   */
  public void loadProps() {
    ioTDBConf.setRpcImplClassName(TSServiceClusterImpl.class.getName());
    ioTDBConf.setEnableWal(false);
    conf.setDefaultPath();
    InputStream inputStream;
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + ClusterConfig.CONFIG_NAME;
      } else {
        LOGGER.warn(
            "Cannot find IOTDB_HOME or CLUSTER_CONF environment variable when loading "
                + "config file {}, use default configuration",
            ClusterConfig.CONFIG_NAME);
        conf.createAllPath();
        return;
      }
    } else {
      url += (File.separatorChar + ClusterConfig.CONFIG_NAME);
    }

    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      LOGGER.warn("Fail to find config file {}", url, e);
      conf.createAllPath();
      return;
    }

    LOGGER.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
      conf.setNodes(properties.getProperty("nodes", ClusterConfig.DEFAULT_NODE)
          .split(","));

      conf.setReplication(Integer
          .parseInt(properties.getProperty("replication",
              Integer.toString(conf.getReplication()))));

      conf.setIp(properties.getProperty("ip", conf.getIp()));

      conf.setPort(Integer.parseInt(properties.getProperty("port",
          Integer.toString(conf.getPort()))));

      conf.setRaftLogPath(properties.getProperty("raft_log_path", conf.getRaftLogPath()));

      conf.setRaftSnapshotPath(properties.getProperty("raft_snapshot_path", conf.getRaftSnapshotPath()));

      conf.setRaftMetadataPath(properties.getProperty("raft_metadata_path", conf.getRaftMetadataPath()));

      conf.setMaxCatchUpLogNum(Integer
          .parseInt(properties.getProperty("max_catch_up_log_num",
              Integer.toString(conf.getMaxCatchUpLogNum()))));

      conf.setDelaySnapshot(Boolean
          .parseBoolean(properties.getProperty("delay_snapshot",
              Boolean.toString(conf.isDelaySnapshot()))));

      conf.setDelayHours(Integer
          .parseInt(properties.getProperty("delay_hours",
              Integer.toString(conf.getDelayHours()))));

      conf.setTaskRedoCount(Integer
          .parseInt(properties.getProperty("task_redo_count",
              Integer.toString(conf.getTaskRedoCount()))));

      conf.setTaskTimeoutMs(Integer
          .parseInt(properties.getProperty("task_timeout_ms",
              Integer.toString(conf.getTaskTimeoutMs()))));

      conf.setNumOfVirtualNodes(Integer
          .parseInt(properties.getProperty("num_of_virtula_nodes",
              Integer.toString(conf.getNumOfVirtualNodes()))));

      conf.setMaxNumOfInnerRpcClient(Integer
          .parseInt(properties.getProperty("max_num_of_inner_rpc_client",
              Integer.toString(conf.getMaxNumOfInnerRpcClient()))));

      conf.setMaxQueueNumOfInnerRpcClient(Integer
          .parseInt(properties.getProperty("max_queue_num_of_inner_rpc_client",
              Integer.toString(conf.getMaxQueueNumOfInnerRpcClient()))));

      conf.setReadMetadataConsistencyLevel(Integer
          .parseInt(properties.getProperty("read_metadata_consistency_level",
              Integer.toString(conf.getReadMetadataConsistencyLevel()))));

      conf.setReadDataConsistencyLevel(Integer
          .parseInt(properties.getProperty("read_data_consistency_level",
              Integer.toString(conf.getReadDataConsistencyLevel()))));

    } catch (IOException e) {
      LOGGER.warn("Cannot load config file because, use default configuration", e);
    } catch (Exception e) {
      LOGGER.warn("Incorrect format in config file, use default configuration", e);
    } finally {
      conf.createAllPath();
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.error("Fail to close config file input stream because ", e);
      }
    }
  }

  private static class ClusterDescriptorHolder {
    private static final ClusterDescriptor INSTANCE = new ClusterDescriptor();
  }
}
