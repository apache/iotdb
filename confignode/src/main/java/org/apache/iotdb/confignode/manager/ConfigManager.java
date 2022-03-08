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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.confignode.config.ConfigNodeConfig;
import org.apache.iotdb.confignode.config.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.hash.APHashExecutor;
import org.apache.iotdb.confignode.manager.hash.BKDRHashExecutor;
import org.apache.iotdb.confignode.manager.hash.DeviceGroupHashExecutor;
import org.apache.iotdb.confignode.manager.hash.JSHash;
import org.apache.iotdb.confignode.manager.hash.SDBMHash;
import org.apache.iotdb.confignode.partition.PartitionTable;
import org.apache.iotdb.confignode.service.balancer.LoadBalancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ConfigManager Maintains consistency between PartitionTables in the ConfigNodeGroup. Expose the
 * query interface for the PartitionTable
 */
public class ConfigManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  private DeviceGroupHashExecutor hashExecutor;

  private Lock partitionTableLock;
  private PartitionTable partitionTable;

  private LoadBalancer loadBalancer;

  public ConfigManager(String hashType, int deviceGroupCount) {
    setHashExecutor(hashType, deviceGroupCount);
  }

  public ConfigManager() {
    ConfigNodeConfig config = ConfigNodeDescriptor.getInstance().getConfig();

    setHashExecutor(config.getDeviceGroupHashAlgorithm(), config.getDeviceGroupCount());

    this.partitionTableLock = new ReentrantLock();
    this.partitionTable = new PartitionTable();

    this.loadBalancer = new LoadBalancer(partitionTableLock, partitionTable);
  }

  private void setHashExecutor(String hashAlgorithm, int deviceGroupCount) {
    switch (hashAlgorithm) {
      case "BKDR":
        hashExecutor = new BKDRHashExecutor(deviceGroupCount);
        break;
      case "AP":
        hashExecutor = new APHashExecutor(deviceGroupCount);
        break;
      case "JS":
        hashExecutor = new JSHash(deviceGroupCount);
        break;
      case "SDBM":
        hashExecutor = new SDBMHash(deviceGroupCount);
    }
  }

  public int getDeviceGroupID(String device) {
    return hashExecutor.getDeviceGroupID(device);
  }

  // TODO: Interfaces for metadata operations

  // TODO: Interfaces for data operations

  // TODO: Interfaces for LoadBalancer control
}
