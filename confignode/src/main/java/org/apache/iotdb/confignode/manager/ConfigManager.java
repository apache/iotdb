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

import org.apache.iotdb.confignode.partition.PartitionTable;
import org.apache.iotdb.confignode.service.balancer.LoadBalancer;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ConfigManager Maintains consistency between PartitionTables in the ConfigNodeGroup. Expose the
 * query interface for the PartitionTable
 */
public class ConfigManager {

  private final Lock partitionTableLock;
  private final PartitionTable partitionTable;

  private final LoadBalancer loadBalancer;

  public ConfigManager() {
    this.partitionTableLock = new ReentrantLock();
    this.partitionTable = new PartitionTable();

    this.loadBalancer = new LoadBalancer(partitionTableLock, partitionTable);
  }

  public int getDeviceGroupID(String device) {
    return -1;
  }

  // TODO: Interfaces for metadata operations

  // TODO: Interfaces for data operations

  // TODO: Interfaces for LoadBalancer control
}
