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
package org.apache.iotdb.db.concurrent;

public enum ThreadName {
  METRICS_SERVICE("Metrics-ServerServiceImpl"),
  RPC_SERVICE("RPC-ServerServiceImpl"),
  RPC_CLIENT("RPC-Client"),
  MERGE_SERVICE("Merge-ServerServiceImpl"),
  CLOSE_MERGE_SERVICE("Close-Merge-ServerServiceImpl"),
  CLOSE_MERGE_DAEMON("Close-Merge-Daemon-Thread"),
  CLOSE_DAEMON("Close-Daemon-Thread"),
  MERGE_DAEMON("Merge-Daemon-Thread"),
  MEMORY_MONITOR("IoTDB-MemMonitor-Thread"),
  MEMORY_STATISTICS("IoTDB-MemStatistic-Thread"),
  FLUSH_PARTIAL_POLICY("IoTDB-FlushPartialPolicy-Thread"),
  FORCE_FLUSH_ALL_POLICY("IoTDB-ForceFlushAllPolicy-Thread"),
  STAT_MONITOR("StatMonitor-ServerServiceImpl"),
  FLUSH_SERVICE("Flush-ServerServiceImpl"),
  FLUSH_SUB_TASK_SERVICE("Flush-SubTask-ServerServiceImpl"),
  FLUSH_VM_SERVICE("Flush-Vm-ServerServiceImpl"),
  WAL_DAEMON("IoTDB-MultiFileLogNodeManager-Sync-Thread"),
  WAL_FORCE_DAEMON("IoTDB-MultiFileLogNodeManager-Force-Thread"),
  INDEX_SERVICE("Index-ServerServiceImpl"),
  SYNC_CLIENT("Sync-Client"),
  SYNC_SERVER("Sync-Server"),
  SYNC_MONITOR("Sync-Monitor"),
  LOAD_TSFILE("Load TsFile"),
  TIME_COST_STATSTIC("TIME_COST_STATSTIC"),
  QUERY_SERVICE("Query-ServerServiceImpl");

  private String name;

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
