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

package org.apache.iotdb.commons.service.metric.enums;

public enum Metric {
  ENTRY,
  OPERATION,
  COST_TASK,
  QUEUE,
  FILE_SIZE,
  FILE_COUNT,
  MEM,
  CACHE,
  CACHE_HIT,
  QUANTITY,
  DATA_WRITTEN,
  DATA_READ,
  COMPACTION_TASK_COUNT,
  CLUSTER_NODE_STATUS,
  CLUSTER_NODE_LEADER_COUNT,
  PROCESS_CPU_LOAD,
  PROCESS_CPU_TIME,
  PROCESS_MAX_MEM,
  PROCESS_USED_MEM,
  PROCESS_TOTAL_MEM,
  PROCESS_FREE_MEM,
  PROCESS_THREADS_COUNT,
  PROCESS_MEM_RATIO,
  PROCESS_STATUS,
  SYS_CPU_LOAD,
  SYS_CPU_CORES,
  SYS_TOTAL_PHYSICAL_MEMORY_SIZE,
  SYS_FREE_PHYSICAL_MEMORY_SIZE,
  SYS_TOTAL_SWAP_SPACE_SIZE,
  SYS_FREE_SWAP_SPACE_SIZE,
  SYS_COMMITTED_VM_SIZE,
  SYS_DISK_TOTAL_SPACE,
  SYS_DISK_FREE_SPACE,
  CONFIG_NODE,
  DATA_NODE,
  STORAGE_GROUP,
  REGION,
  SLOT,
  THRIFT_CONNECTIONS,
  THRIFT_ACTIVE_THREADS,
  IOT_CONSENSUS,
  STAGE,
  POINTS,
  QUERY_PLAN_COST,
  OPERATOR_EXECUTION_COST,
  OPERATOR_EXECUTION_COUNT,
  SERIES_SCAN_COST,
  DISPATCHER,
  QUERY_EXECUTION,
  AGGREGATION,
  QUERY_RESOURCE,
  DATA_EXCHANGE,
  DRIVER_SCHEDULER;

  @Override
  public String toString() {
    return super.toString().toLowerCase();
  }
}
