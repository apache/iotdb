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
  COST_TASK,
  QUEUE,
  FILE_SIZE,
  FILE_COUNT,
  DISK_IO_SIZE,
  DISK_IO_OPS,
  DISK_IO_TIME,
  DISK_IO_AVG_TIME,
  DISK_IO_SECTOR_NUM,
  DISK_IO_BUSY_PERCENTAGE,
  DISK_IO_QUEUE_SIZE,
  PROCESS_IO_SIZE,
  PROCESS_IO_OPS,
  MEM,
  CACHE,
  CACHE_HIT,
  QUANTITY,
  DATA_WRITTEN,
  DATA_READ,
  COMPACTION_TASK_COUNT,
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

  NODE_NUM,
  DATABASE_NUM,
  REGION_NUM,
  REGION_NUM_IN_DATA_NODE,
  REGION_GROUP_LEADER_NUM_IN_DATA_NODE,
  SERIES_SLOT_NUM_IN_DATABASE,
  REGION_GROUP_NUM_IN_DATABASE,

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
  DATA_EXCHANGE_COST,
  DATA_EXCHANGE_COUNT,
  DRIVER_SCHEDULER,
  PERFORMANCE_OVERVIEW,
  PERFORMANCE_OVERVIEW_DETAIL,
  PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
  PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
  PERFORMANCE_OVERVIEW_STORAGE_DETAIL,
  PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
  WAL_NODE_NUM,
  WAL_NODE_INFO,
  WAL_BUFFER,
  PENDING_FLUSH_TASK,
  WAL_COST,
  FLUSH_COST,
  FLUSH_SUB_TASK_COST,
  FLUSHING_MEM_TABLE_STATUS,
  DATA_REGION_MEM_COST,
  SCHEMA_REGION,
  SCHEMA_ENGINE,
  SESSION_IDLE_TIME;

  @Override
  public String toString() {
    return super.toString().toLowerCase();
  }
}
