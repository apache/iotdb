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
  // performance overview related
  PERFORMANCE_OVERVIEW("performance_overview"),
  PERFORMANCE_OVERVIEW_DETAIL("performance_overview_detail"),
  PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL("performance_overview_schedule_detail"),
  PERFORMANCE_OVERVIEW_LOCAL_DETAIL("performance_overview_local_detail"),
  PERFORMANCE_OVERVIEW_STORAGE_DETAIL("performance_overview_storage_detail"),
  PERFORMANCE_OVERVIEW_ENGINE_DETAIL("performance_overview_engine_detail"),
  // cluster related
  NODE_NUM("node_num"),
  DATABASE_NUM("database_num"),
  REGION_NUM("region_num"),
  REGION_NUM_IN_DATA_NODE("region_num_in_data_node"),
  REGION_GROUP_LEADER_NUM_IN_DATA_NODE("region_group_leader_num_in_data_node"),
  SERIES_SLOT_NUM_IN_DATABASE("series_slot_num_in_database"),
  TIME_SLOT_NUM_IN_DATABASE("time_slot_num_in_database"),
  REGION_GROUP_NUM_IN_DATABASE("region_group_num_in_database"),
  // protocol related
  ENTRY("entry"),
  SESSION_IDLE_TIME("session_idle_time"),
  THRIFT_CONNECTIONS("thrift_connections"),
  THRIFT_ACTIVE_THREADS("thrift_active_threads"),
  CLIENT_MANAGER("client_manager"),
  // consensus related
  STAGE("stage"),
  IOT_CONSENSUS("iot_consensus"),
  RATIS_CONSENSUS_WRITE("ratis_consensus_write"),
  RATIS_CONSENSUS_READ("ratis_consensus_read"),
  // storage engine related
  POINTS("points"),
  COST_TASK("cost_task"),
  QUEUE("queue"),
  FLUSHING_MEM_TABLE_STATUS("flushing_mem_table_status"),
  DATA_REGION_MEM_COST("data_region_mem_cost"),
  WAL_NODE_NUM("wal_node_num"),
  WAL_NODE_INFO("wal_node_info"),
  WAL_BUFFER("wal_buffer"),
  PENDING_FLUSH_TASK("pending_flush_task"),
  WAL_COST("wal_cost"),
  FLUSH_COST("flush_cost"),
  FLUSH_SUB_TASK_COST("flush_sub_task_cost"),
  // compaction related
  DATA_WRITTEN("data_written"),
  DATA_READ("data_read"),
  COMPACTION_TASK_COUNT("compaction_task_count"),
  // schema engine related
  MEM("mem"),
  CACHE("cache"),
  CACHE_HIT_RATE("cache_hit"),
  QUANTITY("quantity"),
  SCHEMA_REGION("schema_region"),
  SCHEMA_ENGINE("schema_engine"),
  // query engine related
  QUERY_PLAN_COST("query_plan_cost"),
  OPERATOR_EXECUTION_COST("operator_execution_cost"),
  OPERATOR_EXECUTION_COUNT("operator_execution_count"),
  SERIES_SCAN_COST("series_scan_cost"),
  DISPATCHER("dispatcher"),
  QUERY_EXECUTION("query_execution"),
  AGGREGATION("aggregation"),
  QUERY_RESOURCE("query_resource"),
  DATA_EXCHANGE_COST("data_exchange_cost"),
  DATA_EXCHANGE_COUNT("data_exchange_count"),
  DATA_EXCHANGE_SIZE("data_exchange_size"),
  DRIVER_SCHEDULER("driver_scheduler"),
  COORDINATOR("coordinator"),
  FRAGMENT_INSTANCE_MANAGER("fragment_instance_manager"),
  MEMORY_POOL("memory_pool"),
  LOCAL_EXECUTION_PLANNER("local_execution_planner"),
  // file related
  FILE_SIZE("file_size"),
  FILE_COUNT("file_count");

  final String value;

  Metric(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
