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
  SCATTER_WIDTH_NUM_IN_DATA_NODE("scatter_width_num_in_data_node"),
  SERIES_SLOT_NUM_IN_DATABASE("series_slot_num_in_database"),
  TIME_SLOT_NUM_IN_DATABASE("time_slot_num_in_database"),
  REGION_GROUP_NUM_IN_DATABASE("region_group_num_in_database"),
  REPLICATION_FACTOR("replication_factor"),
  PROCEDURE_WORKER_THREAD_COUNT("procedure_worker_thread_count"),
  PROCEDURE_ACTIVE_WORKER_THREAD_COUNT("procedure_active_worker_thread_count"),
  PROCEDURE_QUEUE_LENGTH("procedure_queue_length"),
  PROCEDURE_SUBMITTED_COUNT("procedure_submitted_count"),
  PROCEDURE_FAILED_COUNT("procedure_failed_count"),
  PROCEDURE_EXECUTION_TIME("procedure_execution_time"),
  // protocol related
  ENTRY("entry"),
  SESSION_IDLE_TIME("session_idle_time"),
  THRIFT_CONNECTIONS("thrift_connections"),
  THRIFT_ACTIVE_THREADS("thrift_active_threads"),
  CLIENT_MANAGER("client_manager"),
  // consensus related
  STAGE("stage"),
  IOT_CONSENSUS("iot_consensus"),
  IOT_SEND_LOG("iot_send_log"),
  IOT_RECEIVE_LOG("iot_receive_log"),
  PIPE_CONSENSUS("pipe_consensus"),
  PIPE_CONSENSUS_MODE("pipe_consensus_mode"),
  PIPE_SEND_EVENT("pipe_send_event"),
  PIPE_RETRY_SEND_EVENT("pipe_retry_send_event"),
  PIPE_RECEIVE_EVENT("pipe_receive_event"),
  RATIS_CONSENSUS_WRITE("ratis_consensus_write"),
  RATIS_CONSENSUS_READ("ratis_consensus_read"),
  // storage engine related
  POINTS("points"),
  POINTS_IN("points_in"),
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
  MEMTABLE_THRESHOLD("memtable_threshold"),
  FLUSH_MEMTABLE_COUNT("flush_memtable_count"),
  ACTIVE_MEMTABLE_COUNT("active_memtable_count"),
  ACTIVE_TIME_PARTITION_COUNT("active_time_partition_count"),
  MEMTABLE_LIVE_DURATION("memtable_live_duration"),

  // compaction related
  DATA_WRITTEN("data_written"),
  DATA_READ("data_read"),
  COMPACTION_TASK_COUNT("compaction_task_count"),
  COMPACTION_TASK_MEMORY("compaction_task_memory"),
  COMPACTION_TASK_MEMORY_DISTRIBUTION("compaction_task_memory_distribution"),
  COMPACTION_TASK_SELECTION("compaction_task_selection"),
  COMPACTION_TASK_SELECTION_COST("compaction_task_selection_cost"),
  COMPACTION_TASK_SELECTED_FILE("compaction_task_selected_file"),
  COMPACTION_TASK_SELECTED_FILE_SIZE("compaction_task_selected_file_size"),
  // schema engine related
  MEM("mem"),
  CACHE("cache"),
  CACHE_HIT_RATE("cache_hit"),
  QUANTITY("quantity"),
  LEADER_QUANTITY("leader_quantity"),
  SCHEMA_REGION("schema_region"),
  SCHEMA_ENGINE("schema_engine"),
  // query engine related
  QUERY_PLAN_COST("query_plan_cost"),
  OPERATOR_EXECUTION_COST("operator_execution_cost"),
  OPERATOR_EXECUTION_COUNT("operator_execution_count"),
  SERIES_SCAN_COST("series_scan_cost"),
  MEMORY_USAGE_MONITOR("memory_usage_monitor"),
  METRIC_LOAD_TIME_SERIES_METADATA("metric_load_time_series_metadata"),
  METRIC_QUERY_CACHE("metric_query_cache"),
  QUERY_METADATA_COST("query_metadata_cost"),
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
  QUERY_DISK_READ("query_disk_read"),

  // file related
  FILE_SIZE("file_size"),
  FILE_COUNT("file_count"),
  // pipe related
  UNASSIGNED_TABLET_COUNT("unassigned_tablet_count"),
  UNASSIGNED_TSFILE_COUNT("unassigned_tsfile_count"),
  UNASSIGNED_HEARTBEAT_COUNT("unassigned_heartbeat_count"),
  UNPROCESSED_TABLET_COUNT("unprocessed_tablet_count"),
  UNPROCESSED_HISTORICAL_TSFILE_COUNT("unprocessed_historical_tsfile_count"),
  UNPROCESSED_REALTIME_TSFILE_COUNT("unprocessed_realtime_tsfile_count"),
  UNPROCESSED_HEARTBEAT_COUNT("unprocessed_heartbeat_count"),
  UNTRANSFERRED_TABLET_COUNT("untransferred_tablet_count"),
  UNTRANSFERRED_TSFILE_COUNT("untransferred_tsfile_count"),
  UNTRANSFERRED_HEARTBEAT_COUNT("untransferred_heartbeat_count"),
  PIPE_DATANODE_RECEIVER("pipe_datanode_receiver"),
  PIPE_CONFIGNODE_RECEIVER("pipe_confignode_receiver"),
  PIPE_EXTRACTOR_TABLET_SUPPLY("pipe_extractor_tablet_supply"),
  PIPE_EXTRACTOR_TSFILE_SUPPLY("pipe_extractor_tsfile_supply"),
  PIPE_EXTRACTOR_HEARTBEAT_SUPPLY("pipe_extractor_heartbeat_supply"),
  PIPE_PROCESSOR_TABLET_PROCESS("pipe_processor_tablet_process"),
  PIPE_PROCESSOR_TSFILE_PROCESS("pipe_processor_tsfile_process"),
  PIPE_PROCESSOR_HEARTBEAT_PROCESS("pipe_processor_heartbeat_process"),
  PIPE_CONNECTOR_TABLET_TRANSFER("pipe_connector_tablet_transfer"),
  PIPE_CONNECTOR_TSFILE_TRANSFER("pipe_connector_tsfile_transfer"),
  PIPE_CONNECTOR_HEARTBEAT_TRANSFER("pipe_connector_heartbeat_transfer"),
  PIPE_HEARTBEAT_EVENT("pipe_heartbeat_event"),
  PIPE_WAL_INSERT_NODE_CACHE_HIT_RATE("pipe_wal_insert_node_cache_hit_rate"),
  PIPE_WAL_INSERT_NODE_CACHE_HIT_COUNT("pipe_wal_insert_node_cache_hit_count"),
  PIPE_WAL_INSERT_NODE_CACHE_REQUEST_COUNT("pipe_wal_insert_node_cache_request_count"),
  PIPE_EXTRACTOR_TSFILE_EPOCH_STATE("pipe_extractor_tsfile_epoch_state"),
  PIPE_MEM("pipe_mem"),
  PIPE_PINNED_MEMTABLE_COUNT("pipe_pinned_memtable_count"),
  PIPE_LINKED_TSFILE_COUNT("pipe_linked_tsfile_count"),
  PIPE_PHANTOM_REFERENCE_COUNT("pipe_phantom_reference_count"),
  PIPE_ASYNC_CONNECTOR_RETRY_EVENT_QUEUE_SIZE("pipe_async_connector_retry_event_queue_size"),
  PIPE_EVENT_COMMIT_QUEUE_SIZE("pipe_event_commit_queue_size"),
  PIPE_PROCEDURE("pipe_procedure"),
  PIPE_TASK_STATUS("pipe_task_status"),
  PIPE_SCHEMA_LINKED_QUEUE_SIZE("pipe_schema_linked_queue_size"),
  UNTRANSFERRED_SCHEMA_COUNT("untransferred_schema_count"),
  PIPE_CONNECTOR_SCHEMA_TRANSFER("pipe_connector_schema_transfer"),
  PIPE_DATANODE_REMAINING_EVENT_COUNT("pipe_datanode_remaining_event_count"),
  PIPE_DATANODE_REMAINING_TIME("pipe_datanode_remaining_time"),
  PIPE_CONFIG_LINKED_QUEUE_SIZE("pipe_config_linked_queue_size"),
  UNTRANSFERRED_CONFIG_COUNT("untransferred_config_count"),
  PIPE_CONNECTOR_CONFIG_TRANSFER("pipe_connector_config_transfer"),
  PIPE_CONFIGNODE_REMAINING_TIME("pipe_confignode_remaining_time"),
  PIPE_GLOBAL_REMAINING_EVENT_COUNT("pipe_global_remaining_event_count"),
  PIPE_GLOBAL_REMAINING_TIME("pipe_global_remaining_time"),
  // subscription related
  SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT("subscription_uncommitted_event_count"),
  SUBSCRIPTION_CURRENT_COMMIT_ID("subscription_current_commit_id"),
  SUBSCRIPTION_EVENT_TRANSFER("subscription_event_transfer"),
  // load related
  ACTIVE_LOADING_FILES_NUMBER("active_loading_files_number"),
  ACTIVE_LOADING_FILES_SIZE("active_loading_files_size"),
  LOAD_MEM("load_mem"),
  LOAD_DISK_IO("load_disk_io"),
  LOAD_TIME_COST("load_time_cost"),
  LOAD_POINT_COUNT("load_point_count"),
  MEMTABLE_POINT_COUNT("memtable_point_count"),
  BINARY_ALLOCATOR("binary_allocator"),
  ;

  final String value;

  Metric(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
