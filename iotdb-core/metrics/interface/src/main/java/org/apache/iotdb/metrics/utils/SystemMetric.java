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

package org.apache.iotdb.metrics.utils;

// universal metric related to system
public enum SystemMetric {
  // disk related
  DISK_IO_SIZE("disk_io_size"),
  DISK_IO_OPS("disk_io_ops"),
  DISK_IO_TIME("disk_io_time"),
  DISK_IO_AVG_TIME("disk_io_avg_time"),
  DISK_IO_AVG_SIZE("disk_io_avg_size"),
  DISK_IO_SECTOR_NUM("disk_io_sector_num"),
  DISK_IO_BUSY_PERCENTAGE("disk_io_busy_percentage"),
  DISK_IO_QUEUE_SIZE("disk_io_queue_size"),
  // process related
  PROCESS_IO_SIZE("process_io_size"),
  PROCESS_IO_OPS("process_io_ops"),
  PROCESS_CPU_LOAD("process_cpu_load"),
  PROCESS_CPU_TIME("process_cpu_time"),
  PROCESS_MAX_MEM("process_max_mem"),
  PROCESS_USED_MEM("process_used_mem"),
  PROCESS_TOTAL_MEM("process_total_mem"),
  PROCESS_FREE_MEM("process_free_mem"),
  PROCESS_THREADS_COUNT("process_threads_count"),
  PROCESS_MEM_RATIO("process_mem_ratio"),
  PROCESS_STATUS("process_status"),
  // system related
  SYS_CPU_LOAD("sys_cpu_load"),
  SYS_CPU_CORES("sys_cpu_cores"),
  SYS_TOTAL_PHYSICAL_MEMORY_SIZE("sys_total_physical_memory_size"),
  SYS_FREE_PHYSICAL_MEMORY_SIZE("sys_free_physical_memory_size"),
  SYS_TOTAL_SWAP_SPACE_SIZE("sys_total_swap_space_size"),
  SYS_FREE_SWAP_SPACE_SIZE("sys_free_swap_space_size"),
  SYS_COMMITTED_VM_SIZE("sys_committed_vm_size"),
  SYS_DISK_TOTAL_SPACE("sys_disk_total_space"),
  SYS_DISK_FREE_SPACE("sys_disk_free_space"),
  // cpu related
  MODULE_CPU_USAGE("module_cpu_usage"),
  POOL_CPU_USAGE("pool_cpu_usage"),
  MODULE_USER_TIME_PERCENTAGE("module_user_time_percentage"),
  POOL_USER_TIME_PERCENTAGE("user_time_percentage"),
  // jvm related
  JVM_CLASSES_LOADED_CLASSES("jvm_classes_loaded_classes"),
  JVM_CLASSES_UNLOADED_CLASSES("jvm_classes_unloaded_classes"),
  JVM_COMPILATION_TIME_MS("jvm_compilation_time_ms"),
  JVM_BUFFER_COUNT_BUFFERS("jvm_buffer_count_buffers"),
  JVM_BUFFER_MEMORY_USED_BYTES("jvm_buffer_memory_used_bytes"),
  JVM_BUFFER_TOTAL_CAPACITY_BYTES("jvm_buffer_total_capacity_bytes"),
  JVM_MEMORY_USED_BYTES("jvm_memory_used_bytes"),
  JVM_MEMORY_COMMITTED_BYTES("jvm_memory_committed_bytes"),
  JVM_MEMORY_MAX_BYTES("jvm_memory_max_bytes"),
  JVM_THREADS_PEAK_THREADS("jvm_threads_peak_threads"),
  JVM_THREADS_DAEMON_THREADS("jvm_threads_daemon_threads"),
  JVM_THREADS_LIVE_THREADS("jvm_threads_live_threads"),
  JVM_THREADS_STATUS_THREADS("jvm_threads_states_threads"),
  JVM_GC_MAX_DATA_SIZE_BYTES("jvm_gc_max_data_size_bytes"),
  JVM_GC_LIVE_DATA_SIZE_BYTES("jvm_gc_live_data_size_bytes"),
  JVM_GC_MEMORY_ALLOCATED_BYTES("jvm_gc_memory_allocated_bytes"),
  JVM_GC_MEMORY_PROMOTED_BYTES("jvm_gc_memory_promoted_bytes"),
  // net related
  RECEIVED_BYTES("received_bytes"),
  RECEIVED_PACKETS("received_packets"),
  TRANSMITTED_BYTES("transmitted_bytes"),
  TRANSMITTED_PACKETS("transmitted_packets"),
  CONNECTION_NUM("connection_num"),
  // logback related
  LOGBACK_EVENTS("logback_events"),
  // uptime related
  UP_TIME("up_time");

  final String value;

  SystemMetric(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
