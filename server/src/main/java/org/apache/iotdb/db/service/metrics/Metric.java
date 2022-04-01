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

package org.apache.iotdb.db.service.metrics;

public enum Metric {
  ENTRY,
  COST_TASK,
  QUEUE,
  FILE_SIZE,
  FILE_COUNT,
  MEM,
  CACHE_HIT,
  ERROR_LOG,
  QUANTITY,
  DATA_WRITTEN,
  DATA_READ,
  COMPACTION_TASK_COUNT,
  CLUSTER_NODE_STATUS,
  CLUSTER_NODE_LEADER_COUNT,
  CLUSTER_ELECT,
  CLUSTER_UNCOMMITTED_LOG,
  PROCESS_CPU_LOAD,
  PROCESS_CPU_TIME,
  PROCESS_MAX_MEM,
  PROCESS_TOTAL_MEM,
  PROCESS_FREE_MEM,
  SYS_CPU_LOAD,
  SYS_CPU_CORES,
  SYS_TOTAL_PHYSICAL_MEMORY_SIZE,
  SYS_FREE_PHYSICAL_MEMORY_SIZE,
  SYS_TOTAL_SWAP_SPACE_SIZE,
  SYS_FREE_SWAP_SPACE_SIZE,
  SYS_COMMITTED_VM_SIZE;

  @Override
  public String toString() {
    return super.toString().toLowerCase();
  }
}
