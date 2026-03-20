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

package org.apache.iotdb.confignode.procedure.state;

public enum DataPartitionTableIntegrityCheckProcedureState {
  /** Collect earliest timeslot information from all DataNodes */
  COLLECT_EARLIEST_TIMESLOTS,
  /** Analyze missing data partitions */
  ANALYZE_MISSING_PARTITIONS,
  /** Request DataPartitionTable generation from DataNodes */
  REQUEST_PARTITION_TABLES,
  /** Round robin get DataPartitionTable generation result from DataNodes */
  REQUEST_PARTITION_TABLES_HEART_BEAT,
  /** Merge DataPartitionTables from all DataNodes */
  MERGE_PARTITION_TABLES,
  /** Write final DataPartitionTable to raft log */
  WRITE_PARTITION_TABLE_TO_CONSENSUS
}
