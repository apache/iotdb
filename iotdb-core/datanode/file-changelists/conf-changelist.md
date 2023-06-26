<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# 0.11.0 -> 0.12.0

## add:

## removed:

debug_state=false

# 0.10.0 -> 0.11.0

## add：
enable_mem_control=true

write_read_schema_free_memory_proportion=4:3:1:2

flush_proportion=0.3

buffered_arrays_memory_proportion=0.6

reject_proportion=0.8

storage_group_report_threshold=16777216

max_deduplicated_path_num=1000

waiting_time_when_insert_blocked=10

max_waiting_time_when_insert_blocked=10000

estimated_series_size=300

compaction_strategy=LEVEL_COMPACTION

seq_file_num_in_each_level=6

seq_level_num=4

unseq_file_num_in_each_level=10

unseq_level_num=1

merge_chunk_point_number=100000

merge_page_point_number=1000

merge_fileSelection_time_budget=30000

compaction_thread_num=10

merge_write_throughput_mb_per_sec=8

frequency_interval_in_minute=1

slow_query_threshold=5000

debug_state=false

enable_discard_out_of_order_data=false

enable_partial_insert=true

enable_mtree_snapshot=false

mtree_snapshot_interval=100000

mtree_snapshot_threshold_time=3600

rpc_selector_thread_num=1

rpc_min_concurrent_client_num=1

## remove：
merge_thread_num=1

time_zone=+08:00

enable_parameter_adapter=true

write_read_free_memory_proportion=6:3:1

## change：
tsfile_size_threshold=0

avg_series_point_number_threshold=10000

