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

package org.apache.iotdb.confignode.client.sync;

public enum CnToDnSyncRequestType {
  // Node Maintenance
  CLEAN_DATA_NODE_CACHE,
  STOP_AND_CLEAR_DATA_NODE,
  SET_SYSTEM_STATUS,
  SHOW_CONFIGURATION,

  // Region Maintenance
  CREATE_DATA_REGION,
  CREATE_SCHEMA_REGION,
  DELETE_REGION,
  CREATE_NEW_REGION_PEER,
  ADD_REGION_PEER,
  REMOVE_REGION_PEER,
  DELETE_OLD_REGION_PEER,
  RESET_PEER_LIST,

  // PartitionCache
  INVALIDATE_PARTITION_CACHE,
  INVALIDATE_PERMISSION_CACHE,
  INVALIDATE_SCHEMA_CACHE,

  // Template
  UPDATE_TEMPLATE,

  // Schema
  KILL_QUERY_INSTANCE,

  // Table
  UPDATE_TABLE,
}
