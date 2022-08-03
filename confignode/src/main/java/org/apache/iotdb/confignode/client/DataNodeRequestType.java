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

package org.apache.iotdb.confignode.client;

public enum DataNodeRequestType {
  DELETE_REGIONS,
  INVALIDATE_PARTITION_CACHE,
  INVALIDATE_PERMISSION_CACHE,
  INVALIDATE_SCHEMA_CACHE,
  MIGRATE_REGION,
  DISABLE_DATA_NODE,
  STOP_DATA_NODE,

  SET_TTL,
  CREATE_DATA_REGIONS,
  CREATE_SCHEMA_REGIONS,
  CREATE_FUNCTION,
  DROP_FUNCTION,
  FLUSH,
  UPDATE_REGION_ROUTE_MAP,
  BROADCAST_LATEST_CONFIG_NODE_GROUP,
  UPDATE_TEMPLATE,
  CLEAR_CACHE
}
