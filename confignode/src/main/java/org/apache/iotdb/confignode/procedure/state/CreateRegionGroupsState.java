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

public enum CreateRegionGroupsState {

  // Create RegionGroups on remote DataNodes
  CREATE_REGION_GROUPS,

  // Shunt the RegionReplicas, including:
  // 1. Persist successfully created RegionGroups' record
  // 2. Add recreate RegionReplicas task in RegionMaintainer
  // for those RegionReplicas that failed to create, when
  // there are more than half of RegionReplicas created successfully on the same RegionGroup
  // 3. Delete redundant RegionReplicas in contrast to case 2.
  SHUNT_REGION_REPLICAS,

  // Mark RegionGroupCache as available for those RegionGroups that created successfully.
  // For DataRegionGroups that use iot consensus protocol, select leader by the way
  ACTIVATE_REGION_GROUPS,

  CREATE_REGION_GROUPS_FINISH
}
