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
package org.apache.iotdb.confignode.consensus.request;

public enum ConfigPhysicalPlanType {
  RegisterDataNode,
  GetDataNodeConfiguration,
  SetStorageGroup,
  SetTTL,
  SetSchemaReplicationFactor,
  SetDataReplicationFactor,
  SetTimePartitionInterval,
  AdjustMaxRegionGroupCount,
  DeleteStorageGroup,
  PreDeleteStorageGroup,
  GetStorageGroup,
  CountStorageGroup,
  CreateRegionGroups,
  DeleteRegionGroups,
  GetSchemaPartition,
  CreateSchemaPartition,
  GetOrCreateSchemaPartition,
  GetDataPartition,
  CreateDataPartition,
  GetOrCreateDataPartition,
  UpdateProcedure,
  DeleteProcedure,
  Author,
  CreateUser,
  CreateRole,
  DropUser,
  DropRole,
  GrantRole,
  GrantUser,
  GrantRoleToUser,
  RevokeUser,
  RevokeRole,
  RevokeRoleFromUser,
  UpdateUser,
  ListUser,
  ListRole,
  ListUserPrivilege,
  ListRolePrivilege,
  @Deprecated
  ListUserRoles,
  @Deprecated
  ListRoleUsers,
  ApplyConfigNode,
  RemoveDataNode,
  RemoveConfigNode,
  CreateFunction,
  DropFunction,
  GetRegionInfoList,
  GetDataNodesInfoList,
  GetNodePathsPartition,
  CreateSchemaTemplate,
  GetAllSchemaTemplate,
  GetSchemaTemplate,
  UpdateRegionLocation,
  CheckTemplateSettable,
  SetSchemaTemplate,
  GetPathsSetTemplate,
  GetAllTemplateSetInfo;
}
