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

import java.util.HashMap;
import java.util.Map;

public enum ConfigPhysicalPlanType {

  /** ConfigNode */
  ApplyConfigNode((short) 0),
  RemoveConfigNode((short) 1),

  /** DataNode */
  RegisterDataNode((short) 100),
  GetDataNodeConfiguration((short) 101),
  RemoveDataNode((short) 102),
  UpdateDataNodeConfiguration((short) 103),

  /** Database */
  CreateDatabase((short) 200),
  SetTTL((short) 201),
  SetSchemaReplicationFactor((short) 202),
  SetDataReplicationFactor((short) 203),
  SetTimePartitionInterval((short) 204),
  AdjustMaxRegionGroupNum((short) 205),
  DeleteDatabase((short) 206),
  PreDeleteDatabase((short) 207),
  GetDatabase((short) 208),
  CountDatabase((short) 209),
  AlterDatabase((short) 210),

  /** Region */
  CreateRegionGroups((short) 300),
  DeleteRegionGroups((short) 301),
  GetRegionInfoList((short) 302),
  UpdateRegionLocation((short) 303),
  OfferRegionMaintainTasks((short) 304),
  PollRegionMaintainTask((short) 305),
  GetRegionId((short) 306),
  GetSeriesSlotList((short) 307),
  GetTimeSlotList((short) 308),
  PollSpecificRegionMaintainTask((short) 309),

  /** Partition */
  GetSchemaPartition((short) 400),
  CreateSchemaPartition((short) 401),
  GetOrCreateSchemaPartition((short) 402),
  GetDataPartition((short) 403),
  CreateDataPartition((short) 404),
  GetOrCreateDataPartition((short) 405),
  GetNodePathsPartition((short) 406),

  /** Procedure */
  UpdateProcedure((short) 500),
  DeleteProcedure((short) 501),

  /** Authority */
  Author((short) 600),
  CreateUser((short) 601),
  CreateRole((short) 602),
  DropUser((short) 603),
  DropRole((short) 604),
  GrantRole((short) 605),
  GrantUser((short) 606),
  GrantRoleToUser((short) 607),
  RevokeUser((short) 608),
  RevokeRole((short) 609),
  RevokeRoleFromUser((short) 610),
  UpdateUser((short) 611),
  ListUser((short) 612),
  ListRole((short) 613),
  ListUserPrivilege((short) 614),
  ListRolePrivilege((short) 615),
  @Deprecated
  ListUserRoles((short) 616),
  @Deprecated
  ListRoleUsers((short) 617),

  /** Function */
  CreateFunction((short) 700),
  DropFunction((short) 701),
  GetFunctionTable((short) 702),
  GetFunctionJar((short) 703),

  /** Template */
  CreateSchemaTemplate((short) 800),
  GetAllSchemaTemplate((short) 801),
  GetSchemaTemplate((short) 802),
  CheckTemplateSettable((short) 803),
  SetSchemaTemplate((short) 804),
  GetPathsSetTemplate((short) 805),
  GetAllTemplateSetInfo((short) 806),
  GetTemplateSetInfo((short) 807),
  PreUnsetTemplate((short) 808),
  RollbackUnsetTemplate((short) 809),
  UnsetTemplate((short) 810),
  DropSchemaTemplate((short) 811),

  /** Sync */
  CreatePipeSink((short) 900),
  DropPipeSink((short) 901),
  GetPipeSink((short) 902),
  PreCreatePipe((short) 903),
  SetPipeStatus((short) 904),
  DropPipe((short) 905),
  ShowPipe((short) 906),
  RecordPipeMessage((short) 907),

  /** Trigger */
  AddTriggerInTable((short) 1000),
  DeleteTriggerInTable((short) 1001),
  GetTriggerTable((short) 1002),
  UpdateTriggerStateInTable((short) 1003),
  GetTriggerJar((short) 1004),
  UpdateTriggersOnTransferNodes((short) 1005),
  UpdateTriggerLocation((short) 1006),
  GetTransferringTriggers((short) 1007),
  GetTriggerLocation((short) 1008),

  /** CQ */
  DROP_CQ((short) 1100),
  ACTIVE_CQ((short) 1101),
  ADD_CQ((short) 1102),
  UPDATE_CQ_LAST_EXEC_TIME((short) 1103),
  SHOW_CQ((short) 1104),

  /** Ml model */
  CreateModel((short) 1200),
  UpdateModelInfo((short) 1201),
  UpdateModelState((short) 1202),
  DropModel((short) 1203),
  ShowModel((short) 1204),
  ShowTrail((short) 1205),

  /** Pipe Plugin */
  CreatePipePlugin((short) 1300),
  DropPipePlugin((short) 1301),
  GetPipePluginTable((short) 1302),
  GetPipePluginJar((short) 1303);

  private final short planType;

  private static final Map<Short, ConfigPhysicalPlanType> PLAN_TYPE_MAP = new HashMap<>();

  static {
    for (ConfigPhysicalPlanType planType : ConfigPhysicalPlanType.values()) {
      PLAN_TYPE_MAP.put(planType.getPlanType(), planType);
    }
  }

  ConfigPhysicalPlanType(short planType) {
    this.planType = planType;
  }

  public short getPlanType() {
    return planType;
  }

  /** Notice: the result might be null */
  public static ConfigPhysicalPlanType convertToConfigPhysicalPlanType(short planType) {
    return PLAN_TYPE_MAP.getOrDefault(planType, null);
  }
}
