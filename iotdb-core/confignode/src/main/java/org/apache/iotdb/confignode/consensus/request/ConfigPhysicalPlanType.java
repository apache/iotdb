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

  /** ConfigNode. */
  ApplyConfigNode((short) 0),
  RemoveConfigNode((short) 1),
  UpdateVersionInfo((short) 2),
  UpdateClusterId((short) 3),

  /** DataNode. */
  RegisterDataNode((short) 100),
  GetDataNodeConfiguration((short) 101),
  RemoveDataNode((short) 102),
  UpdateDataNodeConfiguration((short) 103),

  /** AINode. */
  RegisterAINode((short) 104),
  UpdateAINodeConfiguration((short) 105),
  RemoveAINode((short) 106),
  GetAINodeConfiguration((short) 107),

  /** Database. */
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
  ShowTTL((short) 211),

  /** Region. */
  CreateRegionGroups((short) 300),
  DeleteRegionGroups((short) 301),
  GetRegionInfoList((short) 302),
  @Deprecated
  UpdateRegionLocation((short) 303),
  OfferRegionMaintainTasks((short) 304),
  PollRegionMaintainTask((short) 305),
  GetRegionId((short) 306),
  GetSeriesSlotList((short) 307),
  GetTimeSlotList((short) 308),
  PollSpecificRegionMaintainTask((short) 309),
  CountTimeSlotList((short) 310),
  AddRegionLocation((short) 311),
  RemoveRegionLocation((short) 312),

  /** Partition. */
  GetSchemaPartition((short) 400),
  CreateSchemaPartition((short) 401),
  GetOrCreateSchemaPartition((short) 402),
  GetDataPartition((short) 403),
  CreateDataPartition((short) 404),
  GetOrCreateDataPartition((short) 405),
  GetNodePathsPartition((short) 406),
  AutoCleanPartitionTable((short) 407),

  /** Procedure. */
  UpdateProcedure((short) 500),
  DeleteProcedure((short) 501),

  /** Authority. */
  Author((short) 600),

  // For version earlier than 1.2. Dep for Deprecated.
  CreateUserDep((short) 601),
  CreateRoleDep((short) 602),
  DropUserDep((short) 603),
  DropRoleDep((short) 604),
  GrantRoleDep((short) 605),
  GrantUserDep((short) 606),
  GrantRoleToUserDep((short) 607),
  RevokeUserDep((short) 608),
  RevokeRoleDep((short) 609),
  RevokeRoleFromUserDep((short) 610),
  UpdateUserDep((short) 611),
  ListUserDep((short) 612),
  ListRoleDep((short) 613),
  ListUserPrivilegeDep((short) 614),
  ListRolePrivilegeDep((short) 615),
  @Deprecated
  ListUserRolesDep((short) 616),
  @Deprecated
  ListRoleUsersDep((short) 617),

  // For version after and equal 1.2
  CreateUser((short) 621),
  CreateRole((short) 622),
  @Deprecated
  DropUser((short) 623),
  DropRole((short) 624),
  GrantRole((short) 625),
  GrantUser((short) 626),
  GrantRoleToUser((short) 627),
  RevokeUser((short) 628),
  RevokeRole((short) 629),
  RevokeRoleFromUser((short) 630),
  @Deprecated
  UpdateUser((short) 631),
  ListUser((short) 632),
  ListRole((short) 633),
  ListUserPrivilege((short) 634),
  ListRolePrivilege((short) 635),
  @Deprecated
  ListUserRoles((short) 636),
  @Deprecated
  ListRoleUsers((short) 637),
  CreateUserWithRawPassword((short) 638),
  UpdateUserMaxSession((short) 639),
  UpdateUserMinSession((short) 640),
  AccountUnlock((short) 641),

  /** Table Author */
  RCreateUser((short) 641),
  RCreateRole((short) 642),
  @Deprecated
  RUpdateUser((short) 643),
  @Deprecated
  RDropUser((short) 644),
  RDropRole((short) 645),
  RGrantUserRole((short) 646),
  RRevokeUserRole((short) 647),
  RGrantUserAny((short) 648),
  RGrantRoleAny((short) 649),
  RGrantUserAll((short) 650),
  RGrantRoleAll((short) 652),
  RGrantUserDBPriv((short) 653),
  RGrantUserTBPriv((short) 654),
  RGrantRoleDBPriv((short) 655),
  RGrantRoleTBPriv((short) 656),
  RRevokeUserAny((short) 657),
  RRevokeRoleAny((short) 658),
  RRevokeUserAll((short) 659),
  RRevokeRoleAll((short) 660),
  RRevokeUserDBPriv((short) 661),
  RRevokeUserTBPriv((short) 662),
  RRevokeRoleDBPriv((short) 663),
  RRevokeRoleTBPriv((short) 664),
  RGrantUserSysPri((short) 665),
  RGrantRoleSysPri((short) 666),
  RRevokeUserSysPri((short) 667),
  RRevokeRoleSysPri((short) 668),
  RListUser((short) 669),
  RListRole((short) 670),
  RListUserPrivilege((short) 671),
  RListRolePrivilege((short) 672),
  RUpdateUserMaxSession((short) 673),
  RUpdateUserMinSession((short) 674),
  RAccountUnlock((short) 675),

  /** Function. */
  CreateFunction((short) 700),
  DropTreeModelFunction((short) 701),
  GetFunctionTable((short) 702),
  GetFunctionJar((short) 703),
  GetAllFunctionTable((short) 704),
  UpdateFunction((short) 705),
  DropTableModelFunction((short) 706),

  /** Template. */
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
  PreSetSchemaTemplate((short) 812),
  CommitSetSchemaTemplate((short) 813),
  ExtendSchemaTemplate((short) 814),

  /* Table or View */
  PreCreateTable((short) 850),
  RollbackCreateTable((short) 851),
  CommitCreateTable((short) 852),
  AddTableColumn((short) 853),
  SetTableProperties((short) 854),
  ShowTable((short) 855),
  FetchTable((short) 856),
  RenameTableColumn((short) 857),
  PreDeleteTable((short) 858),
  CommitDeleteTable((short) 859),
  PreDeleteColumn((short) 860),
  CommitDeleteColumn((short) 861),
  DescTable((short) 862),
  ShowTable4InformationSchema((short) 863),
  DescTable4InformationSchema((short) 864),
  SetTableColumnComment((short) 865),
  SetTableComment((short) 866),
  RenameTable((short) 867),
  PreCreateTableView((short) 868),
  SetViewComment((short) 869),
  AddViewColumn((short) 870),
  CommitDeleteViewColumn((short) 871),
  CommitDeleteView((short) 872),
  RenameView((short) 873),
  SetViewProperties((short) 874),
  PreDeleteViewColumn((short) 875),
  PreDeleteView((short) 876),
  RenameViewColumn((short) 877),
  AlterColumnDataType((short) 878),
  CommitAlterColumnDataType((short) 879),

  /** Deprecated types for sync, restored them for upgrade. */
  @Deprecated
  CreatePipeSinkV1((short) 900),
  @Deprecated
  DropPipeSinkV1((short) 901),
  @Deprecated
  GetPipeSinkV1((short) 902),
  @Deprecated
  PreCreatePipeV1((short) 903),
  @Deprecated
  SetPipeStatusV1((short) 904),
  @Deprecated
  DropPipeV1((short) 905),
  @Deprecated
  ShowPipeV1((short) 906),
  @Deprecated
  RecordPipeMessageV1((short) 907),

  /** Trigger. */
  AddTriggerInTable((short) 1000),
  DeleteTriggerInTable((short) 1001),
  GetTriggerTable((short) 1002),
  UpdateTriggerStateInTable((short) 1003),
  GetTriggerJar((short) 1004),
  UpdateTriggersOnTransferNodes((short) 1005),
  UpdateTriggerLocation((short) 1006),
  GetTransferringTriggers((short) 1007),
  GetTriggerLocation((short) 1008),

  /** CQ. */
  DROP_CQ((short) 1100),
  ACTIVE_CQ((short) 1101),
  ADD_CQ((short) 1102),
  UPDATE_CQ_LAST_EXEC_TIME((short) 1103),
  SHOW_CQ((short) 1104),

  /** AI model. */
  CreateModel((short) 1200),
  UpdateModelInfo((short) 1201),
  UpdateModelState((short) 1202),
  DropModel((short) 1203),
  ShowModel((short) 1204),
  GetModelInfo((short) 1206),
  DropModelInNode((short) 1207),

  /** Pipe Plugin. */
  CreatePipePlugin((short) 1300),
  DropPipePlugin((short) 1301),
  GetPipePluginTable((short) 1302),
  GetPipePluginJar((short) 1303),

  /** Quota. */
  setSpaceQuota((short) 1400),
  setThrottleQuota((short) 1401),

  /** Pipe Task. */
  CreatePipeV2((short) 1500),
  SetPipeStatusV2((short) 1501),
  DropPipeV2((short) 1502),
  ShowPipeV2((short) 1503),
  AlterPipeV2((short) 1504),
  OperateMultiplePipesV2((short) 1505),

  /** Pipe Runtime. */
  PipeHandleLeaderChange((short) 1600),
  PipeHandleMetaChange((short) 1601),

  /** Pipe PayLoad. */
  PipeEnriched((short) 1700),
  PipeUnsetTemplate((short) 1701),
  PipeDeleteTimeSeries((short) 1702),
  PipeDeleteLogicalView((short) 1703),
  PipeDeactivateTemplate((short) 1704),
  PipeSetTTL((short) 1705),
  PipeCreateTableOrView((short) 1706),
  PipeDeleteDevices((short) 1707),
  PipeAlterEncodingCompressor((short) 1708),
  PipeAlterTimeSeries((short) 1709),

  /** Subscription */
  CreateTopic((short) 1800),
  DropTopic((short) 1801),
  ShowTopic((short) 1802),
  AlterTopic((short) 1803),
  AlterMultipleTopics((short) 1804),
  TopicHandleMetaChange((short) 1805),

  AlterConsumerGroup((short) 1900),
  ConsumerGroupHandleMetaChange((short) 1901),

  ShowSubscription((short) 2000),

  // Authority version after and equal 2.0
  DropUserV2((short) 2100),
  UpdateUserV2((short) 2101),
  RUpdateUserV2((short) 2102),
  RDropUserV2((short) 2103),
  RenameUser((short) 2104),
  RRenameUser((short) 2105),

  EnableSeparationOfAdminPowers((short) 2200),

  CreateExternalService((short) 2301),
  StartExternalService((short) 2302),
  StopExternalService((short) 2303),
  DropExternalService((short) 2304),
  ShowExternalService((short) 2305),

  /** Test Only. */
  TestOnly((short) 30000),
  ;

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

  /** Notice: the result might be null. */
  public static ConfigPhysicalPlanType convertToConfigPhysicalPlanType(short planType) {
    return PLAN_TYPE_MAP.getOrDefault(planType, null);
  }
}
