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

package org.apache.iotdb.confignode.i18n;

public final class ConfigNodeMessages {

  public static final String ACQUIRE_TRIGGERTABLELOCK = "获取 TriggerTableLock";
  public static final String ACQUIRE_UDFTABLELOCK = "获取 UDFTableLock";
  public static final String ACTIVATING = "正在激活 {}...";
  public static final String ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] Database: {} 的 DataRegionGroup 数量上限已调整为：{}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] Database: {} 的 SchemaRegionGroup 数量上限已调整为：{}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] Database: {} 的 DataRegionGroup 数量下限已调整为：{}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] Database: {} 的 SchemaRegionGroup 数量下限已调整为：{}";
  public static final String CANNOT_FIND_REGIONGROUP_FOR_REGION_WHEN_ADDREGIONNEWLOCATION_IN =
      "在 {} 中执行 addRegionNewLocation 时，找不到 region {} 对应的 RegionGroup";
  public static final String CANNOT_FIND_REGIONGROUP_FOR_REGION_WHEN_REMOVEREGIONOLDLOCATION_IN =
      "在 {} 中执行 removeRegionOldLocation 时，找不到 region {} 对应的 RegionGroup";
  public static final String CAN_ONLY_ALTER_DATATYPE_OF_FIELD_COLUMNS = "只能修改 FIELD 列的数据类型";
  public static final String CAN_T_CLOSE_STANDALONELOG_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
      "无法为 ConfigNode SimpleConsensus 模式关闭 StandAloneLog，";
  public static final String CAN_T_CONNECT_TO_DATA_NODE = "无法连接到 DataNode：{}";
  public static final String CAN_T_CONSTRUCT_CLUSTERSCHEMAINFO = "无法构建 ClusterSchemaInfo";
  public static final String CAN_T_DELETE_TEMPORARY_SNAPSHOT_FILE_RETRYING =
      "无法删除临时快照文件：{}，正在重试...";
  public static final String CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_FLUSHWALFORSIMPLECONSENSUS =
      "无法为 ConfigNode flushWALForSimpleConsensus 强制写入 logWriter";
  public static final String CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
      "无法为 ConfigNode SimpleConsensus 模式强制写入 logWriter";
  public static final String
      CAN_T_SERIALIZE_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
          "无法为 ConfigNode SimpleConsensus 模式序列化当前 ConfigPhysicalPlan";
  public static final String CAN_T_START_CONFIGNODE_CONSENSUS_GROUP = "无法启动 ConfigNode 共识组！";
  public static final String CHANGE_REGIONS_LEADER_ERROR_ON_DATE_NODE =
      "在 DataNode: {} 上切换 region leader 失败";
  public static final String CHECK_BEFORE_DROPPING_TOPIC_TOPIC_EXISTS =
      "删除 topic 前的检查：{}，topic 是否存在：{}";
  public static final String CHECK_BEFORE_DROP_PIPE_PIPE_EXISTS = "删除 pipe {} 前的检查，pipe 是否存在：{}。";
  public static final String CLUSTERID_HAS_BEEN_GENERATED = "已生成 clusterID：{}";
  public static final String CLUSTERID_HAS_BEEN_RECOVERED_FROM_SNAPSHOT = "已从快照恢复 clusterID：{}";
  public static final String CLUSTERID_NOT_GENERATED_YET_SHOULD_NEVER_HAPPEN =
      "clusterId 尚未生成，理论上不应发生。";
  public static final String CONFIGNODESNAPSHOT_FINISH_TO_TAKE_SNAPSHOT_FOR_TIME_CONSUMPTION_MS =
      "[ConfigNodeSnapshot] 完成为 {} 创建快照，耗时：{} ms";
  public static final String CONFIGNODESNAPSHOT_LOAD_SNAPSHOT_FOR_COST_MS =
      "[ConfigNodeSnapshot] 加载 {} 的快照耗时 {} ms";
  public static final String CONFIGNODESNAPSHOT_LOAD_SNAPSHOT_SUCCESS_LATESTSNAPSHOTROOTDIR =
      "[ConfigNodeSnapshot] 加载快照成功，latestSnapshotRootDir: {}";
  public static final String CONFIGNODESNAPSHOT_START_TO_LOAD_SNAPSHOT_FOR_FROM =
      "[ConfigNodeSnapshot] 开始从 {} 加载 {} 的快照";
  public static final String CONFIGNODESNAPSHOT_START_TO_TAKE_SNAPSHOT_FOR_INTO =
      "[ConfigNodeSnapshot] 开始为 {} 创建快照，目标路径：{}";
  public static final String CONFIGNODESNAPSHOT_TASK_SNAPSHOT_SUCCESS_SNAPSHOTDIR =
      "[ConfigNodeSnapshot] 任务快照成功，snapshotDir: {}";
  public static final String CONFIGNODE_EXITING = "ConfigNode 正在退出...";
  public static final String CONFIGNODE_NEED_REDIRECT_TO_RETRY = "ConfigNode 需要重定向到 {}，重试 {} ...";
  public static final String CONFIGNODE_MEMORY_PROPORTION_SHOULD_BE_IN_THE_FORM_OF_PIPE_FREE =
      "参数 confignode_memory_proportion 应为 Pipe:Free 格式，" + "但当前值为 {}。将使用默认值 1:9。";
  public static final String INITIAL_CONFIGNODE_ALLOCATE_MEMORY_FOR_PIPE =
      "初始化 ConfigNode allocateMemoryForPipe = {}";
  public static final String INITIAL_CONFIGNODE_FREE_MEMORY = "初始化 ConfigNode freeMemory = {}";
  public static final String CONFIGNODE_PORT_CHECK_SUCCESSFUL = "ConfigNode 端口检查成功。";
  public static final String CONFIGNODE_RPC_SERVICE_FINISHED_TO_REMOVE_AINODE_RESULT =
      "ConfigNode RPC Service 完成移除 AINode，结果：{}";
  public static final String CONFIGNODE_RPC_SERVICE_FINISHED_TO_REMOVE_DATANODE_REQ_RESULT =
      "ConfigNode RPC Service 完成移除 DataNode，请求：{}，结果：{}";
  public static final String CONFIGNODE_RPC_SERVICE_START_TO_REMOVE_AINODE =
      "ConfigNode RPC Service 开始移除 AINode";
  public static final String CONFIGNODE_RPC_SERVICE_START_TO_REMOVE_DATANODE_REQ =
      "ConfigNode RPC Service 开始移除 DataNode，请求：{}";
  public static final String CONFIGNODE_SIMPLECONSENSUSFILE_HAS_EXISTED_FILEPATH =
      "ConfigNode SimpleConsensusFile 已存在，filePath:{}";
  public static final String
      CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_THE_HISTORICAL =
          "Config Region Listening Queue 监听快照失败，历史数据可能未被传输。";
  public static final String CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_WHEN_STARTUP =
      "Config Region Listening Queue 启动时监听快照失败，将在启动 schema 传输 pipe 时再次尝试";
  public static final String CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER =
      "continuous_query_min_every_interval_in_ms 应大于 0，但当前值为 {}，忽略并使用默认值 {}";
  public static final String CONTINUOUS_QUERY_SUBMIT_THREAD_SHOULD_BE_GREATER_THAN_0 =
      "continuous_query_submit_thread 应大于 0，但当前值为 {}，忽略并使用默认值 {}";
  public static final String COULDN_T_LOAD_CONFIGNODE_CONF_FILE_REJECT_CONFIGNODE_STARTUP =
      "无法加载 ConfigNode 配置文件，拒绝启动 ConfigNode。";
  public static final String COULDN_T_LOAD_THE_CONFIGURATION_FROM_ANY_OF_THE_KNOWN =
      "无法从任何已知来源加载配置 {}。";
  public static final String
      CREATEREGIONGROUPS_DATABASE_HAS_BEEN_DELETED_CORRESPONDING_REGIONGROUPS =
          "[CreateRegionGroups] Database {} 已被删除，不会创建对应的 RegionGroup。";
  public static final String CREATE_CONFIGNODE_SIMPLECONSENSUSFILE =
      "创建 ConfigNode SimpleConsensusFile: {}";
  public static final String CREATE_CONFIGNODE_SIMPLECONSENSUSFILE_FAILED_FILEPATH =
      "创建 ConfigNode SimpleConsensusFile 失败，filePath: {}";
  public static final String CURRENT_NODE_NODEID_IP_PORT_AS_CONFIG_REGION_LEADER_IS =
      "当前节点 [nodeId: {}, ip:port: {}] 作为 config region leader 已就绪";
  public static final String CURRENT_NODE_NODEID_IP_PORT_BECOMES_CONFIG_REGION_LEADER =
      "当前节点 [nodeId: {}, ip:port: {}] 成为 config region leader";
  public static final String CURRENT_NODE_NODEID_IP_PORT_IS_NO_LONGER_THE_LEADER =
      "当前节点 [nodeId:{}, ip:port: {}] 已不再是 leader，";
  public static final String
      MESSAGE_CURRENT_NODE_NODEID_ARG_IP_PORT_ARG_IS_NO_LONGER_THE_LEADER_SKIP_STARTING_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_CCF39435 =
          "当前节点 [nodeId:{}，ip:port: {}] 已不再是 leader，跳过 leader 服务启动，因为 leader epoch 已过期";
  public static final String
      DATABASE_INCONSISTENCY_DETECTED_WHEN_ADJUSTING_MAX_REGION_GROUP_COUNT_MESSAGE =
          "调整最大 region group 数量时检测到数据库不一致，消息：{}，将通过以下调整计划进行修正";
  public static final String DATABASE_NOT_EXIST = "数据库不存在";
  public static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "data_region_consensus_protocol_class";
  public static final String DEACTIVATING = "正在停用 {}...";
  public static final String DEFAULT_CHARSET_IS = "{} 默认字符集为：{}";
  public static final String DELETED_FAILED_TAKE_APPROPRIATE_ACTION = "{} 删除失败，请采取相应处理。";
  public static final String DELETE_USELESS_PROCEDURE_WAL_DIR_FAIL = "删除无用的 procedure WAL 目录失败。";
  public static final String DESERIALIZATION_ERROR_FOR_WRITE_PLAN_REQUEST_BYTEBUFFER =
      "反序列化写入计划出错，请求：{}，bytebuffer: {}";
  public static final String DOES_NOT_EXIST = "%s 不存在";
  public static final String DROPPING_TAG_OR_TIME_COLUMN_IS_NOT_SUPPORTED = "不支持删除标签列或时间列。";
  public static final String DROP_CQ_FAILED_BECAUSE_ITS_TOKEN_DOESN_T_MATCH =
      "删除 CQ {} 失败，因为其 token 不匹配。";
  public static final String DROP_CQ_FAILED_BECAUSE_IT_DOESN_T_EXIST = "删除 CQ {} 失败，因为该 CQ 不存在。";
  public static final String DROP_CQ_SUCCESSFULLY = "成功删除 CQ {}。";
  public static final String DUPLICATED_TEMPLATE_NAME = "模板名称重复：";
  public static final String ENABLESEPARATIONOFADMINPOWERS_IS_NOT_SUPPORTED =
      "不支持 EnableSeparationOfAdminPowers";
  public static final String ENVIRONMENT_VARIABLES = "{} 环境变量：{}";
  public static final String ERROR_GET_MATCHED_PATHS_IN_GIVEN_LEVEL = "在给定层级获取匹配路径时出错。";
  public static final String ERROR_GET_MATCHED_PATHS_IN_NEXT_LEVEL = "在下一层级获取匹配路径时出错。";
  public static final String ERROR_OCCURRED_WHEN_GET_PATHS_SET_ON_TEMPLATE = "获取模板 {} 上设置的路径时出错";
  public static final String ERROR_STARTING = "启动出错";
  public static final String EXECUTE_ALTERDATABASE_WITH_RESULT = "执行 AlterDatabase: {}，结果：{}";
  public static final String EXECUTE_GETCLUSTERID_WITH_RESULT = "执行 getClusterId，结果 {}";
  public static final String EXECUTE_GETSYSTEMCONFIGURATION_WITH_RESULT =
      "执行 GetSystemConfiguration，结果 {}";
  public static final String EXECUTE_NON_QUERY_PLAN_FAILED = "执行非查询计划失败";
  public static final String EXECUTE_QUERY_PLAN_FAILED = "执行查询计划失败";
  public static final String EXECUTE_REGISTERAINODEREQUEST_WITH_RESULT =
      "执行 RegisterAINodeRequest {}，结果 {}";
  public static final String EXECUTE_REGISTERCONFIGNODEREQUEST_WITH_RESULT =
      "执行 RegisterConfigNodeRequest {}，结果 {}";
  public static final String EXECUTE_REGISTERDATANODEREQUEST_WITH_RESULT =
      "执行 RegisterDataNodeRequest {}，结果 {}";
  public static final String EXECUTE_RESTARTAINODEREQUEST_WITH_RESULT =
      "执行 RestartAINodeRequest {}，结果 {}";
  public static final String EXECUTE_RESTARTDATANODEREQUEST_WITH_RESULT =
      "执行 RestartDataNodeRequest {}，结果 {}";
  public static final String EXECUTE_GET_METADATA_WITH_RESULT = "执行 GetMetaDataCache, 结果 result {}";
  public static final String EXECUTE_SETDATABASE_WITH_RESULT = "执行 SetDatabase: {}，结果：{}";
  public static final String FAILED_IN_THE_READ_API_EXECUTING_THE_CONSENSUS_LAYER_DUE =
      "执行共识层读取 API 失败：";
  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE =
      "执行共识层写入 API 失败：";
  public static final String FAILED_ON_AINODE = "{} 在 AINode {} 上失败";
  public static final String FAILED_ON_AINODE_RETRYING = "{} 在 AINode {} 上失败，重试 {}...";
  public static final String FAILED_ON_CONFIGNODE = "{} 在 ConfigNode {} 上失败";
  public static final String FAILED_ON_CONFIGNODE_BECAUSE_RETRYING =
      "{} 在 ConfigNode {} 上失败，原因：{}，重试 {}...";
  public static final String FAILED_ON_DATANODE = "{} 在 DataNode {} 上失败";
  public static final String FAILED_ON_DATANODE_RETRYING = "{} 在 DataNode {} 上失败，重试 {}...";
  public static final String FAILED_TO_ALTER_PIPE = "修改 pipe 失败";
  public static final String FAILED_TO_CHECK_SCHEMA_REGION_USING_TEMPLATE_ON_DATANODE =
      "在 DataNode {} 上检查 schema region 是否使用模板失败，{}";
  public static final String FAILED_TO_CHECK_TIMESERIES_EXISTENCE_ON_DATANODE =
      "在 DataNode {} 上检查时间序列是否存在失败，{}";
  public static final String FAILED_TO_COUNT_PATHS_USING_TEMPLATE_ON_DATANODE =
      "在 DataNode {} 上统计使用模板的路径数量失败，{}";
  public static final String FAILED_TO_CREATE_MULTIPLE_PIPES = "批量创建 pipe 失败";
  public static final String FAILED_TO_CREATE_PIPE = "创建 pipe 失败";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_SOURCE_PIPEPLUGIN_FAILED_TO_LOAD =
      "创建 PipePlugin [%s] 失败，源 PipePlugin [%s] 加载失败：%s";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_SOURCE_PIPEPLUGIN_JAR_DOES_NOT_EXIST =
      "创建 PipePlugin [%s] 失败，源 PipePlugin [%s] 的 jar [%s] 在安装目录中不存在。";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_THE_SAME_NAME_PIPEPLUGIN_HAS_BEEN =
      "创建 PipePlugin [%s] 失败，同名 PipePlugin 已被创建";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_THIS_PIPEPLUGIN_EXISTS_BUT_FAILED_TO =
      "创建 PipePlugin [%s] 失败，该 PipePlugin 已存在但加载失败：%s";
  public static final String FAILED_TO_CREATE_TEMPLATE_BECAUSE_TEMPLATE_NAME_EXISTS =
      "创建模板失败，模板名称 {} 已存在";
  public static final String FAILED_TO_CREATE_TRIGGER_THE_SAME_NAME_JAR_BUT_DIFFERENT =
      "创建 trigger [%s] 失败，同名 Jar [%s] 但 MD5 [%s] 不同已存在";
  public static final String FAILED_TO_CREATE_TRIGGER_THE_SAME_NAME_TRIGGER_HAS_BEEN =
      "创建 trigger [%s] 失败，同名 trigger 已被创建";
  public static final String FAILED_TO_CREATE_UDF_THE_SAME_NAME_JAR_BUT_DIFFERENT =
      "创建 UDF [%s] 失败，同名 Jar [%s] 但 MD5 [%s] 不同已存在";
  public static final String FAILED_TO_CREATE_UDF_THE_SAME_NAME_UDF_HAS_BEEN =
      "创建 UDF [%s] 失败，同名 UDF 已被创建";
  public static final String FAILED_TO_DECREASE_LISTENER_REFERENCE = "减少 listener 引用计数失败";
  public static final String FAILED_TO_DROP_PIPE = "删除 pipe 失败";
  public static final String FAILED_TO_DROP_PIPEPLUGIN_THE_PIPEPLUGIN_IS_A_BUILT_IN =
      "删除 PipePlugin [%s] 失败，该 PipePlugin 是内置 PipePlugin";
  public static final String FAILED_TO_DROP_PIPEPLUGIN_THIS_PIPEPLUGIN_HAS_NOT_BEEN_CREATED =
      "删除 PipePlugin [%s] 失败，该 PipePlugin 尚未被创建";
  public static final String FAILED_TO_DROP_TRIGGER_THIS_TRIGGER_HAS_NOT_BEEN_CREATED =
      "删除 trigger [%s] 失败，该 trigger 尚未被创建";
  public static final String FAILED_TO_DROP_UDF_THIS_UDF_HAS_NOT_BEEN_CREATED =
      "删除 UDF [%s] 失败，该 UDF 尚未被创建";
  public static final String
      FAILED_TO_ENRICH_PIPE_WITH_ROOT_USER_FOR_COMPATIBILITY_BECAUSE_ROOT_USER_DOES_NOT_EXIST =
          "为兼容性向 pipe %s 补充 root 用户失败，因为 root 用户 %s 不存在。";
  public static final String FAILED_TO_FETCH_SCHEMAENGINE_BLACK_LIST_ON_DATANODE =
      "在 DataNode {} 上拉取 schemaengine 黑名单失败，{}";
  public static final String FAILED_TO_GET_FIELD = "获取字段 {} 失败";
  public static final String FAILED_TO_HANDLE_LEADER_CHANGE = "处理 leader 变更失败";
  public static final String FAILED_TO_HANDLE_META_CHANGES = "处理元数据变更失败";
  public static final String FAILED_TO_INCREASE_LISTENER_REFERENCE = "增加 listener 引用计数失败";
  public static final String FAILED_TO_LOAD_PIPE_INFO_FROM_SNAPSHOT = "从快照加载 pipe 信息失败，";
  public static final String FAILED_TO_LOAD_PIPE_PLUGIN_INFO_FROM_SNAPSHOT =
      "从快照加载 pipe plugin 信息失败";
  public static final String FAILED_TO_LOAD_PIPE_TASK_INFO_FROM_SNAPSHOT = "从快照加载 pipe task 信息失败";
  public static final String FAILED_TO_LOAD_PLUGIN_CLASS_FOR_PLUGIN_WHEN_LOADING_SNAPSHOT =
      "加载快照 [{}] 时，为 plugin [{}] 加载插件类失败 ";
  public static final String FAILED_TO_LOAD_SNAPSHOT_BECAUSE_GET_NULL_DATABASE_NAME =
      "加载快照失败，因为获取到的数据库名为空";
  public static final String FAILED_TO_LOAD_SNAPSHOT_BECAUSE_SNAPSHOT_DIR_NOT_EXISTS =
      "加载快照失败，因为快照目录 [{}] 不存在。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_CQINFO_SNAPSHOT_FILE_DOES_NOT =
      "加载 CQInfo 的快照失败，快照文件 [{}] 不存在。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_TEMPLATEPRESETTABLE_SNAPSHOT_FILE_IS_NOT =
      "加载 TemplatePreSetTable 的快照失败，快照文件 [{}] 不是有效文件。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_TTLINFO_SNAPSHOT_FILE_DOES_NOT =
      "加载 TTLInfo 的快照失败，快照文件 [{}] 不存在。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST =
      "加载快照失败，快照文件 [{}] 不存在。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2 =
      "加载快照失败，快照文件 [{}] 不存在。";
  public static final String FAILED_TO_LOAD_SUBSCRIPTION_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST =
      "加载订阅快照失败，快照文件 {} 不存在。";
  public static final String FAILED_TO_ON_CONFIGNODE_RESPONSE = "在 ConfigNode: {} 上执行 {} 失败，响应：{}";
  public static final String FAILED_TO_ON_DATANODE = "在 DataNode {} 上执行 {} 失败，{}";
  public static final String FAILED_TO_ON_DATANODE_EXCEPTION = "在 DataNode: {} 上执行 {} 失败，异常：{}";
  public static final String FAILED_TO_ON_DATANODE_RESPONSE = "在 DataNode: {} 上执行 {} 失败，响应：{}";
  public static final String FAILED_TO_OPERATE_PIPE = "操作 pipe 失败";
  public static final String FAILED_TO_SET_PIPE_STATUS = "设置 pipe 状态失败";
  public static final String FAILED_TO_SET_PIPE_STATUS_WITH_STOPPED_BY_RUNTIME_EXCEPTION =
      "设置 pipe 状态及运行时异常停止标记失败";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_CREATE_TMP_DIR_FAIL =
      "创建快照失败，因为创建临时目录 [{}] 失败。";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_DIR_IS_ALREADY_EXIST =
      "创建快照失败，因为快照目录 [{}] 已存在。";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY_EXIST =
      "创建快照失败，因为快照文件 [{}] 已存在。";
  public static final String FAILED_TO_TAKE_SNAPSHOT_OF_CQINFO_BECAUSE_SNAPSHOT_FILE_IS =
      "创建 CQInfo 的快照失败，因为快照文件 [{}] 已存在。";
  public static final String
      FAILED_TO_TAKE_SNAPSHOT_OF_TEMPLATEPRESETTABLE_BECAUSE_SNAPSHOT_FILE_IS =
          "创建 TemplatePreSetTable 的快照失败，因为快照文件 [{}] 已存在。";
  public static final String FAILED_TO_TAKE_SNAPSHOT_OF_TTLINFO_BECAUSE_SNAPSHOT_FILE_IS =
      "创建 TTLInfo 的快照失败，因为快照文件 [{}] 已存在。";
  public static final String FAILED_TO_TAKE_SUBSCRIPTION_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY =
      "创建订阅快照失败，因为快照文件 {} 已存在。";
  public static final String FAILED_TO_UPDATE_CONFIG_FILE = "更新配置文件失败";
  public static final String FILE_NOT_EXISTS = "文件 {} 不存在";
  public static final String FOR_RECEIVES = "{} for {} 收到响应：{}";
  public static final String GET_DATANODE_CPU_CORE_FAIL_WILL_BE_TREATED_AS_ZERO =
      "获取 DataNode {} CPU 核数失败，将按 0 处理。";
  public static final String GET_PIPEPLUGIN_JAR_FAILED = "获取 PipePlugin Jar 失败";
  public static final String GET_TRIGGERJAR_FAILED = "获取 TriggerJar 失败";
  public static final String GET_UDF_JAR_FAILED = "获取 UDF Jar 失败";
  public static final String GET_URL_FAILED = "获取 URL 失败";
  public static final String GET_USER_OR_ROLE_PERMISSIONINFO_FAILED_BECAUSE = "获取用户或角色权限信息失败，原因：";
  public static final String HANDLING_CONSUMER_GROUP_META_CHANGES = "正在处理 consumer group 元数据变更 ...";
  public static final String HANDLING_PIPE_META_CHANGES = "正在处理 pipe 元数据变更 ...";
  public static final String HANDLING_TOPIC_META_CHANGES = "正在处理 topic 元数据变更 ...";
  public static final String HAS_REGISTERED_SUCCESSFULLY_WAITING_FOR_THE_LEADER_S_SCHEDULING_TO =
      "{} {} 已成功注册。等待 leader 调度以加入集群：{}。";
  public static final String HAS_SUCCESSFULLY_RESTARTED_AND_JOINED_THE_CLUSTER =
      "{} 已成功重启并加入集群：{}。";
  public static final String HAS_SUCCESSFULLY_STARTED_AND_JOINED_THE_CLUSTER = "{} 已成功启动并加入集群：{}。";
  public static final String ID_TOOK_SNAPSHOT_FAIL = "{} id {} 创建快照失败";
  public static final String
      INITSTANDALONECONFIGNODE_MEETS_ERROR_CAN_T_FIND_STANDALONE_LOG_FILES_FILEPATH =
          "InitStandAloneConfigNode 出错，找不到 standalone log 文件，filePath: {}";
  public static final String INVALID_AUTHOR_TYPE_ORDINAL = "无效的 Author 类型序号";
  public static final String IOTDB_STARTED = "IoTDB 已启动";
  public static final String IS_DEACTIVATED = "{} 已停用。";
  public static final String IS_IN_RESTARTING_PROCESS = "{} 正在重启...";
  public static final String LEADER_DISTRIBUTION_POLICY = "leader_distribution_policy";
  public static final String LEADER_HAS_NOT_BEEN_ELECTED_YET_WAIT_FOR_1_SECOND =
      "Leader 尚未选出，等待 1 秒";
  public static final String LOAD_FAILED_IT_WILL_BE_DELETED = "加载 {} 失败，将其删除。";
  public static final String LOAD_PROCEDURE_WAL_FAILED = "加载 procedure WAL 失败。";
  public static final String LOAD_SNAPSHOT_ERROR = "加载快照出错";
  public static final String MAKE_DIRS = "创建目录：{}";
  public static final String MEET_ERROR_WHEN_DEACTIVATE_CONFIGNODE = "停用 ConfigNode 时遇到错误";
  public static final String MEET_ERROR_WHEN_DOING_START_CHECKING = "启动检查时遇到错误";
  public static final String MEET_ERROR_WHILE_STARTING_UP = "启动时遇到错误。";
  public static final String NEW_TYPE_IS_NOT_COMPATIBLE_WITH_THE_EXISTING_ONE =
      "新类型 %s 与已有类型 %s 不兼容";
  public static final String NODE_IS_ALREADY_IN_REGION_LOCATIONS_WHEN_ADDREGIONNEWLOCATION_IN =
      "执行 addRegionNewLocation 时节点已在 region locations 中，{}，";
  public static final String NODE_IS_NOT_IN_REGION_LOCATIONS_WHEN_REMOVEREGIONOLDLOCATION_IN =
      "执行 removeRegionOldLocation 时节点不在 region locations 中，{}，";
  public static final String OLD_PROCEDURE_FILES_HAVE_BEEN_LOADED_SUCCESSFULLY_TAKING_SNAPSHOT =
      "旧的 procedure 文件已成功加载，正在创建快照...";
  public static final String PARTITIONTABLECLEANER_THE_TIMEPARTITIONS_ARE_REMOVED_FROM_DATABASE =
      "[PartitionTableCleaner] TimePartitions: {} 已从 Database: {} 中移除";
  public static final String PATH1_SHOULD_NOT_BE_NULL = "Path1 不应为 null";
  public static final String PIPEMETASYNCER_IS_TRYING_TO_RESTART_THE_PIPES =
      "PipeMetaSyncer 正在尝试重启 pipe：{}";
  public static final String PIPE_IS_USING_EXTERNAL_SOURCE_SKIP_REGION =
      "Pipe {} 使用外部 source，跳过 region leader 切换。PipeHandleLeaderChangePlan: {}";
  public static final String PLAN_TYPE_IS_NOT_SUPPORTED = "不支持的计划类型 %s。";
  public static final String PLEASE_SET_THE_CN_SEED_CONFIG_NODE_PARAMETER_IN_IOTDB =
      "请在 iotdb-system.properties 文件中设置 cn_seed_config_node 参数。";
  public static final String PORTS_USED_IN_CONFIGNODE_HAVE_REPEAT = "ConfigNode 使用的端口存在重复。";
  public static final String REACH_EOF = "到达 EOF";
  public static final String RECORDING_CONSUMER_GROUP_META = "正在记录 consumer group 元数据：{}";
  public static final String RECORDING_TOPIC_META = "正在记录 topic 元数据：{}";
  public static final String RECOVERED_CONSENSUS_PIPES_AS_RUNNING_DURING_SNAPSHOT_LOAD =
      "加载快照期间将 consensus pipe {} 恢复为 RUNNING 状态。";
  public static final String RELEASE_TRIGGERTABLELOCK = "释放 TriggerTableLock";
  public static final String RELEASE_UDFTABLELOCK = "释放 UDFTableLock";
  public static final String REMOVED_THE_AINODE_FROM_CLUSTER = "已从集群中移除 AINode {}";
  public static final String REMOVED_THE_DATANODE_FROM_CLUSTER = "已从集群中移除 datanode {}";
  public static final String REMOVE_ONLINE_CONFIGNODE_FAILED = "移除在线 ConfigNode 失败。";
  public static final String REPORTING_CONFIGNODE_SHUTDOWN_FAILED_THE_CLUSTER_WILL_STILL_TAKE_THE =
      "上报 ConfigNode 关闭失败。集群在接下来几秒内仍会将当前 ConfigNode 视为 Running。";
  public static final String RETRY_WAIT_FAILED = "重试等待失败。";
  public static final String ROUTE_PRIORITY_POLICY = "route_priority_policy";
  public static final String SCHEMA_OF_MEASUREMENT_IS_NOT_COMPATIBLE_WITH_EXISTING_MEASUREMENT_IN =
      "测点 %s 的 schema 与模板 %s 中的已有测点不兼容";
  public static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "schema_region_consensus_protocol_class";
  public static final String SEND_RPC_TO_DATA_NODE_FOR_CHANGING_REGIONS_LEADER_ON =
      "向 data node: {} 发送 RPC 以切换其上的 region leader";
  public static final String SETTTL_THE_TTL_OF_DATABASE_IS_ADJUSTED_TO =
      "[SetTTL] Database: {} 的 TTL 已调整为：{}";
  public static final String SNAPSHOT_DIRECTORY_CAN_NOT_BE_CREATED = "无法创建快照目录 [{}]。";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EMPTY = "快照目录 [{}] 不为空。";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EXIST_CAN_NOT_LOAD_SNAPSHOT_WITH =
      "快照目录 [{}] 不存在，无法使用该目录加载快照。";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EXIST_START_TO_CREATE_IT =
      "快照目录 [{}] 不存在，开始创建。";
  public static final String STARTING_IOTDB = "正在启动 IoTDB {}";
  public static final String START_CONFIGNODE_FAILED_BECAUSE_COULDN_T_MAKE_SYSTEM_DIRS =
      "启动 ConfigNode 失败，无法创建系统目录：%s。";
  public static final String START_READING_CONFIGNODE_CONF_FILE = "开始读取 ConfigNode 配置文件：{}";
  public static final String SUCCESSFULLY_APPLY_CONFIGNODE_CURRENT_CONFIGNODEGROUP =
      "成功应用 ConfigNode: {}。当前 ConfigNodeGroup: {}";
  public static final String SUCCESSFULLY_CHECK_SCHEMA_REGION_USING_TEMPLATE_ON_DATANODE =
      "在 DataNode: {} 上成功检查 schema region 是否使用模板";
  public static final String SUCCESSFULLY_CHECK_TIMESERIES_EXISTENCE_ON_DATANODE =
      "在 DataNode: {} 上成功检查时间序列是否存在";
  public static final String SUCCESSFULLY_COUNT_PATHS_USING_TEMPLATE_ON_DATANODE =
      "在 DataNode: {} 上成功统计使用模板的路径数量";
  public static final String SUCCESSFULLY_FETCH_SCHEMAENGINE_BLACK_LIST_ON_DATANODE =
      "在 DataNode: {} 上成功拉取 schemaengine 黑名单";
  public static final String SUCCESSFULLY_INITIALIZE_CONFIGMANAGER = "成功初始化 ConfigManager。";
  public static final String SUCCESSFULLY_ON_CONFIGNODE = "在 ConfigNode: {} 上成功执行 {}";
  public static final String SUCCESSFULLY_ON_DATANODE = "在 DataNode: {} 上成功执行 {}";
  public static final String SUCCESSFULLY_REMOVE_CONFIGNODE_CURRENT_CONFIGNODEGROUP =
      "成功移除 ConfigNode: {}。当前 ConfigNodeGroup: {}";
  public static final String SUCCESSFULLY_SETUP_INTERNAL_SERVICES = "成功初始化内部服务。";
  public static final String SUCCESSFULLY_UPDATE_NODE_S_VERSION = "成功更新 Node {} 的版本。";
  public static final String SYSTEMPROPERTIES_NORMALIZE_FROM_TO_FOR_COMPATIBILITY =
      "[SystemProperties] 为兼容性将 {} 从 {} 规范化为 {}。";
  public static final String SYSTEMPROPERTIES_STORE_CONFIG_NODE_ID =
      "[SystemProperties] 存储 config_node_id: {}";
  public static final String SYSTEMPROPERTIES_STORE_IS_SEED_CONFIG_NODE =
      "[SystemProperties] 存储 is_seed_config_node: {}";
  public static final String TAKE_SNAPSHOT_ERROR = "创建快照出错";
  public static final String TAKING_SNAPSHOT_FAIL_PROCEDURE_UPGRADE_FAIL = "创建快照失败，procedure 升级失败";
  public static final String TEMPLATE_ALREADY_EXISTS_ON = "模板已存在于 ";
  public static final String TEMPLATE_DOES_NOT_EXIST = "模板 %s 不存在";
  public static final String TEMPLATE_FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY =
      "模板创建快照失败，因为快照文件 [{}] 已存在。";
  public static final String TEMPLATE_IS_NOT_SET_ON_PATH = "模板 %s 未设置在路径 %s 上";
  public static final String TEMPLATE_WITH_ID_DOES_NOT_EXIST = "id=%s 的模板不存在";
  public static final String THERE_ARE_AI_NODES_IN_CLUSTER_AFTER_EXECUTED_REMOVEAINODEPLAN =
      "{}, 执行 RemoveAINodePlan 后集群中有 {} 个 AINode";
  public static final String THERE_ARE_AI_NODES_IN_CLUSTER_BEFORE_EXECUTED_REMOVEAINODEPLAN =
      "{}, 执行 RemoveAINodePlan 前集群中有 {} 个 AINode";
  public static final String THERE_ARE_DATA_NODE_IN_CLUSTER_AFTER_EXECUTED_REMOVEDATANODEPLAN =
      "{}, 执行 RemoveDataNodePlan 后集群中有 {} 个 data node";
  public static final String THERE_ARE_DATA_NODE_IN_CLUSTER_BEFORE_EXECUTED_REMOVEDATANODEPLAN =
      "{}, 执行 RemoveDataNodePlan 前集群中有 {} 个 data node";
  public static final String THESE_REQUEST_TYPES_SHOULD_BE_ADDED_TO_ACTIONMAP =
      "这些请求类型应被加入 actionMap：%s";
  public static final String THE_CHECK_SUM_OF_THE_NO_LOG_BATCH_IS_INCORRECT = "第 %d 批日志的校验和不正确！位于 ";
  public static final String THE_CURRENT_CONFIGNODE_CAN_T_JOINED_THE_CLUSTER_BECAUSE_LEADER =
      "当前 ConfigNode 无法加入集群，因为 leader 调度失败。可能的原因是 ip:port 配置不正确。";
  public static final String THE_CURRENT_CONFIGNODE_CAN_T_SEND_REGISTER_REQUEST_TO_THE =
      "当前 ConfigNode 经过所有重试后仍无法向 ConfigNode-leader 发送注册请求！";
  public static final String THE_CURRENT_IS_NOW_STARTING_AS_THE_SEED_CONFIGNODE =
      "当前 {} 正作为 Seed-ConfigNode 启动。";
  public static final String THE_DATA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE =
      "data_replication_factor 应为正数";
  public static final String THE_DEFAULT_DATA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE =
      "default_data_region_group_num 应为正数";
  public static final String THE_DEFAULT_SCHEMA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE =
      "default_schema_region_group_num 应为正数";
  public static final String THE_PARAMETER_CN_TARGET_CONFIG_NODE_LIST_HAS_BEEN_ABANDONED =
      "参数 cn_target_config_node_list 已废弃，仅使用第一个 ConfigNode 地址加入集群。请改用 cn_seed_config_node。";
  public static final String THE_PARAMETER_CONFIG_NODE_ID_DOESN_T_EXIST_IN =
      "参数 config_node_id 不存在于 ";
  public static final String THE_PROCEDURE_FRAMEWORK_HAS_BEEN_SUCCESSFULLY_UPGRADED_NOW_IT_USES =
      "Procedure 框架已成功升级。现在它使用共识层的服务，而不再自行维护 WAL。";
  public static final String THE_REMOVE_CONFIGNODE_SCRIPT_HAS_BEEN_DEPRECATED_PLEASE_CONNECT_TO =
      "remove-confignode 脚本已被废弃。请连接 CLI 并使用 SQL：remove confignode [confignode_id]。";
  public static final String THE_RESULT_OF_REGISTER_CONFIGNODE_IS_EMPTY = "注册 ConfigNode 的结果为空！";
  public static final String THE_RESULT_OF_REGISTER_SELF_CONFIGNODE_IS_RETRY =
      "注册自身 ConfigNode 的结果为 {}，重试 {} ...";
  public static final String
      THE_RESULT_OF_SUBMITTING_REMOVECONFIGNODE_JOB_IS_REMOVECONFIGNODEREQUEST =
          "提交 RemoveConfigNode 任务的结果为 {}。RemoveConfigNodeRequest: {}";
  public static final String THE_SCHEMA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE =
      "schema_replication_factor 应为正数";
  public static final String THE_SEEDCONFIGNODE_SETTING_IN_CONF_IS_EMPTY =
      "配置中 seedConfigNode 设置为空";
  public static final String THE_S_CREATION_HAS_NOT_PASSED_IN_JARNAME_WHICH_DOES =
      "%s 的创建未传入 jarName，且其他 pipePlugin 中也不存在。请检查";
  public static final String THE_TIMESTAMP_PRECISION_SHOULD_BE_MS_US_OR_NS =
      "timestamp_precision 应为 ms、us 或 ns";
  public static final String THE_TIME_PARTITION_INTERVAL_SHOULD_BE_POSITIVE =
      "time_partition_interval 应为正数";
  public static final String THE_TIME_PARTITION_ORIGIN_SHOULD_BE_NON_NEGATIVE =
      "time_partition_origin 应为非负数";
  public static final String TRY_LISTEN_TO_PLAN_FAILED = "尝试监听计划失败";
  public static final String UNDEFINED_TEMPLATE = "未定义的模板 {}";
  public static final String UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER =
      "关闭 logWriter 过程中发生意外中断";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_LEADER_ELECTION =
      "等待 leader 选举过程中发生意外中断。";
  public static final String UNEXPECTED_READ_PLAN = "意外的读取计划：{}";
  public static final String UNEXPECTED_WRITE_PLAN_REQUEST_BYTEBUFFER =
      "意外的写入计划，请求：{}，bytebuffer: {}";
  public static final String UNKNOWN_FAILURE_DETECTOR =
      "未知 failure_detector：%s，请设置为 \"fixed\" 或 \"phi_accrual\"";
  public static final String UNKNOWN_HOST_WHEN_CHECKING_SEED_CONFIGNODE_IP =
      "检查 seed configNode IP {} 时遇到未知主机";
  public static final String UNKNOWN_LEADER_DISTRIBUTION_POLICY =
      "未知 leader_distribution_policy：%s，请设置为 \"GREEDY\"、\"CFS\" 或 \"HASH\"";
  public static final String UNKNOWN_PHYSICALPLAN_CONFIGPHYSICALPLANTYPE =
      "未知的 PhysicalPlan configPhysicalPlanType: ";
  public static final String UNKNOWN_READ_CONSISTENCY_LEVEL_PLEASE_SET_TO =
      "未知 read_consistency_level：%s，请设置为 \"strong\" 或 \"weak\"";
  public static final String UNKNOWN_ROUTE_PRIORITY_POLICY_PLEASE_SET_TO =
      "未知 route_priority_policy：%s，请设置为 \"LEADER\" 或 \"GREEDY\"";
  public static final String UNRECOGNIZED_LOG_CONFIGPHYSICALPLANTYPE =
      "无法识别的 log configPhysicalPlanType: ";
  public static final String UNRECOGNIZED_REGIONMAINTAINTYPE = "无法识别的 RegionMaintainType: ";
  public static final String UNSUPPORTED_SUBPLAN_TYPE = "不支持的 subPlan 类型：%s";
  public static final String UNSUPPORTED_SUB_PLAN_TYPE = "不支持的 sub plan 类型：";
  public static final String UPDATE_ONLINE_CONFIGNODE_FAILED = "更新在线 ConfigNode 失败。";
  public static final String UPDATE_PROCEDURE_PID_WAL_FAILED = "更新 Procedure (pid={}) WAL 失败";
  public static final String UTILITY_CLASS_SYSTEMPROPERTIESUTILS = "工具类：SystemPropertiesUtils。";
  public static final String VIEW_IS_NOT_SUPPORTED = "不支持视图。";
  public static final String WRITE_CONFIGNODE_SYSTEM_PROPERTIES_FAILED =
      "写入 confignode-system.properties 失败";
  public static final String WRONG_MNODE_TYPE = "错误的 MNode 类型";
  public static final String WRONG_NODE_TYPE = "错误的节点类型";
  public static final String YOU_SHOULD_MANUALLY_DELETE_THE_PROCEDURE_WAL_DIR_BEFORE_CONFIGNODE =
      "在 ConfigNode 重启之前，应手动删除 procedure wal 目录。{}";
  public static final String NOT_SUPPORT = "不支持";

  public static final String THE_TTL_SHOULD_BE_POSITIVE = "TTL 应为正数。";
  public static final String CONFIGPROCEDURESTORE_START_FAILED = "ConfigProcedureStore 启动失败 ";
  public static final String MAKE_PROCEDURE_WAL_DIR = "创建 procedure WAL 目录：{}";
  public static final String FAIL_TO_GET_LOCATION_TRIGGER = "获取 Location trigger[%s] 失败";
  public static final String GET_TRIGGERJAR_FAILED_BECAUSE = "获取 TriggerJar 失败，原因：";
  public static final String GET_UDF_JAR_FAILED_BECAUSE = "获取 UDF Jar 失败，原因：";
  public static final String FAILED_TO_CREATE_PIPE_BECAUSE = "创建 pipe 失败，原因：";
  public static final String FAILED_TO_SET_PIPE_STATUS_BECAUSE = "设置 pipe 状态失败，原因：";
  public static final String FAILED_TO_DROP_PIPE_BECAUSE = "删除 pipe 失败，原因：";
  public static final String FAILED_TO_ALTER_PIPE_BECAUSE = "修改 pipe 失败，原因：";
  public static final String FAILED_TO_CREATE_MULTIPLE_PIPES_BECAUSE = "批量创建 pipe 失败，原因：";
  public static final String FAILED_TO_START_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST =
      "启动 pipe %s 失败，pipe 不存在";
  public static final String FAILED_TO_START_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED =
      "启动 pipe %s 失败，pipe 已被删除";
  public static final String FAILED_TO_STOP_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST =
      "停止 pipe %s 失败，pipe 不存在";
  public static final String FAILED_TO_STOP_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED =
      "停止 pipe %s 失败，pipe 已被删除";
  public static final String FAILED_TO_HANDLE_LEADER_CHANGE_BECAUSE = "处理 leader 变更失败，原因：";
  public static final String FAILED_TO_HANDLE_META_CHANGES_BECAUSE = "处理元数据变更失败，原因：";
  public static final String GET_PIPEPLUGIN_JAR_FAILED_BECAUSE = "获取 PipePlugin Jar 失败，原因：";
  public static final String RECORDING_PIPE_META = "记录 pipe 元数据：{}";
  public static final String UNRECOGNIZED_NODE_TYPE_WHEN_RECOVERING_THE_MTREE =
      "恢复 mTree 时遇到无法识别的节点类型 {}。";
  public static final String IOTDB_CLUSTER_COULD_PROVIDE_DATA_SERVICE_NOW_ENJOY_YOURSELF =
      "IoTDB 集群已可提供数据服务，尽情使用吧！";
  public static final String FAILED_TO_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TTL_YET =
      "修改数据库失败。暂不支持 ALTER TTL。";
  public static final String NO_REGISTERED_AINODE_FOUND = "未找到已注册的 AINode";
  public static final String AINODE_LOCATION_RESOLVED = "AINode 位置已解析";
  public static final String GETAINODELOCATION_FAILED = "getAINodeLocation 失败：";
  public static final String REMOVE_CONSENSUSGROUP_SUCCESS = "移除 ConsensusGroup 成功。";
  public static final String STOP_AND_CLEAR_CONFIGNODE_SUCCESS = "停止并清理 ConfigNode 成功。";
  public static final String CANNOT_CLOSE_LOG_FILE = "无法关闭日志文件 {}";
  public static final String OPEN_WAL_FILE_SIZE_IS = "打开 WAL 文件：{} 大小为 {}";
  public static final String FAIL_TO_TRUNCATE_LOG_FILE_TO_SIZE = "截断日志文件到大小 {} 失败";
  public static final String ALL_RETRY_FAILED_DUE_TO = "所有重试均失败，原因：";

  public static final String AUTHENTICATION_FAILED = "认证失败。";

  private ConfigNodeMessages() {}

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String
      MESSAGE_ILLEGAL_PATTERN_PATH_ARG_PATTERN_PATH_SHOULD_END_OTHERWISE_IT_50E969BB =
          "非法的路径模式：%s，路径模式应以 ** 结尾；否则，它应是不含 * 的具体数据库或设备路径";
  public static final String MESSAGE_NUMBER_TTL_RULES_HAS_REACHED_LIMIT_8575FF1E = "TTL 规则数量已达到上限 ";
  public static final String
      MESSAGE_CAPACITY_ARG_REQUESTED_TOTAL_ARG_PLEASE_DELETE_SOME_EXISTING_RULES_35B24B22 =
          "（容量：%d，请求总数：%d）。请先删除一些已有规则。";
  public static final String LOG_EXITS_JVM_MEMORY_USAGE_ARG_0BCD1CCF = " 退出。JVM 内存使用情况：{}";
  public static final String
      EXCEPTION_TOPOLOGY_PROBING_BASE_INTERVAL_MS_MUST_POSITIVE_BUT_GOT_18C9B7A2 =
          "topology_probing_base_interval_in_ms 必须为正数，实际为：";
  public static final String EXCEPTION_TOPOLOGY_PROBING_TIMEOUT_RATIO_MUST_0_1_BUT_GOT_FBD0E28B =
      "topology_probing_timeout_ratio 必须在 (0, 1) 范围内，实际为：";
  public static final String
      EXCEPTION_DATA_CONFIGNODE_SYSTEM_CONFIGNODE_SYSTEM_PROPERTIES_786349AB =
          "data/confignode/system/confignode-system.properties. ";
  public static final String
      EXCEPTION_PLEASE_DELETE_DATA_DIR_DATA_CONFIGNODE_RESTART_AGAIN_8527BE66 =
          "请删除数据目录 data/confignode 后重新启动。";
  public static final String EXCEPTION_CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS_SET_E7A83ED6 =
      "config_node_consensus_protocol_class 被设置为";
  public static final String EXCEPTION_AVAILABLE_ONLY_SCHEMA_REPLICATION_FACTOR_SET_1_45667207 =
      "仅在 schema_replication_factor 设置为 1 时可用";
  public static final String EXCEPTION_MESSAGE_E81C4E4F = "或";
  public static final String EXCEPTION_AVAILABLE_ONLY_DATA_REPLICATION_FACTOR_SET_1_71748D3D =
      "仅在 data_replication_factor 设置为 1 时可用";
  public static final String EXCEPTION_ARG_ARG_6E068B23 = "%s 或 %s";
  public static final String
      EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUS_84350FD1 =
          "SchemaRegion 不支持 org.apache.iotdb.consensus.iot.IoTConsensus";
  public static final String
      EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUSV2_BA353C6D =
          "SchemaRegion 不支持 org.apache.iotdb.consensus.iot.IoTConsensusV2";
  public static final String EXCEPTION_GREEDY_MIN_COST_FLOW_HASH_C07DA2EE =
      "GREEDY、MIN_COST_FLOW 或 HASH";
  public static final String EXCEPTION_UNRECOGNIZED_LEADER_DISTRIBUTION_POLICY_SET_F9FFB410 =
      "设置了无法识别的 leader_distribution_policy";
  public static final String EXCEPTION_LEADER_GREEDY_55C6B994 = "LEADER 或 GREEDY";
  public static final String EXCEPTION_UNRECOGNIZED_ROUTE_PRIORITY_POLICY_SET_C0012AE4 =
      "设置了无法识别的 route_priority_policy";
  public static final String EXCEPTION_THERE_NO_AVAILABLE_ARG_DATABASES_ARG_CURRENTLY_9B8297B3 =
      "当前数据库 %s 没有可用的 %s，";
  public static final String
      EXCEPTION_PLEASE_USE_SHOW_CLUSTER_SHOW_REGIONS_CHECK_CLUSTER_STATUS_611120DF =
          "请使用 \"show cluster\" 或 \"show regions\" 检查集群状态";
  public static final String EXCEPTION_SCHEMAREGIONGROUPS_3C409207 = "SchemaRegionGroups";
  public static final String EXCEPTION_DATAREGIONGROUPS_945CCE78 = "DataRegionGroups";
  public static final String EXCEPTION_DATANODE_NOT_ENOUGH_PLEASE_REGISTER_MORE_9F5EEDF5 =
      "DataNode 数量不足，请注册更多 DataNode。";
  public static final String EXCEPTION_CURRENT_DATANODES_ARG_REPLICATIONFACTOR_ARG_5D686D2B =
      "当前 DataNode: %s, replicationFactor: %d";
  public static final String
      EXCEPTION_THERE_NO_AVAILABLE_AINODES_CURRENTLY_PLEASE_USE_SHOW_CLUSTER_CHECK_FD32EB52 =
          "当前没有可用的 AINode，请使用 \"show cluster\" 检查集群状态。";
  public static final String EXCEPTION_ADD_CONSENSUSGROUP_ARG_FAILED_D3FDDC1B =
      "向 %s 添加 ConsensusGroup 失败。";
  public static final String EXCEPTION_ADD_PEER_ARG_FAILED_17DEB6CA = "添加 peer: %s 失败。";
  public static final String EXCEPTION_DATABASE_ARG_DOESN_T_EXIST_778BBF66 = "数据库 %s 不存在。";
  public static final String
      LOG_REDIRECTION_RECOMMENDED_REMOVECONFIGNODE_BUT_NO_LEADER_ENDPOINT_PROVIDED_ABORT_RETRY_520A4C64 =
          "removeConfigNode 建议重定向，但未提供 leader 端点，终止重试。";
  public static final String LOG_FAILED_WRITE_AUDIT_LOG_DATANODE_ARG_RESPONSE_ARG_691ABC90 =
      "向 DataNode {} 写入审计日志失败，响应：{}";
  public static final String LOG_FAILED_WRITE_AUDIT_LOG_DATANODE_ARG_90F15E13 =
      "向 DataNode {} 写入审计日志失败";
  public static final String EXCEPTION_UNKNOWN_PHYSICALPLANTYPE_ARG_7F21B699 =
      "未知的 PhysicalPlanType: %d";
  public static final String
      LOG_CANNOT_READ_MORE_PHYSICALPLANS_ARG_SUCCESSFULLY_READ_INDEX_ARG_REASON_2EC90E78 =
          "无法从 {} 读取更多 PhysicalPlans，已成功读取的索引为 {}。原因是";
  public static final String EXCEPTION_FILE_11296840 = "文件: ";
  public static final String EXCEPTION_ARG_CALCULATED_ARG_0EEEE191 = "%d，计算值：%d。";
  public static final String
      LOG_CANNOT_DESERIALIZE_PHYSICALPLANS_BYTEBUFFER_IGNORE_REMAINING_LOGS_06AE778F =
          "无法从 ByteBuffer 反序列化 PhysicalPlans，忽略剩余日志";
  public static final String
      MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_SCHEMAREPLICATIONFACTOR_YET_AD96111F =
          "修改数据库失败。暂不支持 ALTER SchemaReplicationFactor。";
  public static final String
      MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_DATAREPLICATIONFACTOR_YET_2E7FF6E7 =
          "修改数据库失败。暂不支持 ALTER DataReplicationFactor。";
  public static final String
      MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TIMEPARTITIONORIGIN_YET_B315F2E3 =
          "修改数据库失败。暂不支持 ALTER TimePartitionOrigin。";
  public static final String
      MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TIMEPARTITIONINTERVAL_YET_F539A76F =
          "修改数据库失败。暂不支持 ALTER TimePartitionInterval。";
  public static final String
      MESSAGE_REMOVE_CONSENSUSGROUP_FAILED_BECAUSE_TARGET_CONFIGNODE_NOT_CURRENT_CONFIGNODE_608E64F9 =
          "移除 ConsensusGroup 失败，原因：目标 ConfigNode 不是当前 ConfigNode。";
  public static final String
      MESSAGE_REMOVE_CONSENSUSGROUP_FAILED_BECAUSE_INTERNAL_FAILURE_SEE_OTHER_LOGS_MORE_51858EC2 =
          "移除 ConsensusGroup 失败，原因：内部错误。更多详情请查看其他日志";
  public static final String EXCEPTION_LOADPIPETASKINFOEXCEPTION_2270468E = "加载 PipeTaskInfo 异常=";
  public static final String EXCEPTION_LOADPIPEPLUGININFOEXCEPTION_40362E11 =
      ", 加载 PipePluginInfo 异常=";
  public static final String
      MESSAGE_FAILED_SET_PIPE_STATUS_STOPPED_RUNTIME_EXCEPTION_FLAG_BECAUSE_BFEA15AA =
          "设置 pipe 状态及运行时异常停止标记失败，原因：";
  public static final String EXCEPTION_UNKNOWN_TYPE_7618F8F4 = "未知的类型: ";
  public static final String EXCEPTION_NO_SUCH_USER_ARG_D11B1046 = "没有该用户：%s";
  public static final String EXCEPTION_NO_SUCH_USER_ID_99CA691B = "没有该用户 id: ";
  public static final String LOG_HANDLING_COMMIT_PROGRESS_META_CHANGES_FA21A080 =
      "正在处理提交进度元数据变更 ...";
  public static final String
      EXCEPTION_FAILED_CREATE_ALTER_TOPIC_ILLEGAL_ARG_ARG_EXPECTED_1_POSITIVE_A33070FB =
          "创建或修改 topic 失败，非法参数 %s=%s，期望为 -1 或正 long 值";
  public static final String LOG_TRYING_GET_MAX_TTL_UNDER_ONE_DATABASE_USE_LONG_MAX_9D70ACB2 =
      " 尝试获取单个数据库下的最大 ttl 时，使用 Long.MAX_VALUE。";
  public static final String
      MESSAGE_ENABLE_IOTDB_CLUSTER_S_DATA_SERVICE_PLEASE_REGISTER_ARG_MORE_F48F3890 =
          "要启用 IoTDB-Cluster 的数据服务，请再注册 %d 个 IoTDB-DataNode";
  public static final String
      MESSAGE_APPLY_NEW_CONFIGNODE_FAILED_BECAUSE_CURRENT_CONFIGNODE_CAN_T_STORE_1BB6A6BF =
          "应用新 ConfigNode 失败，原因：当前 ConfigNode 无法存储 ConfigNode 信息。";
  public static final String
      MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_CURRENT_CONFIGNODE_CAN_T_STORE_CONFIGNODE_8AB3BCB4 =
          "移除 ConfigNode 失败，原因：当前 ConfigNode 无法存储 ConfigNode 信息。";
  public static final String LOG_NODE_ARG_REGION_ARG_70A7CD4F = "节点: {}, Region: {}";
  public static final String LOG_NO_NEED_REMOVE_IT_NODE_ARG_REGION_ARG_D14062CE =
      "无需移除，节点：{}，Region：{}";
  public static final String
      LOG_PID_ARG_FAILED_WRITE_UPDATE_API_EXECUTING_CONSENSUS_LAYER_824FB30E =
          "pid={} 执行共识层写入更新 API 失败，原因：";
  public static final String
      LOG_PID_ARG_FAILED_WRITE_DELETE_API_EXECUTING_CONSENSUS_LAYER_0E758BF5 =
          "pid={} 执行共识层写入删除 API 失败，原因：";
  public static final String LOG_NEW_LEADER_NODEID_ARG_0A63760B = "新的 leader 为 [nodeId:{}]";
  public static final String LOG_START_CLEANING_UP_RELATED_SERVICES_A409E261 = "开始清理相关服务";
  public static final String LOG_ALL_SERVICES_OLD_LEADER_UNAVAILABLE_NOW_8A22E60F =
      "旧 leader 上的所有服务现在均不可用。";
  public static final String LOG_FILEPATH_ARG_RETRY_ARG_16284354 = "filePath：{}，重试：{}";
  public static final String EXCEPTION_COLON_5D70AD09 = ":";
  public static final String MESSAGE_COLON_CEFF3F4D = ": ";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_PROCEDURE_TYPE_IS_NULL_93147BD3 = "Procedure 类型不能为空";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FAILED_TO_START_LEADER_SERVICE_ARG_THE_1754011A =
          "当前 ConfigNode(nodeId: {}, ip: {}) 启动 leader 服务 [{}] 失败，该";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FAILED_TO_START_LEADER_SERVICE_ARG_THE_NODE_WILL_STILL_FINISH_WARMING_UP_THIS_SERVICE_STAYS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_E89A98E7 =
          "当前 ConfigNode(nodeId: {}, ip: {}) 启动 leader 服务 [{}] 失败，该节点仍会完成预热；在下次 leader 切换前，该服务保持不可用。";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_0C57A408 =
          "当前 ConfigNode(nodeId: {}, ip: {}) 已完成 leader 服务启动，但 load";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31 =
          "当前 ConfigNode(nodeId: {}, ip: {}) 已完成 leader 服务启动，但 load 预热仍在进行中：{}";
  public static final String
      MESSAGE_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7 =
          "等待 ConfigNode leader load 预热时遭到意外中断。";
  public static final String
      EXCEPTION_PROCEDURE_FILE_ARG_EXCEEDS_THE_LOAD_BUFFER_LIMIT_ARG_ACTUAL_SIZE_ARG_62375B4C =
          "Procedure 文件 %s 超过了加载缓冲区限制 %s，实际大小为 %s";
  public static final String
      MESSAGE_DROPPING_LEGACY_REGION_DELETE_TASK_FOR_ARG_WHILE_REPLAYING_OFFER_PLAN_REGION_DELETION_IS_NOW_HANDLED_BY_REMOVEREGIONGROUPPROCEDURE_2A81A649 =
          "重放 offer plan 时丢弃 {} 的遗留 region-delete 任务；region 删除现已由 RemoveRegionGroupProcedure 处理。";
  public static final String
      MESSAGE_DROPPING_LEGACY_REGION_DELETE_TASK_FOR_ARG_WHILE_LOADING_SNAPSHOT_REGION_DELETION_IS_NOW_HANDLED_BY_REMOVEREGIONGROUPPROCEDURE_A9D409A0 =
          "加载快照时丢弃 {} 的遗留 region-delete 任务；region 删除现已由 RemoveRegionGroupProcedure 处理。";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_IS_WARMING_UP_BEFORE_SERVING_THE_REGISTERING_CONFIGNODE_WILL_WAIT_2E051639 =
          "ConfigNode leader 在服务注册中的 ConfigNode 前正在进行预热，将等待";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_IS_WARMING_UP_BEFORE_SERVING_THE_REGISTERING_CONFIGNODE_WILL_WAIT_AND_RETRY_STATUS_ARG_RETRY_ARG_3C924873 =
          "ConfigNode leader 在服务注册中的 ConfigNode 前正在进行预热，将等待并重试。状态：{}，重试：{}";
  public static final String EXCEPTION_ARG_SHOULD_BE_AN_INTEGER_BUT_WAS_ARG_56B5D91B =
      "%s 应为整数，但实际为 %s。";
  public static final String
      EXCEPTION_HEARTBEAT_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_WAS_2269997B =
          "heartbeat_interval_in_ms 应大于 0，但当前值为 ";
  public static final String
      EXCEPTION_CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_CURRENT_VALUE_IS_F9A1BEC4 =
          "continuous_query_min_every_interval_in_ms 应大于 0，但当前值为 ";
  public static final String
      EXCEPTION_DEFAULT_SCHEMA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_C8F77774 =
          "default_schema_region_group_num_per_database 应为正数。";
  public static final String EXCEPTION_SCHEMA_REGION_PER_DATA_NODE_SHOULD_BE_POSITIVE_CDEB9FC1 =
      "schema_region_per_data_node 应为正数。";
  public static final String
      EXCEPTION_DEFAULT_DATA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_8E68B5A0 =
          "default_data_region_group_num_per_database 应为正数。";
  public static final String EXCEPTION_DATA_REGION_PER_DATA_NODE_SHOULD_BE_NON_NEGATIVE_D2960368 =
      "data_region_per_data_node 应为非负数。";
  public static final String
      EXCEPTION_PROCEDURE_COMPLETED_CLEAN_INTERVAL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_8781558E =
          "procedure_completed_clean_interval 应大于 0，但当前值为 ";
  public static final String
      EXCEPTION_PROCEDURE_COMPLETED_EVICT_TTL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_5A4D0CF6 =
          "procedure_completed_evict_ttl 应大于 0，但当前值为 ";
  public static final String
      EXCEPTION_FAILED_TO_CREATE_OR_ALTER_TOPIC_MODE_CONSENSUS_DOES_NOT_SUPPORT_TOPIC_ATTRIBUTES_ARG_3C2D0BDA =
          "创建或修改 topic 失败，mode=consensus 不支持 topic 属性 %s";
}
