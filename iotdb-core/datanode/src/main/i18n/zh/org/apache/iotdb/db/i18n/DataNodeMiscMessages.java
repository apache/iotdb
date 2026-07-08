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

package org.apache.iotdb.db.i18n;

/** 编译时国际化常量 - DataNode 杂项子系统（中文）。 */
public final class DataNodeMiscMessages {

    public static final String INVALID_PIPE_NAME =
      "无效的 pipeName";
  public static final String READ_OBJECT_CONTENT_FROM_REMOTE_FILE =
      "readObjectContentFromRemoteFile";
  public static final String ERROR_EXCEPTION =
      "错误，异常：";
  public static final String ACCOUNT_BLOCKED_DUE_TO_CONSECUTIVE_FAILED_LOGINS =
      "账户因连续登录失败已被锁定。";
  public static final String VERSION_INCOMPATIBLE_PLEASE_UPGRADE_TO =
      "版本不兼容，请升级至 ";
  public static final String ADD_PEER_FOR_REGION_SUCCEED =
      "addPeer %s for region %s 成功";
  public static final String REMOVE_PEER_FOR_REGION_SUCCEED =
      "removePeer %s for region %s 成功";
  public static final String DELETE_PEER_FROM_CONSENSUS_GROUP_SUCCEED =
      "从共识组 %s 删除 peer 成功";
  public static final String DELETE_REGION_ERROR =
      "deleteRegion %s 错误，%s";
  public static final String DELETE_REGION_SUCCEED =
      "deleteRegion %s 成功";
  public static final String START_TO_ADD_PEER_FOR_REGION =
      "{}, 开始将 addPeer {} 添加到 region {}";
  public static final String EXECUTED_ADD_PEER_FOR_REGION_ERROR =
      "{}, 执行 addPeer {} 于 region {} 时出错";
  public static final String REGION_MIGRATE_UNEXPECTED_EXCEPTION =
      "发生意外异常";
  public static final String ADD_PEER_FOR_REGION_ERROR_FMT =
      "%s, 为 region 执行 AddPeer 出错，peerId：%s，regionId：%s";
  public static final String SUCCEED_TO_ADD_PEER_FOR_REGION =
      "{}, 成功将 addPeer {} 添加到 region {}";
  public static final String START_TO_REMOVE_PEER_FOR_REGION =
      "{}, 开始执行 removePeer {}，目标 region {}";
  public static final String EXECUTED_REMOVE_PEER_FOR_REGION_ERROR_RETRY_TIMES =
      "{}, 执行 removePeer {} 于 region {} 时出错，重试次数：{}";
  public static final String REMOVE_PEER_FOR_REGION_ERROR_AFTER_MAX_RETRY_TIMES_FMT =
      "%s, 为 region 执行 RemovePeer 达到最大重试次数后仍出错，peerId：%s，regionId：%s";
  public static final String SUCCEED_TO_REMOVE_PEER_FOR_REGION =
      "{}, 成功移除 removePeer {}，region {}";
  public static final String START_TO_DELETE_PEER_FOR_REGION =
      "{}, 开始执行 deletePeer {}，region {}";
  public static final String DELETE_PEER_ERROR_WITH_ERROR_MESSAGE_FMT =
      "deletePeer 出错，regionId：%s，错误信息：%s";
  public static final String DELETE_PEER_ERROR_WITH_REGION_ID =
      "{}, deletePeer 出错，regionId：{}";
  public static final String DELETE_PEER_FOR_REGION_ERROR_EXCEPTION_FMT =
      "deletePeer 于 region：%s 出错，异常：%s";
  public static final String SUCCEED_TO_DELETE_PEER_FROM_CONSENSUS_GROUP =
      "{}, 成功从共识组删除 deletePeer {}";
  public static final String START_TO_DELETE_REGION_FOR_DATANODE =
      "{}, 开始删除 deleteRegion {}，DataNode {}";
  public static final String DELETE_REGION_ERROR_LOG =
      "{}, deleteRegion {} 出错";
  public static final String SUCCEED_TO_DELETE_REGION =
      "{}, deleteRegion {} 成功";
  public static final String ERROR_PROCESSING_DATA_REGION =
      "处理数据 region 出错：{}";
  public static final String FAILED_TO_PROCESS_TSFILE =
      "处理 TsFile {} 失败，{}";

  public static final String CREATE_NEW_REGION_ERROR_FMT = "创建新 region %s 错误，异常：%s";
  public static final String CREATE_NEW_REGION_SUCCEED_FMT = "创建新 region %s 成功";
  private DataNodeMiscMessages() {}

  // ---------------------------------------------------------------------------
  // protocol – BaseServerContextHandler
  // ---------------------------------------------------------------------------
  public static final String MULTIPLE_SERVER_CONTEXT_FACTORY =
      "存在多个 ServerContextFactory 实现，请检查。";
  public static final String SET_SERVER_CONTEXT_FACTORY =
      "将从 {} 设置 ServerContextFactory";

  // ---------------------------------------------------------------------------
  // protocol – ConfigNodeInfo
  // ---------------------------------------------------------------------------
  public static final String UPDATE_CONFIG_NODE_SUCCESSFULLY =
      "成功更新 ConfigNode：{}，耗时 {} 毫秒。";
  public static final String UPDATE_CONFIG_NODE_FAILED = "更新 ConfigNode 失败。";
  public static final String SYSTEM_PROPERTIES_NOT_EXIST =
      "系统属性文件不存在，无需存储 ConfigNode 列表";
  public static final String LOAD_CONFIG_NODE_SUCCESSFULLY =
      "成功加载 ConfigNode：{}，耗时 {} 毫秒。";
  public static final String CANNOT_PARSE_CONFIG_NODE_LIST =
      "无法解析 system.properties 中的 ConfigNode 列表";

  // ---------------------------------------------------------------------------
  // protocol – ConfigNodeClient
  // ---------------------------------------------------------------------------
  public static final String NODE_LEADER_MAY_DOWN_TRY_NEXT =
      "当前节点 Leader {} 可能已宕机，尝试下一个节点";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE =
      "等待连接 ConfigNode 时发生意外中断";
  public static final String NODE_MAY_DOWN_TRY_NEXT =
      "当前节点 {} 可能已宕机，尝试下一个节点";
  public static final String FAILED_CONNECT_CONFIG_NODE_NOT_LEADER =
      "从 DataNode {} 连接 ConfigNode {} 失败，因为当前节点不是 Leader 或尚未就绪，稍后将重试";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE_BREAK =
      "等待连接 ConfigNode 时发生意外中断，可能是当前节点已宕机，将中断当前执行流程以避免无意义等待。";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeInternalClient
  // ---------------------------------------------------------------------------
  public static final String USER_OPENS_INTERNAL_SESSION =
      "用户 {} 打开了内部 Session-{}。";
  public static final String USER_OPENS_INTERNAL_SESSION_FAILED =
      "用户 {} 打开内部 Session 失败。";
  public static final String USER_OPENS_INTERNAL_SESSION_FAILED_FMT =
      "用户 %s 打开内部 Session 失败。";

  // ---------------------------------------------------------------------------
  // protocol – AsyncTSStatusRPCHandler / AsyncConfigNodeTSStatusRPCHandler
  // ---------------------------------------------------------------------------
  public static final String SUCCESSFULLY_ON_DATANODE =
      "在 DataNode {} 上成功执行 {}";
  public static final String FAILED_ON_DATANODE =
      "在 DataNode {} 上执行 {} 失败，响应：{}";
  public static final String SUCCESSFULLY_ON_CONFIG_NODE =
      "在 ConfigNode {} 上成功执行 {}";
  public static final String FAILED_ON_CONFIG_NODE =
      "在 ConfigNode {} 上执行 {} 失败，响应：{}";

  // ---------------------------------------------------------------------------
  // protocol – AINodeClient
  // ---------------------------------------------------------------------------
  public static final String AINODE_MAY_DOWN =
      "当前 AINode {} 可能已宕机，原因：";
  public static final String CANNOT_CONNECT_ANY_AINODE =
      "无法连接任何 AINode，因为没有可用的节点。";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_AINODE =
      "等待连接 AINode 时发生意外中断，可能是当前节点已宕机，将中断当前执行流程以避免无意义等待。";

  // ---------------------------------------------------------------------------
  // protocol – SessionManager
  // ---------------------------------------------------------------------------
  public static final String LOGIN_STATUS =
      "{}: 登录状态：{}。用户：{}，打开 Session-{}";
  public static final String CLIENT_TRYING_CLOSE_ANOTHER_SESSION =
      "客户端 %s 正在尝试关闭另一个会话 %s，请检查是否存在 Bug";
  public static final String SESSION_CLOSING = "Session-%s 正在关闭";
  public static final String FAILED_RELEASE_PREPARED_STATEMENT =
      "释放会话 {} 的 PreparedStatement 资源失败：{}";
  public static final String FAILED_RELEASE_PREPARED_STATEMENT_CLOSE =
      "关闭语句 {} 时释放会话 {} 的 PreparedStatement '{}' 资源失败：{}";
  public static final String NOT_LOGIN = "{}: 未登录。";
  public static final String CLIENT_SESSION_REGISTERED_REPEATEDLY =
      "客户端会话被重复注册，请检查是否存在 Bug。";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeRegionManager
  // ---------------------------------------------------------------------------
  public static final String CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH =
      "创建 Schema Region {} 失败，因为路径非法。";
  public static final String CREATE_SCHEMA_REGION_FAILED =
      "创建 Schema Region {} 失败，原因：{}";
  public static final String CREATE_SCHEMA_REGION_FAILED_FMT =
      "创建 Schema Region 失败，原因：%s";
  public static final String SCHEMA_REGION_ALREADY_EXISTS_FMT =
      "SchemaRegion %d 已存在。";
  public static final String CREATE_DATA_REGION_FAILED =
      "创建 Data Region {} 失败，原因：{}";
  public static final String CREATE_DATA_REGION_FAILED_FMT =
      "创建 Data Region 失败，原因：%s";
  public static final String DATA_REGION_ALREADY_EXISTS_FMT = "DataRegion %d 已存在。";
  public static final String START_CREATE_NEW_REGION = "开始创建新 Region {}";
  public static final String CREATE_NEW_REGION_ERROR = "创建新 Region {} 失败";
  public static final String SUCCEED_CREATE_NEW_REGION = "成功创建新 Region {}";
  public static final String METADATA_ERROR = "{}: 元数据错误：";
  public static final String CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH_MSG =
      "创建 Schema Region 失败，因为存储组路径非法。";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeInternalRPCServiceImpl
  // ---------------------------------------------------------------------------
  public static final String CONSENSUS_NOT_STARTED =
      "共识协议在 {} 秒后仍未启动，拒绝 Region 请求";
  public static final String RECEIVE_FRAGMENT_INSTANCE =
      "收到 FragmentInstance，目标组 [{}]";
  public static final String DESERIALIZE_CONSENSUS_GROUP_ID_FAILED =
      "反序列化 ConsensusGroupId 失败。";
  public static final String DESERIALIZE_FRAGMENT_INSTANCE_FAILED =
      "反序列化 FragmentInstance 失败。";
  public static final String RECEIVE_LOAD_NODE = "收到 UUID 为 {} 的加载节点请求。";
  public static final String SCHEMA_CACHE_INVALIDATED =
      "{} 的 Schema 缓存已失效";
  public static final String ERROR_PUSHING_PIPE_META =
      "推送 Pipe 元数据时发生错误";
  public static final String ERROR_PUSHING_SINGLE_PIPE_META =
      "推送单个 Pipe 元数据时发生错误";
  public static final String ERROR_PUSHING_MULTI_PIPE_META =
      "推送多个 Pipe 元数据时发生错误";
  public static final String ERROR_PUSHING_TOPIC_META =
      "推送 Topic 元数据时发生错误";
  public static final String ERROR_PUSHING_SINGLE_TOPIC_META =
      "推送单个 Topic 元数据时发生错误";
  public static final String ERROR_PUSHING_MULTI_TOPIC_META =
      "推送多个 Topic 元数据时发生错误";
  public static final String ERROR_PUSHING_TOPIC_OWNER_LEASE =
      "推送 Topic owner 租约时发生错误";
  public static final String ERROR_PUSHING_CONSUMER_GROUP_META =
      "推送消费者组元数据时发生错误";
  public static final String ERROR_PUSHING_SINGLE_CONSUMER_GROUP_META =
      "推送单个消费者组元数据时发生错误";
  public static final String EXCEPTION_EXECUTING_INTERNAL_SCHEMA_TASK =
      "执行内部 Schema 任务时发生异常：";
  public static final String UNSUPPORTED_TYPE_UPDATING_TABLE =
      "更新表时遇到不支持的类型 {}";
  public static final String UNSUPPORTED_TYPE_UPDATING_TEMPLATE =
      "更新模板时遇到不支持的类型 {}";
  public static final String FAILED_GET_MEMORY_FROM_METRIC =
      "从指标获取内存信息失败，原因：";
  public static final String CHANGE_REGION_LEADER = "[ChangeRegionLeader] {}";
  public static final String REGION_TYPE_ILLEGAL = "Region {} 的类型非法";
  public static final String START_DISABLE_DATA_NODE =
      "开始在请求中禁用 DataNode：{}";
  public static final String EXECUTE_STOP_AND_CLEAR = "执行 stopAndClearDataNode RPC 方法";
  public static final String INTERRUPTED_STOP_AND_CLEAR =
      "在 stopAndClearDataNode RPC 方法中遇到中断异常";
  public static final String STOP_AND_CLEAR_ERROR = "停止并清理 DataNode 时发生错误";
  public static final String RETRIEVED_EARLIEST_TIMESLOTS =
      "已获取 {} 个数据库的最早时间槽";
  public static final String FAILED_GET_EARLIEST_TIMESLOTS = "获取最早时间槽失败";
  public static final String FAILED_GENERATE_DATA_PARTITION_TABLE =
      "生成数据分区表失败";
  public static final String FAILED_CHECK_DATA_PARTITION_TABLE_STATUS =
      "检查数据分区表生成状态失败";
  public static final String DATA_PARTITION_TABLE_COMPLETED =
      "数据分区表生成完成，任务 ID：{}";
  public static final String DATA_PARTITION_TABLE_FAILED =
      "数据分区表生成失败，任务 ID：{}";
  public static final String PROCESS_DATA_DIR_COMPLETED =
      "处理数据目录以获取最早时间槽已成功完成";
  public static final String ERROR_EXECUTING_BATCH_STATEMENT =
      "执行批量语句时发生错误：";

  // ---------------------------------------------------------------------------
  // protocol – ClientRPCServiceImpl
  // ---------------------------------------------------------------------------
  public static final String IOTDB_SERVER_VERSION = "IoTDB 服务器版本：{}";
  public static final String TEST_INSERT_BATCH_RECEIVE = "收到测试批量插入请求。";
  public static final String TEST_INSERT_ROW_RECEIVE = "收到测试行插入请求。";
  public static final String TEST_INSERT_STRING_RECORD_RECEIVE =
      "收到测试字符串记录插入请求。";
  public static final String TEST_INSERT_ROW_IN_BATCH_RECEIVE =
      "收到测试批量行插入请求。";
  public static final String TEST_INSERT_ROWS_IN_BATCH_RECEIVE =
      "收到测试批量多行插入请求。";
  public static final String TEST_INSERT_STRING_RECORDS_RECEIVE =
      "收到测试字符串记录批量插入请求。";
  public static final String START_BATCH_EXECUTING_TREE =
      "开始在树模型中批量执行 {} 个子语句，queryId：{}";
  public static final String EXECUTING_SUB_STATEMENT_TREE =
      "正在树模型中执行第 {}/{} 个子语句，queryId：{}";
  public static final String FAILED_EXECUTE_SUB_STATEMENT_TREE =
      "树模型中第 {}/{} 个子语句执行失败，queryId：{}，已完成：{}，剩余：{}，进度：{}%，错误：{}";
  public static final String SUCCESSFULLY_EXECUTED_SUB_STATEMENT_TREE =
      "树模型中第 {}/{} 个子语句执行成功，queryId：{}";
  public static final String COMPLETED_BATCH_EXECUTING_TREE =
      "树模型中全部 {} 个子语句批量执行完成，queryId：{}";
  public static final String START_BATCH_EXECUTING_TABLE =
      "开始在表模型中批量执行 {} 个子语句，queryId：{}";
  public static final String EXECUTING_SUB_STATEMENT_TABLE =
      "正在表模型中执行第 {}/{} 个子语句，queryId：{}";
  public static final String FAILED_EXECUTE_SUB_STATEMENT_TABLE =
      "表模型中第 {}/{} 个子语句执行失败，queryId：{}，已完成：{}，剩余：{}，进度：{}%，错误：{}";
  public static final String SUCCESSFULLY_EXECUTED_SUB_STATEMENT_TABLE =
      "表模型中第 {}/{} 个子语句执行成功，queryId：{}";
  public static final String COMPLETED_BATCH_EXECUTING_TABLE =
      "表模型中全部 {} 个子语句批量执行完成，queryId：{}";

  // ---------------------------------------------------------------------------
  // service – DataNode
  // ---------------------------------------------------------------------------
  public static final String DATANODE_ENV_VARS =
      "IoTDB-DataNode 环境变量：{}";
  public static final String DATANODE_DEFAULT_CHARSET =
      "IoTDB-DataNode 默认字符集：{}";
  public static final String STARTING_DATANODE = "正在启动 DataNode...";
  public static final String DATANODE_FIRST_START =
      "DataNode 首次启动中...";
  public static final String DATANODE_RESTARTING = "DataNode 正在重启...";
  public static final String IOTDB_CONFIGURATION = "IoTDB 配置信息：{}";
  public static final String DATANODE_SETUP_SUCCESSFULLY =
      "恭喜，IoTDB DataNode 已成功启动，祝使用愉快！";
  public static final String FAIL_TO_START_SERVER = "启动服务器失败";
  public static final String DATANODE_STARTED = "DataNode 已启动";
  public static final String DATANODE_PREPARED_SUCCESSFULLY =
      "DataNode 准备就绪，耗时 {} 毫秒";
  public static final String PULLING_SYSTEM_CONFIGURATIONS =
      "正在从 ConfigNode Leader 拉取系统配置...";
  public static final String CANNOT_PULL_SYSTEM_CONFIGURATIONS =
      "无法从 ConfigNode Leader 拉取系统配置";
  public static final String SENDING_REGISTER_REQUEST =
      "正在向 ConfigNode Leader 发送注册请求...";
  public static final String CANNOT_REGISTER_TO_CLUSTER =
      "无法注册到集群，原因：{}";
  public static final String CANNOT_REGISTER_AFTER_RETRIES =
      "重试 {} 次后仍无法注册到集群。";
  public static final String PRECHECK_PASSED =
      "预检查通过，即将进行正式注册。";
  public static final String DELETE_SUCCEED = "删除 {} 成功。";
  public static final String DELETE_FAILED_NOT_EXIST =
      "删除 {} 失败，因为目录不存在。";
  public static final String SENDING_RESTART_REQUEST =
      "正在向 ConfigNode Leader 发送重启请求...";
  public static final String CLEANED_SORT_TEMP_DIR =
      "已清理过期的排序临时目录：{}";
  public static final String MEET_ERROR_STARTING_UP = "启动过程中遇到错误。";
  public static final String IOTDB_DATANODE_HAS_STARTED = "IoTDB DataNode 已启动。";
  public static final String SETTING_UP_DATANODE = "正在配置 IoTDB DataNode...";
  public static final String RECOVER_SCHEMA = "正在恢复 Schema...";
  public static final String DATANODE_FAILED_SETUP = "IoTDB DataNode 启动失败。";
  public static final String WAIT_DATABASES_READY =
      "等待所有数据库就绪，耗时 {} 毫秒。";
  public static final String PREPARE_PIPE_RESOURCES =
      "Pipe 资源准备完成，耗时 {} 毫秒。";
  public static final String RECOVER_SCHEMA_SUCCESSFULLY =
      "Schema 恢复完成，耗时 {} 毫秒。";
  public static final String LOAD_CLASS_ERROR = "加载类失败：";
  public static final String EXCEPTION_SCHEMA_REGION_CONSENSUS_STOPPING =
      "停止 SchemaRegionConsensusImpl 时发生异常";
  public static final String EXCEPTION_DATA_REGION_CONSENSUS_STOPPING =
      "停止 DataRegionConsensusImpl 时发生异常";

  // ---------------------------------------------------------------------------
  // service – DataNodeShutdownHook
  // ---------------------------------------------------------------------------
  public static final String DATANODE_EXITING = "DataNode 正在退出...";
  public static final String INTERRUPTED_WAITING_PIPE_FINISH =
      "等待 Pipe 完成时被中断";
  public static final String TIMED_OUT_WAITING_PIPES =
      "等待 Pipe 完成超时，将终止等待";
  public static final String FAILED_BORROW_CONFIG_NODE_CLIENT =
      "借用 ConfigNodeClient 失败";
  public static final String FAILED_REPORT_SHUTDOWN = "上报关闭状态失败";

  // ---------------------------------------------------------------------------
  // service – RegionMigrateService
  // ---------------------------------------------------------------------------
  public static final String REGION_BEGIN_MIGRATING =
      "Region {} 收到开始迁移通知";
  public static final String REGION_FINISH_MIGRATING =
      "Region {} 收到完成迁移通知";
  public static final String RESET_PEER_LIST_FAIL = "重置对等节点列表失败";
  public static final String REGION_MIGRATE_SERVICE_START = "Region 迁移服务已启动";
  public static final String REGION_MIGRATE_SERVICE_STOP = "Region 迁移服务已停止";

  // ---------------------------------------------------------------------------
  // service – SettleService
  // ---------------------------------------------------------------------------
  public static final String START_ERROR = "启动错误";
  public static final String WAITING_SETTLE_POOL_SHUTDOWN =
      "正在等待 Settle 任务池关闭";
  public static final String SETTLE_SERVICE_STOPPED = "Settle 服务已停止";

  // ---------------------------------------------------------------------------
  // service – IoTDBInternalLocalReporter
  // ---------------------------------------------------------------------------
  public static final String CHECK_OR_CREATE_DATABASE_FAILED =
      "IoTDBSessionReporter 检查或创建数据库失败。";
  public static final String CHECK_OR_CREATE_DATABASE_FAILED_BECAUSE =
      "IoTDBSessionReporter 检查或创建数据库失败，原因：";
  public static final String INTERNAL_REPORTER_ALREADY_STARTED =
      "IoTDB 内部 Reporter 已经启动";
  public static final String INTERNAL_REPORTER_START = "IoTDB 内部 Reporter 启动！";
  public static final String INTERNAL_REPORTER_STOP = "IoTDB 内部 Reporter 停止！";
  public static final String FAILED_UPDATE_METRIC_VALUE =
      "更新指标值失败，状态：{}";
  public static final String FAILED_AUTO_CREATE_TIMESERIES =
      "自动创建时间序列 {} 失败，状态：{}";

  // ---------------------------------------------------------------------------
  // service – ExternalService
  // ---------------------------------------------------------------------------
  public static final String FAILED_MAKE_EXTERNAL_SERVICE_DIR =
      "创建外部服务目录失败";
  public static final String EXTERNAL_SERVICE_LIB_ROOT = "外部服务库根目录：{}";
  public static final String FAILED_GET_OPEN_FILE_NUMBER =
      "获取打开文件数失败，原因：";
  public static final String UNEXPECTED_ERROR_GETTING_TSFILE_NAME =
      "获取 TsFile 名称时发生意外错误";

  // ---------------------------------------------------------------------------
  // service – metrics
  // ---------------------------------------------------------------------------
  public static final String FAILED_GET_PROCESS_RESIDENT_MEMORY =
      "获取进程 {} 的常驻内存失败";
  public static final String DATANODE_PORT_CHECK_SUCCESSFUL = "DataNode 端口检查通过。";

  // ---------------------------------------------------------------------------
  // tools – WalChecker
  // ---------------------------------------------------------------------------
  public static final String CHECKING_FOLDER = "正在检查目录：{}";
  public static final String NO_SUB_DIRECTORIES =
      "指定目录下无子目录，检查结束";
  public static final String CHECKING_DIRECTORY = "正在检查第 {} 个目录 {}";
  public static final String WAL_FILE_NOT_EXIST = "WAL 文件不存在，跳过";
  public static final String WAL_CHECK_FAILED = "{} 检查未通过，原因：";
  public static final String CHECK_FINISHED_NO_DAMAGED =
      "检查完成，没有损坏的文件";
  public static final String FAILED_FILES_FOUND =
      "共有 {} 个文件检查失败，文件列表：{}";
  public static final String NO_ENOUGH_ARGS =
      "参数不足：需要提供 WAL 根目录路径";

  // ---------------------------------------------------------------------------
  // tools – TsFileSketchTool
  // ---------------------------------------------------------------------------
  public static final String FAIL_INIT_SKETCH_TOOL = "初始化 TsFileSketchTool 失败，{}";
  public static final String FAIL_PARSE_TSFILE_METADATA = "解析 TsFileMetadata 失败，{}";
  public static final String FAIL_PRINT_FILE_INFO = "输出文件信息失败，{}";
  public static final String FAIL_PARSE_CHUNK = "解析 Chunk 失败，{}";
  public static final String FAIL_PRINT_TIMESERIES_INDEX = "输出时间序列索引失败，{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSplitTool
  // ---------------------------------------------------------------------------
  public static final String SPLITTING_TSFILE = "正在拆分 TsFile {}...";
  public static final String UNSUPPORTED_SPLIT_WITH_MODIFICATION =
      "暂不支持拆分带有修改记录的 TsFile。";
  public static final String UNSUPPORTED_SPLIT_WITH_ALIGNED =
      "暂不支持拆分包含对齐时间序列的 TsFile。";

  // ---------------------------------------------------------------------------
  // tools – TsFileSplitByPartitionTool
  // ---------------------------------------------------------------------------
  public static final String DELETE_UNCOMPLETED_FILE = "删除未完成的文件 {}";
  public static final String CREATE_TSFILE_FAILED_EXISTS =
      "创建新 TsFile {} 失败，因为文件已存在";
  public static final String CREATE_TSFILE_FAILED = "创建新 TsFile {} 失败";
  public static final String INCORRECT_MAGIC_STRING =
      "文件的 MAGIC STRING 不正确，文件路径：{}";
  public static final String INCORRECT_VERSION_NUMBER =
      "文件的版本号不正确，文件路径：{}";
  public static final String FILE_NOT_CLOSED_CORRECTLY =
      "文件未正确关闭，文件路径：{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSelfCheckTool
  // ---------------------------------------------------------------------------
  public static final String ERROR_GETTING_TIMESERIES_METADATA =
      "获取 TsFile 中所有带偏移量的时间序列元数据时发生错误。";
  public static final String FILE_PATH = "文件路径：{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileValidationTool
  // ---------------------------------------------------------------------------
  public static final String NOT_DIRECTORY_OR_NOT_EXIST =
      "{} 不是目录或不存在，跳过。";

  // ---------------------------------------------------------------------------
  // tools – TsFileValidationScan / TsFileStatisticScan
  // ---------------------------------------------------------------------------
  public static final String MEET_ERRORS_READING_FILE =
      "读取文件 {} 时遇到错误，跳过。";
  public static final String MEET_ERROR = "遇到错误。";

  // ---------------------------------------------------------------------------
  // tools – MLogParser / PBTreeFileSketchTool
  // ---------------------------------------------------------------------------
  public static final String TOO_FEW_PARAMS =
      "输入参数过少，请参考以下提示。";
  public static final String PARSE_ERROR = "解析错误：{}";
  public static final String ENCOUNTER_ERROR = "遇到错误，原因：{}";
  public static final String USE_HELP = "使用 -help 获取更多信息";

  // ---------------------------------------------------------------------------
  // tools – SchemaRegionSnapshotParser
  // ---------------------------------------------------------------------------
  public static final String IOEXCEPTION_GET_FOLDER =
      "获取 {} 的目录时发生 IO 异常";

  // ---------------------------------------------------------------------------
  // tools – SRStatementGenerator
  // ---------------------------------------------------------------------------
  public static final String ERROR_PARSER_TAG_ATTRIBUTES =
      "解析标签和属性文件时发生错误";
  public static final String MEASUREMENT_ATTRIBUTES_NO_SNAPSHOT =
      "测点已设置属性或标签，但未找到快照文件";

  // ---------------------------------------------------------------------------
  // tools – TsFileAndModSettleTool
  // ---------------------------------------------------------------------------
  public static final String CANNOT_FIND_TSFILE = "找不到 TsFile：{}";
  public static final String NOT_DIRECTORY_PATH = "不是目录路径：{}";
  public static final String CANNOT_FIND_DIRECTORY = "找不到目录：{}";
  public static final String START_SETTLING_TSFILE =
      "开始整理 TsFile：{}";
  public static final String FINISH_SETTLING_ALL =
      "所有 TsFile 整理完成！";
  public static final String FAIL_SERIALIZE_TSFILE_RESOURCE =
      "序列化新的 TsFile 资源失败。";
  public static final String FAILED_DELETE_SETTLE_LOG =
      "删除整理日志失败，日志路径：{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSettleByCompactionTool
  // ---------------------------------------------------------------------------
  public static final String PARSE_COMMAND_LINE_FAILED =
      "解析命令行参数失败：{}";
  public static final String ADD_SETTLE_COMPACTION_TASK_SUCCESS =
      "添加整理合并任务成功";
  public static final String ADD_SETTLE_COMPACTION_TASK_FAILED =
      "添加整理合并任务失败，状态码：{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileResourcePipeStatisticsSetTool
  // ---------------------------------------------------------------------------
  public static final String UNKNOWN_ARGUMENT = "未知参数：{}";
  public static final String NO_DATA_DIRS_PROVIDED =
      "未提供数据目录，请使用 --dirs <dir1> <dir2> ... 指定。";
  public static final String VALIDATION_REPAIR_COMPLETED =
      "校验和修复已完成。统计信息：";
  public static final String SEPARATOR_LINE = "------------------------------------------------------";
  public static final String IS_GENERATED_BY_PIPE_MARK = "isGeneratedByPipe 标记: {}";
  public static final String RESET_PROGRESS_INDEX = "resetProgressIndex: {}";
  public static final String DATA_DIRECTORIES = "数据目录: ";
  public static final String INDENT_PATH = "  {}";
  public static final String ERROR_VALIDATING_REPAIRING_RESOURCE = "校验或修复资源 {} 时出错: {}";
  public static final String ERROR_LOADING_RESOURCES_FROM_PARTITION = "从分区 {} 加载资源时出错: {}";
  public static final String TIME_PARTITION_PROCESS_COMPLETED = "时间分区 {} 共有 {} 个资源，{} 个需要设置 isGeneratedByPipe，{} 个需要重置 progressIndex，{} 个已更改。处理完成。";
  public static final String SKIPPED_RESOURCE_FILE_NOT_EXIST = "{} 已跳过，因为资源文件不存在。";
  public static final String REPAIRING_TSFILE_RESOURCE = "正在修复 TsFileResource: {}，isGeneratedByPipe 标记: {}，实际标记: {}";
  public static final String RESETTING_PROGRESS_INDEX_TO_MINIMUM = "正在将 TsFileResource:{} 的 progressIndex 重置为最小值，原始 progressIndex: {}";
  public static final String MARKED_TSFILE_RESOURCE_AS = "已将 TsFileResource 标记为 {} ，资源: {}";
  public static final String RESET_PROGRESS_INDEX_TO_MINIMUM = "已将 TsFileResource:{} 的 progressIndex 重置为最小值。";
  public static final String FAILED_TO_REPAIR_TSFILE_RESOURCE = "错误：修复 TsFileResource 失败：{}";
  public static final String TOTAL_TIME_TAKEN = "总耗时: {} 毫秒，TsFile 资源总数: {}，设置 isGeneratedByPipe 的资源数: {}，重置 progressIndex 的资源数: {}，已更改的资源数: {}";

  // ---------------------------------------------------------------------------
  // tools – DelayAnalyzer
  // ---------------------------------------------------------------------------
  public static final String DELAY_ANALYZER_RESET = "[DelayAnalyzer] DelayAnalyzer 已重置";

  // ---------------------------------------------------------------------------
  // utils – DataNodeObjectFileService
  // ---------------------------------------------------------------------------
  public static final String FAILED_REMOVE_OBJECT_FILE =
      "删除对象文件 {} 失败";
  public static final String FAILED_REMOVE_EMPTY_OBJECT_DIR =
      "删除空对象目录 {} 失败";
  public static final String REMOVE_OBJECT_FILE =
      "删除对象文件 {}，大小为 {}（字节）";

  // ---------------------------------------------------------------------------
  // utils – OpenFileNumUtil
  // ---------------------------------------------------------------------------
  public static final String CANNOT_GET_PID =
      "无法获取 IoTDB 进程的 PID，原因：";
  public static final String UNSUPPORTED_OS_GET_PID =
      "不支持的操作系统 {}，无法通过 OpenFileNumUtil 获取 IoTDB 的 PID。";
  public static final String CANNOT_GET_OPEN_FILE_NUMBER =
      "无法获取 IoTDB 进程的打开文件数，原因：";

  // ---------------------------------------------------------------------------
  // utils – MemUtils
  // ---------------------------------------------------------------------------
  public static final String UNSUPPORTED_DATA_POINT_TYPE = "不支持的数据点类型";

  // ---------------------------------------------------------------------------
  // utils – ErrorHandlingUtils
  // ---------------------------------------------------------------------------
  public static final String ERROR_OPERATION_LOG =
      "状态码：{}，操作 {} 失败";

  // ---------------------------------------------------------------------------
  // utils – CommonUtils
  // ---------------------------------------------------------------------------
  public static final String INPUT_FLOAT_INFINITY = "输入的浮点数值为 Infinity";
  public static final String INPUT_DOUBLE_INFINITY = "输入的双精度数值为 Infinity";
  public static final String BOOLEAN_PARSE_ERROR =
      "BOOLEAN 值应为 true/TRUE、false/FALSE 或 0/1";
  public static final String UNSUPPORTED_DATA_TYPE_FMT = "不支持的数据类型：%s";
  public static final String UNSUPPORTED_DATA_TYPE = "不支持的数据类型：";
  public static final String AGGREGATE_FUNCTION_NAME_NULL =
      "聚合函数名称不能为空";
  public static final String INVALID_AGGREGATION_FUNCTION =
      "无效的聚合函数：";
  public static final String INVALID_AGGREGATION_FUNCTION_FMT =
      "无效的聚合函数：%s";
  public static final String SCALAR_FUNCTION_NAME_NULL =
      "标量函数名称不能为空。";
  public static final String DELETE_CURSOR_SIZE_ERROR =
      "deleteCursor 应为大小为 1 的数组";

  // ---------------------------------------------------------------------------
  // utils – ThreadUtils
  // ---------------------------------------------------------------------------
  public static final String WAITING_TERMINATED_TIMEOUT =
      "等待 {} 终止超时";
  public static final String POOL_NOT_EXIT_AFTER_TIMEOUT =
      "{} 在 60 秒后仍未退出";

  // ---------------------------------------------------------------------------
  // utils – WindowEvaluationTaskPoolManager
  // ---------------------------------------------------------------------------
  public static final String WINDOW_EVAL_POOL_INIT =
      "WindowEvaluationTaskPoolManager 正在初始化，线程数：{}";

  // ---------------------------------------------------------------------------
  // utils – LogWriter
  // ---------------------------------------------------------------------------
  public static final String INTERRUPTED_NO_WRITE =
      "当前线程被中断，为保证 IO 安全跳过写入操作";

  // ---------------------------------------------------------------------------
  // conf – IoTDBStartCheck
  // ---------------------------------------------------------------------------
  public static final String STARTING_IOTDB = "正在启动 IoTDB {}";
  public static final String CANNOT_CREATE_SCHEMA_DIR = "无法创建 Schema 目录：{}";
  public static final String SCHEMA_DIR_CREATED = " {} 目录已创建。";
  public static final String IOTDB_VERSION_TOO_OLD = "IoTDB 版本过旧";
  public static final String REPAIR_SYSTEM_PROPERTIES = "修复 system.properties，缺少 {}";
  public static final String UNEXPECTED_CONSENSUS_GROUP_TYPE =
      "未预期的共识组类型";
  public static final String ENCRYPT_MAGIC_STRING_NOT_MATCHED =
      "encrypt_magic_string 不匹配";

  // ---------------------------------------------------------------------------
  // conf – IoTDBDescriptor
  // ---------------------------------------------------------------------------
  public static final String FAILED_UPDATE_CONFIG_FILE = "更新配置文件失败";
  public static final String WILL_RELOAD_PROPERTIES = "将从 {} 重新加载配置";
  public static final String GET_URL_FAILED = "获取 URL 失败";
  public static final String START_READ_CONFIG_FILE = "开始读取配置文件 {}";
  public static final String FAIL_FIND_CONFIG_FILE =
      "找不到配置文件 {}，拒绝启动 DataNode。";
  public static final String CANNOT_LOAD_CONFIG_FILE =
      "无法加载配置文件，拒绝启动 DataNode。";
  public static final String INCORRECT_FORMAT_CONFIG_FILE =
      "配置文件格式不正确，拒绝启动 DataNode。";
  public static final String COULD_NOT_LOAD_CONFIG =
      "无法从任何已知来源加载配置。";
  public static final String START_RELOAD_CONFIG_FILE = "开始重新加载配置文件 {}";
  public static final String FAIL_RELOAD_CONFIG_FILE = "重新加载配置文件 {} 失败";
  public static final String RELOAD_METRIC_SERVICE = "重新加载指标服务，级别 {}";
  public static final String PAGE_SIZE_GREATER_THAN_GROUP_SIZE =
      "page_size 大于 group size，将设置为与 group size 相同";
  public static final String MQTT_HOST_NOT_CONFIGURED =
      "MQTT 主机未配置，将使用 dn_rpc_address。";
  public static final String FAILED_PARSE_TRUSTED_URI =
      "解析 trusted_uri_pattern {} 失败";
  public static final String FAILED_GET_FILE_SIZE = "获取 {} 的文件大小失败，原因：";
  public static final String SET_DELAY_ANALYZER_WINDOW_SIZE =
      "[DelayAnalyzer] 设置 delay_analyzer_window_size 为 {}";
  public static final String FAIL_RELOAD_CONFIGURATION_FMT =
      "重新加载配置失败，原因：%s";

  // ---------------------------------------------------------------------------
  // conf – IoTDBConfig
  // ---------------------------------------------------------------------------
  public static final String FAIL_GET_CANONICAL_PATH = "获取 {} 的规范路径失败";
  public static final String NO_DATA_DIR_SET =
      "未设置数据目录，loadTsFileDirs 保持默认值。";
  public static final String FAILED_GET_FIELD = "获取字段 {} 失败";
  public static final String SKIP_FAILED_TABLE_SCHEMA_CHECK =
      "skipFailedTableSchemaCheck 已设置为 {}。";
  public static final String DIR_REMOVED_FROM_DATA_DIRS =
      "%s 已从 data_dirs 参数中移除，请将其添加回去。";

  // ---------------------------------------------------------------------------
  // conf – DataNodeMemoryConfig
  // ---------------------------------------------------------------------------
  public static final String FAIL_RELOAD_MEMORY_CONFIG_FMT =
      "重新加载配置失败，原因：%s";

  // ---------------------------------------------------------------------------
  // conf – DataNodeStartupCheck
  // ---------------------------------------------------------------------------
  public static final String PORTS_HAVE_REPEAT =
      "DataNode 使用的端口存在重复。";

  // ---------------------------------------------------------------------------
  // conf – REST service
  // ---------------------------------------------------------------------------
  public static final String REST_COULD_NOT_LOAD_CONFIG =
      "无法从任何已知来源加载 REST 服务配置。";
  public static final String REST_START_READ_CONFIG = "开始读取配置文件 {}";
  public static final String REST_FAIL_FIND_CONFIG =
      "REST 服务找不到配置文件 {}";
  public static final String REST_CANNOT_LOAD_CONFIG =
      "REST 服务无法加载配置文件，使用默认配置";
  public static final String REST_INCORRECT_FORMAT =
      "REST 服务配置文件格式不正确，使用默认配置";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionBroker
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_PREFETCHING_QUEUE_STATE =
      "订阅：SubscriptionPrefetchingQueue 状态 {}";
  public static final String SUBSCRIPTION_UNEXPECTED_EXCEPTION =
      "订阅：意外异常（不变量被破坏）{}";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionReceiverV1
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_UNKNOWN_REQUEST_TYPE =
      "订阅：未知的 PipeSubscribeRequestType，响应状态 = {}。";
  public static final String SUBSCRIPTION_CONSUMER_HEARTBEAT_SUCCESS =
      "订阅：消费者 {} 心跳成功";
  public static final String SUBSCRIPTION_CONSUMER_SUBSCRIBE_SUCCESS =
      "订阅：消费者 {} 订阅 {} 成功";
  public static final String SUBSCRIPTION_CONSUMER_CLOSE_SUCCESS =
      "订阅：消费者 {} 关闭成功";
  public static final String SUBSCRIPTION_EXCEPTION_HANDSHAKING =
      "握手请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_HEARTBEAT =
      "心跳请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_SUBSCRIBING =
      "订阅请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_UNSUBSCRIBING =
      "取消订阅请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_POLLING =
      "拉取请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_COMMITTING =
      "提交请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_CLOSING =
      "关闭请求 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_CREATING_CONSUMER =
      "在 ConfigNode 上创建消费者 {} 时发生异常";
  public static final String SUBSCRIPTION_EXCEPTION_CLOSING_CONSUMER =
      "在 ConfigNode 上关闭消费者 {} 时发生异常";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionBrokerAgent
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_CREATE_BROKER =
      "订阅：创建绑定到消费者组 [{}] 的 Broker";
  public static final String SUBSCRIPTION_DROP_BROKER =
      "订阅：删除绑定到消费者组 [{}] 的 Broker";
  public static final String SUBSCRIPTION_BROKER_NOT_EXIST_FMT =
      "订阅：绑定到消费者组 [%s] 的 Broker 不存在";
  public static final String SUBSCRIPTION_PIPE_BROKER_NOT_EMPTY =
      "订阅：绑定到消费者组 [{}] 的 Pipe Broker 非空，无法删除";
  public static final String SUBSCRIPTION_CONSENSUS_BROKER_NOT_EMPTY =
      "订阅：绑定到消费者组 [{}] 的共识 Broker 非空，无法删除";
  public static final String SUBSCRIPTION_DROP_CONSENSUS_BROKER =
      "订阅：删除绑定到消费者组 [{}] 的共识 Broker";
  public static final String SUBSCRIPTION_CREATE_PIPE_BROKER_FOR_BINDING =
      "订阅：绑定到消费者组 [{}] 的 Pipe Broker 不存在，为绑定预取队列创建新 Broker";
  public static final String SUBSCRIPTION_CREATE_CONSENSUS_BROKER_FOR_BINDING =
      "订阅：绑定到消费者组 [{}] 的共识 Broker 不存在，为绑定共识预取队列创建新 Broker";
  public static final String SUBSCRIPTION_CONSENSUS_UNEXPECTED_IN_FLIGHT_RESPONSE_FMT =
      "ConsensusPrefetchingQueue %s：消费者 %s 的处理中响应不符合预期，提交上下文 %s，偏移量 %s";
  public static final String SUBSCRIPTION_UNSUPPORTED_CONSENSUS_PROGRESS_FILE_VERSION_FMT =
      "不支持的共识订阅进度文件版本 %s";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionConsumerAgent
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DROPPING_CONSUMER_GROUP =
      "删除消费者组 {} 时发生异常";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionTopicAgent
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DROPPING_TOPIC =
      "删除 Topic {} 时发生异常";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionEvent
  // ---------------------------------------------------------------------------
  public static final String EVENT_NACKED_TIMES = "{} 已被否定确认 {} 次";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPollResponseCache
  // ---------------------------------------------------------------------------
  public static final String NULL_RESPONSE_INVALIDATING =
      "使缓存失效时响应为空，跳过";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionEventTsFileResponse
  // ---------------------------------------------------------------------------
  public static final String UNEXPECTED_RESPONSE_TYPE = "意外的响应类型：{}";
  public static final String UNEXPECTED_MESSAGE_TYPE = "意外的消息类型：{}";
  public static final String UNEXPECTED_RESPONSE_TYPE_FMT = "意外的响应类型：%s";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPipeEventBatches
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_SEALING_EVENTS =
      "从批次 {} 封存事件时发生异常";
  public static final String EXCEPTION_CONSTRUCT_NEW_BATCH =
      "构造新批次时发生异常";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPrefetchingQueue
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_EXECUTE_RECEIVER_SUBTASK =
      "异常 {} 在 {} 执行接收子任务时发生";
  public static final String EXCEPTION_CONSTRUCT_TABLET_ITERATOR =
      "异常 {} 在 {} 构造 ToTabletIterator 时发生";
  public static final String EXCEPTION_EMIT_EVENTS_BEFORE_COMMIT_TERMINATE_EVENT =
      "订阅：SubscriptionPrefetchingQueue {} 在提交 PipeTerminateEvent {} 前封存剩余事件失败";
  public static final String COMMIT_TERMINATE_EVENT =
      "订阅：SubscriptionPrefetchingQueue {} 提交 PipeTerminateEvent {}";

  // ---------------------------------------------------------------------------
  // consensus – BaseStateMachine
  // ---------------------------------------------------------------------------
  public static final String UNEXPECTED_CONSENSUS_REQUEST =
      "未预期的 IConsensusRequest：{}";
  public static final String UNEXPECTED_CONSENSUS_REQUEST_EXCEPTION =
      "未预期的 IConsensusRequest！";

  // ---------------------------------------------------------------------------
  // consensus – SchemaExecutionVisitor
  // ---------------------------------------------------------------------------
  public static final String IO_ERROR = "{}: IO 错误：";
  public static final String OPENED_PIPE_LISTENING_QUEUE =
      "已在 Schema Region {} 上打开 Pipe 监听队列";
  public static final String CLOSED_PIPE_LISTENING_QUEUE =
      "已在 Schema Region {} 上关闭 Pipe 监听队列";

  // ---------------------------------------------------------------------------
  // consensus – SchemaRegionStateMachine
  // ---------------------------------------------------------------------------
  public static final String FAIL_LOAD_SNAPSHOT = "从 {} 加载快照失败";

  // ---------------------------------------------------------------------------
  // consensus – DataExecutionVisitor
  // ---------------------------------------------------------------------------
  public static final String ERROR_EXECUTING_PLAN_NODE =
      "执行计划节点 {} 时发生错误";
  public static final String ERROR_EXECUTING_PLAN_NODE_CAUSED =
      "执行计划节点 {} 时发生错误，原因：{}";
  public static final String REJECT_EXECUTING_PLAN_NODE =
      "拒绝执行计划节点 {}，原因：{}";
  public static final String BATCH_FAILURE_INSERT_ROWS =
      "执行 InsertRowsNode 时出现批量失败。";
  public static final String BATCH_FAILURE_INSERT_MULTI_TABLETS =
      "执行 InsertMultiTabletsNode 时出现批量失败。";
  public static final String BATCH_FAILURE_INSERT_ROWS_ONE_DEVICE =
      "执行 InsertRowsOfOneDeviceNode 时出现批量失败。";

  // ---------------------------------------------------------------------------
  // consensus – DataRegionStateMachine
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_REPLACING_DATA_REGION =
      "替换存储引擎中的数据 Region 时发生异常。";
  public static final String UNEXPECTED_PLAN_NODE_TYPE =
      "未预期的 PlanNode 类型 {}，不是 SearchNode";
  public static final String TABLE_NOT_EXISTS_OR_LOST =
      "表不存在或已丢失，结果码为 {}";
  public static final String GET_FRAGMENT_INSTANCE_FAILED = "获取 FragmentInstance 失败";
  public static final String CANNOT_GET_CANONICAL_FILE =
      "{}: 无法获取 {} 的规范文件路径，原因：{}";

  // ---------------------------------------------------------------------------
  // auth – LoginLockManager
  // ---------------------------------------------------------------------------
  public static final String IP_LOGIN_ATTEMPTS_DISABLED =
      "IP 级别登录尝试已禁用（设置为 {}）";
  public static final String USER_LOGIN_ATTEMPTS_DISABLED =
      "用户级别登录尝试已禁用（设置为 {}）";
  public static final String IP_LOCKED = "IP '{}' 已锁定，用户 ID '{}'";
  public static final String USER_UNLOCKED_MANUAL = "用户 ID '{}' 已解锁（手动）";
  public static final String IP_UNLOCKED_MANUAL =
      "IP '{}' 已为用户 ID '{}' 解锁（手动）";
  public static final String USER_UNLOCKED_EXPIRED = "用户 ID '{}' 已解锁（过期）";
  public static final String IP_UNLOCKED_EXPIRED =
      "IP '{}' 已为用户 ID '{}' 解锁（过期）";
  public static final String IP_LOCKED_MULTIPLE_USERS =
      "IP '{}' 被 {} 个不同用户锁定 → 可能遭受攻击";
  public static final String USER_MULTIPLE_IP_LOCKS =
      "用户 ID '{}' 有 {} 个 IP 锁定 → 可能遭受攻击";
  public static final String FAILED_CHECK_IP_UP =
      "检查 IP 地址 {} 是否可达失败";

  // ---------------------------------------------------------------------------
  // auth – ClusterAuthorityFetcher
  // ---------------------------------------------------------------------------
  public static final String CACHE_USER_PATH_PRIVILEGES_ERROR =
      "缓存用户路径权限时发生错误";
  public static final String CACHE_ROLE_PATH_PRIVILEGES_ERROR =
      "缓存角色路径权限时发生错误";

  // ---------------------------------------------------------------------------
  // auth – BasicAuthorityCache
  // ---------------------------------------------------------------------------
  public static final String DATANODE_CACHE_INIT_FAILED =
      "DataNode 缓存初始化失败";

  // ---------------------------------------------------------------------------
  // trigger – TriggerExecutor
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_FIRE_ERROR =
      "触发器触发时发生错误，触发器：{}，原因：{}";

  // ---------------------------------------------------------------------------
  // trigger – TriggerInformationUpdater
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_INFO_UPDATER_STARTED =
      "有状态触发器信息更新器已成功启动。";
  public static final String TRIGGER_INFO_UPDATER_STOPPED =
      "有状态触发器信息更新器已成功停止。";
  public static final String ERROR_UPDATING_TRIGGER_INFO =
      "更新触发器信息时遇到错误：";

  // ---------------------------------------------------------------------------
  // trigger – TriggerFireVisitor
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_INTERRUPTED_SLEEP =
      "{} 在休眠时被中断";

  // ---------------------------------------------------------------------------
  // trigger – TriggerClassLoaderManager / TriggerClassLoader
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_LIB_ROOT = "触发器库根目录：{}";

  // ---------------------------------------------------------------------------
  // trigger – TriggerManagementService
  // ---------------------------------------------------------------------------
  public static final String ERROR_READING_MD5 =
      "尝试读取 {} 的 MD5 时发生错误";

  // ---------------------------------------------------------------------------
  // partition – DataPartitionTableGenerator
  // ---------------------------------------------------------------------------
  public static final String TASK_ALREADY_STARTED =
      "任务已启动或已完成";

  public static final String FROM_CONFIG_NODE = "' 来自 ConfigNode。'";
  public static final String IS_NOT_SUPPORTED = " 不受支持";
  public static final String CANNOT_SSL_HANDSHAKE_WITH_CN_LEADER = "无法与 ConfigNode-leader 进行 SSL 握手。";
  public static final String CANNOT_CONNECT_TO_CN_LEADER = "无法连接到 ConfigNode-leader。";
  public static final String CAPACITY_LARGER_THAN_INITIAL_PERMITS = "容量应大于初始许可数。";
  public static final String CURRENT_TV_LIST_NOT_SORTED = "当前 TVList 未排序";
  public static final String DN_CLIENT_NOT_SUPPORT_ADD_CONSENSUS_GROUP = "DataNode 到 ConfigNode 客户端不支持 addConsensusGroup。";
  public static final String DN_CLIENT_NOT_SUPPORT_GET_HEARTBEAT = "DataNode 到 ConfigNode 客户端不支持 getConfigNodeHeartBeat。";
  public static final String DN_CLIENT_NOT_SUPPORT_NOTIFY_REGISTER = "DataNode 到 ConfigNode 客户端不支持 notifyRegisterSuccess。";
  public static final String DN_CLIENT_NOT_SUPPORT_REGISTER_CN = "DataNode 到 ConfigNode 客户端不支持 registerConfigNode。";
  public static final String DN_CLIENT_NOT_SUPPORT_REMOVE_CONSENSUS_GROUP = "DataNode 到 ConfigNode 客户端不支持 removeConsensusGroup。";
  public static final String DN_CLIENT_NOT_SUPPORT_REPORT_SHUTDOWN = "DataNode 到 ConfigNode 客户端不支持 reportConfigNodeShutdown。";
  public static final String DN_CLIENT_NOT_SUPPORT_SET_STATUS = "DataNode 到 ConfigNode 客户端不支持 setDataNodeStatus。";
  public static final String DN_CLIENT_NOT_SUPPORT_STOP_AND_CLEAR = "DataNode 到 ConfigNode 客户端不支持 stopAndClearConfigNode。";
  public static final String ERROR_OCCURRED_DURING_CREATING_DIR = "创建目录时出错：";
  public static final String EXPECTING_NON_EMPTY_STRING_FOR = "期望非空字符串：";
  public static final String FAILED_TO_CONSTRUCT_PIPE_SINK = "构造 PipeSink 失败，原因：";
  public static final String FAILED_TO_GET_UDF_JAR = "从 ConfigNode 获取 UDF jar 失败。";
  public static final String FAILED_TO_GET_CONSUMER_GROUP_META = "从 ConfigNode 获取消费者组元数据失败。";
  public static final String FAILED_TO_GET_TOPIC_META = "从 ConfigNode 获取 Topic 元数据失败。";
  public static final String FAILED_TO_GET_TRIGGER_JAR = "从 ConfigNode 获取 Trigger jar 失败。";
  public static final String FETCH_SCHEMA_FAILED = "获取 Schema 失败。";
  public static final String INDEX_BELOW_START_POSITION = "索引低于起始位置：";
  public static final String INDEX_EXCEEDS_END_POSITION = "索引超过结束位置：";
  public static final String INDEX_OUT_OF_BOUND_ERROR = "索引越界错误！";
  public static final String INVALID_PUSH_MULTI_PIPE_META_REQ = "无效的 TPushMultiPipeMetaReq";
  public static final String INVALID_PUSH_MULTI_TOPIC_META_REQ = "无效的 TPushMultiTopicMetaReq";
  public static final String INVALID_PUSH_SINGLE_PIPE_META_REQ = "无效的 TPushSinglePipeMetaReq";
  public static final String INVALID_PARAM = "无效参数";
  public static final String INVALID_PARAMETERS_CHECK_USER_GUIDE = "参数无效，请查看用户指南。";
  public static final String AGGREGATE_MIN_MAX_VALUE_ONLY_SUPPORT_ALLOWED_TYPES =
      "聚合函数 [MIN_VALUE, MAX_VALUE] 仅支持数据类型 [INT32, INT64, FLOAT, DOUBLE, STRING, DATE, TIMESTAMP]";
  public static final String AGGREGATE_AVG_SUM_STDDEV_ONLY_SUPPORT_NUMERIC_TYPES =
      "聚合函数 [AVG, SUM, EXTREME, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP] 仅支持数值类型 [INT32, INT64, FLOAT, DOUBLE]";
  public static final String AGGREGATE_SKEWNESS_KURTOSIS_ONLY_SUPPORT_NUMERIC_TYPES =
      "聚合函数 [SKEWNESS, KURTOSIS] 仅支持数值类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String AGGREGATE_FUNCTION_INPUT_SERIES_ONLY_SUPPORTS_BOOLEAN =
      "聚合函数 [%s] 的输入序列仅支持数据类型 [BOOLEAN]";
  public static final String AGGREGATE_CORR_COVAR_REGR_ONLY_SUPPORT_NUMERIC_TYPES =
      "聚合函数 [CORR, COVAR_POP, COVAR_SAMP, REGR_SLOPE, REGR_INTERCEPT] 仅支持数值类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String CHECK_AGGREGATION_KEEP_CONDITION =
      "请检查聚合函数 [%s] 的输入 keep 条件";
  public static final String AGGREGATION_KEEP_CONDITION_REQUIREMENT =
      "聚合函数 [%s] 的 keep 条件必须是常量，或由 keep 和 long 类型数字构成的比较表达式";
  public static final String DATABASE_NAME_IS_TREE_MODEL_DATABASE =
      "数据库名称 %s 是树模型数据库，不允许设置到客户端会话中。";
  public static final String INVALID_REQUEST = "无效请求 ";
  public static final String PREPARED_STMT_NOT_SUPPORTED_FOR_TREE = "Tree 模型不支持 PreparedStatement";
  public static final String FILE_LENGTH_LARGER_THAN_MAX = "文件长度超过 max_object_file_size_in_bytes";
  public static final String UNKNOWN_CONSENSUS_GROUP_TYPE = "未知共识组类型：";
  public static final String UNKNOWN_DATA_TYPE = "未知数据类型：";
  public static final String UNKNOWN_PARAMETER_TYPE = "未知参数类型：";
  public static final String UNKNOWN_SQL_DIALECT = "未知 sql_dialect：";
  public static final String UNRECOGNIZED_MNODE_TYPE = "无法识别的 MNode 类型";
  public static final String UNRECOGNIZED_DATATYPE = "无法识别的数据类型：";
  public static final String UNSUPPORTED_COLUMN_GENERATOR_TYPE = "不支持的 ColumnGeneratorType：";
  public static final String UNSUPPORTED_TRIGGER_FIRE_RESULT_TYPE = "不支持的 TriggerFireResult 类型";
  public static final String UTILITY_CLASS = "工具类";
  public static final String APPEND_SIZE_MUST_BE_POSITIVE = "appendSize 必须为正";
  public static final String BLOCKS_SHOULD_NEVER_BE_ZERO = "blocks 不应为零。";
  public static final String END_INDEX_MUST_BE_GE_START_INDEX = "endIndex 必须 >= startIndex";
  public static final String ERROR_CODE = "错误码：";
  public static final String NULL_RESPONSE_WHEN_SERIALIZING = "序列化时响应为空";
  public static final String OBJECT_STORAGE_NOT_SUPPORTED_YET = "暂不支持对象存储";
  public static final String REGISTERED_TASK_COUNT_LT_ZERO = "registeredTaskCount < 0";
  public static final String REGISTERED_TASK_COUNT_LE_ZERO = "registeredTaskCount <= 0";
  public static final String REQUEST_TYPE_NOT_SUPPORTED = "不支持的请求类型：";
  public static final String UNEXPECTED_REQUEST_TYPE = "意外的请求类型：%s";

  // --- DataNodeInternalRPCServiceImpl ---
  public static final String LOAD_COMMAND_REQUIRES_TIME_PARTITION_TO_PROGRESS_INDEX_MAP =
      "Load 命令需要时间分区到进度索引的映射";
  public static final String TOPOLOGY_PROBING_TIMED_OUT_AFTER_S_MS =
      "拓扑探测在 %s 毫秒后超时";
  public static final String NO_SUCH_QUERY = "查询不存在";
  public static final String CHANGE_REGION_LEADER_ERROR_REGION_TYPE =
      "[ChangeRegionLeader] 错误的 Region 类型: ";
  public static final String SUBMIT_ADD_REGION_PEER_TASK_FAILED_REGION =
      "提交 addRegionPeer 任务失败，region: ";
  public static final String SUBMIT_REMOVE_REGION_PEER_TASK_FAILED_REGION =
      "提交 removeRegionPeer 任务失败，region: ";
  public static final String SUBMIT_DELETE_OLD_REGION_PEER_TASK_FAILED_REGION =
      "提交 deleteOldRegionPeer 任务失败，region: ";
  public static final String CREATE_NEW_REGION_PEER_SUCCEED_REGION_ID =
      "createNewRegionPeer 成功，regionId: ";
  public static final String DISABLE_DATANODE_SUCCEED = "禁用 DataNode 成功";
  public static final String STOP_AND_CLEAR_DATA_NODE_SUCCEED = "停止并清理 DataNode 成功";
  public static final String NO_DATA_PARTITION_TABLE_GENERATION_TASK_FOUND =
      "未找到 DataPartitionTable 生成任务";

  // --- DataNode ---
  public static final String SUCCESSFULLY_REGISTERED_ALL_UDFS_TAKES_MS =
      "成功注册所有 UDF，耗时 {} 毫秒。";
  public static final String GET_TREE_UDF = "获取树模型 UDF: {}";
  public static final String GET_TABLE_UDF = "获取表模型 UDF: {}";
  public static final String GET_TRIGGER = "获取触发器: {}";
  // ---------------------------------------------------------------------------
  // 补充日志消息
  // ---------------------------------------------------------------------------
  public static final String MISC_LOG_DELAYANALYZER_CALCULATED_SAFE_WATERMARK_CURRENTTIME_P_DELAY_74E1214C =
      "[DelayAnalyzer] 已计算安全水位线：{}（当前时间：{}，P{} 延迟：{}ms）";
  public static final String MISC_LOG_LOGIN_LOCK_MANAGER_INITIALIZED_WITH_IP_LEVEL_ATTEMPTS_USER_57AE7966 =
      "登录锁管理器已初始化：IP 级尝试次数={}，用户级尝试次数={}，锁定时间={} 分钟";
  public static final String MISC_LOG_USER_ID_LOCKED_DUE_TO_FAILED_ATTEMPTS_743CFB3A =
      "用户 ID '{}' 因连续失败 {} 次而被锁定";
  public static final String MISC_LOG_USER_LEVEL_ATTEMPTS_AUTO_ENABLED_WITH_DEFAULT_1000_BECAUSE_FAB86B7D =
      "用户级尝试次数已自动启用并使用默认值 1000，因为 IP 级已启用（设置为 {}）";
  public static final String MISC_LOG_INVALID_LOCK_TIME_VALUE_RESET_TO_DEFAULT_10_MINUTES_8DCE21EF =
      "无效的锁定时间值（{}），重置为默认值（10 分钟）";
  public static final String MISC_LOG_SUCCESSFULLY_WAITED_FOR_PIPE_TO_FINISH_FBDF5157 =
      "已成功等待 Pipe {} 完成。";
  public static final String MISC_LOG_DATANODE_EXITS_JVM_MEMORY_USAGE_BE69D1F5 =
      "DataNode 退出。JVM 内存使用情况：{}";
  public static final String MISC_LOG_FAILED_TO_REPORT_DATANODE_S_SHUTDOWN_TO_CONFIGNODE_THE_CLUSTER_E6727497 =
      "无法向 ConfigNode 上报 DataNode 关闭状态。集群在接下来的几秒内仍会将当前 DataNode 视为 Running。";
  public static final String MISC_LOG_SOMETHING_WRONG_HAPPENED_WHILE_CALLING_CONSENSUS_LAYER_S_8B8FBB16 =
      "调用共识层的 triggerSnapshot API 时发生错误。";
  public static final String MISC_LOG_THE_ADDREGIONPEERTASK_HAS_ALREADY_BEEN_SUBMITTED_AND_WILL_4D398F73 =
      "{} AddRegionPeerTask {} 已提交，不会重复提交。";
  public static final String MISC_LOG_THE_REMOVEREGIONPEER_HAS_ALREADY_BEEN_SUBMITTED_AND_WILL_6754D9FB =
      "{} RemoveRegionPeer {} 已提交，不会重复提交。";
  public static final String MISC_LOG_THE_DELETEOLDREGIONPEERTASK_HAS_ALREADY_BEEN_SUBMITTED_AND_75815D37 =
      "{} DeleteOldRegionPeerTask {} 已提交，不会重复提交。";
  public static final String MISC_LOG_RESET_PEER_LIST_FAIL_THIS_DATANODE_NOT_CONTAINS_PEER_OF_6539945C =
      "重置 peer 列表失败，该 DataNode 不包含共识组 {} 的 peer，可能是创建本地 peer 失败导致。";
  public static final String MISC_LOG_SUBMIT_ADDREGIONPEERTASK_ERROR_FOR_REGION_4E999BA9 =
      "{}, 提交 AddRegionPeerTask 失败，Region：{}";
  public static final String MISC_LOG_SUBMIT_REMOVEREGIONPEER_TASK_ERROR_FOR_REGION_200E7F68 =
      "{}, 提交 RemoveRegionPeer 任务失败，Region：{}";
  public static final String MISC_LOG_SUBMIT_DELETEOLDREGIONPEERTASK_ERROR_FOR_REGION_460C308A =
      "{}, 提交 DeleteOldRegionPeerTask 失败，Region：{}";
  public static final String MISC_LOG_GET_TRIGGER_EXECUTOR_1727D243 = "获取触发器执行器：{}";
  public static final String MISC_LOG_SUCCESSFULLY_PULL_SYSTEM_CONFIGURATIONS_FROM_CONFIGNODE_C8E04BF8 =
      "成功从 ConfigNode-leader 拉取系统配置，耗时 {} ms";
  public static final String MISC_LOG_SUCCESSFULLY_REGISTER_TO_THE_CLUSTER_WHICH_TAKES_MS_ED479CA7 =
      "成功注册到集群：{}，耗时 {} ms。";
  public static final String MISC_LOG_RESTART_REQUEST_TO_CLUSTER_IS_ACCEPTED_WHICH_TAKES_MS_E8305E02 =
      "向集群发送的重启请求已被接受：{}，耗时 {} ms。";
  public static final String MISC_LOG_SCHEMAREGION_CONSENSUS_START_SUCCESSFULLY_WHICH_TAKES_MS_3D1B8523 =
      "SchemaRegion 共识启动成功，耗时 {} ms。";
  public static final String MISC_LOG_DATAREGION_CONSENSUS_START_SUCCESSFULLY_WHICH_TAKES_MS_2B2DB4CB =
      "DataRegion 共识启动成功，耗时 {} ms。";
  public static final String MISC_LOG_IOTDB_DATANODE_IS_SETTING_UP_SOME_DATABASES_MAY_NOT_BE_READY_961523F0 =
      "IoTDB DataNode 正在初始化，部分数据库可能尚未就绪，请等待几秒。";
  public static final String MISC_LOG_SUCCESSFULLY_REGISTERED_ALL_THE_TRIGGERS_WHICH_TAKES_MS_246178BB =
      "成功注册所有 trigger，耗时 {} ms。";
  public static final String MISC_LOG_PREPARE_EXTERNAL_SERVICE_RESOURCES_SUCCESSFULLY_WHICH_TAKES_00E62CB0 =
      "成功准备 external-service 资源，耗时 {} ms。";
  public static final String MISC_LOG_CANNOT_SEND_RESTART_REQUEST_TO_THE_CONFIGNODE_LEADER_BECAUSE_AB17D41A =
      "无法向 ConfigNode-leader 发送重启请求，原因：{}";
  public static final String MISC_LOG_CANNOT_PULL_SYSTEM_CONFIGURATIONS_FROM_CONFIGNODE_LEADER_FE630DAE =
      "重试 {} 次后仍无法从 ConfigNode-leader 拉取系统配置。";
  public static final String MISC_LOG_CANNOT_SEND_RESTART_DATANODE_REQUEST_TO_CONFIGNODE_LEADER_4F50C19F =
      "重试 {} 次后仍无法向 ConfigNode-leader 发送重启 DataNode 请求。";
  public static final String MISC_LOG_TOTALLY_FIND_TSFILES_TO_BE_SETTLED_DB47A63C =
      "共找到 {} 个待 settle 的 TsFile。";
  public static final String MISC_LOG_SYSTEM_PROPERTIES_FILE_HAS_BEEN_MOVED_SUCCESSFULLY_4445A448 =
      "system.properties 文件已成功移动：{} -> {}";
  public static final String MISC_LOG_SERIALIZE_MUTABLE_SYSTEM_PROPERTIES_SUCCESSFULLY_WHICH_TAKES_4656A206 =
      "成功序列化 mutable system properties，耗时 {} ms。";
  public static final String MISC_LOG_SYSTEMPROPERTIES_NORMALIZE_FROM_TO_FOR_COMPATIBILITY_BE1C725F =
      "[SystemProperties] 为兼容性将 {} 从 {} 规范化为 {}。";
  public static final String MISC_LOG_DO_NOT_UPGRADE_IOTDB_FROM_V0_9_OR_LOWER_VERSION_TO_V1_0_9878EC88 =
      "请勿将 IoTDB 从 v0.9 或更低版本直接升级到 v1.0！请先升级到 v0.10";
  public static final String MISC_LOG_CANNOT_FIND_IOTDB_HOME_OR_IOTDB_CONF_ENVIRONMENT_VARIABLE_BE01B2FE =
      "加载配置文件 {} 时找不到 IOTDB_HOME 或 IOTDB_CONF 环境变量，使用默认配置";
  public static final String MISC_LOG_COULDN_T_LOAD_THE_CONFIGURATION_FROM_ANY_OF_THE_KNOWN_SOURCES_EE3ED103 =
      "无法从任何已知来源加载配置 {}。";
  public static final String MISC_LOG_THE_PARAMETER_DN_MAX_CONNECTION_FOR_INTERNAL_SERVICE_IS_D2F24BEB =
      "参数 dn_max_connection_for_internal_service 已过期。请将其重命名为 "
          + "dn_max_client_count_for_each_node_in_client_manager。";
  public static final String MISC_LOG_PARTITION_TABLE_RECOVER_WORKER_NUM_SHOULD_BE_GREATER_THAN_74A2512B =
      "partition_table_recover_worker_num 应大于 0，但当前值为 {}，忽略该值并使用默认值 {}";
  public static final String MISC_LOG_PARTITION_TABLE_RECOVER_MAX_READ_MB_PER_SEC_SHOULD_42BCDFBC =
      "partition_table_recover_max_read_megabytes_per_second 应大于 0，但当前值为 {}，忽略该值并使用默认值 {}";
  public static final String MISC_LOG_THE_THROTTLE_THRESHOLD_PARAMS_IS_DEPRECATED_PLEASE_USE_AA0E8EC7 =
      "throttle threshold 参数 {} 已废弃，请使用 {}";
  public static final String MISC_LOG_FAILED_TO_PARSE_QUERY_SAMPLE_THROUGHPUT_BYTES_PER_SEC_TO_00144244 =
      "无法将 query_sample_throughput_bytes_per_sec {} 解析为整数";
  public static final String MISC_LOG_THE_PARAMETER_DN_TARGET_CONFIG_NODE_LIST_HAS_BEEN_ABANDONED_6C0DE50B =
      "参数 dn_target_config_node_list 已废弃，仅会使用第一个 ConfigNode 地址加入集群。请改用 "
          + "dn_seed_config_node。";
  public static final String MISC_LOG_FAILED_TO_RELOAD_PROPERTIES_FROM_REJECT_DATANODE_STARTUP_74E66EEC =
      "无法从 {} 重新加载 properties，拒绝启动 DataNode。";
  public static final String MISC_LOG_CONFIGNODES_ARE_SET_IN_WRONG_FORMAT_PLEASE_SET_THEM_LIKE_18E97679 =
      "ConfigNodes 格式错误，请按 127.0.0.1:10710 的格式设置";
  public static final String MISC_LOG_INITIAL_ALLOCATEMEMORYFORWRITE_B90EC7D9 =
      "初始 allocateMemoryForWrite = {}";
  public static final String MISC_LOG_INITIAL_ALLOCATEMEMORYFORREAD_07FB30F0 =
      "初始 allocateMemoryForRead = {}";
  public static final String MISC_LOG_INITIAL_ALLOCATEMEMORYFORSCHEMA_965D4CE3 =
      "初始 allocateMemoryForSchema = {}";
  public static final String MISC_LOG_INITIAL_ALLOCATEMEMORYFORCONSENSUS_18B40138 =
      "初始 allocateMemoryForConsensus = {}";
  public static final String MISC_LOG_INITIAL_ALLOCATEMEMORYFORPIPE_616F9713 =
      "初始 allocateMemoryForPipe = {}";
  public static final String MISC_LOG_ALLOCATEMEMORYFORSCHEMAREGION_3BE141E8 =
      "allocateMemoryForSchemaRegion = {}";
  public static final String MISC_LOG_ALLOCATEMEMORYFORSCHEMACACHE_61BFCE7D =
      "allocateMemoryForSchemaCache = {}";
  public static final String MISC_LOG_ALLOCATEMEMORYFORPARTITIONCACHE_809AA695 =
      "allocateMemoryForPartitionCache = {}";
  public static final String MISC_LOG_THE_PARAMETER_STORAGE_QUERY_SCHEMA_CONSENSUS_FREE_MEMORY_51C9A377 =
      "参数 storage_query_schema_consensus_free_memory_proportion 自 v1.2.3 起已废弃，请改用 "
          + "datanode_memory_proportion。";
  public static final String MISC_LOG_THE_SUM_OF_REJECT_PROPORTION_WAL_BUFFER_QUEUE_PROPORTION_185B1C49 =
      "reject_proportion、wal_buffer_queue_proportion 和 device_path_cache_proportion 的总和过大，使用默认值 "
          + "0.8、0.1 和 0.05。";
  public static final String MISC_LOG_THE_VALUE_OF_STORAGE_ENGINE_MEMORY_PROPORTION_IS_ILLEGAL_22CA9433 =
      "storage_engine_memory_proportion 的值非法，使用默认值 8:2。";
  public static final String MISC_LOG_THE_VALUE_OF_WRITE_MEMORY_PROPORTION_IS_ILLEGAL_USE_DEFAULT_EE4FA112 =
      "write_memory_proportion 的值非法，使用默认值 19:1。";
  public static final String MISC_LOG_SET_LOADTSFILESPILTPARTITIONMAXSIZE_FROM_TO_560BA8F7 =
      "将 loadTsFileSpiltPartitionMaxSize 从 {} 设置为 {}";
  public static final String MISC_LOG_LOADTSFILESTATEMENTSPLITTHRESHOLD_CHANGED_FROM_TO_1CB90529 =
      "loadTsFileStatementSplitThreshold 已从 {} 改为 {}";
  public static final String MISC_LOG_LOADTSFILESUBSTATEMENTBATCHSIZE_CHANGED_FROM_TO_D4EF3D07 =
      "loadTsFileSubStatementBatchSize 已从 {} 改为 {}";
  public static final String MISC_LOG_CANNOT_FIND_GIVEN_DIRECTORY_STRATEGY_USING_THE_DEFAULT_VALUE_7997B145 =
      "找不到指定的目录策略 {}，使用默认值";
  public static final String MISC_LOG_CONFIG_PROPERTY_BOOLEAN_STRING_INFER_TYPE_CAN_ONLY_BE_BOOLEAN_2FA3AFC5 =
      "配置项 boolean_string_infer_type 只能是 BOOLEAN 或 TEXT，实际为 {}";
  public static final String MISC_LOG_CONFIG_PROPERTY_FLOATING_STRING_INFER_TYPE_CAN_ONLY_BE_FLOAT_8041EAC4 =
      "配置项 floating_string_infer_type 只能是 FLOAT、DOUBLE 或 TEXT，实际为 {}";
  public static final String MISC_LOG_CONFIG_PROPERTY_NAN_STRING_INFER_TYPE_CAN_ONLY_BE_FLOAT_61F60E0E =
      "配置项 nan_string_infer_type 只能是 FLOAT、DOUBLE 或 TEXT，实际为 {}";
  public static final String MISC_LOG_ILLEGAL_DEFAULTDATABASELEVEL_SHOULD_1_USE_DEFAULT_VALUE_97F43732 =
      "非法的 defaultDatabaseLevel：{}，应 >= 1，使用默认值 1";
  public static final String MISC_LOG_INVALID_LOADTSFILESTATEMENTSPLITTHRESHOLD_VALUE_USING_DEFAULT_45EA7FBF =
      "无效的 loadTsFileStatementSplitThreshold 值：{}。使用默认值：10";
  public static final String MISC_LOG_INVALID_LOADTSFILESUBSTATEMENTBATCHSIZE_VALUE_USING_DEFAULT_5C285109 =
      "无效的 loadTsFileSubStatementBatchSize 值：{}。使用默认值：10";
  public static final String MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_CONNECTION_B0FC4929 =
      "因连接失败无法更新 metric 值，原因：";
  public static final String MISC_LOG_FAILED_TO_UPDATE_THE_VALUE_OF_METRIC_BECAUSE_OF_INTERNAL_E5C64806 =
      "因内部错误无法更新 metric 值，原因：";
  public static final String MISC_LOG_FAILED_TO_STOP_EXTERNAL_SERVICE_S_BECAUSE_S_IT_WILL_BE_DROP_B2909C1E =
      "无法停止 External Service %s，原因：%s。将强制丢弃该服务";
  public static final String MISC_LOG_CHANGEREGIONLEADER_START_CHANGE_THE_LEADER_OF_REGIONGROUP_248A99AD =
      "[ChangeRegionLeader] 开始将 RegionGroup：{} 的 leader 切换到 DataNode：{}";
  public static final String MISC_LOG_SUCCESSFULLY_SUBMIT_ADDREGIONPEER_TASK_FOR_REGION_TARGET_64183781 =
      "成功提交 addRegionPeer 任务，Region：{}，目标 DataNode：{}";
  public static final String MISC_LOG_SUCCESSFULLY_SUBMIT_REMOVEREGIONPEER_TASK_FOR_REGION_DATANODE_9B74B948 =
      "成功提交 removeRegionPeer 任务，Region：{}，待移除 DataNode：{}";
  public static final String MISC_LOG_SUCCESSFULLY_SUBMIT_DELETEOLDREGIONPEER_TASK_FOR_REGION_3F3BB495 =
      "成功提交 deleteOldRegionPeer 任务，Region：{}，待移除 DataNode：{}";
  public static final String MISC_LOG_START_TO_CREATENEWREGIONPEER_TO_REGION_6DCE04AD =
      "{}, 开始创建 createNewRegionPeer {}，目标 Region {}";
  public static final String MISC_LOG_SUCCEED_TO_CREATENEWREGIONPEER_FOR_REGION_FDF176E3 =
      "{}, createNewRegionPeer {} 创建成功，Region {}";
  public static final String MISC_LOG_EXECUTING_SYSTEM_EXIT_0_IN_STOPANDCLEARDATANODE_RPC_METHOD_647927C6 =
      "stopAndClearDataNode RPC 方法将在 30 秒后执行 system.exit(0)";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_PULLING_COMMIT_PROGRESS_48C12E4B =
      "拉取提交进度时发生错误";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_RECEIVING_SUBSCRIPTION_PROGRESS_BROADCAST_94B2CF10 =
      "接收订阅进度广播时发生错误";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_PUSHING_SUBSCRIPTION_RUNTIME_STATE_D4E71CE3 =
      "推送订阅运行时状态时发生错误";
  public static final String MISC_LOG_FAILED_TO_PROCESS_CONSENSUS_SUBSCRIPTION_ROUTE_UPDATE_80D73E2B =
      "处理共识订阅路由更新失败";
  public static final String MISC_LOG_THE_AVAILABLE_DISK_SPACE_IS_THE_TOTAL_DISK_SPACE_IS_AND_4506856F =
      "可用磁盘空间：{}，总磁盘空间：{}，剩余磁盘使用率：{} 小于 disk_space_warning_threshold：{}，系统将设置为只读！";
  public static final String MISC_LOG_CHANGEREGIONLEADER_FAILED_TO_CHANGE_THE_LEADER_OF_REGIONGROUP_F1A1DC14 =
      "[ChangeRegionLeader] 切换 RegionGroup：{} 的 leader 失败";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_CREATING_TRIGGER_INSTANCE_FOR_TRIGGER_5A8F8890 =
      "创建 trigger：{} 的 trigger instance 时发生错误。原因：{}。";
  public static final String MISC_LOG_ERROR_OCCURRED_DURING_ACTIVE_TRIGGER_INSTANCE_FOR_TRIGGER_7731ECF2 =
      "激活 trigger：{} 的 trigger instance 时发生错误。原因：{}。";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_TRY_TO_INACTIVE_TRIGGER_INSTANCE_FOR_FA93D7E7 =
      "停用 trigger：{} 的 trigger instance 时发生错误。原因：{}。 ";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_DROPPING_TRIGGER_INSTANCE_FOR_TRIGGER_23B94EBE =
      "删除 trigger：{} 的 trigger instance 时发生错误。原因：{}。";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_UPDATING_LOCATION_FOR_TRIGGER_THE_CAUSE_1C076D98 =
      "更新 trigger：{} 的 Location 时发生错误。原因：{}。";
  public static final String MISC_LOG_CREATENEWREGIONPEER_ERROR_PEERS_REGIONID_ERRORMESSAGE_2EDAE3C8 =
      "{}, CreateNewRegionPeer 出错，peers：{}，RegionId：{}，错误信息";
  public static final String MISC_LOG_FAILED_TO_SERIALIZE_DATABASESCOPEDDATAPARTITIONTABLE_FOR_2EFDD270 =
      "无法序列化数据库 {} 的 DatabaseScopedDataPartitionTable";
  public static final String MISC_LOG_ERROR_OCCURRED_WHEN_TRYING_TO_FIRE_TRIGGER_ON_TENDPOINT_BFCBA56E =
      "尝试触发 trigger({}) 到 TEndPoint：{} 时发生错误，原因：{}";
  public static final String MISC_LOG_FAILED_TO_UPDATE_LOCATION_OF_STATEFUL_TRIGGER_THROUGH_CONFIG_E6777439 =
      "无法通过 config node 更新 stateful trigger({}) 的 location。原因：{}。";
  public static final String MISC_LOG_TRIGGER_WAS_FIRED_WITH_WRONG_EVENT_43D89454 =
      "Trigger {} 触发了错误的事件 {}";
  public static final String MISC_LOG_DOES_NOT_EXIST_SKIP_IT_EFB94454 = "{} 不存在，跳过。";
  public static final String MISC_LOG_ERROR_WHEN_PARSE_TAG_AND_ATTRIBUTES_FILE_OF_NODE_PATH_D1492217 =
      "解析节点路径 {} 的 tag 和 attributes 文件时发生错误";
  public static final String MISC_LOG_TOTALLY_FIND_TSFILES_TO_BE_SETTLED_INCLUDING_TSFILES_TO_522BCA28 =
      "共找到 {} 个待 settle 的 TsFile，其中 {} 个待恢复。";
  public static final String MISC_LOG_FINISH_SETTLING_SUCCESSFULLY_FOR_TSFILE_C8BF06D7 =
      "TsFile {} settle 成功";
  public static final String MISC_LOG_MEET_ERROR_WHILE_SETTLING_THE_TSFILE_A3515E1A =
      "settle TsFile {} 时遇到错误";
  public static final String MISC_LOG_FINISH_SETTLING_TSFILES_MEET_ERRORS_6B564B68 =
      "settle 完成，{} 个 TsFile 遇到错误。";
  public static final String MISC_LOG_THE_TSFILE_SHOULD_BE_SEALED_WHEN_REWRITTING_8B631F6C =
      "重写 TsFile {} 时，该文件应处于 sealed 状态。";
  public static final String MISC_LOG_MEET_ERROR_WHEN_READING_SETTLE_LOG_LOG_PATH_2B076234 =
      "读取 settle log 时遇到错误，日志路径：{}";

  // ---------------------------------------------------------------------------
  // 补充异常消息
  // ---------------------------------------------------------------------------
  public static final String MISC_EXCEPTION_CANNOT_LOAD_FILE_S_BECAUSE_THE_FILE_HAS_CRASHED_6C180DF9 =
      "无法加载文件 %s，因为该文件已损坏。";
  public static final String MISC_EXCEPTION_THE_VERSION_OF_THIS_TSFILE_IS_TOO_LOW_PLEASE_UPGRADE_IT_19CC276C =
      "该 TsFile 版本过低，请升级到版本 4。";
  public static final String MISC_EXCEPTION_TSFILE_REWRITE_PROCESS_CANNOT_PROCEED_AT_POSITION_SBECAUSE_3763D32F =
      "TsFile 重写流程无法在位置 %s 继续，原因：%s";
  public static final String MISC_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "不支持数据类型 %s。";
  public static final String MISC_EXCEPTION_WINDOW_SIZE_MUST_BE_BETWEEN_D_AND_D_GOT_D_3559BE09 =
      "窗口大小必须在 %d 和 %d 之间，当前为 %d";
  public static final String MISC_EXCEPTION_CONFIDENCE_LEVEL_MUST_BE_BETWEEN_0_AND_1_GOT_F_2CDA358E =
      "置信度必须在 0 到 1 之间，当前为 %f";
  public static final String MISC_EXCEPTION_PERCENTILE_MUST_BE_BETWEEN_0_AND_1_GOT_F_DE6B1311 =
      "百分位必须在 0 到 1 之间，当前为 %f";
  public static final String MISC_EXCEPTION_IRREGULAR_DATA_DIR_STRUCTURE_THERE_SHOULD_BE_A_SEQUENCE_95B4D431 =
      "数据目录结构不规范。data 目录 %s 下应包含 sequence 和 unsequence 目录";
  public static final String MISC_EXCEPTION_IRREGULAR_DATA_DIR_STRUCTURE_THERE_SHOULD_BE_DATABASE_DIRECTORIES_10C36DC2 =
      "数据目录结构不规范。sequence/unsequence 目录 %s 下应包含数据库目录";
  public static final String MISC_EXCEPTION_IRREGULAR_DATA_DIR_STRUCTURE_THERE_SHOULD_BE_DATAREGION_6BCDBFA1 =
      "数据目录结构不规范。数据库目录 %s 下应包含 dataRegion 目录";
  public static final String MISC_EXCEPTION_IRREGULAR_DATA_DIR_STRUCTURE_THERE_SHOULD_BE_TIMEINTERVAL_8D074700 =
      "数据目录结构不规范。数据库目录 %s 下应包含 timeInterval 目录";
  public static final String MISC_EXCEPTION_IRREGULAR_DATA_DIR_STRUCTURE_THERE_SHOULD_BE_TSFILES_UNDER_6381FDA5 =
      "数据目录结构不规范。timeInterval 目录 %s 下应包含 TsFile";
  public static final String MISC_EXCEPTION_USER_S_DOES_NOT_EXIST_0CE725D8 = "用户 %s 不存在";
  public static final String MISC_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_AE9DBDC0 =
      "数据类型不一致，输入：%s，注册类型：%s";
  public static final String MISC_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_BECAUSE_50C4BF31 =
      "数据类型不一致，输入：%s，注册类型：%s，原因：%s";
  public static final String MISC_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =
      "客户端转换数据时不支持数据类型 %s";
  public static final String MISC_EXCEPTION_DEVICEID_SHOULD_NOT_BE_EMPTY_IN_GETTTL_METHOD_IN_TIMEFILTERFORDEVICETTL_8A501A45 =
      "TimeFilterForDeviceTTL 的 getTTL 方法中 deviceID 不能为空";
  public static final String MISC_EXCEPTION_FAILED_TO_DECOMPRESS_COMPRESSEDBUFFER_56398D3E =
      "解压 compressedBuffer 失败";
  public static final String MISC_EXCEPTION_ENCODING_S_DOES_NOT_SUPPORT_S_58301155 =
      "encoding %s 不支持 %s";
  public static final String MISC_EXCEPTION_STORAGEENGINE_FAILED_TO_STOP_BECAUSE_OF_S_84D26574 =
      "StorageEngine 停止失败，原因：%s。";
  public static final String MISC_EXCEPTION_THE_FILE_LENGTH_S_IS_NOT_EQUAL_TO_THE_OFFSET_S_73905F07 =
      "文件长度 %s 不等于 offset %s";
  public static final String MISC_EXCEPTION_THE_REMOVE_DATANODE_SCRIPT_HAS_BEEN_DEPRECATED_PLEASE_CONNECT_F91DF360 =
      "remove-datanode 脚本已废弃。请连接 CLI 并使用 SQL：remove datanode [datanode_id]。";
  public static final String MISC_EXCEPTION_CANNOT_PULL_SYSTEM_CONFIGURATIONS_FROM_CONFIGNODE_LEADER_BAD295DC =
      "无法从 ConfigNode-leader 拉取系统配置。请检查 iotdb-system.properties 中的 dn_seed_config_node 是否正确或存活。";
  public static final String MISC_EXCEPTION_CANNOT_REGISTER_INTO_THE_CLUSTER_PLEASE_CHECK_WHETHER_THE_D8B29F58 =
      "无法注册到集群。请检查 iotdb-system.properties 中的 dn_seed_config_node 是否正确或存活。";
  public static final String MISC_EXCEPTION_CANNOT_SEND_RESTART_DATANODE_REQUEST_TO_CONFIGNODE_LEADER_368BE214 =
      "无法向 ConfigNode-leader 发送重启 DataNode 请求。请检查 iotdb-system.properties 中的 dn_seed_config_node 是否正确或存活。";
  public static final String MISC_EXCEPTION_FAIL_TO_GET_SG_OF_THIS_TSFILE_WHILE_PARSING_THE_FILE_PATH_9EADADE1 =
      "解析文件路径时无法获取该 TsFile 的 sg。";
  public static final String MISC_EXCEPTION_CONFIGURING_THE_DATA_DIRECTORIES_AS_CROSS_DISK_DIRECTORIES_FC0A3875 =
      "RatisConsensus 下不支持将 data directories 配置为跨磁盘目录（后续版本将支持）。";
  public static final String MISC_EXCEPTION_CONFIGURING_THE_WALMODE_AS_DISABLE_IS_NOT_SUPPORTED_UNDER_49298819 =
      "IoTConsensus 和 IoTConsensusV2 stream mode 下不支持将 WALMode 配置为 disable";
  public static final String MISC_EXCEPTION_ENCRYPTTYPE_IS_NOT_UNENCRYPTED_BUT_USER_ENCRYPT_TOKEN_IS_F828C20B =
      "encryptType 不是 UNENCRYPTED，但未设置 user_encrypt_token。请在环境变量中设置。";
  public static final String MISC_EXCEPTION_USER_ENCRYPT_TOKEN_HINT_SHOULD_NOT_INCLUDE_USER_ENCRYPT_50531D40 =
      "user_encrypt_token_hint 不应包含 user_encrypt_token，请检查环境变量。";
  public static final String MISC_EXCEPTION_USER_ENCRYPT_TOKEN_HINT_SHOULD_NOT_INCLUDE_THE_REVERSE_OF_39B2D35C =
      "user_encrypt_token_hint 不应包含 user_encrypt_token 的反转字符串，请检查环境变量。";
  public static final String MISC_EXCEPTION_RESTART_SYSTEM_AFTER_NOT_STORING_KEY_BUT_USER_ENCRYPT_TOKEN_61CCF9A2 =
      "未存储密钥后重启系统，但未设置 user_encrypt_token。请在重启前通过环境变量设置。token hint 信息：%s";
  public static final String MISC_EXCEPTION_CHANGING_ENCRYPT_TYPE_OR_KEY_FOR_TSFILE_ENCRYPTION_AFTER_0668F74E =
      "首次启动后不允许修改 TsFile 加密类型或密钥。token hint 信息：%s";
  public static final String MISC_EXCEPTION_FAIL_TO_RELOAD_CONFIG_FILE_S_BECAUSE_S_93CCAB8D =
      "无法重新加载配置文件 %s，原因：%s";
  public static final String MISC_EXCEPTION_EACH_SUBSECTION_OF_CONFIGURATION_ITEM_UDF_READER_TRANSFORMER_97CA8962 =
      "配置项 udf_reader_transformer_collector_memory_proportion 的每个子项都应为整数，当前为 %s";
  public static final String MISC_EXCEPTION_EACH_SUBSECTION_OF_CONFIGURATION_ITEM_CHUNKMETA_CHUNK_TIMESERIESMETA_77A43CE2 =
      "配置项 chunkmeta_chunk_timeseriesmeta_free_memory_proportion 的每个子项都应为整数，当前为 %s";
  public static final String MISC_EXCEPTION_ILLEGAL_DEFAULTDATABASELEVEL_D_SHOULD_1_03088B38 =
      "非法的 defaultDatabaseLevel：%d，应 >= 1";
  public static final String MISC_EXCEPTION_LOADTSFILESPILTPARTITIONMAXSIZE_SHOULD_BE_GREATER_THAN_OR_95B4DB23 =
      "loadTsFileSpiltPartitionMaxSize 应大于或等于 0";
  public static final String MISC_EXCEPTION_REMOVING_IS_ONLY_ALLOWED_IN_AN_ENVIRONMENT_WHERE_THE_DATANODE_5A3E1FEA =
      "只有在 DataNode 已成功启动的环境中才允许移除。请检查它是否已在 ConfigNode 上移除，或是否误删了 system.properties 文件。";
  public static final String MISC_EXCEPTION_STATEMENTID_SDOESN_T_EXIST_IN_THIS_SESSION_S_BD5B4733 =
      "StatementId：%s 在会话 %s 中不存在";
  public static final String MISC_EXCEPTION_INTERNALCLIENTSESSION_SHOULD_NEVER_CALL_PREPARE_STATEMENT_CCAB3CDC =
      "InternalClientSession 不应调用 PREPARE statement 方法。";
  public static final String MISC_EXCEPTION_STATEMENTID_S_DOESN_T_EXIST_IN_THIS_SESSION_S_4AA25E49 =
      "StatementId：%s 在会话 %s 中不存在";
  public static final String MISC_EXCEPTION_MQTT_CLIENT_SESSION_DOES_NOT_SUPPORT_PREPARE_STATEMENT_B42FBC65 =
      "MQTT client session 不支持 PREPARE statement。";
  public static final String MISC_EXCEPTION_DATAREGIONLIST_SIZE_SHOULD_ONLY_BE_1_NOW_CURRENT_SIZE_IS_282E453C =
      "dataRegionList.size() 当前只能为 1，当前大小为 %s";
  public static final String MISC_EXCEPTION_PARAMETER_WINDOWSIZE_D_SHOULD_BE_POSITIVE_D95CBF33 =
      "参数 windowSize(%d) 应为正数。";
  public static final String MISC_EXCEPTION_PARAMETER_SLIDINGSTEP_D_SHOULD_BE_POSITIVE_C0C25C2C =
      "参数 slidingStep(%d) 应为正数。";
  public static final String MISC_EXCEPTION_PARAMETER_TIMEINTERVAL_D_SHOULD_BE_POSITIVE_53A6CE3B =
      "参数 timeInterval(%d) 应为正数。";
  public static final String MISC_EXCEPTION_FAILED_TO_REFLECT_TRIGGER_INSTANCE_WITH_CLASSNAME_S_BECAUSE_C0CC44E2 =
      "无法反射 className(%s) 对应的 trigger instance，原因：%s";
  public static final String MISC_EXCEPTION_S_IS_NOT_ALLOWED_ONLY_SUPPORT_S_862A4D86 =
      "%s 不允许，仅支持 %s";
  public static final String MISC_EXCEPTION_S_IS_NOT_ALLOWED_ONLY_SUPPORT_S_1B06E0B7 =
      " %s 不允许，仅支持 %s";
  public static final String PARAMETER_CANNOT_BE_MODIFIED_AFTER_FIRST_STARTUP_FMT =
      "%s 首次启动后不能修改";
  public static final String UNSUPPORTED_INVOCATION_BY_DATANODE =
      "DataNode 不支持调用此方法";
  public static final String UNSUPPORTED_INVOCATION_BY_DATANODE_USE_SUBMIT_LOAD_CONFIGURATION_TASK =
      "DataNode 不支持调用此方法，请改用 submitLoadConfigurationTask";
  public static final String INVALID_REQUEST_FROM_CONFIG_NODE_FMT =
      "来自 ConfigNode 的无效请求 %s。";
  public static final String INVALID_METHOD_NAME_FMT = "无效的方法名：'%s'";
  public static final String DATA_PARTITION_TABLE_GENERATION_IN_PROGRESS_FMT =
      "DataPartitionTable 生成中：%.1f%%";
  public static final String DATA_PARTITION_TABLE_GENERATION_WITH_TASK_ID_IN_PROGRESS_FMT =
      "任务 ID 为 %s 的 DataPartitionTable 生成中：%.1f%%";
  public static final String DATA_PARTITION_TABLE_GENERATION_COMPLETED_STATS =
      "DataPartitionTable 生成成功完成。已处理：{}，失败：{}";
  public static final String DATA_PARTITION_TABLE_GENERATION_ALREADY_IN_PROGRESS_FMT =
      "DataPartitionTable 生成任务已在进行中：%.1f%%";
  public static final String DATA_PARTITION_TABLE_GENERATION_COMPLETED_SUCCESSFULLY =
      "DataPartitionTable 生成成功完成";
  public static final String DATA_PARTITION_TABLE_GENERATION_FAILED_FMT =
      "DataPartitionTable 生成失败：%s";
  public static final String BATCH_PROCESS_FAILED_FMT = "批处理失败：%s";
  public static final String STORAGE_GROUP_NOT_READY_FMT =
      "存储组 %s 可能尚未就绪，请稍后重试";
  public static final String BAD_NODE_URL_FORMAT_FMT =
      "节点 url %s 格式错误，应为 {IP/DomainName}:{Port}";
  public static final String INCONSISTENT_DATA_TYPES_FMT =
      "数据类型不一致，已有数据类型：%s，传入数据类型：%s";
  public static final String CREATE_SYSTEM_DIRECTORY_FAILED = "创建系统目录失败！";
  public static final String DATABASE_READ_ONLY_NON_QUERY =
      "数据库为只读状态，当前不接受非查询操作";
  public static final String AINODE_CONNECTION_FAILED_FMT =
      "连接 AINode 失败，原因：[%s]，请检查 AINode 的状态。";
  public static final String AINODE_CLIENT_REFRESH_FROM_CONFIG_NODE_FAILED =
      "[AINodeClient] 从 ConfigNode 刷新失败：{}";

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_DB_34B9E556 = " 所属数据库：";
  public static final String MESSAGE_MESSAGE_CECB319D = " 在 ";
  public static final String MESSAGE_MESSAGE_57992626 = " 在 ";
  public static final String MESSAGE_REGION_ARG_STATE_ARG_EXECUTED_SUCCEED_F78C5849 =
      "Region：%s，状态：%s，执行成功";
  public static final String MESSAGE_FAILED_CREATE_EXTERNAL_SERVICE_ARG_IT_ALREADY_EXISTS_50AE69DF =
      "创建 External Service %s 失败，该服务已存在！";
  public static final String MESSAGE_FAILED_START_EXTERNAL_SERVICE_ARG_BECAUSE_IT_NOT_EXISTED_29D6D3F2 =
      "启动 External Service %s 失败，原因：该服务不存在！";
  public static final String MESSAGE_FAILED_START_EXTERNAL_SERVICE_ARG_BECAUSE_ITS_INSTANCE_CAN_NOT_F5A6C198 =
      "启动 External Service %s 失败，原因：其实例无法成功构造。异常：%s";
  public static final String MESSAGE_FAILED_STOP_EXTERNAL_SERVICE_ARG_BECAUSE_IT_NOT_EXISTED_3CCCDA42 =
      "停止 External Service %s 失败，原因：该服务不存在！";
  public static final String MESSAGE_FAILED_DROP_EXTERNAL_SERVICE_ARG_BECAUSE_IT_NOT_EXISTED_FDD11F17 =
      "删除 External Service %s 失败，原因：该服务不存在！";
  public static final String MESSAGE_FAILED_DROP_EXTERNAL_SERVICE_ARG_BECAUSE_IT_BUILT_59858F3F =
      "删除 External Service %s 失败，原因：该服务是内置服务！";
  public static final String MESSAGE_FAILED_DROP_EXTERNAL_SERVICE_ARG_BECAUSE_IT_RUNNING_89B38F80 =
      "删除 External Service %s 失败，原因：该服务正在运行！";
  public static final String MESSAGE_CHANGEREGIONLEADER_SUCCESSFULLY_CHANGE_LEADER_REGIONGROUP_462797B4 =
      "[ChangeRegionLeader] 成功切换 RegionGroup 的 leader：";
  public static final String MESSAGE_MESSAGE_3501C7E6 = " 到 ";
  public static final String MESSAGE_EXECUTEFASTLASTDATAQUERYFORONEPREFIXPATH_DOS_NOT_SUPPORT_WILDCARDS_8E8F44F5 = "\"executeFastLastDataQueryForOnePrefixPath\" 不支持通配符。";
  public static final String MESSAGE_TEMPLATE_NULL_TRYING_ACTIVATE_TEMPLATE_MAY_TEMPLATE_BEING_UNSET_1CE92779 =
      "尝试激活模板时模板为空，可能正在取消设置该模板。";
  public static final String MESSAGE_DOT_9D9B854A = ".";
  public static final String MESSAGE_ERROR_OCCURRED_WHILE_PARSING_SQL_TO_PHYSICAL_PLAN_COLON_5C9F2C59 =
      "将 SQL 解析为物理计划时发生错误：";
  public static final String MESSAGE_ERROR_OCCURRED_IN_READ_PROCESS_COLON_CD184195 =
      "读取过程中发生错误：";
  public static final String MESSAGE_THE_READ_STATEMENT_IS_NOT_ALLOWED_IN_BATCH_COLON_D6A3D5EB =
      "批处理中不允许读取语句：";
  public static final String MESSAGE_LEFT_BRACKET_ARG_RIGHT_BRACKET_EXCEPTION_OCCURRED_COLON_ARG_FAILED_DOT_909D8FFA =
      "[%s] 发生异常：%s 失败。";
  public static final String EXCEPTION_SUFFIX_IS_NULL_6CC6B965 = "后缀不能为空";
  public static final String EXCEPTION_RUNTIMESTATE_D4D018BA = "runtimeState";
  public static final String EXCEPTION_STATEMENTNAME_IS_NULL_C03BB8D4 = "statementName 不能为空";
  public static final String EXCEPTION_SQL_IS_NULL_BEDB2B7A = "sql 不能为空";
  public static final String MESSAGE_CONFIGNODE_LEADER_ARG_IS_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_AAB5F962 = "ConfigNode leader {} 正在预热，暂未对 DataNode {} 提供服务，将等待并重试。";
  public static final String MESSAGE_CONFIGNODE_LEADER_ARG_IS_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_REASON_ARG_3A2A4163 =
      "ConfigNode leader {} 正在预热，暂未对 DataNode {} 提供服务，将等待并重试。原因：{}";
  public static final String EXCEPTION_TRUST_STORE_PATH_MUST_BE_SET_WHEN_THRIFT_SSL_CLIENT_AUTH_IS_TRUE_36016171 = "当 thrift_ssl_client_auth 为 true 时，必须设置 trust_store_path";
  public static final String EXCEPTION_CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_CURRENT_VALUE_IS_F9A1BEC4 = "continuous_query_min_every_interval_in_ms 必须大于 0，但当前值为 ";
  public static final String EXCEPTION_UNKNOWN_READ_CONSISTENCY_LEVEL_ARG_PLEASE_SET_TO_STRONG_OR_WEAK_8CF29949 = "未知的 read_consistency_level：%s，请设置为 \"strong\" 或 \"weak\"";
  public static final String MESSAGE_INITIAL_ALLOCATEMEMORYFORAUTORESIZINGBUFFER_ARG_A0DB6DA0 = "初始 allocateMemoryForAutoResizingBuffer = {}";

}
