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
      "deletePeer 从共识组 %s成功";
  public static final String DELETE_REGION_ERROR =
      "deleteRegion %s 错误，%s";
  public static final String DELETE_REGION_SUCCEED =
      "deleteRegion %s 成功";
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
  public static final String CHANGE_REGION_LEADER = "[变更 Region Leader] {}";
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
      "测量值已设置属性或标签，但未找到快照文件";

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
  public static final String FAILED_TO_REPAIR_TSFILE_RESOURCE = "错误: 修复 TsFileResource 失败: {}";
  public static final String TOTAL_TIME_TAKEN = "总耗时: {} 毫秒，TsFile 资源总数: {}，设置 isGeneratedByPipe 的资源数: {}，重置 progressIndex 的资源数: {}，已更改的资源数: {}";

  // ---------------------------------------------------------------------------
  // tools – DelayAnalyzer
  // ---------------------------------------------------------------------------
  public static final String DELAY_ANALYZER_RESET = "[延迟分析器] 延迟分析器已重置";

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
      "加密魔术字符串不匹配";

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
      "[延迟分析器] 设置 delay_analyzer_window_size 为 {}";
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

  public static final String FROM_CONFIG_NODE = " 从 ConfigNode 获取失败。";
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
}
