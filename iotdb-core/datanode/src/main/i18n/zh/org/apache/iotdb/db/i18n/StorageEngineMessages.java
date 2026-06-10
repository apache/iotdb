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

public final class StorageEngineMessages {

    public static final String IS_NOT_SUPPORTED =
      " 不受支持";

  private StorageEngineMessages() {}

  // ======================== StorageEngine ========================

  public static final String FAIL_TO_RECOVER_WAL = "WAL 恢复失败。";
  public static final String STORAGE_ENGINE_FAILED_TO_SET_UP = "存储引擎启动失败。";
  public static final String SEQ_MEMTABLE_FLUSH_CHECK_THREAD_STARTED = "顺序 memtable 定时 flush 检查线程启动成功。";
  public static final String UNSEQ_MEMTABLE_FLUSH_CHECK_THREAD_STARTED = "乱序 memtable 定时 flush 检查线程启动成功。";
  public static final String STILL_NOT_EXIT_AFTER_30S = "{} 在 30 秒后仍未退出";
  public static final String START_CLOSING_ALL_DB_PROCESSOR = "开始关闭所有数据库处理器";
  public static final String START_FORCE_CLOSING_ALL_DB_PROCESSOR = "开始强制关闭所有数据库处理器";
  public static final String SYSTEM_READ_ONLY_NO_MERGE = "当前系统为只读模式，不支持合并操作";
  public static final String START_REPAIR_DATA = "开始修复数据";
  public static final String STOP_REPAIR_DATA = "停止修复数据";
  public static final String REMOVING_DATA_REGION = "正在移除 DataRegion {}";
  public static final String FAILED_TO_DELETE_SNAPSHOT_DIR = "删除快照目录 {} 失败";
  public static final String REMOVED_DATA_REGION = "已移除 DataRegion {}";
  public static final String EXECUTE_LOAD_COMMAND_ERROR = "执行加载命令 {} 出错。";
  public static final String START_REBOOTING_ALL_TIMED_SERVICE = "开始重启所有定时服务。";
  public static final String STOP_ALL_TIMED_SERVICE_AND_RESTART = "所有定时服务已成功停止，正在重启。";
  public static final String REBOOT_ALL_TIMED_SERVICE_SUCCESSFULLY = "所有定时服务重启成功";
  public static final String FAILED_TO_DELETE = "删除失败: {} -> {}";
  public static final String FAILED_TO_CHECK_OBJECT_FILES = "检查对象文件失败: {}";

  // ======================== Buffer Cache ========================

  public static final String BLOOM_FILTER_CACHE_SIZE = "BloomFilterCache 大小 = {}";
  public static final String GET_BLOOM_FILTER_FROM_CACHE = "从缓存中获取布隆过滤器，文件路径: {}";
  public static final String STOP_SERVICE = "{}: 正在停止 {}...";
  public static final String CHUNK_CACHE_SIZE = "ChunkCache 大小 = {}";
  public static final String GET_CHUNK_FROM_CACHE = "从缓存中获取 Chunk，键为: {}";
  public static final String CACHE_MISS_IN_FILE = "缓存未命中: {}.{}，文件: {}";
  public static final String DEVICE_ALL_SENSORS = "设备: {}，所有传感器: {}";
  public static final String TS_METADATA_FILTERED_BY_BLOOM_FILTER = "时间序列元数据 {} 已被布隆过滤器过滤！";
  public static final String FILE_NO_SUCH_TIME_SERIES = "文件中不包含该时间序列 {}。";

  // ======================== Resource Control - Disk ========================

  public static final String FAILED_TO_DEREGISTER_FILE_LOCK = "注销文件锁失败，原因: {}";
  public static final String ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY = "所有目录已满，切换系统为只读模式。";
  public static final String FAILED_TO_PROCESS_FOLDER = "处理目录失败 '";
  public static final String FAIL_TO_GET_CANONICAL_PATH = "获取数据目录 {} 的规范路径失败";
  public static final String ALL_DISKS_OF_TIER_FULL = "第 {} 层的所有磁盘已满。";
  public static final String FOLDERS_RESET_SUCCESSFULLY = "目录重置成功，耗时 {} 毫秒。";
  public static final String FOLDER_NOT_EXIST_CREATE_IT = "目录 {} 不存在，正在创建";
  public static final String FAILED_TO_STATISTIC_SIZE = "统计 {} 的大小失败，原因";
  public static final String DISK_SPACE_INSUFFICIENT_READ_ONLY = "磁盘空间不足，切换系统为只读模式";
  public static final String CANNOT_CALC_OCCUPIED_SPACE = "无法计算路径 {} 的已用空间。";

  // ======================== Resource Control - Memory ========================

  public static final String WAITING_FOR_THREAD_POOL_SHUTDOWN = "正在等待 {} 线程池关闭。";
  public static final String THREAD_POOL_NOT_EXIT_AFTER_MS = "{} 线程池在 {} 毫秒后仍未退出。";
  public static final String INTERRUPTED_WAITING_THREAD_POOL_EXIT = "等待 {} 线程池退出时被中断。 ";
  public static final String BUFFERED_ARRAY_SIZE_THRESHOLD = "BufferedArraySizeThreshold 为 {}";
  public static final String CURRENT_SG_COST = "当前存储组内存开销为 {}";
  public static final String FORCE_DEGRADE_TSFILE_RESOURCE = "强制降级 TsFile 资源 {}";
  public static final String CANNOT_DEGRADE_TIME_INDEX_ALL_FILE_LEVEL = "无法继续降级时间索引，所有时间索引已为文件级别。";
  public static final String DEGRADE_TSFILE_RESOURCE = "降级 TsFile 资源 {}";

  // ======================== Resource Control - Quotas ========================

  public static final String SPACE_QUOTA_RESTORE_SUCCEEDED = "空间配额限制恢复成功，限制: {}。";
  public static final String SPACE_QUOTA_RESTORE_FAILED = "空间配额限制恢复失败，限制: {}。";
  public static final String THROTTLE_QUOTA_RESTORED_SUCCESSFULLY = "流量配额限制恢复成功。 ";
  public static final String THROTTLE_QUOTA_RESTORED_FAILED = "流量配额限制恢复失败。 ";
  public static final String INVALID_STATEMENT_TYPE = "无效的语句类型: ";

  // ======================== DataRegion ========================

  public static final String CREATE_DB_SYSTEM_DIR_FAILED = "创建数据库系统目录 {} 失败";
  public static final String CREATE_DATA_REGION_DIR_FAILED = "创建 DataRegion 目录 {} 失败";
  public static final String IS_NOT_A_DIRECTORY = "{} 不是目录。";
  public static final String FAIL_TO_CLOSE_TSFILE_WHEN_RECOVERING = "恢复过程中关闭 TsFile {} 失败";
  public static final String FAIL_TO_RECOVER_SEALED_TSFILE_SKIP = "恢复已封闭的 TsFile {} 失败，跳过该文件。";
  public static final String DATA_INCONSISTENT_NOT_TRIGGER_TWICE = "数据不一致异常不应被触发两次";
  public static final String INSERT_TO_TSFILE_PROCESSOR_REJECTED = "写入 TsFileProcessor 被拒绝, {}";
  public static final String INSERT_TO_TSFILE_PROCESSOR_ERROR = "写入 TsFileProcessor 出错 ";
  public static final String IOEXCEPTION_CREATING_TSFILE_PROCESSOR_RETRY = "创建 TsFileProcessor 时遇到 IOException，正在重试";
  public static final String CANNOT_CLOSE_TSFILE_RESOURCE = "无法关闭 TsFileResource {}";
  public static final String CANNOT_REMOVE_MOD_FILE = "无法删除修改文件 {}";
  public static final String FAIL_TO_DELETE_DATA_REGION_FOLDER = "删除 DataRegion 目录 {} 失败";
  public static final String FAIL_TO_DELETE_DATA_REGION_OBJECT_FOLDER = "删除 DataRegion 对象目录 {} 失败";
  public static final String FILES_WERE_CLOSED = "{} 个文件已关闭";
  public static final String FAIL_TO_LOG_DELETE_TO_WAL = "写入删除日志到 WAL 失败。";
  public static final String DELETION_EXECUTING_TABLE_DELETION = "[Deletion] 正在执行表删除 {}";
  public static final String DELETION_UNSEALED_FILES_FOR = "[Deletion] {} 的未封闭文件: {}";
  public static final String DELETION_SEALED_FILES_FOR = "[Deletion] {} 的已封闭文件: {}";
  public static final String WRITING_NO_FILE_RELATED_DELETION_TO_WAL = "将无关文件的删除操作写入 WAL {}";
  public static final String DELETION_SKIPPED_FILE_TIME = "[Deletion] {} 跳过 {}，文件时间 {}";
  public static final String EXPECT_IS_ACTUAL_IS = "期望值为 {}，实际值为 {}";
  public static final String DELETION_DOES_NOT_INVOLVE_ANY_FILE = "[Deletion] 删除操作 {} 不涉及任何文件";
  public static final String FAIL_TO_WRITE_MOD_ENTRY_TO_FILES = "将修改条目 {} 写入文件失败";
  public static final String REMOVE_TSFILE_DIRECTLY_WHEN_DELETE_DATA = "删除数据时直接移除 TsFile {}";
  public static final String MEET_ERROR_IN_COMPACTION_SCHEDULE = "compaction 调度过程中遇到错误。";
  public static final String MEET_ERROR_IN_TTL_CHECK = "TTL 检查过程中遇到错误。";
  public static final String FAILED_TO_EXECUTE_OBJECT_TTL_CHECK = "执行对象 TTL 检查失败";
  public static final String MEET_ERROR_IN_INSERTION_COMPACTION_SCHEDULE = "插入 compaction 调度过程中遇到错误。";
  public static final String EXCEPTION_MOVE_NEW_TSFILE_IN_SETTLING = "在 settle 过程中移动新 TsFile 时发生异常";
  public static final String TSFILE_LOADED_IN_UNSEQ_LIST = "TsFile {} 已成功加载到乱序列表中。";
  public static final String CANNOT_CLOSE_LAST_READER_AFTER_LOAD = "加载 TsFile {} 后无法关闭上一个读取器";
  public static final String FILE_ALREADY_LOADED_IN_UNSEQ_LIST = "文件 {} 已加载到乱序列表中";
  public static final String CANNOT_DELETE_LOCAL_MOD_FILE = "无法删除本地修改文件 {}";
  public static final String REMOVE_TSFILE_SUCCESSFULLY = "成功移除 TsFile {}。";
  public static final String THREAD_INTERRUPTED_WAITING_COMPACTION = "等待 compaction 完成时线程被中断";
  public static final String PARTIAL_FAILED_INSERTING_ROWS_ONE_DEVICE = "单设备部分行插入失败";
  public static final String PARTIAL_FAILED_INSERTING_ROWS = "部分行插入失败";
  public static final String REJECTED_INSERTING_MULTI_TABLETS = "多 tablet 插入被拒绝";
  public static final String PARTIAL_FAILED_INSERTING_MULTI_TABLETS = "多 tablet 部分插入失败";
  public static final String INTERRUPTED_WAITING_DATA_REGION_DELETED = "等待 DataRegion 删除时被中断。";
  public static final String FAILED_TO_RENAME = "重命名 {} 为 {} 失败，";

  // ======================== Compaction ========================

  public static final String SELECTOR_NOT_FOR_INNER_SPACE = "此选择器不能用于选择内部空间任务";
  public static final String SELECTOR_NOT_FOR_CROSS_SPACE = "此选择器不能用于选择跨空间任务";
  public static final String SELECTOR_NOT_FOR_SETTLE = "此选择器不能用于选择 settle 任务";
  public static final String UNSEQ_FILE_NO_OVERLAP_WITH_SEQ = "乱序文件 {} 与任何顺序文件都不重叠。";
  public static final String CANNOT_SELECT_FILE_FOR_CROSS_COMPACTION = "{} 无法为跨空间 compaction 选择文件";
  public static final String CURRENT_FILE_SIZE = "当前文件为 {}，大小为 {}";
  public static final String EXCEPTION_SELECTING_FILES = "选择文件时发生异常";
  public static final String UNIMPLEMENTED = "未实现";
  public static final String ILLEGAL_CROSS_COMPACTION_SELECTOR = "非法的跨空间 compaction 选择器 ";
  public static final String ILLEGAL_COMPACTION_SELECTOR = "非法的 compaction 选择器 ";
  public static final String COMPACTION_SCHEDULE_TASK_MANAGER_STARTED = "compaction 调度任务管理器已启动。";
  public static final String WAITING_COMPACTION_SCHEDULE_POOL_SHUTDOWN = "正在等待 compaction 调度任务线程池关闭";
  public static final String COMPACTION_SCHEDULE_MANAGER_WAIT_TO_STOP = "CompactionScheduleTaskManager 已等待 {} 秒以停止";
  public static final String COMPACTION_SCHEDULE_TASK_MANAGER_STOPPED = "CompactionScheduleTaskManager 已停止";
  public static final String REPAIR_FAILED_RENAME_PROGRESS_FILE = "[RepairTaskManager] 重命名修复数据进度文件失败";
  public static final String REPAIR_SKIP_TASK_STOPPING = "[RepairTaskManager] 修复任务正在停止，跳过当前任务";
  public static final String REPAIR_SCAN_TASK_CANCELLED = "[RepairScheduler] 扫描任务已取消";
  public static final String REPAIR_ERROR_SCAN_TIME_PARTITION = "[RepairScheduler] 扫描时间分区文件时遇到错误";
  public static final String COMPACTION_TASK_MANAGER_STARTED = "compaction 任务管理器已启动。";
  public static final String WAITING_TASK_EXECUTION_POOL_SHUTDOWN = "正在等待任务执行线程池关闭";
  public static final String WAITING_TASK_EXECUTION_POOL_SHUTDOWN_MS = "正在等待任务执行线程池关闭，超时 {} 毫秒";
  public static final String INTERRUPTED_WAITING_ALL_TASK_FINISH = "等待所有任务完成时被中断";
  public static final String ALL_COMPACTION_TASK_FINISH = "所有 compaction 任务已完成";
  public static final String COMPACTION_MANAGER_WAIT_TO_STOP = "CompactionManager 已等待 {} 秒以停止";
  public static final String COMPACTION_MANAGER_STOPPED = "CompactionManager 已停止";
  public static final String COMPACTION_THREAD_POOL_CANNOT_CLOSE = "compaction 线程池在 {} 毫秒内无法关闭";
  public static final String TIMEOUT_WAITING_TASK_FUTURE = "等待任务结果超时";
  public static final String COMPACTION_THREAD_TERMINATES = "CompactionThread-{} 因中断而终止";
  public static final String EXCEPTION_EXECUTING_COMPACTION_TASK = "执行 compaction 任务时发生异常。{}";
  public static final String TIMEOUT_GET_COMPACTION_TASK_SUMMARY = "尝试获取 compaction 任务摘要超时";
  public static final String TTL_CHECK_TASK_FAILED = "[TTLCheckTask-{}] 执行 TTL 检查失败";
  public static final String ERROR_CREATING_SETTLE_LOG = "创建 settle 日志时出错，文件路径: {}";
  public static final String WRITE_SETTLE_LOG_FAILED = "写入 settle 日志文件失败，日志文件: {}";
  public static final String CLOSE_UPGRADE_LOG_FAILED = "关闭升级日志文件失败，日志文件: {}";
  public static final String FIND_SETTLED_FILE = "找到 {} 的 settle 文件";
  public static final String GENERATE_SETTLED_FILE = "为 {} 生成 settle 文件";
  public static final String ALL_FILES_SETTLED_SUCCESSFULLY = "所有文件 settle 成功！ ";
  public static final String SUB_COMPACTION_TASK_MEET_ERRORS = "[Compaction] 子 compaction 任务遇到错误 ";
  public static final String TASK_TYPE_NO_TMP_FILE_SUFFIX = "当前任务类型 {} 没有临时文件后缀。";
  public static final String CANNOT_GET_MOD_FILE = "无法获取 {} 的修改文件";
  public static final String COMPACTION_START_DELETE_REAL_FILE = "{} [Compaction] compaction 开始删除实际文件 ";
  public static final String COMPACTION_START_DELETE_SOURCE_MODS = "{} [Compaction] 开始删除源文件的修改记录";
  public static final String COMPACTION_DELETE_FILE = "[Compaction] 删除文件: {}";
  public static final String FAILED_TO_READ_FILE_ATTRIBUTES = "读取文件属性失败: {}";
  public static final String FAILED_TO_CHECK_TABLE_DIR = "检查表目录失败: {}";
  public static final String REMOVE_OBJECT_FILE_SIZE = "移除对象文件 {}，大小为 {}(字节)";
  public static final String FAILED_TO_DELETE_EXPIRED_OBJECT_FILE = "删除过期对象文件失败: {}";
  public static final String SHOULD_CALL_EXACT_SUB_CLASS = "应调用具体的子类！";
  public static final String NO_NEXT_BLOCK = "没有下一个块";
  public static final String METHOD_NOT_SUPPORTED_FAST_CROSS_WRITER = "FastCrossCompactionWriter 不支持此方法";
  public static final String DEVICE_SHOULD_EXIST_IN_SEQ_FILE = "设备应存在于当前顺序文件中";
  public static final String METHOD_NOT_SUPPORTED_FAST_INNER_WRITER = "FastInnerCompactionWriter 不支持此方法";
  public static final String METHOD_NOT_SUPPORTED_READ_POINT_WRITER = "ReadPointInnerCompactionWriter 不支持此方法";
  public static final String UNKNOWN_DATA_TYPE = "未知的数据类型 ";
  public static final String FAILED_TO_DELETE_TARGET_FILE = "删除目标文件 %s 失败";
  public static final String SOURCE_FILES_CANNOT_BE_DELETED = "源文件无法成功删除";
  public static final String FAIL_TO_GET_TSFILE_NAME = "获取 {} 的 TsFile 名称失败";
  public static final String ERROR_ESTIMATE_INNER_COMPACTION_MEMORY = "估算内部 compaction 内存时遇到错误";
  public static final String CANNOT_RECOVER_INSERTION_CROSS_TASK = "无法恢复 InsertionCrossSpaceCompactionTask";
  public static final String FAILED_TO_REPAIR_FILE = "修复文件 {} 失败";
  public static final String FAILED_DELETE_FULLY_DIRTY_SOURCE = "删除完全脏的源文件失败。";
  public static final String RECOVER_MODS_FILE_ERROR = "列出文件时恢复修改文件出错: {}";
  public static final String UNKNOWN_COMPACTION_TASK_TYPE = "未知的 compaction 任务类型 {}";
  public static final String RECOVER_COMPACTION_ERROR = "恢复 compaction 出错";
  public static final String COMPACTION_RECOVER_FAILED = "{} [Compaction][Recover] 恢复 compaction 失败";
  public static final String MEET_ERROR_WHEN_READ_TSFILE = "读取 TsFile {} 时遇到错误";
  public static final String UNKNOWN_REPAIR_LOG_FORMAT = "未知的修复日志格式";
  public static final String REPAIR_START_CHECK_TSFILE = "[RepairScheduler] 开始检查 TsFile: {}";
  public static final String REPAIR_SKIPPED_BROKEN_FILE = "[RepairScheduler] {} 因损坏而被跳过";
  public static final String REPAIR_FAILED_CREATE_LOGGER = "[RepairScheduler] 创建修复日志器失败";
  public static final String REPAIR_FAILED_CLOSE_LOGGER = "[RepairScheduler] 关闭修复日志器失败";
  public static final String REPAIR_WAIT_COMPACTION_FINISH = "[RepairScheduler] 等待 compaction 调度任务完成";
  public static final String REPAIR_WAIT_ALL_RUNNING_TASK_FINISH = "[RepairScheduler] 等待所有正在运行的 compaction 任务完成";
  public static final String REPAIR_TASK_FINISHED = "[RepairScheduler] 修复任务已完成";
  public static final String REPAIR_SCHEDULE_TASK_ERROR = "[RepairScheduler] 执行修复调度任务时遇到错误";
  public static final String REPAIR_FAILED_INIT_SCHEDULE_TASK = "[RepairScheduler] 初始化修复调度任务失败";
  public static final String REPAIR_ALL_PARTITIONS_DONE_SKIP = "[RepairScheduler] 所有时间分区已修复，跳过修复任务";
  public static final String END_MUST_GREATER_THAN_START = "结束值必须大于起始值";
  public static final String DATA_DIRS_MUST_NOT_BE_EMPTY = "data_dirs 不能为空";
  public static final String DOES_NOT_EXIST = "{} 不存在。";
  public static final String CHECK_FAILED = "检查 {} 失败。";
  public static final String FAILED_TO_DEAL_WITH = "处理 {} 失败";
  public static final String ERROR_OCCURRED = "发生错误";

  // ======================== MemTable ========================

  public static final String CANNOT_DESERIALIZE_OLD_MEMTABLE_SNAPSHOT = "无法反序列化旧版 MemTable 快照";
  public static final String DEVICE_ID_LENGTH_SHOULD_BE_POSITIVE = "DeviceID 的长度应大于 0。";
  public static final String CREATE_NEW_TSFILE_PROCESSOR = "创建新的 TsFile 处理器 {}";
  public static final String REOPEN_TSFILE_PROCESSOR = "重新打开 TsFile 处理器 {}";
  public static final String EXCEPTION_DURING_WAL_FLUSH = "WAL flush 过程中发生异常";
  public static final String DELETION_IN_FLUSHING_MEMTABLE = "[Deletion] 在 flushing 中的 memtable 中执行删除 {}";
  public static final String START_WAIT_UNTIL_FILE_CLOSED = "开始等待文件 {} 关闭";
  public static final String FILE_CLOSED_SYNCHRONOUSLY = "文件 {} 已同步关闭";
  public static final String DATAREGION_TSFILE_ERROR = "{}: {}";
  public static final String DELETION_WRITTEN_WHEN_FLUSH = "[Deletion] flush memtable 时写入删除: {}";
  public static final String FSYNC_MEMTABLE_TO_DISK_ERROR = "将 memtable 数据同步到磁盘出错，";
  public static final String FLUSHING_MEMTABLES_CLEAR = "{} 的 flushing memtable 已清空";
  public static final String START_TO_END_FILE = "开始结束文件 {}";
  public static final String ENDED_FILE = "已结束文件 {}";
  public static final String START_TO_END_EMPTY_FILE = "开始结束空文件 {}";
  public static final String TIME_CHUNK_METADATA_SHOULD_NOT_BE_EMPTY = "对齐设备中的 TimeChunkMetadata 不应为空";
  public static final String WRITABLE_MEM_CHUNK_UNSUPPORTED_TYPE = "WritableMemChunk 不支持数据类型: {}";

  // ======================== Modification ========================

  public static final String UNRECOGNIZED_PREDICATE_TYPE = "无法识别的谓词类型: ";
  public static final String UNSUPPORTED_MOD_TYPE = "不支持的修改类型: ";
  public static final String UNKNOWN_MOD_TYPE = "未知的 ModType: ";
  public static final String CANNOT_CLOSE_MOD_FILE_INPUT_STREAM = "无法关闭 {} 的修改文件输入流";
  public static final String CANNOT_READ_MOD_FILE_INPUT_STREAM = "无法读取 {} 的修改文件输入流";
  public static final String COMPACT_MODS_FILE_EXCEPTION = "压缩 {} 的修改文件时发生异常";
  public static final String SETTLE_SUCCESSFUL = "{} settle 成功";
  public static final String REMOVE_ORIGIN_OR_RENAME_MODS_ERROR = "删除原始文件或重命名新修改文件出错。";
  public static final String DELETE_MODIFICATION_FILE_FAILED = "删除修改文件 {} 失败。";
  public static final String CANNOT_CREATE_HARDLINK = "无法为 {} 创建硬链接";
  public static final String ERROR_READING_MODIFICATIONS = "读取修改记录时发生错误";
  public static final String ERROR_DECODE_LINE_TO_MODIFICATION = "将行 [{}] 解码为修改记录时发生错误";
  public static final String MODIFICATIONS_WILL_BE_TRUNCATED = "修改记录 [{}] 将被截断至大小 {}。";
  public static final String LAST_LINE_OF_MODS_INCOMPLETE = "修改文件的最后一行不完整，将被截断";
  public static final String UNKNOWN_MODIFICATION_TYPE = "未知的修改类型: ";
  public static final String INCORRECT_DELETION_FIELDS_NUMBER = "删除字段数量不正确: ";
  public static final String INVALID_TIMESTAMP = "无效的时间戳: ";
  public static final String INVALID_SERIES_PATH = "无效的序列路径: ";

  // ======================== WAL ========================

  public static final String START_REBOOTING_WAL_DELETE_THREAD = "开始重启 WAL 删除线程。";
  public static final String STOP_WAL_DELETE_THREAD_AND_RESTART = "WAL 删除线程已成功停止，正在重启。";
  public static final String TIMED_WAL_DELETE_THREAD_INTERRUPTED = "定时 WAL 删除线程被中断。";
  public static final String INTERRUPTED_WAITING_WAL_FLUSHED = "等待所有 WAL 日志 flush 完成时被中断。";
  public static final String STOPPING_WAL_MANAGER = "正在停止 WALManager";
  public static final String DELETING_OUTDATED_FILES_BEFORE_EXIT = "退出前删除过期文件";
  public static final String WAL_MANAGER_STOPPED = "WALManager 已停止";
  public static final String WAITING_THREAD_TERMINATED_TIMEOUT = "等待线程 {} 终止超时";
  public static final String THREAD_NOT_EXIT_AFTER_30S = "线程 {} 在 30 秒后仍未退出";
  public static final String FAILED_TO_DELETE_OUTDATED_WAL_FILE = "删除过期 WAL 文件失败";
  public static final String UNRECOGNIZED_CHECKPOINT_TYPE = "无法识别的检查点类型 ";
  public static final String CREATE_FOLDER_FOR_WAL_BUFFER = "为 WAL buffer-{} 创建目录 {}。";
  public static final String FAIL_TO_LOG_MAX_MEMTABLE_ID = "记录最大 memtable ID: {} 失败";
  public static final String FAIL_TO_MAKE_CHECKPOINT = "创建检查点 {} 失败";
  public static final String MEMTABLE_ID_NOT_FOUND_IN_MAP = "在 MemTableId2Info 中未找到 memtable ID {}";
  public static final String FAIL_TO_CLOSE_WAL_CHECKPOINT_WRITER = "关闭 WAL 节点 {} 的检查点写入器失败。";
  public static final String CANNOT_WRITE_TO = "无法写入 {}";
  public static final String REACH_END_OFFSET_OF_WAL_FILE = "已到达 WAL 文件的末尾偏移量";
  public static final String UNEXPECTED_END_OF_FILE = "文件意外结束";
  public static final String WAL_SEGMENT_V1_FAILED_V2_SUCCESS = "以 V1 方式加载 WAL 段失败，以 V2 方式重试成功。";
  public static final String UNEXPECTED_EXCEPTION = "意外异常";
  public static final String FAIL_TO_READ_WAL_ENTRY_SKIP_BROKEN = "从 WAL 文件 {} 读取 WALEntry 失败，跳过损坏的 WALEntry。";
  public static final String INVALID_CHECKPOINT_FILE_NAME = "无效的检查点文件名: ";
  public static final String INVALID_WAL_FILE_NAME = "无效的 WAL 文件名: ";
  public static final String INTERRUPTED_WAITING_FOR_RESULT = "等待结果时被中断。";
  public static final String CANNOT_WRITE_WAL_INTO_FAKE_NODE = "无法将 WAL 写入虚拟节点。 ";
  public static final String CREATE_FOLDER_FOR_WAL_NODE = "为 WAL 节点 {} 创建目录 {}。";
  public static final String FAIL_TO_DELETE_WAL_NODE_OUTDATED_FILES = "删除 WAL 节点 {} 的过期文件失败。";
  public static final String FAIL_TO_GET_DATA_REGION_PROCESSOR = "获取 {} 的 DataRegion 处理器失败";
  public static final String WAITING_TOO_LONG_FOR_MEMTABLE_FLUSH = "等待 memtable flush 完成时间过长。";
  public static final String INTERRUPTED_WAITING_MEMTABLE_FLUSH = "等待 memtable flush 完成时被中断。";
  public static final String FAIL_TO_ROLL_WAL_LOG_WRITER = "滚动 WAL 日志写入器失败。";
  public static final String FAIL_TO_SNAPSHOT_MEMTABLE = "对 {} 的 memtable 进行快照失败";
  public static final String START_RECOVERING_WAL_NODE_IN_DIR = "开始恢复目录 {} 中的 WAL 节点";
  public static final String ERROR_DELETE_CHECKPOINT_FILE = "删除检查点文件 {} 时出错";
  public static final String FAIL_TO_READ_WAL_LOGS_SKIP = "从 {} 读取 WAL 日志失败，跳过这些日志";
  public static final String FAIL_TO_RENAME_FILE = "重命名文件 {} 为 {} 失败";
  public static final String FAIL_TO_RECOVER_WAL_METADATA = "恢复 WAL 文件 {} 的元数据失败";
  public static final String START_RECOVERING_WAL = "开始恢复 WAL。";
  public static final String SUCCESSFULLY_RECOVER_ALL_WAL_NODES = "已成功恢复所有 WAL 节点。";
  public static final String STORAGE_ENGINE_FAILED_TO_RECOVER = "存储引擎恢复失败。";
  public static final String CANNOT_RECOVER_TSFILE_WAL_ALREADY_STARTED = "无法从 WAL 恢复 TsFile，因为 WAL 恢复已经开始";
  public static final String FAIL_TO_REMOVE_RECOVER_PERFORMER = "移除文件 {} 的恢复执行器失败";
  public static final String TSFILE_MISSING_SKIP_RECOVERY = "TsFile {} 缺失，将跳过恢复。";
  public static final String UNSUPPORTED_TYPE = "不支持的类型 ";
  public static final String ERROR_REDO_WAL = "重做 {} 的 WAL 时遇到错误";
  public static final String CREATE_FOLDER_FOR_WAL_NODE_BUFFER = "为 WAL 节点 {} 的 buffer 创建目录 {}。";
  public static final String OPEN_NEW_WAL_FILE_FOR_BUFFER = "为 WAL 节点 {} 的 buffer 打开新 WAL 文件 {}。";
  public static final String FAIL_TO_ALLOCATE_WAL_BUFFER_OOM = "由于内存不足，无法为 WAL 节点 {} 分配 buffer。";
  public static final String INTERRUPTED_WAITING_ADD_WAL_ENTRY = "等待将 WALEntry 添加到 buffer 时被中断。";
  public static final String HANDLE_ROLL_LOG_WRITER_SIGNAL = "处理 WAL 节点 {} 的滚动日志写入器信号。";
  public static final String INTERRUPTED_WAITING_WORKING_BUFFER = "等待可用工作 buffer 时被中断。";
  public static final String FAIL_TO_PUT_CLOSE_SIGNAL = "将 CLOSE_SIGNAL 放入 walEntries 失败。";
  public static final String FAIL_TO_CLOSE_WAL_LOG_WRITER = "关闭 WAL 节点 {} 的日志写入器失败。";
  public static final String UNKNOWN_WAL_ENTRY_TYPE = "未知的 WALEntry 类型";
  public static final String UNKNOWN_WAL_ENTRY_TYPE_WITH_VALUE = "未知的 WALEntry 类型 ";
  public static final String INVALID_WAL_ENTRY_TYPE_CODE = "无效的 WALEntryType 编码: ";
  public static final String CANNOT_SERIALIZE_CHECKPOINT_TO_WAL = "无法将检查点序列化到 WAL 文件。";
  public static final String UNSUPPORTED_WAL_ENTRY_TYPE = "不支持的 WAL 条目类型 ";
  public static final String CANNOT_USE_WAL_INFO_AS_SIGNAL_TYPE = "不能将 WAL 信息类型用作 WAL 信号类型";
  public static final String FAIL_TO_CREATE_WAL_NODE_DISKS_FULL = "由于 WAL 目录的所有磁盘已满，无法创建 WAL 节点。";
  public static final String FAILED_TO_CREATE_WAL_NODE_AFTER_RETRIES = "重试后仍无法创建 WAL 节点，标识符: ";
  public static final String FAIL_TO_CREATE_WAL_NODE = "创建 WAL 节点失败";

  // ======================== Flush ========================

  public static final String RESTORE_FILE_ERROR = "恢复文件出错，原因 ";
  public static final String CANNOT_DELETE_OLD_COMPRESSION_FILE = "无法删除旧的 DataRegion 压缩文件 {}";
  public static final String CANNOT_DELETE_RATIO_FILE = "无法删除压缩率文件 {}";
  public static final String TAKE_TASK_INTO_IO_QUEUE_INTERRUPTED = "将任务放入 ioTaskQueue 时被中断";
  public static final String PUT_TASK_INTO_IO_QUEUE_INTERRUPTED = "将任务放入 ioTaskQueue 时被中断";
  public static final String TAKE_TASK_FROM_IO_QUEUE_INTERRUPTED = "从 ioTaskQueue 取出任务时被中断";
  public static final String FLUSH_SUB_TASK_MANAGER_STARTED = "flush 子任务管理器已启动。";
  public static final String FLUSH_SUB_TASK_MANAGER_STOPPED = "flush 子任务管理器已停止";
  public static final String FLUSH_TASK_MANAGER_STARTED = "flush 任务管理器已启动。";
  public static final String FLUSH_TASK_MANAGER_STOPPED = "flush 任务管理器已停止";

  // ======================== Read ========================

  public static final String MEM_CHUNK_READER_NOT_SUPPORT_METHOD = "内存 Chunk 读取器不支持此方法";
  public static final String MEM_ALIGNED_PAGE_READER_TSBLOCK = "[memAlignedPageReader] TsBlock:{}";
  public static final String AFTER_FILTER_CHUNK_METADATA_LIST = "按过滤器移除后的 Chunk 元数据列表: ";
  public static final String AFTER_MODIFICATION_CHUNK_METADATA_LIST = "修改后的 Chunk 元数据列表: ";
  public static final String TIME_DATA_SIZE_NOT_MATCH = "时间数据大小不匹配";
  public static final String QUERY_OPENED_FILES = "查询已打开 {} 个文件！";
  public static final String CANNOT_CLOSE_TSFILE_SEQUENCE_READER = "无法关闭 TsFileSequenceReader {}！";
  public static final String QUERY_SEALED_FILE_INFO = "[Query Sealed File Info]\n";
  public static final String QUERY_ID_FORMAT = "\t[queryId: {}]\n";
  public static final String QUERY_FILE_PATH_FORMAT = "\t\t{}\n";
  public static final String QUERY_UNSEALED_FILE_INFO = "[Query Unsealed File Info]\n";

  // ======================== Snapshot ========================

  public static final String EXCEPTION_LOAD_SNAPSHOT = "从 {} 加载快照时发生异常";
  public static final String LOADING_SNAPSHOT_FOR = "正在为 {}-{} 加载快照，源目录为 {}";
  public static final String EXCEPTION_LOADING_SNAPSHOT_FOR = "为 {}-{} 加载快照时发生异常";
  public static final String READING_SNAPSHOT_LOG_FILE = "正在读取快照日志文件 {}";
  public static final String REMOVE_ALL_DATA_FILES_IN_ORIGINAL_DIR = "移除原始数据目录中的所有数据文件";
  public static final String FAILED_TO_REMOVE_ORIGIN_DATA_FILES = "移除原始数据文件失败";
  public static final String MOVING_SNAPSHOT_FILE_TO_DATA_DIRS = "正在将快照文件移动到数据目录";
  public static final String CANNOT_FIND_SNAPSHOT_DIRECTORY = "找不到快照目录 %s";
  public static final String NO_SEQ_OR_UNSEQ_FILES_IN_SNAPSHOT =
      "快照 {} 中没有顺序或乱序文件，跳过创建文件链接";
  public static final String EXCEPTION_DELETING_TIME_PARTITION_DIR =
      "删除 {}-{} 的时间分区目录时发生异常";
  public static final String CANNOT_CREATE_LINK_FALLBACK_COPY =
      "无法创建从 {} 到 {} 的链接，回退为复制";
  public static final String FAILED_TO_PROCESS_SNAPSHOT_FILE =
      "处理文件 {} 失败，所在目录为 {}: {}";
  public static final String FAILED_TO_PROCESS_SNAPSHOT_FILE_AFTER_RETRIES =
      "重试后仍无法处理文件。源文件: %s，目标后缀: %s";
  public static final String SNAPSHOT_FILE_NUM_MISMATCH =
      "日志中的文件数为 %d，但磁盘中的文件数为 %d";
  public static final String SNAPSHOT_FILE_NOT_IN_LOG = "文件 %s 不在日志文件列表中";
  public static final String NO_COMPRESSION_RATIO_FILE_IN_DIR = "目录 {} 中没有压缩率文件";
  public static final String CANNOT_LOAD_COMPRESSION_RATIO = "无法从 {} 加载压缩率";
  public static final String LOADED_COMPRESSION_RATIO = "已从 {} 加载压缩率";
  public static final String EXCEPTION_READING_SNAPSHOT_FILE = "读取快照文件时发生异常";
  public static final String SNAPSHOT_NOT_COMPLETE_CANNOT_LOAD = "此快照不完整，无法加载";
  public static final String CREATED_HARD_LINK = "已创建从 {} 到 {} 的硬链接";
  public static final String EXCEPTION_CLOSING_LOG_ANALYZER = "关闭日志分析器时发生异常";
  public static final String CANNOT_CREATE_PARENT_FOLDER = "无法创建父目录: ";
  public static final String CANNOT_CREATE_FILE = "无法创建文件: ";
  public static final String FAILED_TO_CLOSE_SNAPSHOT_LOGGER = "关闭快照日志器失败";
  public static final String SNAPSHOTTING_COMPRESSION_RATIO = "正在快照压缩率文件 {}。";
  public static final String CATCH_IO_EXCEPTION_CREATING_SNAPSHOT = "创建快照时捕获到 IOException";
  public static final String HARD_LINK_TARGET_DIR_NOT_EXIST = "硬链接目标目录 {} 不存在";
  public static final String HARD_LINK_SOURCE_FILE_NOT_EXIST = "硬链接源文件 {} 不存在，该文件将被忽略。";
  public static final String COPY_TARGET_DIR_NOT_EXIST = "复制目标目录 {} 不存在";
  public static final String COPY_SOURCE_FILE_NOT_EXIST = "复制源文件 {} 不存在";
  public static final String CANNOT_CREATE_DIRECTORY = "无法创建目录: ";
  public static final String CLEANING_UP_SNAPSHOT_DIR = "正在清理 {} 的快照目录";
  public static final String FAILED_TO_CREATE_DIR = "创建目录 %s 失败";
  public static final String FAILED_TO_TAKE_SNAPSHOT_CLEAN_UP = "为 {}-{} 创建快照失败，正在清理";
  public static final String SUCCESSFULLY_TAKE_SNAPSHOT = "已成功为 {}-{} 创建快照，快照目录为 {}";
  public static final String EXCEPTION_TAKING_SNAPSHOT = "为 {}-{} 创建快照时发生异常";
  public static final String SNAPSHOT_COMPRESSION_RATIO_IN_DIR = "快照压缩率文件 {} 已保存到 {}。";
  public static final String CANNOT_SNAPSHOT_COMPRESSION_RATIO = "无法快照压缩率文件 {} 到 {}。";
  public static final String CLEAR_SNAPSHOT_DIR_FAIL = "清理快照目录失败，请在再次执行 Region 迁移前手动删除此目录: {}";
  public static final String HARD_LINK_SOURCE_FILE_RETRY = "硬链接源文件 {} 不存在，将重试 {} 次...";
  public static final String TRY_SHOW_FILES_IN_PARENT_DIR = "尝试显示父目录中的所有文件...";
  public static final String CANNOT_SHOW_FILES_PARENT_DIR_NULL = "无法显示文件，因为父目录为空";
  public static final String FAILED_DELETE_FOLDER_CLEANING_UP = "清理时删除目录 {} 失败";

  // ======================== TsFile Resource ========================

  public static final String FAILED_TO_SERIALIZE_SHARED_MOD_FILE = "序列化共享修改文件失败";
  public static final String FAILED_TO_GET_SHARED_MOD_FILE = "获取共享修改文件失败";
  public static final String UPGRADING_MOD_FILE_INTERRUPTED = "升级修改文件被中断";
  public static final String CANNOT_UPGRADE_MOD_FILE = "无法升级修改文件";
  public static final String TIME_INDEX_VALUE = "TimeIndex = {}";
  public static final String RESOURCE_FILE_NOT_FOUND = "资源文件未找到";
  public static final String CANNOT_BUILD_DEVICE_TIME_INDEX = "无法从资源文件构建 DeviceTimeIndex: ";
  public static final String TSFILE_CANNOT_BE_DELETED = "TsFile {} 无法删除: {}";
  public static final String MODIFICATION_FILE_CANNOT_BE_DELETED = "修改文件 {} 无法删除: {}";
  public static final String TSFILE_RESOURCE_CANNOT_BE_DELETED = "TsFileResource {} 无法删除: {}";
  public static final String FILE_NAME_NOT_STANDARD = "文件名可能不符合标准命名规范。";
  public static final String FAILED_TO_READ_MODS = "从 {} 读取 {} 的修改记录失败";
  public static final String INVALID_INPUT = "无效输入: ";
  public static final String ALL_DISKS_FULL_CANNOT_CREATE_TSFILE_DIR = "所有磁盘已满，无法创建 TsFile 目录";
  public static final String DISK_SPACE_INSUFFICIENT = "磁盘空间不足";
  public static final String FAILED_TO_CREATE_TSFILE_DIR_AFTER_RETRIES = "重试后仍无法创建 TsFile 目录";
  public static final String FAILED_TO_CREATE_DIR_AFTER_RETRIES = "重试后仍无法创建目录";
  public static final String TSFILE_NAME_FORMAT_INCORRECT = "TsFile 文件名格式不正确: ";
  public static final String WRONG_TIME_INDEX_TYPE_LOG = "错误的 timeIndex 类型 {}";
  public static final String WRONG_TIME_INDEX_TYPE = "错误的 timeIndex 类型 ";
  public static final String ERROR_RECORD_FILE_TIME_INDEX_CACHE = "记录 FileTimeIndexCache 时遇到错误: {}";
  public static final String ERROR_RECORD_FILE_TIME_INDEX_CACHE_NO_DETAIL = "记录 FileTimeIndexCache 时遇到错误";
  public static final String ERROR_COMPACT_FILE_TIME_INDEX_CACHE = "压缩 FileTimeIndexCache 时遇到错误: {}";
  public static final String ERROR_COMPACT_FILE_TIME_INDEX_CACHE_NO_DETAIL = "压缩 FileTimeIndexCache 时遇到错误";
  public static final String FILE_TIME_INDEX_FILE_ALREADY_EXISTS = "FileTimeIndex 文件已存在，文件路径: {}";
  public static final String ERROR_CLOSE_FILE_TIME_INDEX_CACHE = "关闭 FileTimeIndexCache 时遇到错误: {}";
  public static final String END_OF_STREAM_REACHED = "已到达流的末尾";
  public static final String V012_FILE_TIME_INDEX_SHOULD_NEVER_APPEAR = "V012_FILE_TIME_INDEX 不应出现";
  public static final String INVALID_ORDINAL = "无效的序号";

  // ======================== DataRegion Utils ========================

  public static final String FAILED_TO_SCAN_FILE = "扫描文件 {} 失败";
  public static final String DEVICE_LEVEL_METADATA_INDEX_NOT_SUPPORTED = "不支持设备级别的元数据索引节点";
  public static final String NO_MORE_DATA_IN_SHARED_TIME_BUFFER = "SharedTimeDataBuffer 中没有更多数据";
  public static final String FAILED_TO_CALC_TSFILE_TABLE_SIZES = "计算 TsFile 表大小失败";
  public static final String TIME_INDEX_IS_NULL = "{} {} 时间索引为空";
  public static final String EMPTY_RESOURCE = "{} {} 资源为空";
  public static final String ERROR_VALIDATE_RESOURCE_FILE = "验证 .resource 文件 {} 时出错";
  public static final String ILLEGAL_TSFILE = "{} {} 非法 TsFile";
  public static final String ERROR_VALIDATING_TSFILE = "验证 TsFile {} 时遇到错误, ";
  public static final String EXCEPTION_APPLY_TABLE_DISK_USAGE_INDEX = "应用 TableDiskUsageIndex 操作时遇到异常。";
  public static final String FAILED_RECOVER_TABLE_DISK_USAGE_INDEX = "恢复 TableDiskUsageIndex 失败";
  public static final String FAILED_SYNC_TABLE_SIZE_INDEX = "同步 TsFile 表大小索引失败。";
  public static final String WRITE_OBJECT_DELTA = "writeObjectDelta";
  public static final String EXCEPTION_REMOVE_TABLE_DISK_USAGE_INDEX = "移除 TableDiskUsageIndex 时遇到异常。";
  public static final String INTERRUPTED_ADDING_OP_TO_QUEUE = "将操作 {} 添加到队列时被中断。";
  public static final String FAILED_TO_MOVE_FILE = "移动 {} 到 {} 失败";
  public static final String FAILED_TO_READ_KEY_FILE_DURING_COMPACTION = "compaction 过程中读取键文件失败";
  public static final String FAILED_COMPACTION_TABLE_SIZE_INDEX = "对 TsFile 表大小索引文件执行 compaction 失败";
  public static final String FAILED_TO_READ_TABLE_SIZE_INDEX = "读取表 TsFile 大小索引文件 {} 失败";
  public static final String TABLE_NUM_SHOULD_BE_POSITIVE = "tableNum 应大于 0";
  public static final String BACKWARD_SEEK_NOT_SUPPORTED = "不支持向后查找";
  public static final String THREAD_INTERRUPTED_SKIP_WRITE_FOR_IO_SAFETY = "当前线程被中断，为保证 IO 安全无需执行写入";
  public static final String PARTITION_LOG_FILE_ALREADY_EXISTS = "分区日志文件已存在，文件路径: {}";

  // ======================== Load TsFile ========================

  public static final String UNSUPPORTED_TSFILE_DATA_TYPE = "不支持的 TsFileData 类型: ";
  public static final String DELETE_AFTER_LOADING_ERROR = "加载后删除 {} 出错。";
  public static final String LOAD_TSFILE_DIR_CREATED = "已创建加载 TsFile 目录 {}。";
  public static final String CANNOT_CREATE_TSFILE_FOR_WRITING = "无法创建 TsFile {} 用于写入。";
  public static final String CLOSE_TSFILE_IO_WRITER_ERROR = "关闭 TsFileIOWriter {} 出错。";
  public static final String CLOSE_MODIFICATION_FILE_ERROR = "关闭修改文件 {} 出错。";
  public static final String TASK_DIR_NOT_EMPTY_SKIP_DELETE = "任务目录 {} 非空，跳过删除。";
  public static final String LOAD_CLEANUP_TASK_CANCELED = "加载清理任务 {} 已取消。";
  public static final String LOAD_CLEANUP_TASK_STARTS = "加载清理任务 {} 开始。";
  public static final String LOAD_CLEANUP_TASK_ERROR = "加载清理任务 {} 出错。";
  public static final String FAILED_UPDATE_FILE_COUNTER_DIR_NOT_EXIST = "更新文件计数器失败，目录 ({}) 不存在";
  public static final String UNSUPPORTED_STAGE = "不支持的阶段: ";
  public static final String RELEASE_MEMORY_BLOCK_FAILED = "释放内存块 {} 失败";
  public static final String EXCEED_TOTAL_MEMORY_SIZE = "{} 已超出总内存大小";
  public static final String REDUCE_MEMORY_USAGE_TO_NEGATIVE = "{} 的内存使用量已降为负数";
  public static final String FORCE_ALLOCATE_INTERRUPTED = "forceAllocate: 等待可用内存时被中断";
  public static final String LOAD_ALLOCATED_MEMORY_BLOCK = "Load: 从查询引擎分配内存块，大小: {}";
  public static final String RELEASE_DATA_CACHE_MEMORY_BLOCK = "释放数据缓存内存块 {}";
  public static final String START_DATA_TYPE_CONVERSION_DOT = "开始对 LoadTsFileStatement: {} 进行数据类型转换。";
  public static final String START_DATA_TYPE_CONVERSION = "开始对 LoadTsFileStatement: {} 进行数据类型转换";
  public static final String FAIL_TO_LOAD_TSFILE_TO_ACTIVE_DIR = "加载 TsFile 到 Active 目录失败";
  public static final String FAIL_TO_LOAD_DISK_SPACE = "获取文件 {} 的磁盘空间失败";
  public static final String LOAD_ACTIVE_LISTENING_DIR_NOT_SET = "未设置加载 Active 监听目录。";
  public static final String FAILED_TO_CREATE_TARGET_DIR = "创建目标目录失败: ";
  public static final String FAILED_LOAD_ACTIVE_LISTENING_DIRS = "加载 Active 监听目录失败";
  public static final String INVALID_PARAMETER = "无效的参数 '";
  public static final String INVALID_PARAMETER_FOR_LOAD_TSFILE_COMMAND =
      "LOAD TSFILE 命令中的参数 '%s' 无效。";
  public static final String LOAD_TSFILE_DATABASE_KEY_AND_NAME_CANNOT_COEXIST =
      "参数键 '%s' 和 '%s' 不能同时存在。";
  public static final String DATABASE_LEVEL_LESS_THAN_MINIMUM =
      "给定的数据库层级 %d 小于最小值 %d，请输入有效的数据库层级。";
  public static final String DATABASE_LEVEL_NOT_VALID_INTEGER =
      "给定的数据库层级 %s 不是有效整数，请输入有效的数据库层级。";
  public static final String ON_SUCCESS_VALUE_NOT_SUPPORTED =
      "给定的 on-success 值 '%s' 不受支持，请输入有效的 on-success 值。";
  public static final String PARAMETER_VALUE_NOT_SUPPORTED_BOOLEAN =
      "给定的 %s 值 '%s' 不受支持，请输入有效的布尔值。";
  public static final String TABLET_CONVERSION_THRESHOLD_NON_NEGATIVE =
      "tablet conversion threshold 必须是非负 long 值。";
  public static final String TABLET_CONVERSION_THRESHOLD_NOT_VALID_LONG =
      "tablet conversion threshold '%s' 不是有效的 long 值。";
  public static final String UTILITY_CLASS = "工具类";
  public static final String TSFILE_DATA_BYTE_ARRAY_SIZE_MISMATCH = "TsFileData 字节数组读取错误，大小不匹配。";
  public static final String UNKNOWN_TSFILE_DATA_TYPE = "未知的 TsFileData 类型: ";
  public static final String FILE_MAGIC_STRING_INCORRECT = "文件的 MAGIC STRING 不正确，文件路径: {}";
  public static final String FILE_VERSION_TOO_OLD = "文件的版本号过旧，文件路径: {}";
  public static final String FILE_NOT_CLOSED_CORRECTLY = "文件未正确关闭，文件路径: {}";
  public static final String MINIO_SELECTOR_REQUIRES_ONE_DIR = "MinIO 选择器至少需要一个目录";
  public static final String ADD_MOUNT_POINT = "添加 {} 的挂载点 {}";
  public static final String FAILED_TO_CHECK_DIRECTORY = "检查目录失败: {}";
  public static final String FAILED_TO_LIST_FILES_IN_DIR = "列出目录 {} 中的文件失败";
  public static final String FAILED_TO_DELETE_FILE_OR_DIR = "删除文件或目录 {} 失败";
  public static final String FAILED_TO_CLEANUP_DIRECTORY = "清理目录 {} 失败";
  public static final String CLEANED_UP_ACTIVE_LOAD_DIRS = "已清理 Active 加载监听目录";
  public static final String UNEXPECTED_ERROR_CLEANUP_ACTIVE_DIRS = "清理 Active 加载监听目录时发生意外错误";
  public static final String ACTIVE_LOAD_DIR_SCANNER_REGISTERED = "Active 加载目录扫描定期任务已注册";
  public static final String ERROR_ACTIVE_LOAD_DIR_SCANNING = "Active 加载目录扫描过程中发生错误。";
  public static final String SYSTEM_READ_ONLY_SKIP_ACTIVE_SCAN = "当前系统为只读模式，跳过 Active 加载目录扫描。";
  public static final String FILE_DELETED_IGNORE_EXCEPTION = "文件已被删除，忽略此异常。";
  public static final String EXCEPTION_SCANNING_DIR = "扫描目录 {} 时发生异常";
  public static final String ERROR_CREATING_DIR_FOR_ACTIVE_LOAD = "为 Active 加载创建目录 {} 时发生错误。";
  public static final String FAILED_COUNT_ACTIVE_DIRS_FILE_NUMBER = "统计 Active 监听目录文件数量失败。";
  public static final String ACTIVE_LOAD_METRIC_COLLECTOR_REGISTERED = "Active 加载指标收集定期任务已注册";
  public static final String DATABASE_NAME_MUST_NOT_BE_EMPTY = "数据库名称不能为空。";
  public static final String ERROR_EXECUTING_ACTIVE_LOAD_JOB = "执行 Active 加载定期任务时发生错误。";
  public static final String ACTIVE_LOAD_EXECUTOR_STARTED = "Active 加载定期任务执行器已成功启动。";
  public static final String ACTIVE_LOAD_EXECUTOR_STOPPED = "Active 加载定期任务执行器已成功停止。";
  public static final String ERROR_MOVING_FILE_TO_FAIL_DIR = "将文件 {} 移动到失败目录时发生错误。";
  public static final String FAILED_COUNT_FILES_IN_FAIL_DIR = "统计失败目录中的失败文件数量失败。";

  public static final String STRING_NOT_LEGAL_REPAIR_LOG = "字符串 '%s' 不是合法的修复日志";

  public static final String WRONG_LOAD_COMMAND_S = "错误的 load 命令 %s。";

  public static final String FAILED_TO_FIND_DATA_REGION = "共识组 %s 底层状态机创建失败, 因为 DataRegion 没找到。";

  public static final String DATA_REGION_IS_NULL = "Data region 是空";
  // ---------------------------------------------------------------------------
  // 补充日志消息
  // ---------------------------------------------------------------------------
  public static final String STORAGE_LOG_STORAGE_ENGINE_RECOVER_COST_S_C8AEE9D9 =
      "Storage Engine 恢复cost：{}s.";
  public static final String STORAGE_LOG_DATA_REGIONS_HAVE_BEEN_RECOVERED_D5BD3A80 =
      "Data Regions 已已恢复 {}/{}";
  public static final String STORAGE_LOG_TSFILE_RESOURCE_RECOVER_COST_S_41F074E0 =
      "Ts文件 Resource 恢复cost：{}s.";
  public static final String STORAGE_LOG_CONSTRUCT_A_DATA_REGION_INSTANCE_THE_DATABASE_IS_THREAD_17A16BDF =
      "construct a data Region instance, the 数据库 是{}, Th读取是{}";
  public static final String STORAGE_LOG_DATAREGION_NOT_FOUND_ON_THIS_DATANODE_WHEN_WRITING_PIECE_E5B5A888 =
      "DataRegion {} 未找到 on th是DataNode when writing piece nodeof Ts文件 {} (maybe 由于Region "
          + "migration), 将skip.";
  public static final String STORAGE_LOG_IO_ERROR_WHEN_WRITING_PIECE_NODE_OF_TSFILE_TO_DATAREGION_946738F2 =
      "IO error when writing piece node of Ts文件 {} to DataRegion {}.";
  public static final String STORAGE_LOG_EXCEPTION_OCCURRED_WHEN_WRITING_PIECE_NODE_OF_TSFILE_TO_9EDD09BD =
      "在以下过程发生异常：writing piece node of Ts文件 {} to DataRegion {}.";
  public static final String STORAGE_LOG_FAILED_TO_RECOVER_DATA_REGION_804B162D =
      "无法恢复data Region {}[{}]";
  public static final String STORAGE_LOG_ERROR_OCCURS_WHEN_DELETING_DATA_REGION_8C07B7A0 =
      "在以下过程发生错误：deleting data Region {}-{}";
  public static final String STORAGE_LOG_NEXT_LOAD_CLEANUP_TASK_IS_NOT_READY_TO_RUN_WAIT_FOR_AT_LEAST_CBE0023F =
      "Next 加载cleanup 任务 {} 未就绪 to run, 等待at least {} ms ({}s).";
  public static final String STORAGE_LOG_WRITER_FOR_PARTITION_IS_ALREADY_WRITING_CHUNK_GROUP_FOR_903B1D66 =
      "Writer {} for partition {} 是already writing chunk group for device {}, but the last device "
          + "是{}. ";
  public static final String STORAGE_LOG_CAN_NOT_CREATE_MODIFICATIONFILE_FOR_WRITING_17D14C11 =
      "无法创建Modification文件 {} for writing.";
  public static final String STORAGE_LOG_SKIP_RECOVERING_DATA_REGION_WHEN_CONSENSUS_PROTOCOL_IS_RATIS_43A6A699 =
      "Skip recovering data Region {}[{}] when consensus protocol 是rat是and storage engine 未就绪.";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLET_BECAUSE_C2DC8032 =
      "Won't insert tablet {}, 原因：{}";
  public static final String STORAGE_LOG_TIMESTAMP_MEASUREMENTID_IDEVICEID_04A5AE37 =
      "timestamp {}, measurementId {}, ideviceId {}";
  public static final String STORAGE_LOG_DELETION_SKIPPED_FILE_TIME_DD653236 =
      "[Deletion] {} skipped {}, 文件 time [{}, {}]";
  public static final String STORAGE_LOG_DEVICE_IS_DEVICETABLE_IS_TABLEDELETIONENTRY_GETPREDICATE_E84489E9 =
      "device 是{}, deviceTable 是{}, tableDeletionEntry.getPredicate().matches(device) 是{}";
  public static final String STORAGE_LOG_TABLENAME_IS_MATCHSIZE_IS_ONLYONETABLE_IS_E20FAFAE =
      "tableName 是{}, matchSize 是{}, onlyOneTable 是{}";
  public static final String STORAGE_LOG_TABLENAME_IS_DEVICE_IS_DELETIONSTARTTIME_IS_DELETIONENDTIME_B881E677 =
      "tableName 是{}, device 是{}, deletionStartTime 是{}, deletionEndTime 是{}, 文件StartTime 是{}, "
          + "文件EndTime 是{}";
  public static final String STORAGE_LOG_DELETE_TSFILERESOURCE_IS_29F5A98C = "删除ts文件Resource 是{}";
  public static final String STORAGE_LOG_DELETETSFILECOMPLETELY_EXECUTE_SUCCESSFUL_ALL_TSFILE_ARE_D81FE0D7 =
      "deleteTs文件Completely execute successful, all ts文件 是已删除 成功";
  public static final String STORAGE_LOG_DELETION_DELETION_WRITTEN_INTO_MODS_FILE_F5E26D2A =
      "[Deletion] Deletion {} 已写入 into mods 文件:{}.";
  public static final String STORAGE_LOG_DATABASE_SYSTEM_DIRECTORY_DOESN_T_EXIST_CREATE_IT_9C0E7C68 =
      "数据库 system 目录 {} doesn't exist, 创建it";
  public static final String STORAGE_LOG_DATA_REGION_DIRECTORY_DOESN_T_EXIST_CREATE_IT_EFB0AE77 =
      "Data Region 目录 {} doesn't exist, 创建it";
  public static final String STORAGE_LOG_THE_TSFILES_OF_DATA_REGION_HAS_RECOVERED_E17384CF =
      "The Ts文件s of data Region {}[{}] has 已恢复 {}/{}.";
  public static final String STORAGE_LOG_THE_TSFILES_OF_DATA_REGION_HAS_RECOVERED_COMPLETELY_0D79FC83 =
      "The Ts文件s of data Region {}[{}] has 已恢复 completely {}/{}.";
  public static final String STORAGE_LOG_THE_DATA_REGION_IS_CREATED_SUCCESSFULLY_B991F1D4 =
      "The data Region {}[{}] 是已创建 成功";
  public static final String STORAGE_LOG_THE_DATA_REGION_IS_RECOVERED_SUCCESSFULLY_5AAFF7B7 =
      "The data Region {}[{}] 是已恢复 成功";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLET_BECAUSE_REGION_IS_DELETED_34D893A7 =
      "Won't insert tablet {}, 原因：Region 是已删除";
  public static final String STORAGE_LOG_ASYNC_CLOSE_TSFILE_FILE_START_TIME_FILE_END_TIME_65020832 =
      "Async 关闭ts文件：{}, 文件 start time：{}, 文件 end time：{}";
  public static final String STORAGE_LOG_WILL_CLOSE_ALL_FILES_FOR_DELETING_DATA_FOLDER_93A5B15E =
      "{} 将关闭all 文件s for deleting data 目录 {}";
  public static final String STORAGE_LOG_WILL_CLOSE_ALL_FILES_FOR_DELETING_DATA_FILES_7768D429 =
      "{} 将关闭all 文件s for deleting data 文件s";
  public static final String STORAGE_LOG_EXCEED_SEQUENCE_MEMTABLE_FLUSH_INTERVAL_SO_FLUSH_WORKING_23513D66 =
      "Exceed sequence memtable flush interval, so flush working memtable of time partition {} in "
          + "数据库 {}[{}]";
  public static final String STORAGE_LOG_EXCEED_UNSEQUENCE_MEMTABLE_FLUSH_INTERVAL_SO_FLUSH_WORKING_BADB0B75 =
      "Exceed unsequence memtable flush interval, so flush working memtable of time partition {} "
          + "in 数据库 {}[{}]";
  public static final String STORAGE_LOG_START_TO_WAIT_TSFILES_TO_CLOSE_SEQ_FILES_UNSEQ_FILES_441F7130 =
      "开始wait Ts文件s to close, seq 文件s：{}, unseq 文件s：{}";
  public static final String STORAGE_LOG_ASYNC_FORCE_CLOSE_ALL_FILES_IN_DATABASE_076AB4B9 =
      "async force 关闭all 文件s in 数据库：{}";
  public static final String STORAGE_LOG_FORCE_CLOSE_ALL_PROCESSORS_IN_DATABASE_68C9EB60 =
      "force 关闭all processors in 数据库：{}";
  public static final String STORAGE_LOG_WILL_DELETE_DATA_FILES_DIRECTLY_FOR_DELETING_DATA_BETWEEN_289DD3BF =
      "{} 将删除data 文件s directly for deleting data between {} and {}";
  public static final String STORAGE_LOG_DELETION_DELETION_IS_WRITTEN_INTO_MOD_FILES_DDCDF0AD =
      "[Deletion] Deletion {} 是已写入 into {} mod 文件s";
  public static final String STORAGE_LOG_TTL_START_TTL_AND_MODIFICATION_CHECKING_A37AB173 =
      "[TTL] {}-{} 开始ttl and modification checking.";
  public static final String STORAGE_LOG_TTL_TOTALLY_SELECT_ALL_OUTDATED_FILES_AND_PARTIAL_OUTDATED_5246BD61 =
      "[TTL] {}-{} Totally select {} all-outdated 文件s and {} partial-outdated 文件s.";
  public static final String STORAGE_LOG_WON_T_LOAD_TSFILE_BECAUSE_REGION_IS_DELETED_0E72E8D0 =
      "Won't 加载Ts文件 {}, 原因：Region 是已删除";
  public static final String STORAGE_LOG_TSFILE_MUST_BE_RENAMED_TO_FOR_LOADING_INTO_THE_UNSEQUENCE_70321619 =
      "Ts文件 {} must be renamed to {} for loading into the unsequence list.";
  public static final String STORAGE_LOG_LOAD_TSFILE_IN_UNSEQUENCE_LIST_MOVE_FILE_FROM_TO_21E11AEB =
      "加载ts文件 in unsequence list, move 文件 from {} to {}";
  public static final String STORAGE_LOG_MOVE_TSFILE_TO_TARGET_DIR_SUCCESSFULLY_57288783 =
      "Move ts文件 {} to target dir {} 成功.";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLETS_BECAUSE_REGION_IS_DELETED_48E9720F =
      "Won't insert tablets {}, 原因：Region 是已删除";
  public static final String STORAGE_LOG_HAS_SPENT_S_TO_WAIT_FOR_CLOSING_ALL_TSFILES_6C3EE4CE =
      "{} has spent {}s to 等待closing all Ts文件s.";
  public static final String STORAGE_LOG_SSEQ_FILES_UNSEQ_FILES_918AEB2A =
      "Sseq 文件s：{}, unseq 文件s：{}";
  public static final String STORAGE_LOG_UNRECOGNIZED_LASTCACHELOADSTRATEGY_FALL_BACK_TO_CLEAN_ALL_C200F32D =
      "无法识别的LastCacheLoadStrategy：{}, fall back to CLEAN_ALL";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_TARGET_28E43D85 =
      "文件 renaming failed when loading ts文件. Origin：{}, Target：{}";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_TARGET_9C22DDF3 =
      "文件 renaming failed when loading。resource 文件. Origin：{}, Target：{}";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_TARGET_18A212F3 =
      "文件 renaming failed when loading。mod 文件. Origin：{}, Target：{}";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_DELETING_DATA_REGION_FOLDER_FOR_8ABCF5D1 =
      "在以下过程发生异常：deleting data Region 目录 for {}-{}";
  public static final String STORAGE_LOG_FAIL_TO_RECOVER_UNSEALED_TSFILE_SKIP_IT_CA576205 =
      "无法恢复unsealed Ts文件 {}, skip it.";
  public static final String STORAGE_LOG_REMOTE_REQUEST_CONFIG_NODE_FAILED_THAT_JUDGMENT_IF_TABLE_25FE3602 =
      "Remote 请求 config node failed that judgment if table 是exist, occur exception. {}";
  public static final String STORAGE_LOG_DUE_TSTABLE_IS_NULL_TABLE_SCHEMA_CAN_T_BE_GOT_LEADER_NODE_C3EF524D =
      "Due tsTable 是null, table schema can't be got, leader node occur special situation need to "
          + "resolve.";
  public static final String STORAGE_LOG_DISK_SPACE_IS_INSUFFICIENT_WHEN_CREATING_TSFILE_PROCESSOR_4032BAF0 =
      "disk space 是insufficient when creating Ts文件 processor, change system mode to read-only";
  public static final String STORAGE_LOG_MEET_IOEXCEPTION_WHEN_CREATING_TSFILEPROCESSOR_CHANGE_SYSTEM_4337F729 =
      "meet IOException when creating Ts文件Processor, change system mode to error";
  public static final String STORAGE_LOG_CLOSEFILENODECONDITION_ERROR_OCCURS_WHILE_WAITING_FOR_CLOSING_F33B72A6 =
      "Close文件NodeCondition error occurs while waiting for closing the storage group {}";
  public static final String STORAGE_LOG_CLOSEFILENODECONDITION_ERROR_OCCURS_WHILE_WAITING_FOR_CLOSING_C4B97CC0 =
      "Close文件NodeCondition error occurs while waiting for closing ts文件 processors of {}";
  public static final String STORAGE_LOG_FAILED_TO_APPEND_THE_TSFILE_TO_DATABASE_PROCESSOR_BECAUSE_670341AE =
      "无法append the ts文件 {} to 数据库 processor {} 原因：the disk space 是insufficient.";
  public static final String STORAGE_LOG_GET_TIMESERIES_METADATA_IN_FILE_FROM_CACHE_36652729 =
      "Get timeseries：{}.{}  metadata in 文件：{}  from cache：{}.";
  public static final String STORAGE_LOG_TIMESERIESMETADATACACHE_SIZE_E31733D3 =
      "TimeSeriesMetadataCache size = {}";
  public static final String STORAGE_LOG_FLUSH_TASK_OF_DATABASE_MEMTABLE_IS_CREATED_FLUSHING_TO_FILE_E44B3AA0 =
      "flush 任务 of 数据库 {} memtable 是已创建, flushing to 文件 {}.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_INTO_FILE_DATA_SORT_TIME_COST_3D39AA17 =
      "数据库 {} memtable flushing into 文件 {}：data sort time cost {} ms.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_STARTS_TO_ENCODING_DATA_6A89F32E =
      "数据库 {} memtable flushing to 文件 {} 开始 to encoding data.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_START_IO_CB72C2DA =
      "数据库 {} memtable flushing to 文件 {} start io.";
  public static final String STORAGE_LOG_FLUSHING_A_MEMTABLE_TO_FILE_IN_DATABASE_IO_COST_MS_2306578A =
      "flushing a memtable to 文件 {} in 数据库 {}, io cost {}ms";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_ENCODING_TASK_IS_INTERRUPTED_9D7BF4EF =
      "数据库 {} memtable flushing to 文件 {}, encoding 任务 是被中断.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_IO_TASK_MEETS_ERROR_EC383D33 =
      "数据库 {} memtable {}, io 任务 meets error.";
  public static final String STORAGE_LOG_OLD_RATIO_FILE_DOESN_T_EXIST_FORCE_CREATE_RATIO_FILE_74EDD7DB =
      "Old ratio 文件 {} doesn't exist, force 创建ratio 文件 {}";
  public static final String STORAGE_LOG_COMPRESSION_RATIO_FILE_UPDATED_PREVIOUS_CURRENT_7A9EEDF8 =
      "Compression ratio 文件 updated, previous：{}, current：{}";
  public static final String STORAGE_LOG_AFTER_RESTORING_FROM_COMPRESSION_RATIO_FILE_TOTAL_MEMORY_D5ACB1C4 =
      "After restoring from compression ratio 文件, total 内存 size = {}, total disk size = {}";
  public static final String STORAGE_LOG_THE_COMPRESSION_RATIO_IS_NEGATIVE_CURRENT_MEMTABLESIZE_TOTALMEMTABLESIZE_8C3DD017 =
      "The compression ratio 是negative, 当前memTableSize：{}, totalMemTableSize：{}";
  public static final String STORAGE_LOG_REBOOT_WAL_DELETE_THREAD_SUCCESSFULLY_CURRENT_PERIOD_IS_44B69C7A =
      "Reboot wal 删除th读取成功, 当前period 是{} ms";
  public static final String STORAGE_LOG_WAL_DISK_USAGE_IS_LARGER_THAN_THE_WAL_THROTTLE_THRESHOLD_2396FFCC =
      "WAL disk usage {} 是larger than the wal_throttle_threshold_in_byte * 0.8 {}, please check "
          + "your 写入load, iot consensus and the pipe module. It's better to allocate more disk for WAL.";
  public static final String STORAGE_LOG_FLUSH_A_WORKING_MEMTABLE_IN_ASYNC_CLOSE_TSFILE_MEMTABLE_00158706 =
      "{}：flush a working memtable in async 关闭ts文件 {}, memtable size：{}, ts文件 size：{}, plan "
          + "index：[{}, {}], progress index：{}";
  public static final String STORAGE_LOG_FLUSH_A_NOTIFYFLUSHMEMTABLE_IN_ASYNC_CLOSE_TSFILE_TSFILE_48D1E75A =
      "{}：flush a NotifyFlushMemTable in async 关闭ts文件 {}, ts文件 size：{}";
  public static final String STORAGE_LOG_MEMTABLE_SIGNAL_IS_ADDED_INTO_THE_FLUSHING_MEMTABLE_QUEUE_5D9DA8DB =
      "{}：{} Memtable (signal = {}) 是added into the flushing Memtable, queue size = {}";
  public static final String STORAGE_LOG_MEMTABLE_SIGNAL_IS_REMOVED_FROM_THE_QUEUE_LEFT_DFDB97D2 =
      "{}：{} memtable (signal={}) 是已移除 from the queue. {} left.";
  public static final String STORAGE_LOG_MEM_CONTROL_FLUSH_FINISHED_TRY_TO_RESET_SYSTEM_MEM_COST_3CD8399C =
      "[mem control] {}：{} flush finished, try to reset system mem cost, flushing memtable list "
          + "size：{}";
  public static final String STORAGE_LOG_FLUSH_FINISHED_REMOVE_A_MEMTABLE_FROM_FLUSHING_LIST_FLUSHING_08A00750 =
      "{}：{} flush finished, 移除a memtable from flushing list, flushing memtable list size：{}";
  public static final String STORAGE_LOG_RELEASED_A_MEMTABLE_SIGNAL_FLUSHINGMEMTABLES_SIZE_6D22169F =
      "{}：{} released a memtable (signal={}), flushingMemtables size ={}";
  public static final String STORAGE_LOG_TRY_GET_LOCK_TO_RELEASE_A_MEMTABLE_SIGNAL_B9098E21 =
      "{}：{} try get lock to release a memtable (signal={})";
  public static final String STORAGE_LOG_FLUSHINGMEMTABLES_IS_EMPTY_AND_WILL_CLOSE_THE_FILE_22A07A5C =
      "{}：{} flushingMemtables 为空 and 将关闭the 文件";
  public static final String STORAGE_LOG_TRY_TO_GET_FLUSHINGMEMTABLES_LOCK_F91EA27F =
      "{}：{} try to get flushingMemtables lock.";
  public static final String STORAGE_LOG_RELEASE_FLUSHQUERYLOCK_6DF2C0FC =
      "{}：{} release flushQueryLock";
  public static final String STORAGE_LOG_DELETION_DELETION_WITH_IN_WORKMEMTABLE_POINTS_DELETED_00EA995A =
      "[Deletion] Deletion with {} in workMemTable, {} points 已删除";
  public static final String STORAGE_LOG_SYNC_CLOSE_FILE_WILL_FIRSTLY_ASYNC_CLOSE_IT_34588A7D =
      "Sync 关闭文件：{}, 将firstly async 关闭it";
  public static final String STORAGE_LOG_ASYNC_FLUSH_A_MEMTABLE_TO_TSFILE_00ED383A =
      "Async flush a memtable to ts文件：{}";
  public static final String STORAGE_LOG_THIS_NORMAL_MEMTABLE_IS_EMPTY_SKIP_FLUSH_6C195557 =
      "Th是normal memtable 为空, skip flush. {}：{}";
  public static final String STORAGE_LOG_IS_CLOSED_DURING_FLUSH_ABANDON_FLUSH_TASK_DD47632F =
      "{}：{} 是closed during flush, abandon flush 任务";
  public static final String STORAGE_LOG_THE_COMPRESSION_RATIO_OF_TSFILE_IS_TOTALMEMTABLESIZE_THE_8CE66BE3 =
      "The compression ratio of ts文件 {} 是{}, totalMemTableSize：{}, the 文件 size：{}";
  public static final String STORAGE_LOG_STORAGE_GROUP_CLOSE_AND_REMOVE_EMPTY_FILE_72D42293 =
      "Storage group {} 关闭and 移除empty 文件 {}";
  public static final String STORAGE_LOG_PUT_THE_MEMTABLE_SIGNAL_OUT_OF_FLUSHINGMEMTABLES_BUT_IT_D78AF257 =
      "{}：{} put the memtable (signal={}) out of flushingMemtables but it 是not in the queue.";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSH_FILEMETADATA_TO_RETRY_IT_AGAIN_DAAF298C =
      "{} meet error when flush 文件Metadata to {}, retry it again";
  public static final String STORAGE_LOG_ASYNC_CLOSE_FAILED_BECAUSE_C5B63B78 =
      "{}：{} async 关闭failed, because";
  public static final String STORAGE_LOG_ADD_A_MEMTABLE_INTO_FLUSHING_LIST_FAILED_30FA8E58 =
      "{}：{} add a memtable into flushing list failed";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSHING_A_MEMTABLE_CHANGE_SYSTEM_MODE_TO_0C6D5025 =
      "{}：{} meet error when flushing a memtable, change system mode to error";
  public static final String STORAGE_LOG_IOTASK_MEETS_ERROR_TRUNCATE_THE_CORRUPTED_DATA_E9041D54 =
      "{}：{} IO任务 meets error, truncate the corrupted data";
  public static final String STORAGE_LOG_TRUNCATE_CORRUPTED_DATA_MEETS_ERROR_3757A85E =
      "{}：{} Truncate corrupted data meets error";
  public static final String STORAGE_LOG_RELEASE_RESOURCE_MEETS_ERROR_B62CBC3A =
      "{}：{} Release resource meets error";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_WRITING_INTO_MODIFICATIONFILE_FILE_OF_63B5E24A =
      "Meet error when writing into Modification文件 文件 of {} ";
  public static final String STORAGE_LOG_MARKING_OR_ENDING_FILE_MEET_ERROR_5653B904 =
      "{}：{} marking or ending 文件 meet error";
  public static final String STORAGE_LOG_TRUNCATE_CORRUPTED_DATA_MEETS_ERROR_8F721CC1 =
      "{}：{} truncate corrupted data meets error";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSH_FILEMETADATA_TO_CHANGE_SYSTEM_MODE_0BC79DA5 =
      "{} meet error when flush 文件Metadata to {}, change system mode to error";
  public static final String STORAGE_LOG_UPDATE_COMPRESSION_RATIO_FAILED_8A076DFC =
      "{}：{} update compression ratio failed";
  public static final String STORAGE_LOG_GET_READONLYMEMCHUNK_HAS_ERROR_2366DE2A =
      "{}：{} get ReadOnlyMemChunk has error";
  public static final String STORAGE_LOG_FAILED_TO_TRANSFER_TVLIST_MEMORY_OWNER_TO_QUERY_ENGINE_0DFA506D =
      "无法transfer tvlist 内存 owner to query engine, {}";
  public static final String STORAGE_LOG_THE_FORMAT_OF_MAX_POINT_NUMBER_IS_NOT_CORRECT_USING_DEFAULT_1B78AF69 =
      "The format of MAX_POINT_NUMBER {}  是not correct. Using default float precision.";
  public static final String STORAGE_LOG_THE_MAX_POINT_NUMBER_SHOULDN_T_BE_LESS_THAN_0_USING_DEFAULT_12745217 =
      "The MAX_POINT_NUMBER shouldn't be less than 0. Using default float precision {}.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MOD_FILE_EXPECTING_OFFSET_ACTUALLY_SKIPPED_8B96B670 =
      "无法读取Mod 文件 {}, expecting offset {}, actually skipped {}";
  public static final String STORAGE_LOG_AFTER_THE_MOD_FILE_IS_SETTLED_THE_FILE_SIZE_IS_STILL_GREATER_FA454979 =
      "After the mod 文件 是settled, the 文件 size 是still greater than 1M,the size of the 文件 before "
          + "settle 是{},after settled the 文件 size 是{}";
  public static final String STORAGE_LOG_THE_START_TIME_OF_IS_GREATER_THAN_END_TIME_44DD784A =
      "{} {} the start time of {} 是greater than end time";
  public static final String STORAGE_LOG_THERE_IS_NO_DATA_IN_THE_FILE_F480954E =
      "{} {} there 是no data in the 文件";
  public static final String STORAGE_LOG_CHUNK_START_OFFSET_IS_INCONSISTENT_WITH_THE_VALUE_IN_THE_E1E7AF07 =
      "{} chunk start offset 是inconsistent with the value in the metadata.";
  public static final String STORAGE_LOG_TIME_RANGES_OVERLAP_BETWEEN_PAGES_2A131465 =
      "{} {} time ranges overlap between pages.";
  public static final String STORAGE_LOG_THE_TIMESTAMP_IN_THE_PAGE_IS_REPEATED_OR_NOT_INCREMENTAL_04627FDA =
      "{} {} the timestamp in the page 是repeated or not incremental.";
  public static final String STORAGE_LOG_THE_START_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_C23CE8D4 =
      "{} {} the start time in page 是different from that in page header.";
  public static final String STORAGE_LOG_THE_END_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_5E363FAB =
      "{} {} the end time in page 是different from that in page header.";
  public static final String STORAGE_LOG_DEVICE_ID_IS_NULL_OR_EMPTY_635DD75C =
      "{} {} device id 是null or empty.";
  public static final String STORAGE_LOG_DEVICE_IS_OVERLAPPED_BETWEEN_AND_END_TIME_IN_IS_START_TIME_BA49D2AA =
      "Device {} 是overlapped between {} and {}, end time in {} 是{}, start time in {} 是{}";
  public static final String STORAGE_LOG_PATH_FILE_IS_NOT_SATISFIED_BECAUSE_OF_NO_DEVICE_8BB15136 =
      "Path：{} 文件 {} 是not satisfied 原因：of no device!";
  public static final String STORAGE_LOG_PATH_FILE_IS_NOT_SATISFIED_BECAUSE_OF_TIME_FILTER_71121709 =
      "Path：{} 文件 {} 是not satisfied 原因：of time filter!";
  public static final String STORAGE_LOG_STARTTIME_OF_TSFILERESOURCE_IS_GREATER_THAN_ITS_ENDTIME_BC6CC591 =
      "startTime[{}] of Ts文件Resource[{}] 是greater than its endTime[{}]";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_GETSTARTTIME_OF_IN_FILE_D7F27B92 =
      "meet error when getStartTime of {} in 文件 {}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_GETENDTIME_OF_IN_FILE_350DA42F =
      "meet error when getEndTime of {} in 文件 {}";
  public static final String STORAGE_LOG_CANNOT_SERIALIZE_TSFILERESOURCE_WHEN_UPDATING_PLAN_INDEX_69665DD5 =
      "无法序列化Ts文件Resource {} when updating plan index {}-{}";
  public static final String STORAGE_LOG_DATAREGIONSYSDIR_HAS_EXISTED_FILEPATH_53009475 =
      "DataRegionSysDir has existed，文件Path:{}";
  public static final String STORAGE_LOG_FILETIMEINDEX_LOG_FILE_CREATE_FILED_FILEPATH_D675FBD5 =
      "文件TimeIndex log 文件 创建文件d，文件Path:{}";
  public static final String STORAGE_LOG_CAN_T_READ_FILE_FROM_DISK_F5625609 =
      "Can't 读取文件 {} from disk ";
  public static final String STORAGE_LOG_FAILED_TO_GET_DEVICES_FROM_TSFILE_F94CF47B =
      "无法get devices from ts文件：{}";
  public static final String STORAGE_LOG_TABLEDISKUSAGEINDEX_WORKER_THREAD_WAS_INTERRUPTED_UNEXPECTEDLY_A21172AB =
      "TableDiskUsageIndex worker th读取was 被中断 unexpectedly while waiting for operations.";
  public static final String STORAGE_LOG_SKIP_ADDING_OPERATION_TO_QUEUE_BECAUSE_TABLEDISKUSAGEINDEX_4A606B40 =
      "Skip adding operation {} to queue 原因：TableDiskUsageIndex 已已停止.";
  public static final String STORAGE_LOG_ATTEMPT_TO_DECREASE_ACTIVEREADERNUM_WHEN_IT_IS_ALREADY_0_73756CBB =
      "Attempt to decrease activeReaderNum when it 是already 0. Th是may indicate an incorrect reader "
          + "lifecycle management.";
  public static final String STORAGE_LOG_FAILED_TO_DELETE_OLD_VERSION_TABLE_SIZE_INDEX_FILE_05930C4A =
      "无法删除old version table size index 文件 {}";
  public static final String STORAGE_LOG_FAILED_TO_READ_TABLE_TSFILE_SIZE_INDEX_AFTER_POSITION_AND_74251AF3 =
      "无法读取table ts文件 size index {} after position：{} and {} after position：{}";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_COMPACTION_SCHEDULE_IS_INTERRUPTED_9EF702D1 =
      "[CompactionSchedule任务Worker-{}] compaction schedule 是被中断, isStopByUser：{}";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_FAILED_TO_EXECUTE_COMPACTION_4F302761 =
      "[CompactionSchedule任务Worker-{}] 无法execute compaction schedule 任务";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_FAILED_TO_EXECUTE_COMPACTION_E571F6E3 =
      "[CompactionSchedule任务Worker-{}] 无法execute compaction schedule 任务 and cannot recover";
  public static final String STORAGE_LOG_COMPACTION_SCHEDULE_TASK_THREAD_POOL_CAN_NOT_BE_CLOSED_IN_27D38188 =
      "compaction schedule 任务 th读取pool can not be closed in {} ms";
  public static final String STORAGE_LOG_TTLCHECKTASK_TTL_CHECKER_IS_INTERRUPTED_ISSTOPPEDBYUSER_B1E45A2E =
      "[TTLCheck任务-{}] TTL checker 是被中断, is已停止ByUser：{}";
  public static final String STORAGE_LOG_TTLCHECKTASK_FAILED_TO_EXECUTE_TTL_CHECK_AND_CANNOT_RECOVER_6F4E4A13 =
      "[TTLCheck任务-{}] 无法execute ttl check and cannot recover";
  public static final String STORAGE_LOG_COMPACTION_TASK_START_CHECK_FAILED_BECAUSE_DISK_FREE_RATIO_9D2BE2FE =
      "Compaction 任务 start check failed 原因：disk free ratio 是less than disk_space_warning_threshold";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_ADDING_TASK_TO_COMPACTION_WAITING_QUEUE_84AA345D =
      "meet error when adding 任务-{} to compaction waiting queue：{}";
  public static final String STORAGE_LOG_SETTLE_COMPLETES_FILE_PATH_THE_REMAINING_FILE_TO_BE_SETTLED_32DF95A7 =
      "Settle completes, 文件 path:{} , the remaining 文件 to be settled num：{}";
  public static final String STORAGE_LOG_THE_TSFILE_SHOULD_BE_SEALED_WHEN_SETTLING_8DBD716A =
      "The ts文件 {} should be sealed when settling.";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_SETTLING_FILE_CBA0F9D7 =
      "meet error when settling 文件:{}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_READ_TSFILE_RESOURCE_FILE_IT_MAY_BE_REPAIRED_A8A514C6 =
      "Meet error when 读取ts文件 resource 文件 {}, it may be repaired after reboot";
  public static final String STORAGE_LOG_FILE_HAS_UNSORTED_DATA_1B118A14 =
      "文件 {} has unsorted data：";
  public static final String STORAGE_LOG_FILE_HAS_WRONG_TIME_STATISTICS_4E63345E =
      "文件 {} has wrong time statistics：";
  public static final String STORAGE_LOG_DEVICE_HAS_OVERLAPPED_DATA_START_TIME_IN_CURRENT_FILE_IS_F4F29A22 =
      "Device {} has overlapped data, start time in 当前文件 {} 是{}, end time in previous 文件 {} 是{}";
  public static final String STORAGE_LOG_REPAIR_DATA_LOG_IS_NOT_COMPLETE_TIME_PARTITION_IS_D9D4F01F =
      "[{}][{}]Repair data log 是not complete, time partition 是{}.";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_RECOVER_UNFINISHED_REPAIR_SCHEDULE_TASK_7C5B6D5F =
      "[RepairScheduler] 恢复unfinished repair schedule 任务 from log 文件：{}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_SKIP_REPAIR_TIME_PARTITION_BECAUSE_IT_IS_BDD35739 =
      "[RepairScheduler][{}][{}] skip repair time partition {} 原因：it 是repaired";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_SUBMIT_A_REPAIR_TIME_PARTITION_SCAN_TASK_0E98F12C =
      "[RepairScheduler] submit a repair time partition scan 任务 {}-{}-{}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_PARSE_REPAIR_LOG_FILE_142D2568 =
      "[RepairScheduler] 无法parse repair log 文件 {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_TASK_START_TIME_95552D7E =
      "[RepairScheduler] 无法record repair 任务 start time in log 文件 {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_CLOSE_REPAIR_LOGGER_EC191F6B =
      "[RepairScheduler] 无法关闭repair logger {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_START_SCAN_REPAIR_TIME_PARTITION_1D6789DB =
      "[RepairScheduler][{}][{}] start scan repair time partition {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_CANNOT_SCAN_SOURCE_FILES_IN_BECAUSE_ALLOWCOMPACTION_5E644A6D =
      "[RepairScheduler] cannot scan source 文件s in {} 原因：'allowCompaction' 是false";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_HAS_INTERNAL_UNSORTED_C1596DC3 =
      "[RepairScheduler] {} need to repair 原因：it has internal unsorted data";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_IS_OVERLAPPED_F1AC0C78 =
      "[RepairScheduler] {} need to repair 原因：it 是overlapped with other 文件s";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_TIME_PARTITION_HAS_BEEN_REPAIRED_PROGRESS_697FEA22 =
      "[RepairScheduler][{}][{}] time partition {} 已repaired, progress：{}/{}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_LOG_FOR_TIME_PARTITION_11251247 =
      "[RepairScheduler][{}][{}] 无法record repair log for time partition {}";
  public static final String STORAGE_LOG_COMPACTION_TMP_TARGET_TSFILE_MAY_BE_DELETED_AFTER_COMPACTION_0BFFA73F =
      "{} [Compaction] Tmp target ts文件 {} may be 已删除 after compaction.";
  public static final String STORAGE_LOG_COMPACTION_DELETE_TSFILE_A97320DB =
      "{} [Compaction] 删除Ts文件 {}";
  public static final String STORAGE_LOG_COMPACTION_DELETE_FILE_FAILED_FILE_PATH_IS_6E1D2670 =
      "[Compaction] 删除文件 failed, 文件 path 是{}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_RECOVER_COMPACTION_TASKINFO_24424402 =
      "{} [Compaction][Recover] 无法恢复compaction. 任务Info：{}, Exception：{}";
  public static final String STORAGE_LOG_FAILED_TO_PASS_COMPACTION_VALIDATION_SOURCE_SEQ_FILES_SOURCE_BF5A4525 =
      "无法pass compaction validation, source seq 文件s：{}, source unseq 文件s：{}, target 文件s：{}";
  public static final String STORAGE_LOG_FAILED_TO_PASS_COMPACTION_OVERLAP_VALIDATION_SOURCE_SEQ_9CFDC149 =
      "无法pass compaction overlap validation, source seq 文件s：{}, source unseq 文件s：{}, target 文件s：{}";
  public static final String STORAGE_LOG_COMPACTION_TASK_INTERRUPTED_E31121C0 =
      "{}-{} [Compaction] {} 任务 被中断";
  public static final String STORAGE_LOG_COMPACTION_TASK_MEETS_ERROR_1002C659 =
      "{}-{} [Compaction] {} 任务 meets error：{}.";
  public static final String STORAGE_LOG_UNEXPECTED_CHUNK_TYPE_DETECTED_WHEN_READING_NON_ALIGNED_1C0E4674 =
      "Unexpected chunk type detected when reading non-aligned chunk reader. The chunk metadata "
          + "indicates a non-aligned chunk, but the actual chunk 读取from ts文件 是a value chunk of aligned "
          + "series. ts文件={}, device={}, measurement={}, offsetOfChunkHeader={}";
  public static final String STORAGE_LOG_INNERSPACECOMPACTIONTASK_START_TO_RENAME_MODS_FILE_7C036CBD =
      "{}-{} [InnerSpaceCompaction任务] 开始rename mods 文件";
  public static final String STORAGE_LOG_REPAIRUNSORTEDFILECOMPACTIONTASK_CAN_NOT_REPAIR_UNSORTED_48124B0C =
      "[RepairUnsorted文件Compaction任务] 无法repair unsorted 文件 {} 原因：the required 内存 to repair "
          + "是greater than the total compaction 内存 budget";
  public static final String STORAGE_LOG_COMPACTION_INNERSPACECOMPACTION_TASK_STARTS_WITH_FILES_TOTAL_934B562F =
      "{}-{} [Compaction] {} InnerSpaceCompaction 任务 开始 with {} 文件s, total 文件 size 是{} MB, "
          + "estimated 内存 cost 是{} MB";
  public static final String STORAGE_LOG_COMPACTION_COMPACTION_WITH_SELECTED_FILES_SKIPPED_FILES_ACC66872 =
      "{}-{} [Compaction] compaction with selected 文件s {}, skipped 文件s {}";
  public static final String STORAGE_LOG_COMPACTION_INNERSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_08475DE4 =
      "{}-{} [Compaction] {} InnerSpaceCompaction 任务 finishes 成功, target 文件s 是{},time cost 是{} s, "
          + "compaction speed 是{} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_INSERTIONCROSSSPACECOMPACTION_TASK_STARTS_WITH_A315B8C6 =
      "{}-{} [Compaction] InsertionCrossSpaceCompaction 任务 开始 with unseq 文件 {}, nearest seq 文件s "
          + "是{}, target 文件 name timestamp 是{}, 文件 size 是{} MB.";
  public static final String STORAGE_LOG_COMPACTION_INSERTIONCROSSSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_69360DD0 =
      "{}-{} [Compaction] InsertionCrossSpaceCompaction 任务 finishes 成功, target 文件 是{},time cost "
          + "是{} s.";
  public static final String STORAGE_LOG_INSERTIONCROSSSPACECOMPACTIONTASK_FAILED_TO_GENERATE_TARGET_B03E4C67 =
      "{}-{} [InsertionCrossSpaceCompaction任务] 无法generate target 文件 name, source unseq 文件 是{}";
  public static final String STORAGE_LOG_SETTLE_TASK_DELETES_FULLY_DIRTY_TSFILE_SUCCESSFULLY_18D81225 =
      "Settle 任务 deletes fully_dirty ts文件 {} 成功.";
  public static final String STORAGE_LOG_COMPACTION_SETTLE_COMPACTION_FILE_LIST_IS_EMPTY_END_IT_56CF079D =
      "{}-{} [Compaction] Settle compaction 文件 list 为空, end it";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_STARTS_WITH_FULLY_DIRTY_0962C95A =
      "{}-{} [Compaction] SettleCompaction 任务 开始 with {} fully_dirty 文件s and {} partially_dirty "
          + "文件s. Fully_dirty 文件s ：{}, partially_dirty 文件s ：{}。 Fully_dirty 文件s size 是{} MB, "
          + "partially_dirty 文件 size 是{} MB. 内存 cost 是{} MB.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_SUCCESSFULLY_TIME_2BD3839A =
      "{}-{} [Compaction] SettleCompaction 任务 finishes 成功, time cost 是{} s.Fully_dirty 文件s num 是{}.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_SUCCESSFULLY_TIME_4FEB0F56 =
      "{}-{} [Compaction] SettleCompaction 任务 finishes 成功, time cost 是{} s, compaction speed 是{} "
          + "MB/s.Fully_dirty 文件s num 是{} and partially_dirty 文件s num 是{}.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_WITH_SOME_ERROR_A8A15439 =
      "{}-{} [Compaction] SettleCompaction 任务 finishes with some error, time cost 是{} "
          + "s.Fully_dirty 文件s num 是{} and there 是{} 文件s 无法delete.";
  public static final String STORAGE_LOG_COMPACTION_START_TO_SETTLE_PARTIALLY_DIRTY_FILES_TOTAL_FILE_BAC113C4 =
      "{}-{} [Compaction] 开始settle {} {} partially_dirty 文件s, total 文件 size 是{} MB";
  public static final String STORAGE_LOG_COMPACTION_FINISH_TO_SETTLE_PARTIALLY_DIRTY_FILES_SUCCESSFULLY_9ACFD5C0 =
      "{}-{} [Compaction] Finish to settle {} {} partially_dirty 文件s 成功 , target 文件 是{},time cost "
          + "是{} s, compaction speed 是{} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_START_TO_RECOVER_SETTLE_COMPACTION_C342241D =
      "{}-{} [Compaction][Recover] 开始恢复settle compaction.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FINISH_TO_RECOVER_SETTLE_COMPACTION_SUCCESSFULLY_714EF642 =
      "{}-{} [Compaction][Recover] Finish to 恢复settle compaction 成功.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_IS_DF6FD183 =
      "{}-{} [Compaction][Recover] compaction log 是{}";
  public static final String STORAGE_LOG_SETTLE_TASK_FAIL_TO_DELETE_FULLY_DIRTY_TSFILE_B7DAEA8D =
      "Settle 任务 无法删除fully_dirty ts文件 {}.";
  public static final String STORAGE_LOG_COMPACTION_CROSS_SPACE_COMPACTION_FILE_LIST_IS_EMPTY_END_B8044743 =
      "{}-{} [Compaction] Cross space compaction 文件 list 为空, end it";
  public static final String STORAGE_LOG_COMPACTION_CROSSSPACECOMPACTION_TASK_STARTS_WITH_SEQ_FILES_8CDCBE0F =
      "{}-{} [Compaction] CrossSpaceCompaction 任务 开始 with {} seq 文件s and {} unsequence 文件s. "
          + "Sequence 文件s ：{}, unsequence 文件s ：{}。 Sequence 文件s size 是{} MB, unsequence 文件 size 是{} MB, "
          + "total size 是{} MB";
  public static final String STORAGE_LOG_COMPACTION_CROSSSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_D7F1B1FD =
      "{}-{} [Compaction] CrossSpaceCompaction 任务 finishes 成功, time cost 是{} s, compaction speed "
          + "是{} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_IN_DATA_REGION_DIR_ABD144CC =
      "{} [Compaction][Recover] 恢复compaction in data Region dir {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_IN_TIME_PARTITION_FA2FC44D =
      "{} [Compaction][Recover] 恢复compaction in time partition dir {}";
  public static final String STORAGE_LOG_RECOVER_MODS_FILE_ERROR_ON_DELETE_ORIGIN_FILE_OR_RENAME_7033152A =
      "恢复mods 文件 error on 删除origin 文件 or rename mods settle,";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_IS_0C57C7DA =
      "{} [Compaction][Recover] compaction log 是{}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_FILE_EXISTS_START_TO_RECOVER_74836930 =
      "{} [Compaction][Recover] compaction log 文件 {} exists, 开始恢复it";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_INCOMPLETE_LOG_FILE_ABORT_RECOVER_46472E7C =
      "{} [Compaction][Recover] incomplete log 文件, abort recover";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_SUCCESSFULLY_DELETE_8451AEFB =
      "{} [Compaction][Recover] 恢复compaction 成功, 删除log 文件 {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_ALL_SOURCE_FILES_EXISTS_DELETE_ALL_TARGET_79954E60 =
      "{} [Compaction][Recover] all source 文件s exists, 删除all target 文件s.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_EXCEPTION_OCCURS_WHILE_DELETING_LOG_FILE_49A24E1D =
      "{} [Compaction][Recover] Exception occurs while deleting log 文件 {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_REMOVE_TARGET_FILE_35A1E718 =
      "{} [Compaction][Recover] 无法移除target 文件 {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_EXCEPTION_OCCURS_WHILE_DELETING_COMPACTION_218A56FB =
      "{} [Compaction][Recover] Exception occurs while deleting compaction mods 文件";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_TARGET_FILE_IS_NOT_COMPLETE_865ADA73 =
      "{} [Compaction][ExceptionHandler] target 文件 {} 是not complete, and some source 文件s 是lost, do "
          + "nothing.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_REMOVE_FILE_EXCEPTION_67CEA8E7 =
      "{} [Compaction][Recover] 无法移除文件 {}, exception：{}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_SPACE_COMPACTION_START_HANDLING_1B55549F =
      "{} [Compaction][ExceptionHandler] {} space compaction start handling exception, source "
          + "seq文件s 是{}, source unseq文件s 是{}.";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_FAIL_TO_HANDLE_SPACE_COMPACTION_B21F170F =
      "[Compaction][ExceptionHandler] 无法handle {} space compaction exception, storage group 是{}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_EXCEPTION_OCCURS_WHEN_HANDLING_B6C9751E =
      "[Compaction][ExceptionHandler] exception occurs when handling exception in {} space "
          + "compaction. storage group 是{}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTION_FAIL_TO_DELETE_TARGET_TSFILE_WHEN_HANDLING_DC19DC8A =
      "{} [Compaction][Exception] 无法删除target ts文件 {} when handling exception";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_TARGET_FILE_IS_NOT_COMPLETE_91E81106 =
      "{} [Compaction][ExceptionHandler] target 文件 {} 是not complete, and some source 文件s {} 是lost, "
          + "do nothing.";
  public static final String STORAGE_LOG_BATCH_COMPACTION_CURRENT_DEVICE_IS_FIRST_BATCH_COMPACTED_34910754 =
      "[Batch Compaction] 当前device 是{}, first batch compacted time chunk 是{}";
  public static final String STORAGE_LOG_ADD_TSFILE_CURRENT_SELECT_FILE_NUM_IS_SIZE_IS_17E21BC9 =
      "Add ts文件 {}, 当前select 文件 num 是{}, size 是{}";
  public static final String STORAGE_LOG_SELECTING_CROSS_COMPACTION_TASK_RESOURCES_FROM_SEQFILE_UNSEQFILES_F4E1ABEB =
      "Selecting cross compaction 任务 resources from {} seq文件, {} unseq文件s";
  public static final String STORAGE_LOG_SELECTING_INSERTION_CROSS_COMPACTION_TASK_RESOURCES_FROM_ECB186D1 =
      "Selecting insertion cross compaction 任务 resources from {} seq文件, {} unseq文件s";
  public static final String STORAGE_LOG_ADDING_A_NEW_UNSEQFILE_AND_SEQFILES_AS_CANDIDATES_NEW_COST_07DD0A10 =
      "Adding a new unseq文件 {} and seq文件s {} as candidates, new cost {}, total cost {}";
  public static final String STORAGE_LOG_SELECT_ONE_VALID_SEQ_FILE_FOR_NONOVERLAP_UNSEQ_FILE_TO_COMPACT_456668F1 =
      "Select one valid seq 文件 {} for nonOverlap unseq 文件 to compact with.";
  public static final String STORAGE_LOG_TOTAL_SOURCE_FILES_SEQFILES_UNSEQFILES_CANDIDATE_SOURCE_7511ED9E =
      "{} [{}] Total source 文件s：{} seq文件s, {} unseq文件s. Candidate source 文件s：{} seq文件s, {} "
          + "unseq文件s. 无法select any 文件s 原因：they do not meet the conditions or may be occupied by other "
          + "compaction 线程s.";
  public static final String STORAGE_LOG_TOTAL_SOURCE_FILES_SEQFILES_UNSEQFILES_CANDIDATE_SOURCE_B8B01FC4 =
      "{} [{}] Total source 文件s：{} seq文件s, {} unseq文件s. Candidate source 文件s：{} seq文件s, {} "
          + "unseq文件s. Selected source 文件s：{} seq文件s, {} unseq文件s, estimated 内存 cost {} MB, total "
          + "selected 文件 size 是{} MB, total selected seq 文件 size 是{} MB, total selected unseq 文件 size "
          + "是{} MB, time consumption {}ms.";
  public static final String STORAGE_LOG_CANNOT_SELECT_FILE_FOR_SETTLE_COMPACTION_08C958D3 =
      "{}-{} cannot select 文件 for settle compaction";
  public static final String STORAGE_LOG_HAS_NULL_CHUNK_METADATA_FILE_IS_819E4A49 =
      "{} has null chunk metadata, 文件 是{}";
  public static final String STORAGE_LOG_MODIFICATIONS_SIZE_IS_FOR_FILE_PATH_EED7FD92 =
      "Modifications size 是{} for 文件 Path：{} ";
  public static final String STORAGE_LOG_AN_ERROR_OCCURRED_WHEN_TRUNCATING_MODIFICATIONS_TO_SIZE_F8A0D6D5 =
      "An error occurred when truncating modifications[{}] to size {}.";
  public static final String STORAGE_LOG_FAIL_TO_FSYNC_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_6E1EE226 =
      "无法fsync wal node-{}'s checkpoint writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_ROLL_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_791DDAB7 =
      "无法roll wal node-{}'s checkpoint writer, change system mode to error.";
  public static final String STORAGE_LOG_UNEXPECTED_ERROR_WHEN_LOADING_A_WAL_SEGMENT_IN_45B42CCF =
      "Unexpected error when loading a wal segment {} in {}@{}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_READING_CHECKPOINT_FILE_SKIP_BROKEN_CHECKPOINTS_DADF9E9D =
      "Meet error when reading checkpoint 文件 {}, skip broken checkpoints";
  public static final String STORAGE_LOG_FAILED_TO_SCAN_WAL_FILE_FOR_SEARCHABLE_REQUEST_METADATA_9B4B0198 =
      "无法scan WAL 文件 {} for searchable 请求 metadata";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTROWNODE_THE_SEARCH_INDEX_IS_027450AC =
      "WAL node-{} logs insertRowNode, the search index 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTROWSNODE_THE_SEARCH_INDEX_IS_1AF72E25 =
      "WAL node-{} logs insertRowsNode, the search index 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTTABLETNODE_THE_SEARCH_INDEX_IS_CF9A3600 =
      "WAL node-{} logs insertTabletNode, the search index 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_DELETEDATANODE_THE_SEARCH_INDEX_IS_6E49BC54 =
      "WAL node-{} logs deleteDataNode, the search index 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_RELATIONALDELETEDATANODE_THE_SEARCH_INDEX_33258B30 =
      "WAL node-{} logs relationalDeleteDataNode, the search index 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_NO_WAL_FILE_OR_WAL_FILE_NUMBER_LESS_THAN_OR_EQUAL_3C65641C =
      "wal node-{}:no wal 文件 or wal 文件 number less than or equal to one was found";
  public static final String STORAGE_LOG_EFFECTIVE_INFORMATION_RATIO_IS_ACTIVE_MEMTABLES_COST_IS_D9A13DD2 =
      "Effective information ratio 是{}, active memTables cost 是{}, total cost 是{}";
  public static final String STORAGE_LOG_SUCCESSFULLY_DELETE_OUTDATED_WAL_FILES_FOR_WAL_NODE_C141C741 =
      "成功删除{} outdated wal 文件s for wal node-{}";
  public static final String STORAGE_LOG_UPDATE_FILE_TO_SEARCH_FAILED_THE_NEXT_SEARCH_INDEX_IS_F3DC95F3 =
      "update 文件 to search failed, the next search index 是{}";
  public static final String STORAGE_LOG_SEARCHINDEX_RESULT_FILES_6151DCEB =
      "searchIndex：{}, result：{}, 文件s：{}, ";
  public static final String STORAGE_LOG_FAIL_TO_DELETE_OUTDATED_WAL_FILE_OF_WAL_NODE_1B1F2AF2 =
      "无法删除outdated wal 文件 {} of wal node-{}.";
  public static final String STORAGE_LOG_WAL_NODE_FLUSHES_MEMTABLE_TO_TSFILE_BECAUSE_EFFECTIVE_INFORMATION_8CC86239 =
      "WAL node-{} flushes memTable-{} to Ts文件 {} 原因：Effective information ratio {} 是below wal min "
          + "effective info ratio {}, memTable size 是{}.";
  public static final String STORAGE_LOG_WAL_NODE_SNAPSHOTS_MEMTABLE_TO_WAL_FILES_BECAUSE_EFFECTIVE_0A1304ED =
      "WAL node-{} snapshots memTable-{} to wal 文件s 原因：Effective information ratio {} 是below wal "
          + "min effective info ratio {}, memTable size 是{}.";
  public static final String STORAGE_LOG_TIMEOUT_WHEN_WAITING_FOR_NEXT_WAL_ENTRY_READY_EXECUTE_ROLLWALFILE_FEE9700E =
      "超时 when waiting for next WAL entry ready, execute rollWAL文件. 当前search index in wal buffer "
          + "是{}, and next target index 是{}";
  public static final String STORAGE_LOG_THE_SEARCH_INDEX_OF_NEXT_WAL_ENTRY_SHOULD_BE_BUT_ACTUALLY_177BF8AF =
      "The search index of next WAL entry should be {}, but actually it's {}";
  public static final String STORAGE_LOG_SKIP_FROM_TO_IT_S_A_DANGEROUS_OPERATION_BECAUSE_INSERT_PLAN_9283DC91 =
      "Skip from {} to {}, it's a dangerous operation 原因：insert plan {} may 已lost.";
  public static final String STORAGE_LOG_FAIL_TO_READ_WAL_FROM_WAL_FILE_SKIP_THIS_FILE_06A3B079 =
      "无法读取wal from wal 文件 {}, skip th是文件.";
  public static final String STORAGE_LOG_FAIL_TO_TRIGGER_ROLLING_WAL_NODE_S_WAL_FILE_LOG_WRITER_D1E595DC =
      "无法trigger rolling wal node-{}'s wal 文件 log writer.";
  public static final String STORAGE_LOG_FAIL_TO_FIND_TSFILE_RECOVER_PERFORMER_FOR_WAL_ENTRY_IN_TSFILE_ED4EF3E7 =
      "无法find Ts文件 恢复performer for wal entry in Ts文件 {}";
  public static final String STORAGE_LOG_SUCCESSFULLY_RECOVER_WAL_NODE_IN_THE_DIRECTORY_ADD_THIS_FA6ADE22 =
      "成功恢复WAL node in the 目录 {}, add th是node to WALManger.";
  public static final String STORAGE_LOG_SUCCESSFULLY_RECOVER_WAL_NODE_IN_THE_DIRECTORY_SO_DELETE_A17892D9 =
      "成功恢复WAL node in the 目录 {}, so 删除these wal 文件s.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_5325B5AB =
      "无法读取memTable ids from the wal 文件 {} of wal node：{}";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_FBCE8D93 =
      "无法读取memTable ids from the wal 文件 {} of wal node.";
  public static final String STORAGE_LOG_DATA_REGIONS_HAVE_SUBMITTED_ALL_UNSEALED_TSFILES_START_RECOVERING_208E6A26 =
      "Data Regions have submitted all unsealed Ts文件s, start recovering Ts文件s in each wal node.";
  public static final String STORAGE_LOG_FAIL_TO_ADD_RECOVER_PERFORMER_FOR_FILE_54746E05 =
      "无法add 恢复performer for 文件 {}";
  public static final String STORAGE_LOG_BUFFER_CAPACITY_IS_LIMIT_IS_POSITION_IS_911625D8 =
      "buffer capacity is：{}, limit is：{}, position is：{}";
  public static final String STORAGE_LOG_HANDLE_CLOSE_SIGNAL_FOR_WAL_NODE_THERE_ARE_ENTRIES_LEFT_393393D0 =
      "Handle 关闭signal for wal node-{}, there 是{} entries left.";
  public static final String STORAGE_LOG_SYNC_WAL_BUFFER_FORCEFLAG_BUFFER_USED_C2A75C99 =
      "Sync wal buffer, forceFlag：{}, buffer used：{} / {} = {}%";
  public static final String STORAGE_LOG_FAIL_TO_WRITE_WALENTRY_INTO_WAL_NODE_BECAUSE_THIS_NODE_IS_5D45E73F =
      "无法写入WALEntry into wal node-{} 原因：th是node 是closed. It's ok to see th是log during data Region "
          + "deletion.";
  public static final String STORAGE_LOG_INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM_BLOCKING_0765C068 =
      "被中断 when waiting for taking WALEntry from blocking queue to serialize.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_54B0056E =
      "无法读取memTable ids from the wal 文件 {} of wal node {}：{}";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_D5287E27 =
      "无法读取memTable ids from the wal 文件 {} of wal node {}.";
  public static final String STORAGE_LOG_FAIL_TO_SERIALIZE_WALENTRY_TO_WAL_NODE_S_BUFFER_DISCARD_F0948835 =
      "无法序列化WALEntry to wal node-{}'s buffer, discard it.";
  public static final String STORAGE_LOG_FAIL_TO_SYNC_WAL_NODE_S_BUFFER_CHANGE_SYSTEM_MODE_TO_ERROR_8C379D57 =
      "无法sync wal node-{}'s buffer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_ROLL_WAL_NODE_S_LOG_WRITER_CHANGE_SYSTEM_MODE_TO_A384AA54 =
      "无法roll wal node-{}'s log writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_FSYNC_WAL_NODE_S_LOG_WRITER_CHANGE_SYSTEM_MODE_TO_7930160B =
      "无法fsync wal node-{}'s log writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_CREATE_WAL_NODE_ALLOCATION_STRATEGY_BECAUSE_ALL_72801644 =
      "无法创建wal node allocation strategy 原因：all disks of wal 目录s 是full.";
  public static final String STORAGE_LOG_THIS_TSFILE_ISN_T_CRASHED_NO_NEED_TO_REDO_WAL_LOG_A017A0F0 =
      "Th是Ts文件 {} isn't crashed, no need to redo wal log.";
  public static final String STORAGE_LOG_CANNOT_DESERIALIZE_RESOURCE_FILE_OF_TRY_TO_RECONSTRUCT_IT_F82299C6 =
      "无法de序列化.resource 文件 of {}, try to reconstruct it.";
  public static final String STORAGE_LOG_TRY_TO_RELEASE_MEMORY_FROM_A_MEMORY_BLOCK_WHICH_HAS_NOT_874E7A08 =
      "Try to release 内存 from a 内存 block {} which has not released all 内存";
  public static final String STORAGE_LOG_TRY_TO_SHRINK_A_NEGATIVE_MEMORY_SIZE_FROM_MEMORY_BLOCK_60501B13 =
      "Try to shrink a negative 内存 size {} from 内存 block {}";
  public static final String STORAGE_LOG_LOAD_FORCE_RESIZED_LOADTSFILEMEMORYBLOCK_WITH_MEMORY_FROM_33AC288A =
      "Load：Force resized LoadTs文件内存Block with 内存 from query engine, size added：{}, new size：{}";
  public static final String STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_ALLOCATED_MEMORYBLOCK_44D5B5FB =
      "Load：Query engine's 内存 是not sufficient, allocated 内存Block from DataCache内存Block, size：{}";
  public static final String STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_FORCE_RESIZED_9F85F4CA =
      "Load：Query engine's 内存 是not sufficient, force resized LoadTs文件内存Block with 内存 from "
          + "DataCache内存Block, size added：{}, new size：{}";
  public static final String STORAGE_LOG_CREATE_DATA_CACHE_MEMORY_BLOCK_ALLOCATE_MEMORY_5F3E041D =
      "创建Data Cache 内存 Block {}, allocate 内存 {}";
  public static final String STORAGE_LOG_LOAD_ATTEMPTING_TO_RELEASE_MORE_MEMORY_THAN_ALLOCATED_0E737996 =
      "Load：Attempting to release more 内存 ({}) than allocated ({})";
  public static final String STORAGE_LOG_LOAD_FAILED_TO_SETTOTALMEMORYSIZEINBYTES_MEMORY_BLOCK_TO_DBE9BE56 =
      "Load：无法setTotal内存SizeInBytes 内存 block {} to {} bytes, 当前内存 usage {} bytes";
  public static final String STORAGE_LOG_DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL_99016326 =
      "Data type conversion for LoadTs文件Statement {} 是successful.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_5D132E57 =
      "无法convert data type for LoadTs文件Statement：{}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_STATUS_F0311707 =
      "无法convert data type for LoadTs文件Statement：{}, status code 是{}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPES_FOR_TABLE_MODEL_STATEMENT_CB574D44 =
      "无法convert data types for table model statement {}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPES_FOR_TREE_MODEL_STATEMENT_5C2869D6 =
      "无法convert data types for tree model statement {}.";
  public static final String STORAGE_LOG_LOAD_INSERTING_TABLET_TO_CASTING_TYPE_FROM_TO_AE808A8B =
      "Load：Inserting tablet to {}.{}. Casting type from {} to {}.";
  public static final String STORAGE_LOG_TRY_TO_LOAD_TSFILE_V3_INTO_CURRENT_VERSION_V4_FILE_PATH_B8D38E22 =
      "try to 加载Ts文件 V3 into 当前version (V4), 文件 path：{}";
  public static final String STORAGE_LOG_THE_FILE_S_VERSION_NUMBER_IS_HIGHER_THAN_CURRENT_FILE_PATH_6D17349F =
      "the 文件's Version Number 是higher than current, 文件 path：{}";
  public static final String STORAGE_LOG_FAILED_TO_FIND_MOUNT_POINT_SKIP_REGISTER_IT_TO_MAP_33F38542 =
      "无法find mount point {}, skip 注册it to map";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_READING_DATA_DIR_S_MOUNT_POINT_9421E685 =
      "在以下过程发生异常：reading data dir's mount point {}";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_READING_TARGET_FILE_S_MOUNT_POINT_47567945 =
      "在以下过程发生异常：reading target 文件's mount point {}";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_MEMORY_9A60DF29 =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于内存 constraints, 将retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_16FA5F18 =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于the system 是读取only, 将retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_TIME_E18630DE =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于time out to 等待procedure return, 将retry "
          + "later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_5F811A8B =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于the datanode 是not enough, 将retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FAIL_F59307B8 =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于无法connect to any config node, 将retry "
          + "later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_CURRENT_264E12EE =
      "Rejecting auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于当前query 是time out, 将retry later.";
  public static final String STORAGE_LOG_SUCCESSFULLY_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_ADB5FEC9 =
      "成功auto 加载ts文件 {} (isGeneratedByPipe = {})";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_CREATING_FAIL_DIRECTORY_FOR_ACTIVE_7D3BEB38 =
      "在以下过程发生错误：creating fail 目录 {} for active load.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_STATUS_FILE_F43E9EF7 =
      "无法auto 加载ts文件 {} (isGeneratedByPipe = {}), status：{}. 文件 将be moved to fail 目录.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FILE_5EE1FA08 =
      "无法auto 加载ts文件 {} (isGeneratedByPipe = {}) 由于文件 未找到, 将skip th是文件.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_BECAUSE_OF_07946D74 =
      "无法auto 加载ts文件 {} (isGeneratedByPipe = {}) 原因：of an unexpected exception. 文件 将be moved to "
          + "fail 目录.";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_HOT_RELOAD_ACTIVE_LOAD_DIRS_CURRENT_673AFC0F =
      "在以下过程发生错误：hot re加载active 加载dirs. 当前active 加载listening dirs：{}.";
  public static final String STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_READABLE_SKIP_SCANNING_THIS_DIR_9C8B7E00 =
      "当前dir path 是not readable：{}.Skip scanning th是dir. Please check the permission.";
  public static final String STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_WRITABLE_SKIP_SCANNING_THIS_DIR_4885E78F =
      "当前dir path 是not writable：{}.Skip scanning th是dir. Please check the permission.";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_CHECKING_R_W_PERMISSION_OF_DIR_SKIP_3EC7FC7D =
      "在以下过程发生错误：checking r/w permission of dir：{}. Skip scanning th是dir.";
  public static final String STORAGE_LOG_REPORT_DATABASE_STATUS_TO_THE_SYSTEM_AFTER_ADDING_CURRENT_8982BBD7 =
      "Report 数据库 Status to the system. After adding {}, 当前sg mem cost 是{}.";
  public static final String STORAGE_LOG_THE_TOTAL_DATABASE_MEM_COSTS_ARE_TOO_LARGE_CALL_FOR_FLUSHING_26AD8CDF =
      "The total 数据库 mem costs 是too large, call for flushing. 当前sg cost 是{}";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_STILL_EXCEEDING_FLUSH_PROPORTION_DB68D9D5 =
      "SG ({}) released 内存 (delta：{}) but still exceeding flush proportion (totalSgMemCost：{}), "
          + "call flush.";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SYSTEM_IS_IN_NORMAL_STATUS_TOTALSGMEMCOST_600A4A8D =
      "SG ({}) released 内存 (delta：{}), system 是in normal status (totalSgMemCost：{}).";
  public static final String STORAGE_LOG_CHANGE_SYSTEM_TO_REJECT_STATUS_TRIGGERED_BY_LOGICAL_SG_MEM_6F9BCBD3 =
      "Change system to reject status. Triggered by：logical SG ({}), mem cost delta ({}), "
          + "totalSgMemCost ({}), REJECT_THRESHOLD ({})";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SET_SYSTEM_TO_NORMAL_STATUS_TOTALSGMEMCOST_0F714668 =
      "SG ({}) released 内存 (delta：{}), set system to normal status (totalSgMemCost：{}).";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_SYSTEM_IS_STILL_IN_REJECT_STATUS_AD5E475C =
      "SG ({}) released 内存 (delta：{}), but system 是still in reject status (totalSgMemCost：{}).";
  public static final String STORAGE_LOG_DEGRADE_LASTFLUSHTIMEMAP_OF_OLD_TIMEPARTITIONINFO_MEM_SIZE_BED053EE =
      "[{}]degrade LastFlushTimeMap of old TimePartitionInfo-{}, mem size 是{}, remaining mem cost "
          + "是{}";
  public static final String STORAGE_LOG_LIMIT_OF_ARRAY_DEQUE_SIZE_UPDATED_05DBA95E =
      "limit of {} array deque size updated：{} -> {}";
  public static final String STORAGE_LOG_LIMITUPDATETHRESHOLD_OF_PRIMITIVEARRAYMANAGER_UPDATED_394801AE =
      "limitUpdateThreshold of PrimitiveArrayManager updated：{} -> {}";
  public static final String STORAGE_LOG_CREATE_FOLDER_FAILED_IS_THE_FOLDER_EXISTED_18E29D51 =
      "创建目录 {} failed. Is the 目录 existed：{}";
  public static final String STORAGE_LOG_CAN_T_FIND_STRATEGY_FOR_MULT_DIRECTORIES_A06406EC =
      "Can't find strategy {} for mult-directories.";

  // ---------------------------------------------------------------------------
  // 补充异常消息
  // ---------------------------------------------------------------------------
  public static final String STORAGE_EXCEPTION_SYSTEM_REJECTED_OVER_SMS_94CEF932 =
      "System rejected over %sms";
  public static final String STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950 =
      "无法创建Ts文件WriterManager for uuid %s 原因：of insufficient disk space.";
  public static final String STORAGE_EXCEPTION_STORAGE_ALLOCATION_FAILED_FOR_S_TIER_D_E2C94F74 =
      "Storage allocation failed for %s (tier %d)";
  public static final String STORAGE_EXCEPTION_DATA_REGION_S_S_IS_DOWN_BECAUSE_THE_TIME_OF_TSFILE_S_IS_1F732E71 =
      "data region %s[%s] is down, 原因：the time of ts文件 %s is larger than system 当前time, 文件 time is "
          + "%d while system 当前time is %d, please check it.";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CONTINUE_WRITING_DATA_BECAUSE_THE_SPACE_ALLOCATED_9A5FB99E =
      "无法continue writing data, 原因：the space allocated to the 数据库 %s has already used the upper "
          + "limit";
  public static final String STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEPROCESSOR_FOR_DATABASE_S_TIMEPARTITIONID_0CD885BB =
      "无法创建Ts文件Processor for 数据库 %s, timePartitionId %s";
  public static final String STORAGE_EXCEPTION_DELETE_FAILED_PLEASE_DO_NOT_DELETE_UNTIL_THE_OLD_FILES_SETTLED_6C9F17CC =
      "删除failed. Please do not 删除until the old 文件s settled.";
  public static final String STORAGE_EXCEPTION_MULTIPLE_ERRORS_OCCURRED_WHILE_WRITING_MOD_FILES_SEE_LOGS_529D7145 =
      "Multiple errors occurred while writing mod 文件s, see logs for details.";
  public static final String STORAGE_EXCEPTION_MEET_ERROR_WHEN_SETTLING_FILE_S_4D6ECCEE =
      "Meet error when settling 文件：%s";
  public static final String STORAGE_EXCEPTION_PEER_IS_INACTIVE_AND_NOT_READY_TO_WRITE_REQUEST_S_DATANODE_EDFE5AEF =
      "Peer is inactive and 未就绪 to 写入请求, %s, DataNode Id：%s";
  public static final String STORAGE_EXCEPTION_TSFILE_VALIDATE_FAILED_S_3CDE0677 =
      "ts文件 validate failed, %s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_S_TARGET_37BDA16F =
      "文件 renaming failed when loading ts文件. Origin：%s, Target：%s, 原因：%s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_S_9622AA6D =
      "文件 renaming failed when loading。resource 文件. Origin：%s, Target：%s, 原因：%s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_S_TARGET_EEB4EDE7 =
      "文件 renaming failed when loading。mod 文件. Origin：%s, Target：%s, 原因：%s";
  public static final String STORAGE_EXCEPTION_TOTAL_ALLOCATED_MEMORY_FOR_DIRECT_BUFFER_WILL_BE_S_WHICH_FD7DC149 =
      "Total allocated 内存 for direct buffer will be %s, which is greater than limit mem cost：%s";
  public static final String STORAGE_EXCEPTION_S_ALREADY_EXISTS_AND_IS_NOT_EMPTY_CF0BD6A4 =
      "%s 已存在 and is 非空";
  public static final String STORAGE_EXCEPTION_S_S_WRITE_WAL_FAILED_S_5A7E61FB =
      "%s：%s 写入WAL failed：%s";
  public static final String STORAGE_EXCEPTION_MEMORY_NOT_ENOUGH_TO_CLONE_THE_TVLIST_DURING_FLUSH_PHASE_75C90725 =
      "内存 not enough to clone the tvlist during flush phase";
  public static final String STORAGE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "Data type %s is not supported.";
  public static final String STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURSEQINDEX_D_6B9B1134 =
      "curIndex %d is not equal to curSeqIndex %d";
  public static final String STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURUNSEQINDEX_D_AB32F71D =
      "curIndex %d is not equal to curUnSeqIndex %d";
  public static final String STORAGE_EXCEPTION_PAGEID_IN_SHAREDTIMEDATABUFFER_SHOULD_BE_INCREMENTAL_A5E6C4EE =
      "PageId in SharedTimeDataBuffer should be  incremental.";
  public static final String STORAGE_EXCEPTION_CAN_T_READ_FILE_S_S_FROM_DISK_9D5066C0 =
      "无法读取文件 %s%s from disk";
  public static final String STORAGE_EXCEPTION_SHOULD_NOT_GET_PROGRESS_INDEX_FROM_A_UNCLOSING_TSFILERESOURCE_129FD925 =
      "Should not get progress index from a unclosing Ts文件Resource.";
  public static final String STORAGE_EXCEPTION_DIRECTORY_CREATION_FAILED_S_PERMISSION_DENIED_OR_PARENT_2855777B =
      "目录 creation failed：%s (Permission denied or parent not writable)";
  public static final String STORAGE_EXCEPTION_FAILED_TO_GET_DEVICES_FROM_TSFILE_S_S_412EEA1A =
      "无法get devices from ts文件：%s%s";
  public static final String STORAGE_EXCEPTION_UNSUPPORTED_RECORD_TYPE_IN_FILE_S_TYPE_S_DADEE641 =
      "不支持的record type in 文件：%s, type：%s";
  public static final String STORAGE_EXCEPTION_CORRESPONDING_MEMORY_ESTIMATOR_FOR_S_PERFORMER_OF_S_SPACE_D543D3EF =
      "Corresponding 内存 estimator for %s performer of %s space compaction is 不存在ed.";
  public static final String STORAGE_EXCEPTION_HAS_BEEN_WAITING_OVER_S_SECONDS_FOR_ALL_SUB_COMPACTION_TASKS_76BD45D6 =
      "Has been waiting over %s seconds for all sub compaction tasks to finish.";
  public static final String STORAGE_EXCEPTION_HAS_BEEN_WAITING_OVER_S_SECONDS_FOR_ALL_COMPACTION_TASKS_87E1B82E =
      "Has been waiting over %s seconds for all compaction tasks to finish.";
  public static final String STORAGE_EXCEPTION_EXCEPTION_TO_PARSE_THE_TSFILE_S_IN_SETTLING_D40564AD =
      "Exception to parse the ts文件：%s in settling";
  public static final String STORAGE_EXCEPTION_THESE_DEVICES_S_DO_NOT_EXIST_IN_THE_TSFILE_5A03F30D =
      "These devices (%s) do 不存在 in the ts文件";
  public static final String STORAGE_EXCEPTION_CANNOT_SET_SINGLE_TYPE_OF_SOURCE_FILES_TO_THIS_KIND_OF_PERFORMER_6B422172 =
      "无法set single type of source 文件s to this kind of performer";
  public static final String STORAGE_EXCEPTION_CANNOT_SET_BOTH_SEQ_FILES_AND_UNSEQ_FILES_TO_THIS_KIND_OF_F68F629E =
      "无法set both seq 文件s and unseq 文件s to this kind of performer";
  public static final String STORAGE_EXCEPTION_THIS_TABLENAME_IS_S_MERGE_TABLENAME_IS_S_4B05FA97 =
      "this.tableName is %s merge tableName is %s";
  public static final String STORAGE_EXCEPTION_S_S_COMPACTION_ABORT_7D0CB1E5 =
      "%s-%s [Compaction] abort";
  public static final String STORAGE_EXCEPTION_FAILED_TO_PASS_COMPACTION_VALIDATION_RESOURCES_FILE_OR_TSFILE_4B78731F =
      "无法pass compaction validation,。resources 文件 or ts文件 data is wrong";
  public static final String STORAGE_EXCEPTION_FAILED_TO_DELETE_EMPTY_TARGET_FILE_S_324EF900 =
      "无法删除empty target 文件 %s";
  public static final String STORAGE_EXCEPTION_TARGET_FILE_IS_NOT_COMPLETED_S_E65150DB =
      "Target 文件 is not completed. %s";
  public static final String STORAGE_EXCEPTION_DOES_NOT_SUPPORT_THIS_METHOD_IN_READPOINTCROSSCOMPACTIONWRITER_D024F312 =
      "Does not support this method in ReadPointCrossCompactionWriter";
  public static final String STORAGE_EXCEPTION_UNKNOWN_COMPACTION_LOG_LINE_S_C0A9DC05 =
      "unknown compaction log line：%s";
  public static final String STORAGE_EXCEPTION_PATH_S_CANNOT_BE_PARSED_INTO_FILE_INFO_631C48C8 =
      "Path %s cannot be parsed into 文件 info";
  public static final String STORAGE_EXCEPTION_STRING_S_IS_NOT_A_LEGAL_FILE_INFO_STRING_0CBEAB8E =
      "String %s is not a legal 文件 info string";
  public static final String STORAGE_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_D16A1E9A =
      "不支持的data type：%s";
  public static final String STORAGE_EXCEPTION_DO_NOT_HAVE_A_COMPLETE_PAGE_BODY_EXPECTED_S_ACTUAL_S_3A05EF8F =
      "do not have a complete page body. Expected:%s. Actual:%s";
  public static final String STORAGE_EXCEPTION_COMPACTION_COMPACTION_FOR_TARGET_FILE_S_ABORT_46ECFF41 =
      "[Compaction] compaction for target 文件 %s abort";
  public static final String STORAGE_EXCEPTION_COMPACTIONTASKSUMMARY_FOR_FASTCOMPACTIONPERFORMER_SHOULD_F5710AA8 =
      "CompactionTaskSummary for FastCompactionPerformer should be FastCompactionTaskSummary";
  public static final String STORAGE_EXCEPTION_COMPACTION_COMPACTION_FOR_TARGET_FILES_S_ABORT_AFC87906 =
      "[Compaction] compaction for target 文件s %s abort";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_UNSEQ_INNER_COMPACTION_50D566DF =
      "非法的compaction performer for unseq inner compaction %s";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_SEQ_INNER_COMPACTION_S_2C2F1F66 =
      "非法的compaction performer for seq inner compaction %s";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_CROSS_COMPACTION_S_17C6E05D =
      "非法的compaction performer for cross compaction %s";
  public static final String STORAGE_EXCEPTION_SOURCE_FILE_S_IS_DELETED_D2ED7D90 =
      "source 文件 %s is deleted";
  public static final String STORAGE_EXCEPTION_S_S_EXCEEDS_SHORT_RANGE_1DF75A2D =
      "%s %s exceeds short range";
  public static final String STORAGE_EXCEPTION_THE_ELEMENT_SIZE_OF_WALENTRY_S_IS_LARGER_THAN_THE_TOTAL_E494520D =
      "The element size of WALEntry %s is larger than the total 内存 size of wal buffer queue %s";
  public static final String STORAGE_EXCEPTION_FAIL_TO_GET_WAL_FILE_BY_VERSIONID_S_AND_FILES_S_9CB045F4 =
      "无法get wal 文件 by versionId=%s and 文件s=%s.";
  public static final String STORAGE_EXCEPTION_CANNOT_MAKE_OTHER_CHECKPOINT_TYPES_IN_THE_WAL_BUFFER_TYPE_E9053BC1 =
      "无法make other checkpoint types in the wal buffer, type is %s";
  public static final String STORAGE_EXCEPTION_FAILED_RECOVER_THE_RESOURCE_FILE_S_S_S_E35EF7D5 =
      "Failed 恢复the resource 文件：%s%s%s";
  public static final String STORAGE_EXCEPTION_THE_INITIAL_LIMITED_MEMORY_SIZE_D_IS_LESS_THAN_THE_MINIMUM_FC044302 =
      "The initial limited 内存 size %d is less than the minimum 内存 size %d";
  public static final String STORAGE_EXCEPTION_SETTOTALMEMORYSIZEINBYTES_IS_NOT_SUPPORTED_FOR_LOADTSFILEDATACACHEMEMORYBLOCK_DFAB2A2A =
      "setTotal内存SizeInBytes is not supported for LoadTs文件DataCache内存Block";
  public static final String STORAGE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_FROM_QUERY_ENGINE_F91D5959 =
      "forceAllocate：无法allocate 内存 from query engine after %s retries, total query 内存 %s bytes, "
          + "当前available 内存 for 加载%s bytes, 当前加载used 内存 size %s bytes, 加载请求ed 内存 size %s bytes";
  public static final String STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_POSITIVE_D6586ED3 =
      "Load：无效的内存 size %d bytes, must be positive";
  public static final String STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_NON_NEGATIVE_A0146353 =
      "Load：无效的内存 size %d bytes, must be non-negative";
  public static final String STORAGE_EXCEPTION_MAGIC_STRING_CHECK_ERROR_WHEN_PARSING_TSFILE_S_EA3D68E3 =
      "Magic String check error when parsing Ts文件 %s.";
  public static final String STORAGE_EXCEPTION_EMPTY_NONALIGNED_CHUNK_OR_TIME_CHUNK_WITH_OFFSET_D_IN_TSFILE_B1E462C9 =
      "Empty Nonaligned Chunk or Time Chunk with offset %d in Ts文件 %s.";
  public static final String STORAGE_EXCEPTION_TIME_PARTITION_SLOTS_SIZE_IS_GREATER_THAN_S_D076F78E =
      "Time partition slots size is greater than %s";
  public static final String STORAGE_EXCEPTION_CONSUME_ALIGNED_CHUNK_DATA_ERROR_NEXT_CHUNK_OFFSET_D_CHUNKDATA_D896FAE2 =
      "Consume aligned chunk data error, next chunk offset：%d, chunkData：%s";
  public static final String STORAGE_EXCEPTION_CONSUME_CHUNKDATA_ERROR_CHUNK_OFFSET_D_MEASUREMENT_S_CHUNKDATA_4A1F1EE1 =
      "Consume chunkData error, chunk offset：%d, measurement：%s, chunkData：%s";
  public static final String STORAGE_EXCEPTION_TOTAL_DATABASE_MEMCOST_S_IS_OVER_THAN_MEMORYSIZEFORWRITING_C63E4D72 =
      "Total 数据库 MemCost %s is over than 内存SizeForWriting %s";
  public static final String STORAGE_EXCEPTION_REQUIRED_FILE_NUM_D_IS_GREATER_THAN_THE_MAX_FILE_NUM_D_FOR_AB6DE95B =
      "Required 文件 num %d is greater than the max 文件 num %d for compaction.";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_AFTER_D_SECONDS_C701F750 =
      "无法allocate %d 文件s for compaction after %d seconds, max 文件 num for compaction module is %d, "
          + "%d 文件s is used.";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_MAX_FILE_NUM_FOR_9B954F8C =
      "无法allocate %d 文件s for compaction, max 文件 num for compaction module is %d, %d 文件s is used.";
  public static final String STORAGE_EXCEPTION_REQUIRED_MEMORY_COST_D_BYTES_IS_GREATER_THAN_THE_TOTAL_MEMORY_444D8FE4 =
      "Required 内存 cost %d bytes is greater than the total 内存 budget for compaction %d bytes";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_BYTES_MEMORY_FOR_COMPACTION_TOTAL_MEMORY_33BE3C71 =
      "无法allocate %d bytes 内存 for compaction, total 内存 budget for compaction module is %d bytes, "
          + "%d bytes is used";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_REQUESTS_EXCEEDED_WAIT_SMS_30F0842F =
      "number of 请求s exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_REQUEST_SIZE_LIMIT_EXCEEDED_WAIT_SMS_11C1E549 =
      "请求 size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_WRITE_REQUESTS_EXCEEDED_WAIT_SMS_D11F94D2 =
      "number of 写入请求s exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_WRITE_SIZE_LIMIT_EXCEEDED_WAIT_SMS_AA3796DC =
      "写入size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_READ_REQUESTS_EXCEEDED_WAIT_SMS_C92D6C43 =
      "number of 读取请求s exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_READ_SIZE_LIMIT_EXCEEDED_WAIT_SMS_E19598BA =
      "读取size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_BECAUSE_THERE_IS_FILE_UNDER_1C59ACFC =
      "无法创建目录 %s 原因：there is 文件 under the path, please check configuration and restart.";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_PLEASE_CHECK_CONFIGURATION_BA580B67 =
      "无法创建目录 %s, please check configuration and restart.";
  public static final String STORAGE_EXCEPTION_CONFLICT_IS_DETECTED_IN_DIRECTORY_S_WHICH_MAY_BE_BEING_USED_CB5C77FC =
      "Conflict is detected in 目录 %s, which may be being used by another IoTDB (ProcessId=%s). "
          + "Please check configuration and restart.";
  public static final String COMPACTION_INNER_SPACE = "内部";
  public static final String COMPACTION_CROSS_SPACE = "跨空间";
  public static final String DEVICE_DOES_NOT_EXIST_IN_RESOURCE_FILE_FMT =
      "%s 在资源文件中不存在";
  public static final String TARGET_FILE_SMALLER_THAN_MAGIC_STRING_AND_VERSION_NUMBER_SIZE_FMT =
      "目标文件 %s 小于 magic string 和版本号大小";
  public static final String CURRENT_POINT_TIMESTAMP_SHOULD_BE_LATER_FMT =
      "%s 的当前点时间戳为 %s，应晚于最后时间 %s";
  public static final String DEVICE_TIME_RANGE_VERIFICATION_FAILED_FMT =
      "设备(%s) 的时间范围校验失败。%s";
  public static final String CURRENT_DEVICE_TIME_RANGE_MISMATCH_FMT =
      "当前设备时间范围为 %s，应等于实际设备时间范围 %s";
  public static final String CURRENT_TIMESERIES_METADATA_MISMATCH_FMT =
      "当前 timeseriesMetadata 为 %s，应等于实际时间范围 %s";
  public static final String CURRENT_CHUNK_METADATA_MISMATCH_FMT =
      "当前 chunkMetadata 为 %s，应等于实际 chunk 时间范围 %s";
  public static final String CURRENT_PAGE_TIME_RANGE_MISMATCH_FMT =
      "当前 page 为 %s，应包含实际 page 数据时间范围 %s";
  public static final String COMPACTION_VALIDATION_SEQUENCE_FILES_HAS_OVERLAP_FMT =
      "未通过 compaction 校验，顺序文件存在重叠，文件为 %s";
  public static final String TSFILE_CANNOT_TRANSIT_TO_COMPACTING_FMT =
      "TsFile %s 无法转换为 COMPACTING，当前状态：%s";
  public static final String CURRENT_PAGE_CANNOT_BE_ALIGNED_WITH_TIME_CHUNK_FMT =
      "当前 page %s 无法与 time chunk %s 对齐，page index 为 %s";
  public static final String CURRENT_CHUNK_CANNOT_BE_ALIGNED_WITH_TIME_CHUNK_FMT =
      "当前 chunk %s 无法与 time chunk：%s 对齐，第一批中的所有 time chunk 为 %s";
  public static final String WAL_NODE_CLOSED_FMT = "wal node-%s 已关闭";
  public static final String BROKEN_WAL_FILE_FMT = "WAL 文件 %s 损坏，大小为 %d";
  public static final String TSFILE_READER_CLOSED_BECAUSE_NO_REFERENCE =
      "{} TsFileReader 因没有引用已关闭。";
  public static final String CLOSED_TSFILE_READER_CLOSED =
      "{} closedTsFileReader 已关闭。";
  public static final String UNCLOSED_TSFILE_READER_CLOSED =
      "{} unclosedTsFileReader 已关闭。";

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_NO_LOAD_TSFILE_UUID_ARG_RECORDED_EXECUTE_LOAD_COMMAND_ARG_66722D80 = "No 加载 Ts文件 uuid %s recorded for execute 加载 command %s.";
  public static final String EXCEPTION_NON_MINUS_ALIGNED_CHUNK_ONLY_HAS_ONE_MEASUREMENT_COMMA_BUT_MEASUREMENTINDEX_IS_E1A87F80 = "Non-aligned chunk only has one measurement, but measurementIndex is ";

}
