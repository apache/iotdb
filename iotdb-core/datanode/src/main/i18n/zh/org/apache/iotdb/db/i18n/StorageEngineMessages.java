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
  public static final String INTERRUPTED_WAITING_TABLET_CONVERSION_SLOT =
      "等待 tablet 转换槽位时被中断: ";
  public static final String FAIL_TO_LOAD_TSFILE_TO_ACTIVE_DIR = "加载 TsFile 到 Active 目录失败";
  public static final String FAIL_TO_LOAD_DISK_SPACE = "获取文件 {} 的磁盘空间失败";
  public static final String LOAD_ACTIVE_LISTENING_DIR_NOT_SET = "未设置加载 Active 监听目录。";
  public static final String FAILED_TO_CREATE_TARGET_DIR = "创建目标目录失败: ";
  public static final String FAILED_LOAD_ACTIVE_LISTENING_DIRS = "加载 Active 监听目录失败";
  public static final String INVALID_PARAMETER = "无效的参数 '";
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
  public static final String ACTIVE_LOAD_TEMPORARILY_UNAVAILABLE =
      "拒绝自动加载 TsFile {} (isGeneratedByPipe = {})，原因是系统暂时不可用，将稍后重试。状态: {}";
  public static final String ERROR_MOVING_FILE_TO_FAIL_DIR = "将文件 {} 移动到失败目录时发生错误。";
  public static final String FAILED_COUNT_FILES_IN_FAIL_DIR = "统计失败目录中的失败文件数量失败。";

  public static final String STRING_NOT_LEGAL_REPAIR_LOG = "字符串 '%s' 不是合法的修复日志";

  public static final String WRONG_LOAD_COMMAND_S = "错误的 load 命令 %s。";

  public static final String FAILED_TO_FIND_DATA_REGION = "共识组 %s 底层状态机创建失败, 因为 DataRegion 没找到。";

  public static final String DATA_REGION_IS_NULL = "Data region 是空";
}
