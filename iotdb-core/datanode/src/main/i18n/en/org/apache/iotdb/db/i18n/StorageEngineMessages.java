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
      " is not supported";

  private StorageEngineMessages() {}

  // ======================== StorageEngine ========================

  public static final String FAIL_TO_RECOVER_WAL = "Fail to recover wal.";
  public static final String STORAGE_ENGINE_FAILED_TO_SET_UP = "Storage engine failed to set up.";
  public static final String SEQ_MEMTABLE_FLUSH_CHECK_THREAD_STARTED = "start sequence memtable timed flush check thread successfully.";
  public static final String UNSEQ_MEMTABLE_FLUSH_CHECK_THREAD_STARTED = "start unsequence memtable timed flush check thread successfully.";
  public static final String STILL_NOT_EXIT_AFTER_30S = "{} still doesn't exit after 30s";
  public static final String START_CLOSING_ALL_DB_PROCESSOR = "Start closing all database processor";
  public static final String START_FORCE_CLOSING_ALL_DB_PROCESSOR = "Start force closing all database processor";
  public static final String SYSTEM_READ_ONLY_NO_MERGE = "Current system mode is read only, does not support merge";
  public static final String START_REPAIR_DATA = "start repair data";
  public static final String STOP_REPAIR_DATA = "stop repair data";
  public static final String REMOVING_DATA_REGION = "Removing data region {}";
  public static final String FAILED_TO_DELETE_SNAPSHOT_DIR = "Failed to delete snapshot dir {}";
  public static final String REMOVED_DATA_REGION = "Removed data region {}";
  public static final String EXECUTE_LOAD_COMMAND_ERROR = "Execute load command {} error.";
  public static final String START_REBOOTING_ALL_TIMED_SERVICE = "Start rebooting all timed service.";
  public static final String STOP_ALL_TIMED_SERVICE_AND_RESTART = "Stop all timed service successfully, and now restart them.";
  public static final String REBOOT_ALL_TIMED_SERVICE_SUCCESSFULLY = "Reboot all timed service successfully";
  public static final String FAILED_TO_DELETE = "Failed to delete: {} -> {}";
  public static final String FAILED_TO_CHECK_OBJECT_FILES = "Failed to check Object Files: {}";

  // ======================== Buffer Cache ========================

  public static final String BLOOM_FILTER_CACHE_SIZE = "BloomFilterCache size = {}";
  public static final String GET_BLOOM_FILTER_FROM_CACHE = "get bloomFilter from cache where filePath is: {}";
  public static final String STOP_SERVICE = "{}: stop {}...";
  public static final String CHUNK_CACHE_SIZE = "ChunkCache size = {}";
  public static final String GET_CHUNK_FROM_CACHE = "get chunk from cache whose key is: {}";
  public static final String CACHE_MISS_IN_FILE = "Cache miss: {}.{} in file: {}";
  public static final String DEVICE_ALL_SENSORS = "Device: {}, all sensors: {}";
  public static final String TS_METADATA_FILTERED_BY_BLOOM_FILTER = "TimeSeries meta data {} is filter by bloomFilter!";
  public static final String FILE_NO_SUCH_TIME_SERIES = "The file doesn't have this time series {}.";

  // ======================== Resource Control - Disk ========================

  public static final String FAILED_TO_DEREGISTER_FILE_LOCK = "Failed to deregister file lock because {}";
  public static final String ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY = "All folders are full, change system mode to read-only.";
  public static final String FAILED_TO_PROCESS_FOLDER = "Failed to process folder '";
  public static final String FAIL_TO_GET_CANONICAL_PATH = "Fail to get canonical path of data dir {}";
  public static final String ALL_DISKS_OF_TIER_FULL = "All disks of tier {} are full.";
  public static final String FOLDERS_RESET_SUCCESSFULLY = "The folders is reset successfully, which takes {} ms.";
  public static final String FOLDER_NOT_EXIST_CREATE_IT = "folder {} doesn't exist, create it";
  public static final String FAILED_TO_STATISTIC_SIZE = "Failed to statistic the size of {}, because";
  public static final String DISK_SPACE_INSUFFICIENT_READ_ONLY = "Disk space is insufficient, change system mode to read-only";
  public static final String CANNOT_CALC_OCCUPIED_SPACE = "Cannot calculate occupied space for path {}.";

  // ======================== Resource Control - Memory ========================

  public static final String WAITING_FOR_THREAD_POOL_SHUTDOWN = "Waiting for {} thread pool to shut down.";
  public static final String THREAD_POOL_NOT_EXIT_AFTER_MS = "{} thread pool doesn't exit after {}ms.";
  public static final String INTERRUPTED_WAITING_THREAD_POOL_EXIT = "Interrupted while waiting {} thread pool to exit. ";
  public static final String BUFFERED_ARRAY_SIZE_THRESHOLD = "BufferedArraySizeThreshold is {}";
  public static final String CURRENT_SG_COST = "Current Sg cost is {}";
  public static final String FORCE_DEGRADE_TSFILE_RESOURCE = "Force degrade tsfile resource {}";
  public static final String CANNOT_DEGRADE_TIME_INDEX_ALL_FILE_LEVEL = "Can't degrade time index any more because all time index are file level.";
  public static final String DEGRADE_TSFILE_RESOURCE = "Degrade tsfile resource {}";

  // ======================== Resource Control - Quotas ========================

  public static final String SPACE_QUOTA_RESTORE_SUCCEEDED = "Space quota limit restore succeeded, limit: {}.";
  public static final String SPACE_QUOTA_RESTORE_FAILED = "Space quota limit restore failed, limit: {}.";
  public static final String THROTTLE_QUOTA_RESTORED_SUCCESSFULLY = "Throttle quota limit restored successfully. ";
  public static final String THROTTLE_QUOTA_RESTORED_FAILED = "Throttle quota limit restored failed. ";
  public static final String INVALID_STATEMENT_TYPE = "Invalid statement type: ";

  // ======================== DataRegion ========================

  public static final String CREATE_DB_SYSTEM_DIR_FAILED = "create database system Directory {} failed";
  public static final String CREATE_DATA_REGION_DIR_FAILED = "create data region directory {} failed";
  public static final String IS_NOT_A_DIRECTORY = "{} is not a directory.";
  public static final String FAIL_TO_CLOSE_TSFILE_WHEN_RECOVERING = "Fail to close TsFile {} when recovering";
  public static final String FAIL_TO_RECOVER_SEALED_TSFILE_SKIP = "Fail to recover sealed TsFile {}, skip it.";
  public static final String DATA_INCONSISTENT_NOT_TRIGGER_TWICE = "Data inconsistent exception is not supposed to be triggered twice";
  public static final String INSERT_TO_TSFILE_PROCESSOR_REJECTED = "insert to TsFileProcessor rejected, {}";
  public static final String INSERT_TO_TSFILE_PROCESSOR_ERROR = "insert to TsFileProcessor error ";
  public static final String IOEXCEPTION_CREATING_TSFILE_PROCESSOR_RETRY = "meet IOException when creating TsFileProcessor, retry it again";
  public static final String CANNOT_CLOSE_TSFILE_RESOURCE = "Cannot close a TsFileResource {}";
  public static final String CANNOT_REMOVE_MOD_FILE = "Cannot remove mod file {}";
  public static final String FAIL_TO_DELETE_DATA_REGION_FOLDER = "Fail to delete data region folder {}";
  public static final String FAIL_TO_DELETE_DATA_REGION_OBJECT_FOLDER = "Fail to delete data region object folder {}";
  public static final String FILES_WERE_CLOSED = "{} files were closed";
  public static final String FAIL_TO_LOG_DELETE_TO_WAL = "Fail to log delete to wal.";
  public static final String DELETION_EXECUTING_TABLE_DELETION = "[Deletion] Executing table deletion {}";
  public static final String DELETION_UNSEALED_FILES_FOR = "[Deletion] unsealed files for {}: {}";
  public static final String DELETION_SEALED_FILES_FOR = "[Deletion] sealed files for {}: {}";
  public static final String WRITING_NO_FILE_RELATED_DELETION_TO_WAL = "Writing no-file-related deletion to WAL {}";
  public static final String DELETION_SKIPPED_FILE_TIME = "[Deletion] {} skipped {}, file time {}";
  public static final String EXPECT_IS_ACTUAL_IS = "expect is {}, actual is {}";
  public static final String DELETION_DOES_NOT_INVOLVE_ANY_FILE = "[Deletion] Deletion {} does not involve any file";
  public static final String FAIL_TO_WRITE_MOD_ENTRY_TO_FILES = "Fail to write modEntry {} to files";
  public static final String REMOVE_TSFILE_DIRECTLY_WHEN_DELETE_DATA = "Remove tsfile {} directly when delete data";
  public static final String MEET_ERROR_IN_COMPACTION_SCHEDULE = "Meet error in compaction schedule.";
  public static final String MEET_ERROR_IN_TTL_CHECK = "Meet error in ttl check.";
  public static final String FAILED_TO_EXECUTE_OBJECT_TTL_CHECK = "Failed to execute object ttl check";
  public static final String MEET_ERROR_IN_INSERTION_COMPACTION_SCHEDULE = "Meet error in insertion compaction schedule.";
  public static final String EXCEPTION_MOVE_NEW_TSFILE_IN_SETTLING = "Exception to move new tsfile in settling";
  public static final String TSFILE_LOADED_IN_UNSEQ_LIST = "TsFile {} is successfully loaded in unsequence list.";
  public static final String CANNOT_CLOSE_LAST_READER_AFTER_LOAD = "Cannot close last reader after loading TsFile {}";
  public static final String FILE_ALREADY_LOADED_IN_UNSEQ_LIST = "The file {} has already been loaded in unsequence list";
  public static final String CANNOT_DELETE_LOCAL_MOD_FILE = "Cannot delete localModFile {}";
  public static final String REMOVE_TSFILE_SUCCESSFULLY = "Remove tsfile {} successfully.";
  public static final String THREAD_INTERRUPTED_WAITING_COMPACTION = "Thread get interrupted when waiting compaction to finish";
  public static final String PARTIAL_FAILED_INSERTING_ROWS_ONE_DEVICE = "Partial failed inserting rows of one device";
  public static final String PARTIAL_FAILED_INSERTING_ROWS = "Partial failed inserting rows";
  public static final String REJECTED_INSERTING_MULTI_TABLETS = "Rejected inserting multi tablets";
  public static final String PARTIAL_FAILED_INSERTING_MULTI_TABLETS = "Partial failed inserting multi tablets";
  public static final String INTERRUPTED_WAITING_DATA_REGION_DELETED = "Interrupted When waiting for data region deleted.";
  public static final String FAILED_TO_RENAME = "Failed to rename {} to {},";

  // ======================== Compaction ========================

  public static final String SELECTOR_NOT_FOR_INNER_SPACE = "This kind of selector cannot be used to select inner space task";
  public static final String SELECTOR_NOT_FOR_CROSS_SPACE = "This kind of selector cannot be used to select cross space task";
  public static final String SELECTOR_NOT_FOR_SETTLE = "This kind of selector cannot be used to select settle task";
  public static final String UNSEQ_FILE_NO_OVERLAP_WITH_SEQ = "Unseq file {} does not overlap with any seq files.";
  public static final String CANNOT_SELECT_FILE_FOR_CROSS_COMPACTION = "{} cannot select file for cross space compaction";
  public static final String CURRENT_FILE_SIZE = "Current File is {}, size is {}";
  public static final String EXCEPTION_SELECTING_FILES = "Exception occurs while selecting files";
  public static final String UNIMPLEMENTED = "unimplemented";
  public static final String ILLEGAL_CROSS_COMPACTION_SELECTOR = "Illegal Cross Compaction Selector ";
  public static final String ILLEGAL_COMPACTION_SELECTOR = "Illegal Compaction Selector ";
  public static final String COMPACTION_SCHEDULE_TASK_MANAGER_STARTED = "Compaction schedule task manager started.";
  public static final String WAITING_COMPACTION_SCHEDULE_POOL_SHUTDOWN = "Waiting for compaction schedule task thread pool to shut down";
  public static final String COMPACTION_SCHEDULE_MANAGER_WAIT_TO_STOP = "CompactionScheduleTaskManager has wait for {} seconds to stop";
  public static final String COMPACTION_SCHEDULE_TASK_MANAGER_STOPPED = "CompactionScheduleTaskManager stopped";
  public static final String REPAIR_FAILED_RENAME_PROGRESS_FILE = "[RepairTaskManager] Failed to rename repair data progress file";
  public static final String REPAIR_SKIP_TASK_STOPPING = "[RepairTaskManager] skip current task because repair task is stopping";
  public static final String REPAIR_SCAN_TASK_CANCELLED = "[RepairScheduler] scan task is cancelled";
  public static final String REPAIR_ERROR_SCAN_TIME_PARTITION = "[RepairScheduler] Meet errors when scan time partition files";
  public static final String COMPACTION_TASK_MANAGER_STARTED = "Compaction task manager started.";
  public static final String WAITING_TASK_EXECUTION_POOL_SHUTDOWN = "Waiting for task taskExecutionPool to shut down";
  public static final String WAITING_TASK_EXECUTION_POOL_SHUTDOWN_MS = "Waiting for task taskExecutionPool to shut down in {} ms";
  public static final String INTERRUPTED_WAITING_ALL_TASK_FINISH = "Interrupted when waiting all task finish";
  public static final String ALL_COMPACTION_TASK_FINISH = "All compaction task finish";
  public static final String COMPACTION_MANAGER_WAIT_TO_STOP = "CompactionManager has wait for {} seconds to stop";
  public static final String COMPACTION_MANAGER_STOPPED = "CompactionManager stopped";
  public static final String COMPACTION_THREAD_POOL_CANNOT_CLOSE = "CompactionThreadPool can not be closed in {} ms";
  public static final String TIMEOUT_WAITING_TASK_FUTURE = "Timeout when waiting for task future";
  public static final String COMPACTION_THREAD_TERMINATES = "CompactionThread-{} terminates because interruption";
  public static final String EXCEPTION_EXECUTING_COMPACTION_TASK = "Exception occurred when executing compaction task. {}";
  public static final String TIMEOUT_GET_COMPACTION_TASK_SUMMARY = "Timeout when trying to get compaction task summary";
  public static final String TTL_CHECK_TASK_FAILED = "[TTLCheckTask-{}] Failed to execute ttl check";
  public static final String ERROR_CREATING_SETTLE_LOG = "meet error when creating settle log, file path:{}";
  public static final String WRITE_SETTLE_LOG_FAILED = "write settle log file failed, the log file:{}";
  public static final String CLOSE_UPGRADE_LOG_FAILED = "close upgrade log file failed, the log file:{}";
  public static final String FIND_SETTLED_FILE = "find settled file for {}";
  public static final String GENERATE_SETTLED_FILE = "generate settled file for {}";
  public static final String ALL_FILES_SETTLED_SUCCESSFULLY = "All files settled successfully! ";
  public static final String SUB_COMPACTION_TASK_MEET_ERRORS = "[Compaction] SubCompactionTask meet errors ";
  public static final String TASK_TYPE_NO_TMP_FILE_SUFFIX = "Current task type {} does not have tmp file suffix.";
  public static final String CANNOT_GET_MOD_FILE = "Can not get mod file of {}";
  public static final String COMPACTION_START_DELETE_REAL_FILE = "{} [Compaction] Compaction starts to delete real file ";
  public static final String COMPACTION_START_DELETE_SOURCE_MODS = "{} [Compaction] Start to delete modifications of source files";
  public static final String COMPACTION_DELETE_FILE = "[Compaction] delete file: {}";
  public static final String FAILED_TO_READ_FILE_ATTRIBUTES = "Failed to read file attributes: {}";
  public static final String FAILED_TO_CHECK_TABLE_DIR = "Failed to check table dir: {}";
  public static final String REMOVE_OBJECT_FILE_SIZE = "Remove object file {}, size is {}(byte)";
  public static final String FAILED_TO_DELETE_EXPIRED_OBJECT_FILE = "Failed to delete expired object file: {}";
  public static final String SHOULD_CALL_EXACT_SUB_CLASS = "Should call exact sub class!";
  public static final String NO_NEXT_BLOCK = "no next block";
  public static final String METHOD_NOT_SUPPORTED_FAST_CROSS_WRITER = "Does not support this method in FastCrossCompactionWriter";
  public static final String DEVICE_SHOULD_EXIST_IN_SEQ_FILE = "The device should exist in current seq file";
  public static final String METHOD_NOT_SUPPORTED_FAST_INNER_WRITER = "Does not support this method in FastInnerCompactionWriter";
  public static final String METHOD_NOT_SUPPORTED_READ_POINT_WRITER = "Does not support this method in ReadPointInnerCompactionWriter";
  public static final String UNKNOWN_DATA_TYPE = "Unknown data type ";
  public static final String FAILED_TO_DELETE_TARGET_FILE = "failed to delete target file %s";
  public static final String SOURCE_FILES_CANNOT_BE_DELETED = "source files cannot be deleted successfully";
  public static final String FAIL_TO_GET_TSFILE_NAME = "Fail to get the tsfile name of {}";
  public static final String ERROR_ESTIMATE_INNER_COMPACTION_MEMORY = "Meet error when estimate inner compaction memory";
  public static final String CANNOT_RECOVER_INSERTION_CROSS_TASK = "Can not recover InsertionCrossSpaceCompactionTask";
  public static final String FAILED_TO_REPAIR_FILE = "Failed to repair file {}";
  public static final String FAILED_DELETE_FULLY_DIRTY_SOURCE = "Failed to delete fully_dirty source file.";
  public static final String RECOVER_MODS_FILE_ERROR = "recover mods file error on list files:{}";
  public static final String UNKNOWN_COMPACTION_TASK_TYPE = "Unknown compaction task type {}";
  public static final String RECOVER_COMPACTION_ERROR = "Recover compaction error";
  public static final String COMPACTION_RECOVER_FAILED = "{} [Compaction][Recover] Failed to recover compaction";
  public static final String MEET_ERROR_WHEN_READ_TSFILE = "Meet error when read tsfile {}";
  public static final String UNKNOWN_REPAIR_LOG_FORMAT = "Unknown format of repair log";
  public static final String REPAIR_START_CHECK_TSFILE = "[RepairScheduler] start check tsfile: {}";
  public static final String REPAIR_SKIPPED_BROKEN_FILE = "[RepairScheduler] {} is skipped because it is broken";
  public static final String REPAIR_FAILED_CREATE_LOGGER = "[RepairScheduler] Failed to create repair logger";
  public static final String REPAIR_FAILED_CLOSE_LOGGER = "[RepairScheduler] Failed to close repair logger";
  public static final String REPAIR_WAIT_COMPACTION_FINISH = "[RepairScheduler] Wait compaction schedule task finish";
  public static final String REPAIR_WAIT_ALL_RUNNING_TASK_FINISH = "[RepairScheduler] Wait all running compaction task finish";
  public static final String REPAIR_TASK_FINISHED = "[RepairScheduler] Repair task finished";
  public static final String REPAIR_SCHEDULE_TASK_ERROR = "[RepairScheduler] Meet error when execute repair schedule task";
  public static final String REPAIR_FAILED_INIT_SCHEDULE_TASK = "[RepairScheduler] Failed to init repair schedule task";
  public static final String REPAIR_ALL_PARTITIONS_DONE_SKIP = "[RepairScheduler] All time partitions have been repaired, skip repair task";
  public static final String END_MUST_GREATER_THAN_START = "end must greater than start";
  public static final String DATA_DIRS_MUST_NOT_BE_EMPTY = "data_dirs must not be empty";
  public static final String DOES_NOT_EXIST = "{} doesn't exist.";
  public static final String CHECK_FAILED = "check {} failed.";
  public static final String FAILED_TO_DEAL_WITH = "failed to deal with {}";
  public static final String ERROR_OCCURRED = "error occurred";

  // ======================== MemTable ========================

  public static final String CANNOT_DESERIALIZE_OLD_MEMTABLE_SNAPSHOT = "Cannot deserialize OldMemTableSnapshot";
  public static final String DEVICE_ID_LENGTH_SHOULD_BE_POSITIVE = "DeviceID's length should be larger than 0.";
  public static final String CREATE_NEW_TSFILE_PROCESSOR = "create a new tsfile processor {}";
  public static final String REOPEN_TSFILE_PROCESSOR = "reopen a tsfile processor {}";
  public static final String EXCEPTION_DURING_WAL_FLUSH = "Exception during wal flush";
  public static final String DELETION_IN_FLUSHING_MEMTABLE = "[Deletion] Deletion with {} in flushingMemTable";
  public static final String START_WAIT_UNTIL_FILE_CLOSED = "Start to wait until file {} is closed";
  public static final String FILE_CLOSED_SYNCHRONOUSLY = "File {} is closed synchronously";
  public static final String DATAREGION_TSFILE_ERROR = "{}: {}";
  public static final String DELETION_WRITTEN_WHEN_FLUSH = "[Deletion] Deletion : {} written when flush memtable";
  public static final String FSYNC_MEMTABLE_TO_DISK_ERROR = "fsync memTable data to disk error,";
  public static final String FLUSHING_MEMTABLES_CLEAR = "{} flushingMemtables is clear";
  public static final String START_TO_END_FILE = "Start to end file {}";
  public static final String ENDED_FILE = "Ended file {}";
  public static final String START_TO_END_EMPTY_FILE = "Start to end empty file {}";
  public static final String TIME_CHUNK_METADATA_SHOULD_NOT_BE_EMPTY = "TimeChunkMetadata in aligned device should not be empty";
  public static final String WRITABLE_MEM_CHUNK_UNSUPPORTED_TYPE = "WritableMemChunk does not support data type: {}";

  // ======================== Modification ========================

  public static final String UNRECOGNIZED_PREDICATE_TYPE = "Unrecognized predicate type: ";
  public static final String UNSUPPORTED_MOD_TYPE = "Unsupported mod type: ";
  public static final String UNKNOWN_MOD_TYPE = "Unknown ModType: ";
  public static final String CANNOT_CLOSE_MOD_FILE_INPUT_STREAM = "Cannot close mod file input stream of {}";
  public static final String CANNOT_READ_MOD_FILE_INPUT_STREAM = "Cannot read mod file input stream of {}";
  public static final String COMPACT_MODS_FILE_EXCEPTION = "compact mods file exception of {}";
  public static final String SETTLE_SUCCESSFUL = "{} settle successful";
  public static final String REMOVE_ORIGIN_OR_RENAME_MODS_ERROR = "remove origin file or rename new mods file error.";
  public static final String DELETE_MODIFICATION_FILE_FAILED = "Delete ModificationFile {} failed.";
  public static final String CANNOT_CREATE_HARDLINK = "Cannot create hardlink for {}";
  public static final String ERROR_READING_MODIFICATIONS = "An error occurred when reading modifications";
  public static final String ERROR_DECODE_LINE_TO_MODIFICATION = "An error occurred when decode line-[{}] to modification";
  public static final String MODIFICATIONS_WILL_BE_TRUNCATED = "The modifications[{}] will be truncated to size {}.";
  public static final String LAST_LINE_OF_MODS_INCOMPLETE = "The last line of Mods is incomplete, will be truncated";
  public static final String UNKNOWN_MODIFICATION_TYPE = "Unknown modification type: ";
  public static final String INCORRECT_DELETION_FIELDS_NUMBER = "Incorrect deletion fields number: ";
  public static final String INVALID_TIMESTAMP = "Invalid timestamp: ";
  public static final String INVALID_SERIES_PATH = "Invalid series path: ";

  // ======================== WAL ========================

  public static final String START_REBOOTING_WAL_DELETE_THREAD = "Start rebooting wal delete thread.";
  public static final String STOP_WAL_DELETE_THREAD_AND_RESTART = "Stop wal delete thread successfully, and now restart it.";
  public static final String TIMED_WAL_DELETE_THREAD_INTERRUPTED = "Timed wal delete thread is interrupted.";
  public static final String INTERRUPTED_WAITING_WAL_FLUSHED = "Interrupted when waiting for all write-ahead logs flushed.";
  public static final String STOPPING_WAL_MANAGER = "Stopping WALManager";
  public static final String DELETING_OUTDATED_FILES_BEFORE_EXIT = "Deleting outdated files before exiting";
  public static final String WAL_MANAGER_STOPPED = "WALManager stopped";
  public static final String WAITING_THREAD_TERMINATED_TIMEOUT = "Waiting thread {} to be terminated is timeout";
  public static final String THREAD_NOT_EXIT_AFTER_30S = "Thread {} still doesn't exit after 30s";
  public static final String FAILED_TO_DELETE_OUTDATED_WAL_FILE = "Failed to delete outdated wal file";
  public static final String UNRECOGNIZED_CHECKPOINT_TYPE = "unrecognized checkpoint type ";
  public static final String CREATE_FOLDER_FOR_WAL_BUFFER = "create folder {} for wal buffer-{}.";
  public static final String FAIL_TO_LOG_MAX_MEMTABLE_ID = "Fail to log max memTable id: {}";
  public static final String FAIL_TO_MAKE_CHECKPOINT = "Fail to make checkpoint: {}";
  public static final String MEMTABLE_ID_NOT_FOUND_IN_MAP = "memtableId {} not found in MemTableId2Info";
  public static final String FAIL_TO_CLOSE_WAL_CHECKPOINT_WRITER = "Fail to close wal node-{}'s checkpoint writer.";
  public static final String CANNOT_WRITE_TO = "Cannot write to {}";
  public static final String REACH_END_OFFSET_OF_WAL_FILE = "Reach the end offset of wal file";
  public static final String UNEXPECTED_END_OF_FILE = "Unexpected end of file";
  public static final String WAL_SEGMENT_V1_FAILED_V2_SUCCESS = "Failed to load WAL segment in V1 way, try in V2 way successfully.";
  public static final String UNEXPECTED_EXCEPTION = "Unexpected exception";
  public static final String FAIL_TO_READ_WAL_ENTRY_SKIP_BROKEN = "Fail to read WALEntry from wal file {}, skip broken WALEntries.";
  public static final String INVALID_CHECKPOINT_FILE_NAME = "Invalid checkpoint file name: ";
  public static final String INVALID_WAL_FILE_NAME = "Invalid wal file name: ";
  public static final String INTERRUPTED_WAITING_FOR_RESULT = "Interrupted when waiting for result.";
  public static final String CANNOT_WRITE_WAL_INTO_FAKE_NODE = "Cannot write wal into a fake node. ";
  public static final String CREATE_FOLDER_FOR_WAL_NODE = "create folder {} for wal node-{}.";
  public static final String FAIL_TO_DELETE_WAL_NODE_OUTDATED_FILES = "Fail to delete wal node-{}'s outdated files.";
  public static final String FAIL_TO_GET_DATA_REGION_PROCESSOR = "Fail to get data region processor for {}";
  public static final String WAITING_TOO_LONG_FOR_MEMTABLE_FLUSH = "Waiting too long for memTable flush to be done.";
  public static final String INTERRUPTED_WAITING_MEMTABLE_FLUSH = "Interrupted when waiting for memTable flush to be done.";
  public static final String FAIL_TO_ROLL_WAL_LOG_WRITER = "Fail to roll wal log writer.";
  public static final String FAIL_TO_SNAPSHOT_MEMTABLE = "Fail to snapshot memTable of {}";
  public static final String START_RECOVERING_WAL_NODE_IN_DIR = "Start recovering WAL node in the directory {}";
  public static final String ERROR_DELETE_CHECKPOINT_FILE = "error when delete checkpoint file. {}";
  public static final String FAIL_TO_READ_WAL_LOGS_SKIP = "Fail to read wal logs from {}, skip them";
  public static final String FAIL_TO_RENAME_FILE = "Fail to rename file {} to {}";
  public static final String FAIL_TO_RECOVER_WAL_METADATA = "Fail to recover metadata of wal file {}";
  public static final String START_RECOVERING_WAL = "Start recovering wal.";
  public static final String SUCCESSFULLY_RECOVER_ALL_WAL_NODES = "Successfully recover all wal nodes.";
  public static final String STORAGE_ENGINE_FAILED_TO_RECOVER = "StorageEngine failed to recover.";
  public static final String CANNOT_RECOVER_TSFILE_WAL_ALREADY_STARTED = "Cannot recover tsfile from wal because wal recovery has already started";
  public static final String FAIL_TO_REMOVE_RECOVER_PERFORMER = "Fail to remove recover performer for file {}";
  public static final String TSFILE_MISSING_SKIP_RECOVERY = "TsFile {} is missing, will skip its recovery.";
  public static final String UNSUPPORTED_TYPE = "Unsupported type ";
  public static final String ERROR_REDO_WAL = "meet error when redo wal of {}";
  public static final String CREATE_FOLDER_FOR_WAL_NODE_BUFFER = "Create folder {} for wal node-{}'s buffer.";
  public static final String OPEN_NEW_WAL_FILE_FOR_BUFFER = "Open new wal file {} for wal node-{}'s buffer.";
  public static final String FAIL_TO_ALLOCATE_WAL_BUFFER_OOM = "Fail to allocate wal node-{}'s buffer because out of memory.";
  public static final String INTERRUPTED_WAITING_ADD_WAL_ENTRY = "Interrupted when waiting for adding WALEntry to buffer.";
  public static final String HANDLE_ROLL_LOG_WRITER_SIGNAL = "Handle roll log writer signal for wal node-{}.";
  public static final String INTERRUPTED_WAITING_WORKING_BUFFER = "Interrupted When waiting for available working buffer.";
  public static final String FAIL_TO_PUT_CLOSE_SIGNAL = "Fail to put CLOSE_SIGNAL to walEntries.";
  public static final String FAIL_TO_CLOSE_WAL_LOG_WRITER = "Fail to close wal node-{}'s log writer.";
  public static final String UNKNOWN_WAL_ENTRY_TYPE = "Unknown WALEntry type";
  public static final String UNKNOWN_WAL_ENTRY_TYPE_WITH_VALUE = "Unknown WALEntry type ";
  public static final String INVALID_WAL_ENTRY_TYPE_CODE = "Invalid WALEntryType code: ";
  public static final String CANNOT_SERIALIZE_CHECKPOINT_TO_WAL = "Cannot serialize checkpoint to wal files.";
  public static final String UNSUPPORTED_WAL_ENTRY_TYPE = "Unsupported wal entry type ";
  public static final String CANNOT_USE_WAL_INFO_AS_SIGNAL_TYPE = "Cannot use wal info type as wal signal type";
  public static final String FAIL_TO_CREATE_WAL_NODE_DISKS_FULL = "Fail to create wal node because all disks of wal folders are full.";
  public static final String FAILED_TO_CREATE_WAL_NODE_AFTER_RETRIES = "Failed to create WAL node after retries for identifier: ";
  public static final String FAIL_TO_CREATE_WAL_NODE = "Fail to create wal node";

  // ======================== Flush ========================

  public static final String RESTORE_FILE_ERROR = "restore file error caused by ";
  public static final String CANNOT_DELETE_OLD_COMPRESSION_FILE = "Can't delete old data region compression file {}";
  public static final String CANNOT_DELETE_RATIO_FILE = "Cannot delete ratio file {}";
  public static final String TAKE_TASK_INTO_IO_QUEUE_INTERRUPTED = "Take task into ioTaskQueue Interrupted";
  public static final String PUT_TASK_INTO_IO_QUEUE_INTERRUPTED = "Put task into ioTaskQueue Interrupted";
  public static final String TAKE_TASK_FROM_IO_QUEUE_INTERRUPTED = "take task from ioTaskQueue Interrupted";
  public static final String FLUSH_SUB_TASK_MANAGER_STARTED = "Flush sub task manager started.";
  public static final String FLUSH_SUB_TASK_MANAGER_STOPPED = "Flush sub task manager stopped";
  public static final String FLUSH_TASK_MANAGER_STARTED = "Flush task manager started.";
  public static final String FLUSH_TASK_MANAGER_STOPPED = "Flush task manager stopped";

  // ======================== Read ========================

  public static final String MEM_CHUNK_READER_NOT_SUPPORT_METHOD = "mem chunk reader does not support this method";
  public static final String MEM_ALIGNED_PAGE_READER_TSBLOCK = "[memAlignedPageReader] TsBlock:{}";
  public static final String AFTER_FILTER_CHUNK_METADATA_LIST = "After removed by filter Chunk meta data list is: ";
  public static final String AFTER_MODIFICATION_CHUNK_METADATA_LIST = "After modification Chunk meta data list is: ";
  public static final String TIME_DATA_SIZE_NOT_MATCH = "Time data size not match";
  public static final String QUERY_OPENED_FILES = "Query has opened {} files !";
  public static final String CANNOT_CLOSE_TSFILE_SEQUENCE_READER = "Can not close TsFileSequenceReader {} !";
  public static final String QUERY_SEALED_FILE_INFO = "[Query Sealed File Info]\n";
  public static final String QUERY_ID_FORMAT = "\t[queryId: {}]\n";
  public static final String QUERY_FILE_PATH_FORMAT = "\t\t{}\n";
  public static final String QUERY_UNSEALED_FILE_INFO = "[Query Unsealed File Info]\n";

  // ======================== Snapshot ========================

  public static final String EXCEPTION_LOAD_SNAPSHOT = "Exception occurs while load snapshot from {}";
  public static final String LOADING_SNAPSHOT_FOR = "Loading snapshot for {}-{}, source directory is {}";
  public static final String EXCEPTION_LOADING_SNAPSHOT_FOR = "Exception occurs when loading snapshot for {}-{}";
  public static final String READING_SNAPSHOT_LOG_FILE = "Reading snapshot log file {}";
  public static final String REMOVE_ALL_DATA_FILES_IN_ORIGINAL_DIR = "Remove all data files in original data dir";
  public static final String FAILED_TO_REMOVE_ORIGIN_DATA_FILES = "Failed to remove origin data files";
  public static final String MOVING_SNAPSHOT_FILE_TO_DATA_DIRS = "Moving snapshot file to data dirs";
  public static final String CANNOT_FIND_SNAPSHOT_DIRECTORY = "Cannot find snapshot directory %s";
  public static final String NO_SEQ_OR_UNSEQ_FILES_IN_SNAPSHOT =
      "No seq or unseq files in snapshot {}, skip creating file links";
  public static final String EXCEPTION_DELETING_TIME_PARTITION_DIR =
      "Exception occurs when deleting time partition directory for {}-{}";
  public static final String CANNOT_CREATE_LINK_FALLBACK_COPY =
      "Cannot create link from {} to {}, fallback to copy";
  public static final String FAILED_TO_PROCESS_SNAPSHOT_FILE =
      "Failed to process file {} in dir {}: {}";
  public static final String FAILED_TO_PROCESS_SNAPSHOT_FILE_AFTER_RETRIES =
      "Failed to process file after retries. Source: %s, Target suffix: %s";
  public static final String SNAPSHOT_FILE_NUM_MISMATCH =
      "The file num in log is %d, while file num in disk is %d";
  public static final String SNAPSHOT_FILE_NOT_IN_LOG = "File %s is not in the log file list";
  public static final String NO_COMPRESSION_RATIO_FILE_IN_DIR = "No compression ratio file in dir {}";
  public static final String CANNOT_LOAD_COMPRESSION_RATIO = "Cannot load compression ratio from {}";
  public static final String LOADED_COMPRESSION_RATIO = "Loaded compression ratio from {}";
  public static final String EXCEPTION_READING_SNAPSHOT_FILE = "Exception occurs when reading snapshot file";
  public static final String SNAPSHOT_NOT_COMPLETE_CANNOT_LOAD = "This snapshot is not complete, cannot load it";
  public static final String CREATED_HARD_LINK = "Created hard link from {} to {}";
  public static final String EXCEPTION_CLOSING_LOG_ANALYZER = "Exception occurs when closing log analyzer";
  public static final String CANNOT_CREATE_PARENT_FOLDER = "Cannot create parent folder for ";
  public static final String CANNOT_CREATE_FILE = "Cannot create file ";
  public static final String FAILED_TO_CLOSE_SNAPSHOT_LOGGER = "Failed to close snapshot logger";
  public static final String SNAPSHOTTING_COMPRESSION_RATIO = "Snapshotting compression ratio {}.";
  public static final String CATCH_IO_EXCEPTION_CREATING_SNAPSHOT = "Catch IOException when creating snapshot";
  public static final String HARD_LINK_TARGET_DIR_NOT_EXIST = "Hard link target dir {} doesn't exist";
  public static final String HARD_LINK_SOURCE_FILE_NOT_EXIST = "Hard link source file {} doesn't exist, this file will be ignored.";
  public static final String COPY_TARGET_DIR_NOT_EXIST = "Copy target dir {} doesn't exist";
  public static final String COPY_SOURCE_FILE_NOT_EXIST = "Copy source file {} doesn't exist";
  public static final String CANNOT_CREATE_DIRECTORY = "Cannot create directory ";
  public static final String CLEANING_UP_SNAPSHOT_DIR = "Cleaning up snapshot dir for {}";
  public static final String FAILED_TO_CREATE_DIR = "Failed to create directory %s";
  public static final String FAILED_TO_TAKE_SNAPSHOT_CLEAN_UP = "Failed to take snapshot for {}-{}, clean up";
  public static final String SUCCESSFULLY_TAKE_SNAPSHOT = "Successfully take snapshot for {}-{}, snapshot directory is {}";
  public static final String EXCEPTION_TAKING_SNAPSHOT = "Exception occurs when taking snapshot for {}-{}";
  public static final String SNAPSHOT_COMPRESSION_RATIO_IN_DIR = "Snapshot compression ratio {} in {}.";
  public static final String CANNOT_SNAPSHOT_COMPRESSION_RATIO = "Cannot snapshot compression ratio {} in {}.";
  public static final String CLEAR_SNAPSHOT_DIR_FAIL = "Clear snapshot dir fail, you should manually delete this dir before do region migration again: {}";
  public static final String HARD_LINK_SOURCE_FILE_RETRY = "Hard link source file {} doesn't exist, will retry for {} times...";
  public static final String TRY_SHOW_FILES_IN_PARENT_DIR = "Try to show all files in parent dir...";
  public static final String CANNOT_SHOW_FILES_PARENT_DIR_NULL = "Cannot show files because parent dir is null";
  public static final String FAILED_DELETE_FOLDER_CLEANING_UP = "Failed to delete folder {} when cleaning up";

  // ======================== TsFile Resource ========================

  public static final String FAILED_TO_SERIALIZE_SHARED_MOD_FILE = "Failed to serialize shared mod file";
  public static final String FAILED_TO_GET_SHARED_MOD_FILE = "Failed to get shared mod file";
  public static final String UPGRADING_MOD_FILE_INTERRUPTED = "Upgrading mod file interrupted";
  public static final String CANNOT_UPGRADE_MOD_FILE = "Cannot upgrade mod file";
  public static final String TIME_INDEX_VALUE = "TimeIndex = {}";
  public static final String RESOURCE_FILE_NOT_FOUND = "resource file not found";
  public static final String CANNOT_BUILD_DEVICE_TIME_INDEX = "cannot build DeviceTimeIndex from resource ";
  public static final String TSFILE_CANNOT_BE_DELETED = "TsFile {} cannot be deleted: {}";
  public static final String MODIFICATION_FILE_CANNOT_BE_DELETED = "ModificationFile {} cannot be deleted: {}";
  public static final String TSFILE_RESOURCE_CANNOT_BE_DELETED = "TsFileResource {} cannot be deleted: {}";
  public static final String FILE_NAME_NOT_STANDARD = "File name may not meet the standard naming specifications.";
  public static final String FAILED_TO_READ_MODS = "Failed to read mods from {} for {}";
  public static final String INVALID_INPUT = "Invalid input: ";
  public static final String ALL_DISKS_FULL_CANNOT_CREATE_TSFILE_DIR = "All disks are full, cannot create tsfile directory";
  public static final String DISK_SPACE_INSUFFICIENT = "Disk space insufficient";
  public static final String FAILED_TO_CREATE_TSFILE_DIR_AFTER_RETRIES = "Failed to create tsfile directory after retries";
  public static final String FAILED_TO_CREATE_DIR_AFTER_RETRIES = "Failed to create directory after retries";
  public static final String TSFILE_NAME_FORMAT_INCORRECT = "tsfile file name format is incorrect:";
  public static final String WRONG_TIME_INDEX_TYPE_LOG = "Wrong timeIndex type {}";
  public static final String WRONG_TIME_INDEX_TYPE = "Wrong timeIndex type ";
  public static final String ERROR_RECORD_FILE_TIME_INDEX_CACHE = "Meet error when record FileTimeIndexCache: {}";
  public static final String ERROR_RECORD_FILE_TIME_INDEX_CACHE_NO_DETAIL = "Meet error when record FileTimeIndexCache";
  public static final String ERROR_COMPACT_FILE_TIME_INDEX_CACHE = "Meet error when compact FileTimeIndexCache: {}";
  public static final String ERROR_COMPACT_FILE_TIME_INDEX_CACHE_NO_DETAIL = "Meet error when compact FileTimeIndexCache";
  public static final String FILE_TIME_INDEX_FILE_ALREADY_EXISTS = "FileTimeIndex file has existed，filePath:{}";
  public static final String ERROR_CLOSE_FILE_TIME_INDEX_CACHE = "Meet error when close FileTimeIndexCache: {}";
  public static final String END_OF_STREAM_REACHED = "The end of stream has been reached";
  public static final String V012_FILE_TIME_INDEX_SHOULD_NEVER_APPEAR = "V012_FILE_TIME_INDEX should never appear";
  public static final String INVALID_ORDINAL = "Invalid ordinal";

  // ======================== DataRegion Utils ========================

  public static final String FAILED_TO_SCAN_FILE = "Failed to scan file {}";
  public static final String DEVICE_LEVEL_METADATA_INDEX_NOT_SUPPORTED = "device level metadata index node is not supported";
  public static final String NO_MORE_DATA_IN_SHARED_TIME_BUFFER = "No more data in SharedTimeDataBuffer";
  public static final String FAILED_TO_CALC_TSFILE_TABLE_SIZES = "Failed to calculate tsfile table sizes";
  public static final String TIME_INDEX_IS_NULL = "{} {} time index is null";
  public static final String EMPTY_RESOURCE = "{} {} empty resource";
  public static final String ERROR_VALIDATE_RESOURCE_FILE = "meet error when validate .resource file:{},e";
  public static final String ILLEGAL_TSFILE = "{} {} illegal tsfile";
  public static final String ERROR_VALIDATING_TSFILE = "Meets error when validating TsFile {}, ";
  public static final String EXCEPTION_APPLY_TABLE_DISK_USAGE_INDEX = "Meet exception when apply TableDiskUsageIndex operation.";
  public static final String FAILED_RECOVER_TABLE_DISK_USAGE_INDEX = "Failed to recover TableDiskUsageIndex";
  public static final String FAILED_SYNC_TABLE_SIZE_INDEX = "Failed to sync tsfile table size index.";
  public static final String WRITE_OBJECT_DELTA = "writeObjectDelta";
  public static final String EXCEPTION_REMOVE_TABLE_DISK_USAGE_INDEX = "Meet exception when remove TableDiskUsageIndex.";
  public static final String INTERRUPTED_ADDING_OP_TO_QUEUE = "Interrupted while adding operation {} to queue.";
  public static final String FAILED_TO_MOVE_FILE = "Failed to move {} to {}";
  public static final String FAILED_TO_READ_KEY_FILE_DURING_COMPACTION = "Failed to read key file during compaction";
  public static final String FAILED_COMPACTION_TABLE_SIZE_INDEX = "Failed to execute compaction for tsfile table size index file";
  public static final String FAILED_TO_READ_TABLE_SIZE_INDEX = "Failed to read table tsfile size index file {}";
  public static final String TABLE_NUM_SHOULD_BE_POSITIVE = "tableNum should be greater than 0";
  public static final String BACKWARD_SEEK_NOT_SUPPORTED = "Backward seek is not supported";
  public static final String THREAD_INTERRUPTED_SKIP_WRITE_FOR_IO_SAFETY = "someone interrupt current thread, so no need to do write for io safety";
  public static final String PARTITION_LOG_FILE_ALREADY_EXISTS = "Partition log file has existed，filePath:{}";

  // ======================== Load TsFile ========================

  public static final String UNSUPPORTED_TSFILE_DATA_TYPE = "Unsupported TsFileData type: ";
  public static final String DELETE_AFTER_LOADING_ERROR = "Delete After Loading {} error.";
  public static final String LOAD_TSFILE_DIR_CREATED = "Load TsFile dir {} is created.";
  public static final String CANNOT_CREATE_TSFILE_FOR_WRITING = "Can not create TsFile {} for writing.";
  public static final String CLOSE_TSFILE_IO_WRITER_ERROR = "Close TsFileIOWriter {} error.";
  public static final String CLOSE_MODIFICATION_FILE_ERROR = "Close ModificationFile {} error.";
  public static final String TASK_DIR_NOT_EMPTY_SKIP_DELETE = "Task dir {} is not empty, skip deleting.";
  public static final String LOAD_CLEANUP_TASK_CANCELED = "Load cleanup task {} is canceled.";
  public static final String LOAD_CLEANUP_TASK_STARTS = "Load cleanup task {} starts.";
  public static final String LOAD_CLEANUP_TASK_ERROR = "Load cleanup task {} error.";
  public static final String FAILED_UPDATE_FILE_COUNTER_DIR_NOT_EXIST = "Failed to update file counter, dir({}) does not exist";
  public static final String UNSUPPORTED_STAGE = "Unsupported stage: ";
  public static final String RELEASE_MEMORY_BLOCK_FAILED = "Release memory block {} failed";
  public static final String EXCEED_TOTAL_MEMORY_SIZE = "{} has exceed total memory size";
  public static final String REDUCE_MEMORY_USAGE_TO_NEGATIVE = "{} has reduce memory usage to negative";
  public static final String FORCE_ALLOCATE_INTERRUPTED = "forceAllocate: interrupted while waiting for available memory";
  public static final String LOAD_ALLOCATED_MEMORY_BLOCK = "Load: Allocated MemoryBlock from query engine, size: {}";
  public static final String RELEASE_DATA_CACHE_MEMORY_BLOCK = "Release Data Cache Memory Block {}";
  public static final String START_DATA_TYPE_CONVERSION_DOT = "Start data type conversion for LoadTsFileStatement: {}.";
  public static final String START_DATA_TYPE_CONVERSION = "Start data type conversion for LoadTsFileStatement: {}";
  public static final String INTERRUPTED_WAITING_TABLET_CONVERSION_SLOT =
      "Interrupted while waiting for tablet conversion slot: ";
  public static final String FAIL_TO_LOAD_TSFILE_TO_ACTIVE_DIR = "Fail to load tsfile to Active dir";
  public static final String FAIL_TO_LOAD_DISK_SPACE = "Fail to load disk space of file {}";
  public static final String LOAD_ACTIVE_LISTENING_DIR_NOT_SET = "Load active listening dir is not set.";
  public static final String FAILED_TO_CREATE_TARGET_DIR = "Failed to create target directory ";
  public static final String FAILED_LOAD_ACTIVE_LISTENING_DIRS = "Failed to load active listening dirs";
  public static final String INVALID_PARAMETER = "Invalid parameter '";
  public static final String UTILITY_CLASS = "Utility class";
  public static final String TSFILE_DATA_BYTE_ARRAY_SIZE_MISMATCH = "TsFileData byte array read error, size mismatch.";
  public static final String UNKNOWN_TSFILE_DATA_TYPE = "Unknown TsFileData type: ";
  public static final String FILE_MAGIC_STRING_INCORRECT = "the file's MAGIC STRING is incorrect, file path: {}";
  public static final String FILE_VERSION_TOO_OLD = "the file's Version Number is too old, file path: {}";
  public static final String FILE_NOT_CLOSED_CORRECTLY = "the file is not closed correctly, file path: {}";
  public static final String MINIO_SELECTOR_REQUIRES_ONE_DIR = "MinIO selector requires at least one directory";
  public static final String ADD_MOUNT_POINT = "Add {}'s mount point {}";
  public static final String FAILED_TO_CHECK_DIRECTORY = "Failed to check directory: {}";
  public static final String FAILED_TO_LIST_FILES_IN_DIR = "Failed to list files in directory: {}";
  public static final String FAILED_TO_DELETE_FILE_OR_DIR = "Failed to delete file or directory: {}";
  public static final String FAILED_TO_CLEANUP_DIRECTORY = "Failed to cleanup directory: {}";
  public static final String CLEANED_UP_ACTIVE_LOAD_DIRS = "Cleaned up active load listening directories";
  public static final String UNEXPECTED_ERROR_CLEANUP_ACTIVE_DIRS = "Unexpected error during cleanup of active load listening directories";
  public static final String ACTIVE_LOAD_DIR_SCANNER_REGISTERED = "Active load dir scanner periodical job registered";
  public static final String ERROR_ACTIVE_LOAD_DIR_SCANNING = "Error occurred during active load dir scanning.";
  public static final String SYSTEM_READ_ONLY_SKIP_ACTIVE_SCAN = "Current system is read-only mode. Skip active load dir scanning.";
  public static final String FILE_DELETED_IGNORE_EXCEPTION = "The file has been deleted. Ignore this exception.";
  public static final String EXCEPTION_SCANNING_DIR = "Exception occurred during scanning dir: {}";
  public static final String ERROR_CREATING_DIR_FOR_ACTIVE_LOAD = "Error occurred during creating directory {} for active load.";
  public static final String FAILED_COUNT_ACTIVE_DIRS_FILE_NUMBER = "Failed to count active listening dirs file number.";
  public static final String ACTIVE_LOAD_METRIC_COLLECTOR_REGISTERED = "Active load metric collector periodical jobs registered";
  public static final String DATABASE_NAME_MUST_NOT_BE_EMPTY = "Database name must not be empty.";
  public static final String ERROR_EXECUTING_ACTIVE_LOAD_JOB = "Error occurred when executing active load periodical job.";
  public static final String ACTIVE_LOAD_EXECUTOR_STARTED = "Active load periodical jobs executor is started successfully.";
  public static final String ACTIVE_LOAD_EXECUTOR_STOPPED = "Active load periodical jobs executor is stopped successfully.";
  public static final String ACTIVE_LOAD_TEMPORARILY_UNAVAILABLE =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to temporary unavailability, will retry later. Status: {}";
  public static final String ERROR_MOVING_FILE_TO_FAIL_DIR = "Error occurred during moving file {} to fail directory.";
  public static final String FAILED_COUNT_FILES_IN_FAIL_DIR = "Failed to count failed files in fail directory.";

  public static final String STRING_NOT_LEGAL_REPAIR_LOG = "String '%s' is not a legal repair log";

  public static final String WRONG_LOAD_COMMAND_S = "Wrong load command %s.";

  public static final String FAILED_TO_FIND_DATA_REGION = "Failed to create state machine for consensus group %s, because data region does not exist";

  public static final String DATA_REGION_IS_NULL = "Data region is null";
}
