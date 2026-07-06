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
  public static final String INVALID_PARAMETER_FOR_LOAD_TSFILE_COMMAND =
      "Invalid parameter '%s' for LOAD TSFILE command.";
  public static final String LOAD_TSFILE_DATABASE_KEY_AND_NAME_CANNOT_COEXIST =
      "The parameter key '%s' and '%s' cannot co-exist.";
  public static final String DATABASE_LEVEL_LESS_THAN_MINIMUM =
      "Given database level %d is less than the minimum value %d, please input a valid database level.";
  public static final String DATABASE_LEVEL_NOT_VALID_INTEGER =
      "Given database level %s is not a valid integer, please input a valid database level.";
  public static final String ON_SUCCESS_VALUE_NOT_SUPPORTED =
      "Given on-success value '%s' is not supported, please input a valid on-success value.";
  public static final String PARAMETER_VALUE_NOT_SUPPORTED_BOOLEAN =
      "Given %s value '%s' is not supported, please input a valid boolean value.";
  public static final String TABLET_CONVERSION_THRESHOLD_NON_NEGATIVE =
      "Tablet conversion threshold must be a non-negative long value.";
  public static final String TABLET_CONVERSION_THRESHOLD_NOT_VALID_LONG =
      "Tablet conversion threshold '%s' is not a valid long value.";
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
  // ---------------------------------------------------------------------------
  // Additional log messages
  // ---------------------------------------------------------------------------
  public static final String STORAGE_LOG_STORAGE_ENGINE_RECOVER_COST_S_C8AEE9D9 =
      "Storage Engine recover cost: {}s.";
  public static final String STORAGE_LOG_DATA_REGIONS_HAVE_BEEN_RECOVERED_D5BD3A80 =
      "Data regions have been recovered {}/{}";
  public static final String STORAGE_LOG_TSFILE_RESOURCE_RECOVER_COST_S_41F074E0 =
      "TsFile Resource recover cost: {}s.";
  public static final String STORAGE_LOG_CONSTRUCT_A_DATA_REGION_INSTANCE_THE_DATABASE_IS_THREAD_17A16BDF =
      "construct a data region instance, the database is {}, Thread is {}";
  public static final String STORAGE_LOG_DATAREGION_NOT_FOUND_ON_THIS_DATANODE_WHEN_WRITING_PIECE_E5B5A888 =
      "DataRegion {} not found on this DataNode when writing piece nodeof TsFile {} (maybe due to "
          + "region migration), will skip.";
  public static final String STORAGE_LOG_IO_ERROR_WHEN_WRITING_PIECE_NODE_OF_TSFILE_TO_DATAREGION_946738F2 =
      "IO error when writing piece node of TsFile {} to DataRegion {}.";
  public static final String STORAGE_LOG_EXCEPTION_OCCURRED_WHEN_WRITING_PIECE_NODE_OF_TSFILE_TO_9EDD09BD =
      "Exception occurred when writing piece node of TsFile {} to DataRegion {}.";
  public static final String STORAGE_LOG_FAILED_TO_RECOVER_DATA_REGION_804B162D =
      "Failed to recover data region {}[{}]";
  public static final String STORAGE_LOG_ERROR_OCCURS_WHEN_DELETING_DATA_REGION_8C07B7A0 =
      "Error occurs when deleting data region {}-{}";
  public static final String STORAGE_LOG_NEXT_LOAD_CLEANUP_TASK_IS_NOT_READY_TO_RUN_WAIT_FOR_AT_LEAST_CBE0023F =
      "Next load cleanup task {} is not ready to run, wait for at least {} ms ({}s).";
  public static final String STORAGE_LOG_WRITER_FOR_PARTITION_IS_ALREADY_WRITING_CHUNK_GROUP_FOR_903B1D66 =
      "Writer {} for partition {} is already writing chunk group for device {}, but the last "
          + "device is {}. ";
  public static final String STORAGE_LOG_CAN_NOT_CREATE_MODIFICATIONFILE_FOR_WRITING_17D14C11 =
      "Can not create ModificationFile {} for writing.";
  public static final String STORAGE_LOG_SKIP_RECOVERING_DATA_REGION_WHEN_CONSENSUS_PROTOCOL_IS_RATIS_43A6A699 =
      "Skip recovering data region {}[{}] when consensus protocol is ratis and storage engine is "
          + "not ready.";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLET_BECAUSE_C2DC8032 =
      "Won't insert tablet {}, because {}";
  public static final String STORAGE_LOG_TIMESTAMP_MEASUREMENTID_IDEVICEID_04A5AE37 =
      "timestamp {}, measurementId {}, ideviceId {}";
  public static final String STORAGE_LOG_DELETION_SKIPPED_FILE_TIME_DD653236 =
      "[Deletion] {} skipped {}, file time [{}, {}]";
  public static final String STORAGE_LOG_DEVICE_IS_DEVICETABLE_IS_TABLEDELETIONENTRY_GETPREDICATE_E84489E9 =
      "device is {}, deviceTable is {}, tableDeletionEntry.getPredicate().matches(device) is {}";
  public static final String STORAGE_LOG_TABLENAME_IS_MATCHSIZE_IS_ONLYONETABLE_IS_E20FAFAE =
      "tableName is {}, matchSize is {}, onlyOneTable is {}";
  public static final String STORAGE_LOG_TABLENAME_IS_DEVICE_IS_DELETIONSTARTTIME_IS_DELETIONENDTIME_B881E677 =
      "tableName is {}, device is {}, deletionStartTime is {}, deletionEndTime is {}, "
          + "fileStartTime is {}, fileEndTime is {}";
  public static final String STORAGE_LOG_DELETE_TSFILERESOURCE_IS_29F5A98C =
      "delete tsFileResource is {}";
  public static final String STORAGE_LOG_DELETETSFILECOMPLETELY_EXECUTE_SUCCESSFUL_ALL_TSFILE_ARE_D81FE0D7 =
      "deleteTsFileCompletely execute successful, all tsfile are deleted successfully";
  public static final String STORAGE_LOG_DELETION_DELETION_WRITTEN_INTO_MODS_FILE_F5E26D2A =
      "[Deletion] Deletion {} written into mods file:{}.";
  public static final String STORAGE_LOG_DATABASE_SYSTEM_DIRECTORY_DOESN_T_EXIST_CREATE_IT_9C0E7C68 =
      "Database system Directory {} doesn't exist, create it";
  public static final String STORAGE_LOG_DATA_REGION_DIRECTORY_DOESN_T_EXIST_CREATE_IT_EFB0AE77 =
      "Data region directory {} doesn't exist, create it";
  public static final String STORAGE_LOG_THE_TSFILES_OF_DATA_REGION_HAS_RECOVERED_E17384CF =
      "The TsFiles of data region {}[{}] has recovered {}/{}.";
  public static final String STORAGE_LOG_THE_TSFILES_OF_DATA_REGION_HAS_RECOVERED_COMPLETELY_0D79FC83 =
      "The TsFiles of data region {}[{}] has recovered completely {}/{}.";
  public static final String STORAGE_LOG_THE_DATA_REGION_IS_CREATED_SUCCESSFULLY_B991F1D4 =
      "The data region {}[{}] is created successfully";
  public static final String STORAGE_LOG_THE_DATA_REGION_IS_RECOVERED_SUCCESSFULLY_5AAFF7B7 =
      "The data region {}[{}] is recovered successfully";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLET_BECAUSE_REGION_IS_DELETED_34D893A7 =
      "Won't insert tablet {}, because region is deleted";
  public static final String STORAGE_LOG_ASYNC_CLOSE_TSFILE_FILE_START_TIME_FILE_END_TIME_65020832 =
      "Async close tsfile: {}, file start time: {}, file end time: {}";
  public static final String STORAGE_LOG_WILL_CLOSE_ALL_FILES_FOR_DELETING_DATA_FOLDER_93A5B15E =
      "{} will close all files for deleting data folder {}";
  public static final String STORAGE_LOG_WILL_CLOSE_ALL_FILES_FOR_DELETING_DATA_FILES_7768D429 =
      "{} will close all files for deleting data files";
  public static final String STORAGE_LOG_EXCEED_SEQUENCE_MEMTABLE_FLUSH_INTERVAL_SO_FLUSH_WORKING_23513D66 =
      "Exceed sequence memtable flush interval, so flush working memtable of time partition {} in "
          + "database {}[{}]";
  public static final String STORAGE_LOG_EXCEED_UNSEQUENCE_MEMTABLE_FLUSH_INTERVAL_SO_FLUSH_WORKING_BADB0B75 =
      "Exceed unsequence memtable flush interval, so flush working memtable of time partition {} "
          + "in database {}[{}]";
  public static final String STORAGE_LOG_START_TO_WAIT_TSFILES_TO_CLOSE_SEQ_FILES_UNSEQ_FILES_441F7130 =
      "Start to wait TsFiles to close, seq files: {}, unseq files: {}";
  public static final String STORAGE_LOG_ASYNC_FORCE_CLOSE_ALL_FILES_IN_DATABASE_076AB4B9 =
      "async force close all files in database: {}";
  public static final String STORAGE_LOG_FORCE_CLOSE_ALL_PROCESSORS_IN_DATABASE_68C9EB60 =
      "force close all processors in database: {}";
  public static final String STORAGE_LOG_WILL_DELETE_DATA_FILES_DIRECTLY_FOR_DELETING_DATA_BETWEEN_289DD3BF =
      "{} will delete data files directly for deleting data between {} and {}";
  public static final String STORAGE_LOG_DELETION_DELETION_IS_WRITTEN_INTO_MOD_FILES_DDCDF0AD =
      "[Deletion] Deletion {} is written into {} mod files";
  public static final String STORAGE_LOG_TTL_START_TTL_AND_MODIFICATION_CHECKING_A37AB173 =
      "[TTL] {}-{} Start ttl and modification checking.";
  public static final String STORAGE_LOG_TTL_TOTALLY_SELECT_ALL_OUTDATED_FILES_AND_PARTIAL_OUTDATED_5246BD61 =
      "[TTL] {}-{} Totally select {} all-outdated files and {} partial-outdated files.";
  public static final String STORAGE_LOG_WON_T_LOAD_TSFILE_BECAUSE_REGION_IS_DELETED_0E72E8D0 =
      "Won't load TsFile {}, because region is deleted";
  public static final String STORAGE_LOG_TSFILE_MUST_BE_RENAMED_TO_FOR_LOADING_INTO_THE_UNSEQUENCE_70321619 =
      "TsFile {} must be renamed to {} for loading into the unsequence list.";
  public static final String STORAGE_LOG_LOAD_TSFILE_IN_UNSEQUENCE_LIST_MOVE_FILE_FROM_TO_21E11AEB =
      "Load tsfile in unsequence list, move file from {} to {}";
  public static final String STORAGE_LOG_MOVE_TSFILE_TO_TARGET_DIR_SUCCESSFULLY_57288783 =
      "Move tsfile {} to target dir {} successfully.";
  public static final String STORAGE_LOG_WON_T_INSERT_TABLETS_BECAUSE_REGION_IS_DELETED_48E9720F =
      "Won't insert tablets {}, because region is deleted";
  public static final String STORAGE_LOG_HAS_SPENT_S_TO_WAIT_FOR_CLOSING_ALL_TSFILES_6C3EE4CE =
      "{} has spent {}s to wait for closing all TsFiles.";
  public static final String STORAGE_LOG_SSEQ_FILES_UNSEQ_FILES_918AEB2A =
      "Sseq files: {}, unseq files: {}";
  public static final String STORAGE_LOG_UNRECOGNIZED_LASTCACHELOADSTRATEGY_FALL_BACK_TO_CLEAN_ALL_C200F32D =
      "Unrecognized LastCacheLoadStrategy: {}, fall back to CLEAN_ALL";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_TARGET_28E43D85 =
      "File renaming failed when loading tsfile. Origin: {}, Target: {}";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_TARGET_9C22DDF3 =
      "File renaming failed when loading .resource file. Origin: {}, Target: {}";
  public static final String STORAGE_LOG_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_TARGET_18A212F3 =
      "File renaming failed when loading .mod file. Origin: {}, Target: {}";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_DELETING_DATA_REGION_FOLDER_FOR_8ABCF5D1 =
      "Exception occurs when deleting data region folder for {}-{}";
  public static final String STORAGE_LOG_FAIL_TO_RECOVER_UNSEALED_TSFILE_SKIP_IT_CA576205 =
      "Fail to recover unsealed TsFile {}, skip it.";
  public static final String STORAGE_LOG_REMOTE_REQUEST_CONFIG_NODE_FAILED_THAT_JUDGMENT_IF_TABLE_25FE3602 =
      "Remote request config node failed that judgment if table is exist, occur exception. {}";
  public static final String STORAGE_LOG_DUE_TSTABLE_IS_NULL_TABLE_SCHEMA_CAN_T_BE_GOT_LEADER_NODE_C3EF524D =
      "Due tsTable is null, table schema can't be got, leader node occur special situation need to "
          + "resolve.";
  public static final String STORAGE_LOG_DISK_SPACE_IS_INSUFFICIENT_WHEN_CREATING_TSFILE_PROCESSOR_4032BAF0 =
      "disk space is insufficient when creating TsFile processor, change system mode to read-only";
  public static final String STORAGE_LOG_MEET_IOEXCEPTION_WHEN_CREATING_TSFILEPROCESSOR_CHANGE_SYSTEM_4337F729 =
      "meet IOException when creating TsFileProcessor, change system mode to error";
  public static final String STORAGE_LOG_CLOSEFILENODECONDITION_ERROR_OCCURS_WHILE_WAITING_FOR_CLOSING_F33B72A6 =
      "CloseFileNodeCondition error occurs while waiting for closing the storage group {}";
  public static final String STORAGE_LOG_CLOSEFILENODECONDITION_ERROR_OCCURS_WHILE_WAITING_FOR_CLOSING_C4B97CC0 =
      "CloseFileNodeCondition error occurs while waiting for closing tsfile processors of {}";
  public static final String STORAGE_LOG_FAILED_TO_APPEND_THE_TSFILE_TO_DATABASE_PROCESSOR_BECAUSE_670341AE =
      "Failed to append the tsfile {} to database processor {} because the disk space is "
          + "insufficient.";
  public static final String STORAGE_LOG_GET_TIMESERIES_METADATA_IN_FILE_FROM_CACHE_36652729 =
      "Get timeseries: {}.{}  metadata in file: {}  from cache: {}.";
  public static final String STORAGE_LOG_TIMESERIESMETADATACACHE_SIZE_E31733D3 =
      "TimeSeriesMetadataCache size = {}";
  public static final String STORAGE_LOG_FLUSH_TASK_OF_DATABASE_MEMTABLE_IS_CREATED_FLUSHING_TO_FILE_E44B3AA0 =
      "flush task of database {} memtable is created, flushing to file {}.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_INTO_FILE_DATA_SORT_TIME_COST_3D39AA17 =
      "Database {} memtable flushing into file {}: data sort time cost {} ms.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_STARTS_TO_ENCODING_DATA_6A89F32E =
      "Database {} memtable flushing to file {} starts to encoding data.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_START_IO_CB72C2DA =
      "Database {} memtable flushing to file {} start io.";
  public static final String STORAGE_LOG_FLUSHING_A_MEMTABLE_TO_FILE_IN_DATABASE_IO_COST_MS_2306578A =
      "flushing a memtable to file {} in database {}, io cost {}ms";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_FLUSHING_TO_FILE_ENCODING_TASK_IS_INTERRUPTED_9D7BF4EF =
      "Database {} memtable flushing to file {}, encoding task is interrupted.";
  public static final String STORAGE_LOG_DATABASE_MEMTABLE_IO_TASK_MEETS_ERROR_EC383D33 =
      "Database {} memtable {}, io task meets error.";
  public static final String STORAGE_LOG_OLD_RATIO_FILE_DOESN_T_EXIST_FORCE_CREATE_RATIO_FILE_74EDD7DB =
      "Old ratio file {} doesn't exist, force create ratio file {}";
  public static final String STORAGE_LOG_COMPRESSION_RATIO_FILE_UPDATED_PREVIOUS_CURRENT_7A9EEDF8 =
      "Compression ratio file updated, previous: {}, current: {}";
  public static final String STORAGE_LOG_AFTER_RESTORING_FROM_COMPRESSION_RATIO_FILE_TOTAL_MEMORY_D5ACB1C4 =
      "After restoring from compression ratio file, total memory size = {}, total disk size = {}";
  public static final String STORAGE_LOG_THE_COMPRESSION_RATIO_IS_NEGATIVE_CURRENT_MEMTABLESIZE_TOTALMEMTABLESIZE_8C3DD017 =
      "The compression ratio is negative, current memTableSize: {}, totalMemTableSize: {}";
  public static final String STORAGE_LOG_REBOOT_WAL_DELETE_THREAD_SUCCESSFULLY_CURRENT_PERIOD_IS_44B69C7A =
      "Reboot wal delete thread successfully, current period is {} ms";
  public static final String STORAGE_LOG_WAL_DISK_USAGE_IS_LARGER_THAN_THE_WAL_THROTTLE_THRESHOLD_2396FFCC =
      "WAL disk usage {} is larger than the wal_throttle_threshold_in_byte * 0.8 {}, please check "
          + "your write load, iot consensus and the pipe module. It's better to allocate more disk for "
          + "WAL.";
  public static final String STORAGE_LOG_FLUSH_A_WORKING_MEMTABLE_IN_ASYNC_CLOSE_TSFILE_MEMTABLE_00158706 =
      "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile size: {}, "
          + "plan index: [{}, {}], progress index: {}";
  public static final String STORAGE_LOG_FLUSH_A_NOTIFYFLUSHMEMTABLE_IN_ASYNC_CLOSE_TSFILE_TSFILE_48D1E75A =
      "{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}";
  public static final String STORAGE_LOG_MEMTABLE_SIGNAL_IS_ADDED_INTO_THE_FLUSHING_MEMTABLE_QUEUE_5D9DA8DB =
      "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}";
  public static final String STORAGE_LOG_MEMTABLE_SIGNAL_IS_REMOVED_FROM_THE_QUEUE_LEFT_DFDB97D2 =
      "{}: {} memtable (signal={}) is removed from the queue. {} left.";
  public static final String STORAGE_LOG_MEM_CONTROL_FLUSH_FINISHED_TRY_TO_RESET_SYSTEM_MEM_COST_3CD8399C =
      "[mem control] {}: {} flush finished, try to reset system mem cost, flushing memtable list "
          + "size: {}";
  public static final String STORAGE_LOG_FLUSH_FINISHED_REMOVE_A_MEMTABLE_FROM_FLUSHING_LIST_FLUSHING_08A00750 =
      "{}: {} flush finished, remove a memtable from flushing list, flushing memtable list size: {}";
  public static final String STORAGE_LOG_RELEASED_A_MEMTABLE_SIGNAL_FLUSHINGMEMTABLES_SIZE_6D22169F =
      "{}: {} released a memtable (signal={}), flushingMemtables size ={}";
  public static final String STORAGE_LOG_TRY_GET_LOCK_TO_RELEASE_A_MEMTABLE_SIGNAL_B9098E21 =
      "{}: {} try get lock to release a memtable (signal={})";
  public static final String STORAGE_LOG_FLUSHINGMEMTABLES_IS_EMPTY_AND_WILL_CLOSE_THE_FILE_22A07A5C =
      "{}: {} flushingMemtables is empty and will close the file";
  public static final String STORAGE_LOG_TRY_TO_GET_FLUSHINGMEMTABLES_LOCK_F91EA27F =
      "{}: {} try to get flushingMemtables lock.";
  public static final String STORAGE_LOG_RELEASE_FLUSHQUERYLOCK_6DF2C0FC =
      "{}: {} release flushQueryLock";
  public static final String STORAGE_LOG_DELETION_DELETION_WITH_IN_WORKMEMTABLE_POINTS_DELETED_00EA995A =
      "[Deletion] Deletion with {} in workMemTable, {} points deleted";
  public static final String STORAGE_LOG_SYNC_CLOSE_FILE_WILL_FIRSTLY_ASYNC_CLOSE_IT_34588A7D =
      "Sync close file: {}, will firstly async close it";
  public static final String STORAGE_LOG_ASYNC_FLUSH_A_MEMTABLE_TO_TSFILE_00ED383A =
      "Async flush a memtable to tsfile: {}";
  public static final String STORAGE_LOG_THIS_NORMAL_MEMTABLE_IS_EMPTY_SKIP_FLUSH_6C195557 =
      "This normal memtable is empty, skip flush. {}: {}";
  public static final String STORAGE_LOG_IS_CLOSED_DURING_FLUSH_ABANDON_FLUSH_TASK_DD47632F =
      "{}: {} is closed during flush, abandon flush task";
  public static final String STORAGE_LOG_THE_COMPRESSION_RATIO_OF_TSFILE_IS_TOTALMEMTABLESIZE_THE_8CE66BE3 =
      "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}";
  public static final String STORAGE_LOG_STORAGE_GROUP_CLOSE_AND_REMOVE_EMPTY_FILE_72D42293 =
      "Storage group {} close and remove empty file {}";
  public static final String STORAGE_LOG_PUT_THE_MEMTABLE_SIGNAL_OUT_OF_FLUSHINGMEMTABLES_BUT_IT_D78AF257 =
      "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSH_FILEMETADATA_TO_RETRY_IT_AGAIN_DAAF298C =
      "{} meet error when flush FileMetadata to {}, retry it again";
  public static final String STORAGE_LOG_ASYNC_CLOSE_FAILED_BECAUSE_C5B63B78 =
      "{}: {} async close failed, because";
  public static final String STORAGE_LOG_ADD_A_MEMTABLE_INTO_FLUSHING_LIST_FAILED_30FA8E58 =
      "{}: {} add a memtable into flushing list failed";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSHING_A_MEMTABLE_CHANGE_SYSTEM_MODE_TO_0C6D5025 =
      "{}: {} meet error when flushing a memtable, change system mode to error";
  public static final String STORAGE_LOG_IOTASK_MEETS_ERROR_TRUNCATE_THE_CORRUPTED_DATA_E9041D54 =
      "{}: {} IOTask meets error, truncate the corrupted data";
  public static final String STORAGE_LOG_TRUNCATE_CORRUPTED_DATA_MEETS_ERROR_3757A85E =
      "{}: {} Truncate corrupted data meets error";
  public static final String STORAGE_LOG_RELEASE_RESOURCE_MEETS_ERROR_B62CBC3A =
      "{}: {} Release resource meets error";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_WRITING_INTO_MODIFICATIONFILE_FILE_OF_63B5E24A =
      "Meet error when writing into ModificationFile file of {} ";
  public static final String STORAGE_LOG_MARKING_OR_ENDING_FILE_MEET_ERROR_5653B904 =
      "{}: {} marking or ending file meet error";
  public static final String STORAGE_LOG_TRUNCATE_CORRUPTED_DATA_MEETS_ERROR_8F721CC1 =
      "{}: {} truncate corrupted data meets error";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_FLUSH_FILEMETADATA_TO_CHANGE_SYSTEM_MODE_0BC79DA5 =
      "{} meet error when flush FileMetadata to {}, change system mode to error";
  public static final String STORAGE_LOG_UPDATE_COMPRESSION_RATIO_FAILED_8A076DFC =
      "{}: {} update compression ratio failed";
  public static final String STORAGE_LOG_GET_READONLYMEMCHUNK_HAS_ERROR_2366DE2A =
      "{}: {} get ReadOnlyMemChunk has error";
  public static final String STORAGE_LOG_FAILED_TO_TRANSFER_TVLIST_MEMORY_OWNER_TO_QUERY_ENGINE_0DFA506D =
      "Failed to transfer tvlist memory owner to query engine, {}";
  public static final String STORAGE_LOG_THE_FORMAT_OF_MAX_POINT_NUMBER_IS_NOT_CORRECT_USING_DEFAULT_1B78AF69 =
      "The format of MAX_POINT_NUMBER {}  is not correct. Using default float precision.";
  public static final String STORAGE_LOG_THE_MAX_POINT_NUMBER_SHOULDN_T_BE_LESS_THAN_0_USING_DEFAULT_12745217 =
      "The MAX_POINT_NUMBER shouldn't be less than 0. Using default float precision {}.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MOD_FILE_EXPECTING_OFFSET_ACTUALLY_SKIPPED_8B96B670 =
      "Fail to read Mod file {}, expecting offset {}, actually skipped {}";
  public static final String STORAGE_LOG_AFTER_THE_MOD_FILE_IS_SETTLED_THE_FILE_SIZE_IS_STILL_GREATER_FA454979 =
      "After the mod file is settled, the file size is still greater than 1M,the size of the file "
          + "before settle is {},after settled the file size is {}";
  public static final String STORAGE_LOG_THE_START_TIME_OF_IS_GREATER_THAN_END_TIME_44DD784A =
      "{} {} the start time of {} is greater than end time";
  public static final String STORAGE_LOG_THERE_IS_NO_DATA_IN_THE_FILE_F480954E =
      "{} {} there is no data in the file";
  public static final String STORAGE_LOG_CHUNK_START_OFFSET_IS_INCONSISTENT_WITH_THE_VALUE_IN_THE_E1E7AF07 =
      "{} chunk start offset is inconsistent with the value in the metadata.";
  public static final String STORAGE_LOG_TIME_RANGES_OVERLAP_BETWEEN_PAGES_2A131465 =
      "{} {} time ranges overlap between pages.";
  public static final String STORAGE_LOG_THE_TIMESTAMP_IN_THE_PAGE_IS_REPEATED_OR_NOT_INCREMENTAL_04627FDA =
      "{} {} the timestamp in the page is repeated or not incremental.";
  public static final String STORAGE_LOG_THE_START_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_C23CE8D4 =
      "{} {} the start time in page is different from that in page header.";
  public static final String STORAGE_LOG_THE_END_TIME_IN_PAGE_IS_DIFFERENT_FROM_THAT_IN_PAGE_HEADER_5E363FAB =
      "{} {} the end time in page is different from that in page header.";
  public static final String STORAGE_LOG_DEVICE_ID_IS_NULL_OR_EMPTY_635DD75C =
      "{} {} device id is null or empty.";
  public static final String STORAGE_LOG_DEVICE_IS_OVERLAPPED_BETWEEN_AND_END_TIME_IN_IS_START_TIME_BA49D2AA =
      "Device {} is overlapped between {} and {}, end time in {} is {}, start time in {} is {}";
  public static final String STORAGE_LOG_PATH_FILE_IS_NOT_SATISFIED_BECAUSE_OF_NO_DEVICE_8BB15136 =
      "Path: {} file {} is not satisfied because of no device!";
  public static final String STORAGE_LOG_PATH_FILE_IS_NOT_SATISFIED_BECAUSE_OF_TIME_FILTER_71121709 =
      "Path: {} file {} is not satisfied because of time filter!";
  public static final String STORAGE_LOG_STARTTIME_OF_TSFILERESOURCE_IS_GREATER_THAN_ITS_ENDTIME_BC6CC591 =
      "startTime[{}] of TsFileResource[{}] is greater than its endTime[{}]";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_GETSTARTTIME_OF_IN_FILE_D7F27B92 =
      "meet error when getStartTime of {} in file {}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_GETENDTIME_OF_IN_FILE_350DA42F =
      "meet error when getEndTime of {} in file {}";
  public static final String STORAGE_LOG_CANNOT_SERIALIZE_TSFILERESOURCE_WHEN_UPDATING_PLAN_INDEX_69665DD5 =
      "Cannot serialize TsFileResource {} when updating plan index {}-{}";
  public static final String STORAGE_LOG_DATAREGIONSYSDIR_HAS_EXISTED_FILEPATH_53009475 =
      "DataRegionSysDir has existed，filePath:{}";
  public static final String STORAGE_LOG_FILETIMEINDEX_LOG_FILE_CREATE_FILED_FILEPATH_D675FBD5 =
      "FileTimeIndex log file create filed，filePath:{}";
  public static final String STORAGE_LOG_CAN_T_READ_FILE_FROM_DISK_F5625609 =
      "Can't read file {} from disk ";
  public static final String STORAGE_LOG_FAILED_TO_GET_DEVICES_FROM_TSFILE_F94CF47B =
      "Failed to get devices from tsfile: {}";
  public static final String STORAGE_LOG_TABLEDISKUSAGEINDEX_WORKER_THREAD_WAS_INTERRUPTED_UNEXPECTEDLY_A21172AB =
      "TableDiskUsageIndex worker thread was interrupted unexpectedly while waiting for operations.";
  public static final String STORAGE_LOG_SKIP_ADDING_OPERATION_TO_QUEUE_BECAUSE_TABLEDISKUSAGEINDEX_4A606B40 =
      "Skip adding operation {} to queue because TableDiskUsageIndex has been stopped.";
  public static final String STORAGE_LOG_ATTEMPT_TO_DECREASE_ACTIVEREADERNUM_WHEN_IT_IS_ALREADY_0_73756CBB =
      "Attempt to decrease activeReaderNum when it is already 0. This may indicate an incorrect "
          + "reader lifecycle management.";
  public static final String STORAGE_LOG_FAILED_TO_DELETE_OLD_VERSION_TABLE_SIZE_INDEX_FILE_05930C4A =
      "Failed to delete old version table size index file {}";
  public static final String STORAGE_LOG_FAILED_TO_READ_TABLE_TSFILE_SIZE_INDEX_AFTER_POSITION_AND_74251AF3 =
      "Failed to read table tsfile size index {} after position: {} and {} after position: {}";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_COMPACTION_SCHEDULE_IS_INTERRUPTED_9EF702D1 =
      "[CompactionScheduleTaskWorker-{}] compaction schedule is interrupted, isStopByUser: {}";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_FAILED_TO_EXECUTE_COMPACTION_4F302761 =
      "[CompactionScheduleTaskWorker-{}] Failed to execute compaction schedule task";
  public static final String STORAGE_LOG_COMPACTIONSCHEDULETASKWORKER_FAILED_TO_EXECUTE_COMPACTION_E571F6E3 =
      "[CompactionScheduleTaskWorker-{}] Failed to execute compaction schedule task and cannot "
          + "recover";
  public static final String STORAGE_LOG_COMPACTION_SCHEDULE_TASK_THREAD_POOL_CAN_NOT_BE_CLOSED_IN_27D38188 =
      "compaction schedule task thread pool can not be closed in {} ms";
  public static final String STORAGE_LOG_TTLCHECKTASK_TTL_CHECKER_IS_INTERRUPTED_ISSTOPPEDBYUSER_B1E45A2E =
      "[TTLCheckTask-{}] TTL checker is interrupted, isStoppedByUser: {}";
  public static final String STORAGE_LOG_TTLCHECKTASK_FAILED_TO_EXECUTE_TTL_CHECK_AND_CANNOT_RECOVER_6F4E4A13 =
      "[TTLCheckTask-{}] Failed to execute ttl check and cannot recover";
  public static final String STORAGE_LOG_COMPACTION_TASK_START_CHECK_FAILED_BECAUSE_DISK_FREE_RATIO_9D2BE2FE =
      "Compaction task start check failed because disk free ratio is less than "
          + "disk_space_warning_threshold";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_ADDING_TASK_TO_COMPACTION_WAITING_QUEUE_84AA345D =
      "meet error when adding task-{} to compaction waiting queue: {}";
  public static final String STORAGE_LOG_SETTLE_COMPLETES_FILE_PATH_THE_REMAINING_FILE_TO_BE_SETTLED_32DF95A7 =
      "Settle completes, file path:{} , the remaining file to be settled num: {}";
  public static final String STORAGE_LOG_THE_TSFILE_SHOULD_BE_SEALED_WHEN_SETTLING_8DBD716A =
      "The tsFile {} should be sealed when settling.";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_SETTLING_FILE_CBA0F9D7 =
      "meet error when settling file:{}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_READ_TSFILE_RESOURCE_FILE_IT_MAY_BE_REPAIRED_A8A514C6 =
      "Meet error when read tsfile resource file {}, it may be repaired after reboot";
  public static final String STORAGE_LOG_FILE_HAS_UNSORTED_DATA_1B118A14 =
      "File {} has unsorted data: ";
  public static final String STORAGE_LOG_FILE_HAS_WRONG_TIME_STATISTICS_4E63345E =
      "File {} has wrong time statistics: ";
  public static final String STORAGE_LOG_DEVICE_HAS_OVERLAPPED_DATA_START_TIME_IN_CURRENT_FILE_IS_F4F29A22 =
      "Device {} has overlapped data, start time in current file {} is {}, end time in previous "
          + "file {} is {}";
  public static final String STORAGE_LOG_REPAIR_DATA_LOG_IS_NOT_COMPLETE_TIME_PARTITION_IS_D9D4F01F =
      "[{}][{}]Repair data log is not complete, time partition is {}.";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_RECOVER_UNFINISHED_REPAIR_SCHEDULE_TASK_7C5B6D5F =
      "[RepairScheduler] recover unfinished repair schedule task from log file: {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_SKIP_REPAIR_TIME_PARTITION_BECAUSE_IT_IS_BDD35739 =
      "[RepairScheduler][{}][{}] skip repair time partition {} because it is repaired";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_SUBMIT_A_REPAIR_TIME_PARTITION_SCAN_TASK_0E98F12C =
      "[RepairScheduler] submit a repair time partition scan task {}-{}-{}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_PARSE_REPAIR_LOG_FILE_142D2568 =
      "[RepairScheduler] Failed to parse repair log file {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_TASK_START_TIME_95552D7E =
      "[RepairScheduler] Failed to record repair task start time in log file {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_CLOSE_REPAIR_LOGGER_EC191F6B =
      "[RepairScheduler] Failed to close repair logger {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_START_SCAN_REPAIR_TIME_PARTITION_1D6789DB =
      "[RepairScheduler][{}][{}] start scan repair time partition {}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_CANNOT_SCAN_SOURCE_FILES_IN_BECAUSE_ALLOWCOMPACTION_5E644A6D =
      "[RepairScheduler] cannot scan source files in {} because 'allowCompaction' is false";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_HAS_INTERNAL_UNSORTED_C1596DC3 =
      "[RepairScheduler] {} need to repair because it has internal unsorted data";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_IS_OVERLAPPED_F1AC0C78 =
      "[RepairScheduler] {} need to repair because it is overlapped with other files";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_TIME_PARTITION_HAS_BEEN_REPAIRED_PROGRESS_697FEA22 =
      "[RepairScheduler][{}][{}] time partition {} has been repaired, progress: {}/{}";
  public static final String STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_LOG_FOR_TIME_PARTITION_11251247 =
      "[RepairScheduler][{}][{}] failed to record repair log for time partition {}";
  public static final String STORAGE_LOG_COMPACTION_TMP_TARGET_TSFILE_MAY_BE_DELETED_AFTER_COMPACTION_0BFFA73F =
      "{} [Compaction] Tmp target tsfile {} may be deleted after compaction.";
  public static final String STORAGE_LOG_COMPACTION_DELETE_TSFILE_A97320DB =
      "{} [Compaction] delete TsFile {}";
  public static final String STORAGE_LOG_COMPACTION_DELETE_FILE_FAILED_FILE_PATH_IS_6E1D2670 =
      "[Compaction] delete file failed, file path is {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_RECOVER_COMPACTION_TASKINFO_24424402 =
      "{} [Compaction][Recover] Failed to recover compaction. TaskInfo: {}, Exception: {}";
  public static final String STORAGE_LOG_FAILED_TO_PASS_COMPACTION_VALIDATION_SOURCE_SEQ_FILES_SOURCE_BF5A4525 =
      "Failed to pass compaction validation, source seq files: {}, source unseq files: {}, target "
          + "files: {}";
  public static final String STORAGE_LOG_FAILED_TO_PASS_COMPACTION_OVERLAP_VALIDATION_SOURCE_SEQ_9CFDC149 =
      "Failed to pass compaction overlap validation, source seq files: {}, source unseq files: {}, "
          + "target files: {}";
  public static final String STORAGE_LOG_COMPACTION_TASK_INTERRUPTED_E31121C0 =
      "{}-{} [Compaction] {} task interrupted";
  public static final String STORAGE_LOG_COMPACTION_TASK_MEETS_ERROR_1002C659 =
      "{}-{} [Compaction] {} task meets error: {}.";
  public static final String STORAGE_LOG_UNEXPECTED_CHUNK_TYPE_DETECTED_WHEN_READING_NON_ALIGNED_1C0E4674 =
      "Unexpected chunk type detected when reading non-aligned chunk reader. The chunk metadata "
          + "indicates a non-aligned chunk, but the actual chunk read from tsfile is a value chunk of "
          + "aligned series. tsFile={}, device={}, measurement={}, offsetOfChunkHeader={}";
  public static final String STORAGE_LOG_INNERSPACECOMPACTIONTASK_START_TO_RENAME_MODS_FILE_7C036CBD =
      "{}-{} [InnerSpaceCompactionTask] start to rename mods file";
  public static final String STORAGE_LOG_REPAIRUNSORTEDFILECOMPACTIONTASK_CAN_NOT_REPAIR_UNSORTED_48124B0C =
      "[RepairUnsortedFileCompactionTask] Can not repair unsorted file {} because the required "
          + "memory to repair is greater than the total compaction memory budget";
  public static final String STORAGE_LOG_COMPACTION_INNERSPACECOMPACTION_TASK_STARTS_WITH_FILES_TOTAL_934B562F =
      "{}-{} [Compaction] {} InnerSpaceCompaction task starts with {} files, total file size is {} "
          + "MB, estimated memory cost is {} MB";
  public static final String STORAGE_LOG_COMPACTION_COMPACTION_WITH_SELECTED_FILES_SKIPPED_FILES_ACC66872 =
      "{}-{} [Compaction] compaction with selected files {}, skipped files {}";
  public static final String STORAGE_LOG_COMPACTION_INNERSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_08475DE4 =
      "{}-{} [Compaction] {} InnerSpaceCompaction task finishes successfully, target files are "
          + "{},time cost is {} s, compaction speed is {} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_INSERTIONCROSSSPACECOMPACTION_TASK_STARTS_WITH_A315B8C6 =
      "{}-{} [Compaction] InsertionCrossSpaceCompaction task starts with unseq file {}, nearest "
          + "seq files are {}, target file name timestamp is {}, file size is {} MB.";
  public static final String STORAGE_LOG_COMPACTION_INSERTIONCROSSSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_69360DD0 =
      "{}-{} [Compaction] InsertionCrossSpaceCompaction task finishes successfully, target file is "
          + "{},time cost is {} s.";
  public static final String STORAGE_LOG_INSERTIONCROSSSPACECOMPACTIONTASK_FAILED_TO_GENERATE_TARGET_B03E4C67 =
      "{}-{} [InsertionCrossSpaceCompactionTask] failed to generate target file name, source unseq "
          + "file is {}";
  public static final String STORAGE_LOG_SETTLE_TASK_DELETES_FULLY_DIRTY_TSFILE_SUCCESSFULLY_18D81225 =
      "Settle task deletes fully_dirty tsfile {} successfully.";
  public static final String STORAGE_LOG_COMPACTION_SETTLE_COMPACTION_FILE_LIST_IS_EMPTY_END_IT_56CF079D =
      "{}-{} [Compaction] Settle compaction file list is empty, end it";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_STARTS_WITH_FULLY_DIRTY_0962C95A =
      "{}-{} [Compaction] SettleCompaction task starts with {} fully_dirty files and {} "
          + "partially_dirty files. Fully_dirty files : {}, partially_dirty files : {} . Fully_dirty "
          + "files size is {} MB, partially_dirty file size is {} MB. Memory cost is {} MB.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_SUCCESSFULLY_TIME_2BD3839A =
      "{}-{} [Compaction] SettleCompaction task finishes successfully, time cost is {} "
          + "s.Fully_dirty files num is {}.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_SUCCESSFULLY_TIME_4FEB0F56 =
      "{}-{} [Compaction] SettleCompaction task finishes successfully, time cost is {} s, "
          + "compaction speed is {} MB/s.Fully_dirty files num is {} and partially_dirty files num is {}.";
  public static final String STORAGE_LOG_COMPACTION_SETTLECOMPACTION_TASK_FINISHES_WITH_SOME_ERROR_A8A15439 =
      "{}-{} [Compaction] SettleCompaction task finishes with some error, time cost is {} "
          + "s.Fully_dirty files num is {} and there are {} files fail to delete.";
  public static final String STORAGE_LOG_COMPACTION_START_TO_SETTLE_PARTIALLY_DIRTY_FILES_TOTAL_FILE_BAC113C4 =
      "{}-{} [Compaction] Start to settle {} {} partially_dirty files, total file size is {} MB";
  public static final String STORAGE_LOG_COMPACTION_FINISH_TO_SETTLE_PARTIALLY_DIRTY_FILES_SUCCESSFULLY_9ACFD5C0 =
      "{}-{} [Compaction] Finish to settle {} {} partially_dirty files successfully , target file "
          + "is {},time cost is {} s, compaction speed is {} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_START_TO_RECOVER_SETTLE_COMPACTION_C342241D =
      "{}-{} [Compaction][Recover] Start to recover settle compaction.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FINISH_TO_RECOVER_SETTLE_COMPACTION_SUCCESSFULLY_714EF642 =
      "{}-{} [Compaction][Recover] Finish to recover settle compaction successfully.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_IS_DF6FD183 =
      "{}-{} [Compaction][Recover] compaction log is {}";
  public static final String STORAGE_LOG_SETTLE_TASK_FAIL_TO_DELETE_FULLY_DIRTY_TSFILE_B7DAEA8D =
      "Settle task fail to delete fully_dirty tsfile {}.";
  public static final String STORAGE_LOG_COMPACTION_CROSS_SPACE_COMPACTION_FILE_LIST_IS_EMPTY_END_B8044743 =
      "{}-{} [Compaction] Cross space compaction file list is empty, end it";
  public static final String STORAGE_LOG_COMPACTION_CROSSSPACECOMPACTION_TASK_STARTS_WITH_SEQ_FILES_8CDCBE0F =
      "{}-{} [Compaction] CrossSpaceCompaction task starts with {} seq files and {} unsequence "
          + "files. Sequence files : {}, unsequence files : {} . Sequence files size is {} MB, "
          + "unsequence file size is {} MB, total size is {} MB";
  public static final String STORAGE_LOG_COMPACTION_CROSSSPACECOMPACTION_TASK_FINISHES_SUCCESSFULLY_D7F1B1FD =
      "{}-{} [Compaction] CrossSpaceCompaction task finishes successfully, time cost is {} s, "
          + "compaction speed is {} MB/s, {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_IN_DATA_REGION_DIR_ABD144CC =
      "{} [Compaction][Recover] recover compaction in data region dir {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_IN_TIME_PARTITION_FA2FC44D =
      "{} [Compaction][Recover] recover compaction in time partition dir {}";
  public static final String STORAGE_LOG_RECOVER_MODS_FILE_ERROR_ON_DELETE_ORIGIN_FILE_OR_RENAME_7033152A =
      "recover mods file error on delete origin file or rename mods settle,";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_IS_0C57C7DA =
      "{} [Compaction][Recover] compaction log is {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_COMPACTION_LOG_FILE_EXISTS_START_TO_RECOVER_74836930 =
      "{} [Compaction][Recover] compaction log file {} exists, start to recover it";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_INCOMPLETE_LOG_FILE_ABORT_RECOVER_46472E7C =
      "{} [Compaction][Recover] incomplete log file, abort recover";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_RECOVER_COMPACTION_SUCCESSFULLY_DELETE_8451AEFB =
      "{} [Compaction][Recover] Recover compaction successfully, delete log file {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_ALL_SOURCE_FILES_EXISTS_DELETE_ALL_TARGET_79954E60 =
      "{} [Compaction][Recover] all source files exists, delete all target files.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_EXCEPTION_OCCURS_WHILE_DELETING_LOG_FILE_49A24E1D =
      "{} [Compaction][Recover] Exception occurs while deleting log file {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_REMOVE_TARGET_FILE_35A1E718 =
      "{} [Compaction][Recover] failed to remove target file {}";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_EXCEPTION_OCCURS_WHILE_DELETING_COMPACTION_218A56FB =
      "{} [Compaction][Recover] Exception occurs while deleting compaction mods file";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_TARGET_FILE_IS_NOT_COMPLETE_865ADA73 =
      "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files is "
          + "lost, do nothing.";
  public static final String STORAGE_LOG_COMPACTION_RECOVER_FAILED_TO_REMOVE_FILE_EXCEPTION_67CEA8E7 =
      "{} [Compaction][Recover] failed to remove file {}, exception: {}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_SPACE_COMPACTION_START_HANDLING_1B55549F =
      "{} [Compaction][ExceptionHandler] {} space compaction start handling exception, source "
          + "seqFiles is {}, source unseqFiles is {}.";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_FAIL_TO_HANDLE_SPACE_COMPACTION_B21F170F =
      "[Compaction][ExceptionHandler] Fail to handle {} space compaction exception, storage group "
          + "is {}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_EXCEPTION_OCCURS_WHEN_HANDLING_B6C9751E =
      "[Compaction][ExceptionHandler] exception occurs when handling exception in {} space "
          + "compaction. storage group is {}";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTION_FAIL_TO_DELETE_TARGET_TSFILE_WHEN_HANDLING_DC19DC8A =
      "{} [Compaction][Exception] fail to delete target tsfile {} when handling exception";
  public static final String STORAGE_LOG_COMPACTION_EXCEPTIONHANDLER_TARGET_FILE_IS_NOT_COMPLETE_91E81106 =
      "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files {} "
          + "is lost, do nothing.";
  public static final String STORAGE_LOG_BATCH_COMPACTION_CURRENT_DEVICE_IS_FIRST_BATCH_COMPACTED_34910754 =
      "[Batch Compaction] current device is {}, first batch compacted time chunk is {}";
  public static final String STORAGE_LOG_ADD_TSFILE_CURRENT_SELECT_FILE_NUM_IS_SIZE_IS_17E21BC9 =
      "Add tsfile {}, current select file num is {}, size is {}";
  public static final String STORAGE_LOG_SELECTING_CROSS_COMPACTION_TASK_RESOURCES_FROM_SEQFILE_UNSEQFILES_F4E1ABEB =
      "Selecting cross compaction task resources from {} seqFile, {} unseqFiles";
  public static final String STORAGE_LOG_SELECTING_INSERTION_CROSS_COMPACTION_TASK_RESOURCES_FROM_ECB186D1 =
      "Selecting insertion cross compaction task resources from {} seqFile, {} unseqFiles";
  public static final String STORAGE_LOG_ADDING_A_NEW_UNSEQFILE_AND_SEQFILES_AS_CANDIDATES_NEW_COST_07DD0A10 =
      "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total cost {}";
  public static final String STORAGE_LOG_SELECT_ONE_VALID_SEQ_FILE_FOR_NONOVERLAP_UNSEQ_FILE_TO_COMPACT_456668F1 =
      "Select one valid seq file {} for nonOverlap unseq file to compact with.";
  public static final String STORAGE_LOG_TOTAL_SOURCE_FILES_SEQFILES_UNSEQFILES_CANDIDATE_SOURCE_7511ED9E =
      "{} [{}] Total source files: {} seqFiles, {} unseqFiles. Candidate source files: {} "
          + "seqFiles, {} unseqFiles. Cannot select any files because they do not meet the conditions or "
          + "may be occupied by other compaction threads.";
  public static final String STORAGE_LOG_TOTAL_SOURCE_FILES_SEQFILES_UNSEQFILES_CANDIDATE_SOURCE_B8B01FC4 =
      "{} [{}] Total source files: {} seqFiles, {} unseqFiles. Candidate source files: {} "
          + "seqFiles, {} unseqFiles. Selected source files: {} seqFiles, {} unseqFiles, estimated "
          + "memory cost {} MB, total selected file size is {} MB, total selected seq file size is {} "
          + "MB, total selected unseq file size is {} MB, time consumption {}ms.";
  public static final String STORAGE_LOG_CANNOT_SELECT_FILE_FOR_SETTLE_COMPACTION_08C958D3 =
      "{}-{} cannot select file for settle compaction";
  public static final String STORAGE_LOG_HAS_NULL_CHUNK_METADATA_FILE_IS_819E4A49 =
      "{} has null chunk metadata, file is {}";
  public static final String STORAGE_LOG_MODIFICATIONS_SIZE_IS_FOR_FILE_PATH_EED7FD92 =
      "Modifications size is {} for file Path: {} ";
  public static final String STORAGE_LOG_AN_ERROR_OCCURRED_WHEN_TRUNCATING_MODIFICATIONS_TO_SIZE_F8A0D6D5 =
      "An error occurred when truncating modifications[{}] to size {}.";
  public static final String STORAGE_LOG_FAIL_TO_FSYNC_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_6E1EE226 =
      "Fail to fsync wal node-{}'s checkpoint writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_ROLL_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_791DDAB7 =
      "Fail to roll wal node-{}'s checkpoint writer, change system mode to error.";
  public static final String STORAGE_LOG_UNEXPECTED_ERROR_WHEN_LOADING_A_WAL_SEGMENT_IN_45B42CCF =
      "Unexpected error when loading a wal segment {} in {}@{}";
  public static final String STORAGE_LOG_MEET_ERROR_WHEN_READING_CHECKPOINT_FILE_SKIP_BROKEN_CHECKPOINTS_DADF9E9D =
      "Meet error when reading checkpoint file {}, skip broken checkpoints";
  public static final String STORAGE_LOG_FAILED_TO_SCAN_WAL_FILE_FOR_SEARCHABLE_REQUEST_METADATA_9B4B0198 =
      "Failed to scan WAL file {} for searchable request metadata";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTROWNODE_THE_SEARCH_INDEX_IS_027450AC =
      "WAL node-{} logs insertRowNode, the search index is {}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTROWSNODE_THE_SEARCH_INDEX_IS_1AF72E25 =
      "WAL node-{} logs insertRowsNode, the search index is {}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_INSERTTABLETNODE_THE_SEARCH_INDEX_IS_CF9A3600 =
      "WAL node-{} logs insertTabletNode, the search index is {}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_DELETEDATANODE_THE_SEARCH_INDEX_IS_6E49BC54 =
      "WAL node-{} logs deleteDataNode, the search index is {}.";
  public static final String STORAGE_LOG_WAL_NODE_LOGS_RELATIONALDELETEDATANODE_THE_SEARCH_INDEX_33258B30 =
      "WAL node-{} logs relationalDeleteDataNode, the search index is {}.";
  public static final String STORAGE_LOG_WAL_NODE_NO_WAL_FILE_OR_WAL_FILE_NUMBER_LESS_THAN_OR_EQUAL_3C65641C =
      "wal node-{}:no wal file or wal file number less than or equal to one was found";
  public static final String STORAGE_LOG_EFFECTIVE_INFORMATION_RATIO_IS_ACTIVE_MEMTABLES_COST_IS_D9A13DD2 =
      "Effective information ratio is {}, active memTables cost is {}, total cost is {}";
  public static final String STORAGE_LOG_SUCCESSFULLY_DELETE_OUTDATED_WAL_FILES_FOR_WAL_NODE_C141C741 =
      "Successfully delete {} outdated wal files for wal node-{}";
  public static final String STORAGE_LOG_UPDATE_FILE_TO_SEARCH_FAILED_THE_NEXT_SEARCH_INDEX_IS_F3DC95F3 =
      "update file to search failed, the next search index is {}";
  public static final String STORAGE_LOG_SEARCHINDEX_RESULT_FILES_6151DCEB =
      "searchIndex: {}, result: {}, files: {}, ";
  public static final String STORAGE_LOG_FAIL_TO_DELETE_OUTDATED_WAL_FILE_OF_WAL_NODE_1B1F2AF2 =
      "Fail to delete outdated wal file {} of wal node-{}.";
  public static final String STORAGE_LOG_WAL_NODE_FLUSHES_MEMTABLE_TO_TSFILE_BECAUSE_EFFECTIVE_INFORMATION_8CC86239 =
      "WAL node-{} flushes memTable-{} to TsFile {} because Effective information ratio {} is "
          + "below wal min effective info ratio {}, memTable size is {}.";
  public static final String STORAGE_LOG_WAL_NODE_SNAPSHOTS_MEMTABLE_TO_WAL_FILES_BECAUSE_EFFECTIVE_0A1304ED =
      "WAL node-{} snapshots memTable-{} to wal files because Effective information ratio {} is "
          + "below wal min effective info ratio {}, memTable size is {}.";
  public static final String STORAGE_LOG_TIMEOUT_WHEN_WAITING_FOR_NEXT_WAL_ENTRY_READY_EXECUTE_ROLLWALFILE_FEE9700E =
      "timeout when waiting for next WAL entry ready, execute rollWALFile. Current search index in "
          + "wal buffer is {}, and next target index is {}";
  public static final String STORAGE_LOG_THE_SEARCH_INDEX_OF_NEXT_WAL_ENTRY_SHOULD_BE_BUT_ACTUALLY_177BF8AF =
      "The search index of next WAL entry should be {}, but actually it's {}";
  public static final String STORAGE_LOG_SKIP_FROM_TO_IT_S_A_DANGEROUS_OPERATION_BECAUSE_INSERT_PLAN_9283DC91 =
      "Skip from {} to {}, it's a dangerous operation because insert plan {} may have been lost.";
  public static final String STORAGE_LOG_FAIL_TO_READ_WAL_FROM_WAL_FILE_SKIP_THIS_FILE_06A3B079 =
      "Fail to read wal from wal file {}, skip this file.";
  public static final String STORAGE_LOG_FAIL_TO_TRIGGER_ROLLING_WAL_NODE_S_WAL_FILE_LOG_WRITER_D1E595DC =
      "Fail to trigger rolling wal node-{}'s wal file log writer.";
  public static final String STORAGE_LOG_FAIL_TO_FIND_TSFILE_RECOVER_PERFORMER_FOR_WAL_ENTRY_IN_TSFILE_ED4EF3E7 =
      "Fail to find TsFile recover performer for wal entry in TsFile {}";
  public static final String STORAGE_LOG_SUCCESSFULLY_RECOVER_WAL_NODE_IN_THE_DIRECTORY_ADD_THIS_FA6ADE22 =
      "Successfully recover WAL node in the directory {}, add this node to WALManger.";
  public static final String STORAGE_LOG_SUCCESSFULLY_RECOVER_WAL_NODE_IN_THE_DIRECTORY_SO_DELETE_A17892D9 =
      "Successfully recover WAL node in the directory {}, so delete these wal files.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_5325B5AB =
      "Fail to read memTable ids from the wal file {} of wal node: {}";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_FBCE8D93 =
      "Fail to read memTable ids from the wal file {} of wal node.";
  public static final String STORAGE_LOG_DATA_REGIONS_HAVE_SUBMITTED_ALL_UNSEALED_TSFILES_START_RECOVERING_208E6A26 =
      "Data regions have submitted all unsealed TsFiles, start recovering TsFiles in each wal node.";
  public static final String STORAGE_LOG_FAIL_TO_ADD_RECOVER_PERFORMER_FOR_FILE_54746E05 =
      "Fail to add recover performer for file {}";
  public static final String STORAGE_LOG_BUFFER_CAPACITY_IS_LIMIT_IS_POSITION_IS_911625D8 =
      "buffer capacity is: {}, limit is: {}, position is: {}";
  public static final String STORAGE_LOG_HANDLE_CLOSE_SIGNAL_FOR_WAL_NODE_THERE_ARE_ENTRIES_LEFT_393393D0 =
      "Handle close signal for wal node-{}, there are {} entries left.";
  public static final String STORAGE_LOG_SYNC_WAL_BUFFER_FORCEFLAG_BUFFER_USED_C2A75C99 =
      "Sync wal buffer, forceFlag: {}, buffer used: {} / {} = {}%";
  public static final String STORAGE_LOG_FAIL_TO_WRITE_WALENTRY_INTO_WAL_NODE_BECAUSE_THIS_NODE_IS_5D45E73F =
      "Fail to write WALEntry into wal node-{} because this node is closed. It's ok to see this "
          + "log during data region deletion.";
  public static final String STORAGE_LOG_INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM_BLOCKING_0765C068 =
      "Interrupted when waiting for taking WALEntry from blocking queue to serialize.";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_54B0056E =
      "Fail to read memTable ids from the wal file {} of wal node {}: {}";
  public static final String STORAGE_LOG_FAIL_TO_READ_MEMTABLE_IDS_FROM_THE_WAL_FILE_OF_WAL_NODE_D5287E27 =
      "Fail to read memTable ids from the wal file {} of wal node {}.";
  public static final String STORAGE_LOG_FAIL_TO_SERIALIZE_WALENTRY_TO_WAL_NODE_S_BUFFER_DISCARD_F0948835 =
      "Fail to serialize WALEntry to wal node-{}'s buffer, discard it.";
  public static final String STORAGE_LOG_FAIL_TO_SYNC_WAL_NODE_S_BUFFER_CHANGE_SYSTEM_MODE_TO_ERROR_8C379D57 =
      "Fail to sync wal node-{}'s buffer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_ROLL_WAL_NODE_S_LOG_WRITER_CHANGE_SYSTEM_MODE_TO_A384AA54 =
      "Fail to roll wal node-{}'s log writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_FSYNC_WAL_NODE_S_LOG_WRITER_CHANGE_SYSTEM_MODE_TO_7930160B =
      "Fail to fsync wal node-{}'s log writer, change system mode to error.";
  public static final String STORAGE_LOG_FAIL_TO_CREATE_WAL_NODE_ALLOCATION_STRATEGY_BECAUSE_ALL_72801644 =
      "Fail to create wal node allocation strategy because all disks of wal folders are full.";
  public static final String STORAGE_LOG_THIS_TSFILE_ISN_T_CRASHED_NO_NEED_TO_REDO_WAL_LOG_A017A0F0 =
      "This TsFile {} isn't crashed, no need to redo wal log.";
  public static final String STORAGE_LOG_CANNOT_DESERIALIZE_RESOURCE_FILE_OF_TRY_TO_RECONSTRUCT_IT_F82299C6 =
      "Cannot deserialize .resource file of {}, try to reconstruct it.";
  public static final String STORAGE_LOG_TRY_TO_RELEASE_MEMORY_FROM_A_MEMORY_BLOCK_WHICH_HAS_NOT_874E7A08 =
      "Try to release memory from a memory block {} which has not released all memory";
  public static final String STORAGE_LOG_TRY_TO_SHRINK_A_NEGATIVE_MEMORY_SIZE_FROM_MEMORY_BLOCK_60501B13 =
      "Try to shrink a negative memory size {} from memory block {}";
  public static final String STORAGE_LOG_LOAD_FORCE_RESIZED_LOADTSFILEMEMORYBLOCK_WITH_MEMORY_FROM_33AC288A =
      "Load: Force resized LoadTsFileMemoryBlock with memory from query engine, size added: {}, "
          + "new size: {}";
  public static final String STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_ALLOCATED_MEMORYBLOCK_44D5B5FB =
      "Load: Query engine's memory is not sufficient, allocated MemoryBlock from "
          + "DataCacheMemoryBlock, size: {}";
  public static final String STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_FORCE_RESIZED_9F85F4CA =
      "Load: Query engine's memory is not sufficient, force resized LoadTsFileMemoryBlock with "
          + "memory from DataCacheMemoryBlock, size added: {}, new size: {}";
  public static final String STORAGE_LOG_CREATE_DATA_CACHE_MEMORY_BLOCK_ALLOCATE_MEMORY_5F3E041D =
      "Create Data Cache Memory Block {}, allocate memory {}";
  public static final String STORAGE_LOG_LOAD_ATTEMPTING_TO_RELEASE_MORE_MEMORY_THAN_ALLOCATED_0E737996 =
      "Load: Attempting to release more memory ({}) than allocated ({})";
  public static final String STORAGE_LOG_LOAD_FAILED_TO_SETTOTALMEMORYSIZEINBYTES_MEMORY_BLOCK_TO_DBE9BE56 =
      "Load: Failed to setTotalMemorySizeInBytes memory block {} to {} bytes, current memory usage "
          + "{} bytes";
  public static final String STORAGE_LOG_DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL_99016326 =
      "Data type conversion for LoadTsFileStatement {} is successful.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_5D132E57 =
      "Failed to convert data type for LoadTsFileStatement: {}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_STATUS_F0311707 =
      "Failed to convert data type for LoadTsFileStatement: {}, status code is {}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPES_FOR_TABLE_MODEL_STATEMENT_CB574D44 =
      "Failed to convert data types for table model statement {}.";
  public static final String STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPES_FOR_TREE_MODEL_STATEMENT_5C2869D6 =
      "Failed to convert data types for tree model statement {}.";
  public static final String STORAGE_LOG_LOAD_INSERTING_TABLET_TO_CASTING_TYPE_FROM_TO_AE808A8B =
      "Load: Inserting tablet to {}.{}. Casting type from {} to {}.";
  public static final String STORAGE_LOG_TRY_TO_LOAD_TSFILE_V3_INTO_CURRENT_VERSION_V4_FILE_PATH_B8D38E22 =
      "try to load TsFile V3 into current version (V4), file path: {}";
  public static final String STORAGE_LOG_THE_FILE_S_VERSION_NUMBER_IS_HIGHER_THAN_CURRENT_FILE_PATH_6D17349F =
      "the file's Version Number is higher than current, file path: {}";
  public static final String STORAGE_LOG_FAILED_TO_FIND_MOUNT_POINT_SKIP_REGISTER_IT_TO_MAP_33F38542 =
      "Failed to find mount point {}, skip register it to map";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_READING_DATA_DIR_S_MOUNT_POINT_9421E685 =
      "Exception occurs when reading data dir's mount point {}";
  public static final String STORAGE_LOG_EXCEPTION_OCCURS_WHEN_READING_TARGET_FILE_S_MOUNT_POINT_47567945 =
      "Exception occurs when reading target file's mount point {}";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_MEMORY_9A60DF29 =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to memory constraints, will "
          + "retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_16FA5F18 =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the system is read only, will "
          + "retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_TIME_E18630DE =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to time out to wait for "
          + "procedure return, will retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_5F811A8B =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the datanode is not enough, "
          + "will retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FAIL_F59307B8 =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to fail to connect to any config "
          + "node, will retry later.";
  public static final String STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_CURRENT_264E12EE =
      "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to current query is time out, "
          + "will retry later.";
  public static final String STORAGE_LOG_SUCCESSFULLY_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_ADB5FEC9 =
      "Successfully auto load tsfile {} (isGeneratedByPipe = {})";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_CREATING_FAIL_DIRECTORY_FOR_ACTIVE_7D3BEB38 =
      "Error occurred during creating fail directory {} for active load.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_STATUS_FILE_F43E9EF7 =
      "Failed to auto load tsfile {} (isGeneratedByPipe = {}), status: {}. File will be moved to "
          + "fail directory.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FILE_5EE1FA08 =
      "Failed to auto load tsfile {} (isGeneratedByPipe = {}) due to file not found, will skip "
          + "this file.";
  public static final String STORAGE_LOG_FAILED_TO_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_BECAUSE_OF_07946D74 =
      "Failed to auto load tsfile {} (isGeneratedByPipe = {}) because of an unexpected exception. "
          + "File will be moved to fail directory.";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_HOT_RELOAD_ACTIVE_LOAD_DIRS_CURRENT_673AFC0F =
      "Error occurred during hot reload active load dirs. Current active load listening dirs: {}.";
  public static final String STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_READABLE_SKIP_SCANNING_THIS_DIR_9C8B7E00 =
      "Current dir path is not readable: {}.Skip scanning this dir. Please check the permission.";
  public static final String STORAGE_LOG_CURRENT_DIR_PATH_IS_NOT_WRITABLE_SKIP_SCANNING_THIS_DIR_4885E78F =
      "Current dir path is not writable: {}.Skip scanning this dir. Please check the permission.";
  public static final String STORAGE_LOG_ERROR_OCCURRED_DURING_CHECKING_R_W_PERMISSION_OF_DIR_SKIP_3EC7FC7D =
      "Error occurred during checking r/w permission of dir: {}. Skip scanning this dir.";
  public static final String STORAGE_LOG_REPORT_DATABASE_STATUS_TO_THE_SYSTEM_AFTER_ADDING_CURRENT_8982BBD7 =
      "Report database Status to the system. After adding {}, current sg mem cost is {}.";
  public static final String STORAGE_LOG_THE_TOTAL_DATABASE_MEM_COSTS_ARE_TOO_LARGE_CALL_FOR_FLUSHING_26AD8CDF =
      "The total database mem costs are too large, call for flushing. Current sg cost is {}";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_STILL_EXCEEDING_FLUSH_PROPORTION_DB68D9D5 =
      "SG ({}) released memory (delta: {}) but still exceeding flush proportion (totalSgMemCost: "
          + "{}), call flush.";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SYSTEM_IS_IN_NORMAL_STATUS_TOTALSGMEMCOST_600A4A8D =
      "SG ({}) released memory (delta: {}), system is in normal status (totalSgMemCost: {}).";
  public static final String STORAGE_LOG_CHANGE_SYSTEM_TO_REJECT_STATUS_TRIGGERED_BY_LOGICAL_SG_MEM_6F9BCBD3 =
      "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), "
          + "totalSgMemCost ({}), REJECT_THRESHOLD ({})";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SET_SYSTEM_TO_NORMAL_STATUS_TOTALSGMEMCOST_0F714668 =
      "SG ({}) released memory (delta: {}), set system to normal status (totalSgMemCost: {}).";
  public static final String STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_SYSTEM_IS_STILL_IN_REJECT_STATUS_AD5E475C =
      "SG ({}) released memory (delta: {}), but system is still in reject status (totalSgMemCost: "
          + "{}).";
  public static final String STORAGE_LOG_DEGRADE_LASTFLUSHTIMEMAP_OF_OLD_TIMEPARTITIONINFO_MEM_SIZE_BED053EE =
      "[{}]degrade LastFlushTimeMap of old TimePartitionInfo-{}, mem size is {}, remaining mem "
          + "cost is {}";
  public static final String STORAGE_LOG_LIMIT_OF_ARRAY_DEQUE_SIZE_UPDATED_05DBA95E =
      "limit of {} array deque size updated: {} -> {}";
  public static final String STORAGE_LOG_LIMITUPDATETHRESHOLD_OF_PRIMITIVEARRAYMANAGER_UPDATED_394801AE =
      "limitUpdateThreshold of PrimitiveArrayManager updated: {} -> {}";
  public static final String STORAGE_LOG_CREATE_FOLDER_FAILED_IS_THE_FOLDER_EXISTED_18E29D51 =
      "create folder {} failed. Is the folder existed: {}";
  public static final String STORAGE_LOG_CAN_T_FIND_STRATEGY_FOR_MULT_DIRECTORIES_A06406EC =
      "Can't find strategy {} for mult-directories.";

  // ---------------------------------------------------------------------------
  // Additional exception messages
  // ---------------------------------------------------------------------------
  public static final String STORAGE_EXCEPTION_SYSTEM_REJECTED_OVER_SMS_94CEF932 =
      "System rejected over %sms";
  public static final String STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950 =
      "Failed to create TsFileWriterManager for uuid %s because of insufficient disk space.";
  public static final String STORAGE_EXCEPTION_STORAGE_ALLOCATION_FAILED_FOR_S_TIER_D_E2C94F74 =
      "Storage allocation failed for %s (tier %d)";
  public static final String STORAGE_EXCEPTION_DATA_REGION_S_S_IS_DOWN_BECAUSE_THE_TIME_OF_TSFILE_S_IS_1F732E71 =
      "data region %s[%s] is down, because the time of tsfile %s is larger than system current "
          + "time, file time is %d while system current time is %d, please check it.";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CONTINUE_WRITING_DATA_BECAUSE_THE_SPACE_ALLOCATED_9A5FB99E =
      "Unable to continue writing data, because the space allocated to the database %s has already "
          + "used the upper limit";
  public static final String STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEPROCESSOR_FOR_DATABASE_S_TIMEPARTITIONID_0CD885BB =
      "Failed to create TsFileProcessor for database %s, timePartitionId %s";
  public static final String STORAGE_EXCEPTION_DELETE_FAILED_PLEASE_DO_NOT_DELETE_UNTIL_THE_OLD_FILES_SETTLED_6C9F17CC =
      "Delete failed. Please do not delete until the old files settled.";
  public static final String STORAGE_EXCEPTION_MULTIPLE_ERRORS_OCCURRED_WHILE_WRITING_MOD_FILES_SEE_LOGS_529D7145 =
      "Multiple errors occurred while writing mod files, see logs for details.";
  public static final String STORAGE_EXCEPTION_MEET_ERROR_WHEN_SETTLING_FILE_S_4D6ECCEE =
      "Meet error when settling file: %s";
  public static final String STORAGE_EXCEPTION_PEER_IS_INACTIVE_AND_NOT_READY_TO_WRITE_REQUEST_S_DATANODE_EDFE5AEF =
      "Peer is inactive and not ready to write request, %s, DataNode Id: %s";
  public static final String STORAGE_EXCEPTION_TSFILE_VALIDATE_FAILED_S_3CDE0677 =
      "tsfile validate failed, %s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_S_TARGET_37BDA16F =
      "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_S_9622AA6D =
      "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s";
  public static final String STORAGE_EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_S_TARGET_EEB4EDE7 =
      "File renaming failed when loading .mod file. Origin: %s, Target: %s, because %s";
  public static final String STORAGE_EXCEPTION_TOTAL_ALLOCATED_MEMORY_FOR_DIRECT_BUFFER_WILL_BE_S_WHICH_FD7DC149 =
      "Total allocated memory for direct buffer will be %s, which is greater than limit mem cost: "
          + "%s";
  public static final String STORAGE_EXCEPTION_S_ALREADY_EXISTS_AND_IS_NOT_EMPTY_CF0BD6A4 =
      "%s already exists and is not empty";
  public static final String STORAGE_EXCEPTION_S_S_WRITE_WAL_FAILED_S_5A7E61FB =
      "%s: %s write WAL failed: %s";
  public static final String STORAGE_EXCEPTION_MEMORY_NOT_ENOUGH_TO_CLONE_THE_TVLIST_DURING_FLUSH_PHASE_75C90725 =
      "Memory not enough to clone the tvlist during flush phase";
  public static final String STORAGE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "Data type %s is not supported.";
  public static final String STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURSEQINDEX_D_6B9B1134 =
      "curIndex %d is not equal to curSeqIndex %d";
  public static final String STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURUNSEQINDEX_D_AB32F71D =
      "curIndex %d is not equal to curUnSeqIndex %d";
  public static final String STORAGE_EXCEPTION_PAGEID_IN_SHAREDTIMEDATABUFFER_SHOULD_BE_INCREMENTAL_A5E6C4EE =
      "PageId in SharedTimeDataBuffer should be  incremental.";
  public static final String STORAGE_EXCEPTION_CAN_T_READ_FILE_S_S_FROM_DISK_9D5066C0 =
      "Can't read file %s%s from disk";
  public static final String STORAGE_EXCEPTION_SHOULD_NOT_GET_PROGRESS_INDEX_FROM_A_UNCLOSING_TSFILERESOURCE_129FD925 =
      "Should not get progress index from a unclosing TsFileResource.";
  public static final String STORAGE_EXCEPTION_DIRECTORY_CREATION_FAILED_S_PERMISSION_DENIED_OR_PARENT_2855777B =
      "Directory creation failed: %s (Permission denied or parent not writable)";
  public static final String STORAGE_EXCEPTION_FAILED_TO_GET_DEVICES_FROM_TSFILE_S_S_412EEA1A =
      "Failed to get devices from tsfile: %s%s";
  public static final String STORAGE_EXCEPTION_UNSUPPORTED_RECORD_TYPE_IN_FILE_S_TYPE_S_DADEE641 =
      "Unsupported record type in file: %s, type: %s";
  public static final String STORAGE_EXCEPTION_CORRESPONDING_MEMORY_ESTIMATOR_FOR_S_PERFORMER_OF_S_SPACE_D543D3EF =
      "Corresponding memory estimator for %s performer of %s space compaction is not existed.";
  public static final String STORAGE_EXCEPTION_HAS_BEEN_WAITING_OVER_S_SECONDS_FOR_ALL_SUB_COMPACTION_TASKS_76BD45D6 =
      "Has been waiting over %s seconds for all sub compaction tasks to finish.";
  public static final String STORAGE_EXCEPTION_HAS_BEEN_WAITING_OVER_S_SECONDS_FOR_ALL_COMPACTION_TASKS_87E1B82E =
      "Has been waiting over %s seconds for all compaction tasks to finish.";
  public static final String STORAGE_EXCEPTION_EXCEPTION_TO_PARSE_THE_TSFILE_S_IN_SETTLING_D40564AD =
      "Exception to parse the tsfile: %s in settling";
  public static final String STORAGE_EXCEPTION_THESE_DEVICES_S_DO_NOT_EXIST_IN_THE_TSFILE_5A03F30D =
      "These devices (%s) do not exist in the tsfile";
  public static final String STORAGE_EXCEPTION_CANNOT_SET_SINGLE_TYPE_OF_SOURCE_FILES_TO_THIS_KIND_OF_PERFORMER_6B422172 =
      "Cannot set single type of source files to this kind of performer";
  public static final String STORAGE_EXCEPTION_CANNOT_SET_BOTH_SEQ_FILES_AND_UNSEQ_FILES_TO_THIS_KIND_OF_F68F629E =
      "Cannot set both seq files and unseq files to this kind of performer";
  public static final String STORAGE_EXCEPTION_THIS_TABLENAME_IS_S_MERGE_TABLENAME_IS_S_4B05FA97 =
      "this.tableName is %s merge tableName is %s";
  public static final String STORAGE_EXCEPTION_S_S_COMPACTION_ABORT_7D0CB1E5 =
      "%s-%s [Compaction] abort";
  public static final String STORAGE_EXCEPTION_FAILED_TO_PASS_COMPACTION_VALIDATION_RESOURCES_FILE_OR_TSFILE_4B78731F =
      "Failed to pass compaction validation, .resources file or tsfile data is wrong";
  public static final String STORAGE_EXCEPTION_FAILED_TO_DELETE_EMPTY_TARGET_FILE_S_324EF900 =
      "failed to delete empty target file %s";
  public static final String STORAGE_EXCEPTION_TARGET_FILE_IS_NOT_COMPLETED_S_E65150DB =
      "Target file is not completed. %s";
  public static final String STORAGE_EXCEPTION_DOES_NOT_SUPPORT_THIS_METHOD_IN_READPOINTCROSSCOMPACTIONWRITER_D024F312 =
      "Does not support this method in ReadPointCrossCompactionWriter";
  public static final String STORAGE_EXCEPTION_UNKNOWN_COMPACTION_LOG_LINE_S_C0A9DC05 =
      "unknown compaction log line: %s";
  public static final String STORAGE_EXCEPTION_PATH_S_CANNOT_BE_PARSED_INTO_FILE_INFO_631C48C8 =
      "Path %s cannot be parsed into file info";
  public static final String STORAGE_EXCEPTION_STRING_S_IS_NOT_A_LEGAL_FILE_INFO_STRING_0CBEAB8E =
      "String %s is not a legal file info string";
  public static final String STORAGE_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_D16A1E9A =
      "Unsupported data type: %s";
  public static final String STORAGE_EXCEPTION_DO_NOT_HAVE_A_COMPLETE_PAGE_BODY_EXPECTED_S_ACTUAL_S_3A05EF8F =
      "do not have a complete page body. Expected:%s. Actual:%s";
  public static final String STORAGE_EXCEPTION_COMPACTION_COMPACTION_FOR_TARGET_FILE_S_ABORT_46ECFF41 =
      "[Compaction] compaction for target file %s abort";
  public static final String STORAGE_EXCEPTION_COMPACTIONTASKSUMMARY_FOR_FASTCOMPACTIONPERFORMER_SHOULD_F5710AA8 =
      "CompactionTaskSummary for FastCompactionPerformer should be FastCompactionTaskSummary";
  public static final String STORAGE_EXCEPTION_COMPACTION_COMPACTION_FOR_TARGET_FILES_S_ABORT_AFC87906 =
      "[Compaction] compaction for target files %s abort";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_UNSEQ_INNER_COMPACTION_50D566DF =
      "Illegal compaction performer for unseq inner compaction %s";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_SEQ_INNER_COMPACTION_S_2C2F1F66 =
      "Illegal compaction performer for seq inner compaction %s";
  public static final String STORAGE_EXCEPTION_ILLEGAL_COMPACTION_PERFORMER_FOR_CROSS_COMPACTION_S_17C6E05D =
      "Illegal compaction performer for cross compaction %s";
  public static final String STORAGE_EXCEPTION_SOURCE_FILE_S_IS_DELETED_D2ED7D90 =
      "source file %s is deleted";
  public static final String STORAGE_EXCEPTION_S_S_EXCEEDS_SHORT_RANGE_1DF75A2D =
      "%s %s exceeds short range";
  public static final String STORAGE_EXCEPTION_THE_ELEMENT_SIZE_OF_WALENTRY_S_IS_LARGER_THAN_THE_TOTAL_E494520D =
      "The element size of WALEntry %s is larger than the total memory size of wal buffer queue %s";
  public static final String STORAGE_EXCEPTION_FAIL_TO_GET_WAL_FILE_BY_VERSIONID_S_AND_FILES_S_9CB045F4 =
      "Fail to get wal file by versionId=%s and files=%s.";
  public static final String STORAGE_EXCEPTION_CANNOT_MAKE_OTHER_CHECKPOINT_TYPES_IN_THE_WAL_BUFFER_TYPE_E9053BC1 =
      "Cannot make other checkpoint types in the wal buffer, type is %s";
  public static final String STORAGE_EXCEPTION_FAILED_RECOVER_THE_RESOURCE_FILE_S_S_S_E35EF7D5 =
      "Failed recover the resource file: %s%s%s";
  public static final String STORAGE_EXCEPTION_THE_INITIAL_LIMITED_MEMORY_SIZE_D_IS_LESS_THAN_THE_MINIMUM_FC044302 =
      "The initial limited memory size %d is less than the minimum memory size %d";
  public static final String STORAGE_EXCEPTION_SETTOTALMEMORYSIZEINBYTES_IS_NOT_SUPPORTED_FOR_LOADTSFILEDATACACHEMEMORYBLOCK_DFAB2A2A =
      "setTotalMemorySizeInBytes is not supported for LoadTsFileDataCacheMemoryBlock";
  public static final String STORAGE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_FROM_QUERY_ENGINE_F91D5959 =
      "forceAllocate: failed to allocate memory from query engine after %s retries, total query "
          + "memory %s bytes, current available memory for load %s bytes, current load used memory size "
          + "%s bytes, load requested memory size %s bytes";
  public static final String STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_POSITIVE_D6586ED3 =
      "Load: Invalid memory size %d bytes, must be positive";
  public static final String STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_NON_NEGATIVE_A0146353 =
      "Load: Invalid memory size %d bytes, must be non-negative";
  public static final String STORAGE_EXCEPTION_MAGIC_STRING_CHECK_ERROR_WHEN_PARSING_TSFILE_S_EA3D68E3 =
      "Magic String check error when parsing TsFile %s.";
  public static final String STORAGE_EXCEPTION_EMPTY_NONALIGNED_CHUNK_OR_TIME_CHUNK_WITH_OFFSET_D_IN_TSFILE_B1E462C9 =
      "Empty Nonaligned Chunk or Time Chunk with offset %d in TsFile %s.";
  public static final String STORAGE_EXCEPTION_TIME_PARTITION_SLOTS_SIZE_IS_GREATER_THAN_S_D076F78E =
      "Time partition slots size is greater than %s";
  public static final String STORAGE_EXCEPTION_CONSUME_ALIGNED_CHUNK_DATA_ERROR_NEXT_CHUNK_OFFSET_D_CHUNKDATA_D896FAE2 =
      "Consume aligned chunk data error, next chunk offset: %d, chunkData: %s";
  public static final String STORAGE_EXCEPTION_CONSUME_CHUNKDATA_ERROR_CHUNK_OFFSET_D_MEASUREMENT_S_CHUNKDATA_4A1F1EE1 =
      "Consume chunkData error, chunk offset: %d, measurement: %s, chunkData: %s";
  public static final String STORAGE_EXCEPTION_TOTAL_DATABASE_MEMCOST_S_IS_OVER_THAN_MEMORYSIZEFORWRITING_C63E4D72 =
      "Total database MemCost %s is over than memorySizeForWriting %s";
  public static final String STORAGE_EXCEPTION_REQUIRED_FILE_NUM_D_IS_GREATER_THAN_THE_MAX_FILE_NUM_D_FOR_AB6DE95B =
      "Required file num %d is greater than the max file num %d for compaction.";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_AFTER_D_SECONDS_C701F750 =
      "Failed to allocate %d files for compaction after %d seconds, max file num for compaction "
          + "module is %d, %d files is used.";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_MAX_FILE_NUM_FOR_9B954F8C =
      "Failed to allocate %d files for compaction, max file num for compaction module is %d, %d "
          + "files is used.";
  public static final String STORAGE_EXCEPTION_REQUIRED_MEMORY_COST_D_BYTES_IS_GREATER_THAN_THE_TOTAL_MEMORY_444D8FE4 =
      "Required memory cost %d bytes is greater than the total memory budget for compaction %d "
          + "bytes";
  public static final String STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_BYTES_MEMORY_FOR_COMPACTION_TOTAL_MEMORY_33BE3C71 =
      "Failed to allocate %d bytes memory for compaction, total memory budget for compaction "
          + "module is %d bytes, %d bytes is used";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_REQUESTS_EXCEEDED_WAIT_SMS_30F0842F =
      "number of requests exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_REQUEST_SIZE_LIMIT_EXCEEDED_WAIT_SMS_11C1E549 =
      "request size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_WRITE_REQUESTS_EXCEEDED_WAIT_SMS_D11F94D2 =
      "number of write requests exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_WRITE_SIZE_LIMIT_EXCEEDED_WAIT_SMS_AA3796DC =
      "write size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_NUMBER_OF_READ_REQUESTS_EXCEEDED_WAIT_SMS_C92D6C43 =
      "number of read requests exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_READ_SIZE_LIMIT_EXCEEDED_WAIT_SMS_E19598BA =
      "read size limit exceeded - wait %sms";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_BECAUSE_THERE_IS_FILE_UNDER_1C59ACFC =
      "Unable to create directory %s because there is file under the path, please check "
          + "configuration and restart.";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_PLEASE_CHECK_CONFIGURATION_BA580B67 =
      "Unable to create directory %s, please check configuration and restart.";
  public static final String STORAGE_EXCEPTION_CONFLICT_IS_DETECTED_IN_DIRECTORY_S_WHICH_MAY_BE_BEING_USED_CB5C77FC =
      "Conflict is detected in directory %s, which may be being used by another IoTDB "
          + "(ProcessId=%s). Please check configuration and restart.";
  public static final String COMPACTION_INNER_SPACE = "inner";
  public static final String COMPACTION_CROSS_SPACE = "cross";
  public static final String DEVICE_DOES_NOT_EXIST_IN_RESOURCE_FILE_FMT =
      "%s does not exist in the resource file";
  public static final String TARGET_FILE_SMALLER_THAN_MAGIC_STRING_AND_VERSION_NUMBER_SIZE_FMT =
      "target file %s is smaller than magic string and version number size";
  public static final String CURRENT_POINT_TIMESTAMP_SHOULD_BE_LATER_FMT =
      "Timestamp of the current point of %s is %s, which should be later than the last time %s";
  public static final String DEVICE_TIME_RANGE_VERIFICATION_FAILED_FMT =
      "The device(%s)'s time range verification failed. %s";
  public static final String CURRENT_DEVICE_TIME_RANGE_MISMATCH_FMT =
      "The time range of current device is %s, which should equals actual device time range %s";
  public static final String CURRENT_TIMESERIES_METADATA_MISMATCH_FMT =
      "Current timeseriesMetadata is %s, which should equals actual time range %s";
  public static final String CURRENT_CHUNK_METADATA_MISMATCH_FMT =
      "Current chunkMetadata is %s, which should equals actual chunk time range %s";
  public static final String CURRENT_PAGE_TIME_RANGE_MISMATCH_FMT =
      "Current page is %s, which should contains actual page data time range %s";
  public static final String COMPACTION_VALIDATION_SEQUENCE_FILES_HAS_OVERLAP_FMT =
      "Failed to pass compaction validation, sequence files has overlap, file is %s";
  public static final String TSFILE_CANNOT_TRANSIT_TO_COMPACTING_FMT =
      "TsFile %s cannot transit to COMPACTING. its status: %s";
  public static final String CURRENT_PAGE_CANNOT_BE_ALIGNED_WITH_TIME_CHUNK_FMT =
      "Current page %s cannot be aligned with time chunk %s, page index is %s";
  public static final String CURRENT_CHUNK_CANNOT_BE_ALIGNED_WITH_TIME_CHUNK_FMT =
      "Current chunk %s cannot be aligned with time chunk: %s, all time chunk in first batch is %s";
  public static final String WAL_NODE_CLOSED_FMT = "wal node-%s has been closed";
  public static final String BROKEN_WAL_FILE_FMT = "Broken wal file %s, size %d";
  public static final String TSFILE_READER_CLOSED_BECAUSE_NO_REFERENCE =
      "{} TsFileReader is closed because of no reference.";
  public static final String CLOSED_TSFILE_READER_CLOSED =
      "{} closedTsFileReader is closed.";
  public static final String UNCLOSED_TSFILE_READER_CLOSED =
      "{} unclosedTsFileReader is closed.";

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_NO_LOAD_TSFILE_UUID_ARG_RECORDED_EXECUTE_LOAD_COMMAND_ARG_66722D80 = "No load TsFile uuid %s recorded for execute load command %s.";
  public static final String EXCEPTION_NON_MINUS_ALIGNED_CHUNK_ONLY_HAS_ONE_MEASUREMENT_COMMA_BUT_MEASUREMENTINDEX_IS_E1A87F80 = "Non-aligned chunk only has one measurement, but measurementIndex is ";
  public static final String MESSAGE_LOAD_QUERY_FALLBACK_FAILED_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_50771955 = "Load: Query fallback failed for device {} measurements {} in TsFile {}. ";
  public static final String MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_TSFILE_ARG_AFTER_SCAN_PARSER_FAILURE_143B6037 = "Load: Failed to initialize query fallback for TsFile {} after scan parser failure.";
  public static final String MESSAGE_LOAD_SCAN_PARSER_DETECTED_A_CORRUPTED_SECTION_IN_TSFILE_ARG_AT_DEVICE_ARG_E2632421 = "Load: Scan parser detected a corrupted section in TsFile {} at device {}. ";
  public static final String EXCEPTION_EXPECTED_PIPERAWTABLETINSERTIONEVENT_BUT_GOT_D1D1DD05 = "Expected PipeRawTabletInsertionEvent but got ";
  public static final String MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_0BF15119 = "Load: Failed to initialize query fallback for device {} measurements {} in TsFile {}. ";
  public static final String MESSAGE_LOAD_QUERY_FALLBACK_FAILED_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_SPLIT_OR_SKIP_THIS_QUERY_TASK_AND_CONTINUE_3A4EA407 =
      "Load: Query fallback failed for device {} measurements {} in TsFile {}. Split or skip this "
          + "query task and continue.";
  public static final String MESSAGE_LOAD_SCAN_PARSER_DETECTED_A_CORRUPTED_SECTION_IN_TSFILE_ARG_AT_DEVICE_ARG_SWITCH_TO_QUERY_PARSING_FOR_REMAINING_DEVICES_EFA07985 =
      "Load: Scan parser detected a corrupted section in TsFile {} at device {}. Switch to query "
          + "parsing for remaining devices.";
  public static final String MESSAGE_LOAD_FAILED_TO_INITIALIZE_QUERY_FALLBACK_FOR_DEVICE_ARG_MEASUREMENTS_ARG_IN_TSFILE_ARG_SPLIT_OR_SKIP_THIS_QUERY_TASK_AND_CONTINUE_C6F69685 =
      "Load: Failed to initialize query fallback for device {} measurements {} in TsFile {}. Split "
          + "or skip this query task and continue.";
  public static final String MESSAGE_THE_ASSOCIATED_RESOURCE_FILE_OF_ARG_IS_NOT_FOUND_IN_THE_SNAPSHOT_CB9152B5 = "The associated resource file of {} is not found in the snapshot";
  public static final String MESSAGE_EVICTED_NON_EXISTING_EXISTING_SERIES_COUNT_ARG_ARG_ARG_TOTAL_REQUEST_ARG_3026ADBD = "Evicted non-existing/existing series count: {}/{}({}), total request: {}";

}
