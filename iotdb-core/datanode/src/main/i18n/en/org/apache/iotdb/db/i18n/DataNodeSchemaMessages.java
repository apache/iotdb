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

public final class DataNodeSchemaMessages {

  // ======================== SchemaEngine ========================

  public static final String USED_SCHEMA_ENGINE_MODE = "used schema engine mode: {}.";
  public static final String SCHEMA_REGION_RECOVERY_ERROR =
      "Something wrong happened during SchemaRegion recovery";
  public static final String CLEAR_SCHEMA_REGION_MAP = "clear schema region map.";
  public static final String FAILED_TO_UPDATE_SUBTREE_MEASUREMENT_COUNT =
      "Failed to update subtree measurement count for template {} in schemaRegion {}";
  public static final String RECOVER_SPEND = "Recover [{}] spend: {} ms";
  public static final String SCHEMA_REGION_FAILED_TO_RECOVER =
      "SchemaRegion [%d] in StorageGroup [%s] failed to recover.";
  public static final String SCHEMA_REGION_ALREADY_DELETED =
      "SchemaRegion(id = {}) has been deleted, skiped";
  public static final String FAILED_TO_GET_TABLE_FOR_TIMESERIES_COUNT =
      "Failed to get table {}.{} when calculating the time series number. Maybe the cluster is restarting or the table is being dropped.";
  public static final String PEER_IS_SHUTTING_DOWN = "Peer is shutting down now.";
  public static final String SCHEMA_REGION_DUPLICATED =
      "SchemaRegion [%s] is duplicated between [%s] and [%s], and the former one has been recovered.";

  // ======================== MemSchemaEngineStatistics ========================

  public static final String CURRENT_SERIES_MEMORY_TOO_LARGE =
      "Current series memory {} is too large...";
  public static final String CURRENT_SERIES_MEMORY_BACK_TO_NORMAL =
      "Current series memory {} come back to normal level, total series number is {}.";
  public static final String WRONG_SCHEMA_ENGINE_STATISTICS_TYPE =
      "Wrong SchemaEngineStatistics Type";

  // ======================== MemSchemaRegionStatistics ========================

  public static final String WRONG_SCHEMA_REGION_STATISTICS_TYPE =
      "Wrong SchemaRegionStatistics Type";

  // ======================== SchemaRegionUtils ========================

  public static final String CANNOT_GET_FILES_IN_SCHEMA_REGION_DIR =
      "Can't get files in schema region dir %s";
  public static final String DELETE_SCHEMA_REGION_FILE = "Delete schema region file {}";
  public static final String DELETE_SCHEMA_REGION_FILE_FAILED =
      "Delete schema region file {} failed.";
  public static final String FAILED_TO_DELETE_SCHEMA_REGION_FILE =
      "Failed to delete schema region file %s";
  public static final String DELETE_SCHEMA_REGION_FOLDER = "Delete schema region folder {}";
  public static final String DELETE_SCHEMA_REGION_FOLDER_FAILED =
      "Delete schema region folder {} failed.";
  public static final String FAILED_TO_DELETE_SCHEMA_REGION_FOLDER =
      "Failed to delete schema region folder %s";
  public static final String DELETE_DATABASE_SCHEMA_FOLDER = "Delete database schema folder {}";
  public static final String DELETE_DATABASE_SCHEMA_FOLDER_FAILED =
      "Delete database schema folder {} failed";

  // ======================== SchemaRegionLoader ========================

  public static final String CLASS_NOT_SUBCLASS_OF_ISCHEMAREGION =
      "Class %s is not a subclass of ISchemaRegion.";
  public static final String DUPLICATED_SCHEMA_REGION_IMPL =
      "Duplicated SchemaRegion implementation, {} and {}, with same mode name [{}]";
  public static final String NO_SCHEMA_REGION_IMPL_WITH_TARGET_MODE =
      "There's no SchemaRegion implementation with target mode {}. Use default mode {}";
  public static final String SCHEMA_REGION_LOADER_INFO =
      "[SchemaRegionLoader], schemaEngineMode:{}, currentMode:{}";

  // ======================== SchemaRegionPlanType ========================

  public static final String UNRECOGNIZED_SCHEMA_REGION_PLAN_TYPE =
      "Unrecognized SchemaRegionPlanType of ";

  // ======================== SchemaRegion Init/Dir ========================

  public static final String CREATE_DATABASE_SCHEMA_FOLDER = "create database schema folder {}";
  public static final String CREATE_DATABASE_SCHEMA_FOLDER_FAILED =
      "create database schema folder {} failed.";
  public static final String CREATE_SCHEMA_REGION_FOLDER = "create schema region folder {}";
  public static final String CREATE_SCHEMA_REGION_FOLDER_FAILED =
      "create schema region folder {} failed.";
  public static final String CANNOT_RECOVER_ALL_SCHEMA_INFO =
      "Cannot recover all schema info from {}, we try to recover as possible as we can";
  public static final String CANNOT_RECOVER_ALL_MTREE =
      "Cannot recover all MTree from {} file, we try to recover as possible as we can";

  // ======================== SchemaRegion MLog ========================

  public static final String CANNOT_FORCE_MLOG = "Cannot force {} mlog to the schema region";
  public static final String SPEND_TIME_DESERIALIZE_MTREE =
      "spend {} ms to deserialize {} mtree from mlog.bin";
  public static final String FAILED_TO_PARSE_MLOG = "Failed to parse ";
  public static final String MLOG_BIN_SUFFIX = " mlog.bin";
  public static final String PARSE_MLOG_ERROR = "Parse mlog error at lineNumber {} because:";
  public static final String CANNOT_OPERATE_CMD = "Can not operate cmd {} for err:";
  public static final String MLOG_BIN_CORRUPTED =
      "The mlog.bin has been corrupted. Please remove it or fix it, and then restart IoTDB";
  public static final String CANNOT_CLOSE_METADATA_LOG_WRITER =
      "Cannot close metadata log writer, because:";
  public static final String MLOG_RECOVERY_CHECK_POINT = "MLog recovery check point: {}";
  public static final String CANNOT_GET_MLOG_CHECKPOINT =
      "Can not get check point in MLogDescription file because {}, use default value 0.";
  public static final String FAILED_TO_SKIP_MLOG = "Failed to skip {} from {}";
  public static final String UPDATE_MLOG_DESCRIPTION_FAILED = "Update {} failed because {}";
  public static final String DIRECT_BUFFER_MEMORY_EXCEEDED =
      "Total allocated memory for direct buffer will be ";
  public static final String DIRECT_BUFFER_MEMORY_LIMIT = ", which is greater than limit mem cost: ";

  // ======================== SchemaRegion Snapshot ========================

  public static final String FAILED_TO_CREATE_SNAPSHOT_NOT_INITIALIZED =
      "Failed to create snapshot of schemaRegion {}, because the schemaRegion has not been initialized.";
  public static final String START_CREATE_SNAPSHOT = "Start create snapshot of schemaRegion {}";
  public static final String MTREE_SNAPSHOT_CREATION_COST =
      "MTree snapshot creation of schemaRegion {} costs {}ms.";
  public static final String MTREE_SNAPSHOT_CREATION_COST_WITH_STATUS =
      "MTree snapshot creation of schemaRegion {} costs {}ms. Status: {}";
  public static final String TAG_SNAPSHOT_CREATION_COST =
      "Tag snapshot creation of schemaRegion {} costs {}ms.";
  public static final String TAG_SNAPSHOT_CREATION_COST_WITH_STATUS =
      "Tag snapshot creation of schemaRegion {} costs {}ms. Status: {}";
  public static final String DEVICE_ATTR_SNAPSHOT_CREATION_COST =
      "Device attribute snapshot creation of schemaRegion {} costs {}ms. Status: {}";
  public static final String DEVICE_ATTR_UPDATER_SNAPSHOT_CREATION_COST =
      "Device attribute remote updater snapshot creation of schemaRegion {} costs {}ms. Status: {}";
  public static final String SNAPSHOT_CREATION_COST =
      "Snapshot creation of schemaRegion {} costs {}ms.";
  public static final String SUCCESSFULLY_CREATE_SNAPSHOT =
      "Successfully create snapshot of schemaRegion {}";
  public static final String START_LOADING_SNAPSHOT =
      "Start loading snapshot of schemaRegion {}";
  public static final String DEVICE_ATTR_SNAPSHOT_LOADING_COST =
      "Device attribute snapshot loading of schemaRegion {} costs {}ms.";
  public static final String DEVICE_ATTR_UPDATER_SNAPSHOT_LOADING_COST =
      "Device attribute remote updater snapshot loading of schemaRegion {} costs {}ms.";
  public static final String TAG_SNAPSHOT_LOADING_COST =
      "Tag snapshot loading of schemaRegion {} costs {}ms.";
  public static final String MTREE_SNAPSHOT_LOADING_COST =
      "MTree snapshot loading of schemaRegion {} costs {}ms.";
  public static final String SNAPSHOT_LOADING_COST =
      "Snapshot loading of schemaRegion {} costs {}ms.";
  public static final String SUCCESSFULLY_LOAD_SNAPSHOT =
      "Successfully load snapshot of schemaRegion {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT =
      "Failed to load snapshot for schemaRegion {}  due to {}. Use empty schemaRegion";
  public static final String ERROR_DURING_INIT_SCHEMA_REGION =
      "Error occurred during initializing schemaRegion {}";
  public static final String FAILED_TO_RECOVER_TAG_INDEX =
      "Failed to recover tagIndex for {} in schemaRegion {}.";
  public static final String FAILED_TO_READ_TAG_ATTRIBUTE =
      "Failed to read tag and attribute info because {}";

  // ======================== DeviceAttributeStore ========================

  public static final String FAILED_TO_DELETE_OLD_SNAPSHOT_DEVICE_ATTR =
      "Failed to delete old snapshot {} while creating device attribute snapshot.";
  public static final String FAILED_TO_RENAME_SNAPSHOT_DEVICE_ATTR =
      "Failed to rename {} to {} while creating device attribute snapshot.";
  public static final String FAILED_TO_CREATE_DEVICE_ATTR_SNAPSHOT =
      "Failed to create device attribute snapshot due to {}";
  public static final String DEVICE_ATTR_SNAPSHOT_NOT_FOUND =
      "Device attribute snapshot {} not found, consider it as upgraded from the older version, use empty attributes";
  public static final String LOAD_DEVICE_ATTR_SNAPSHOT_FAILED =
      "Load device attribute snapshot from {} failed";

  // ======================== DeviceAttributeCacheUpdater ========================

  public static final String FAILED_TO_DELETE_OLD_SNAPSHOT_UPDATER =
      "Failed to delete old snapshot {} while creating device attribute remote updater snapshot.";
  public static final String FAILED_TO_RENAME_SNAPSHOT_UPDATER =
      "Failed to rename {} to {} while creating device attribute remote updater snapshot.";
  public static final String FAILED_TO_CREATE_UPDATER_SNAPSHOT =
      "Failed to create device attribute remote updater snapshot due to {}";
  public static final String UPDATER_SNAPSHOT_NOT_FOUND =
      "Device attribute remote updater snapshot {} not found, consider it as upgraded from the older version, will not update remote";
  public static final String LOAD_UPDATER_SNAPSHOT_FAILED =
      "Load device attribute remote updater snapshot from {} failed, continue...";
  public static final String REQUEST_MEMORY_SIZE_NEGATIVE =
      "requestMemory size must not be negative";
  public static final String RELEASE_MEMORY_SIZE_NEGATIVE =
      "releaseMemory size must not be negative";

  // ======================== MetaFormatUtils ========================

  public static final String ILLEGAL_NAME = "%s is an illegal name.";
  public static final String NAME_CONTAINS_UNSUPPORTED_CHAR =
      "The name, %s, contains unsupported character.";
  public static final String DATABASE_NAME_ILLEGAL_CHARS =
      "The database name can only contain english or chinese characters, numbers, backticks and underscores. %s";
  public static final String SDT_COMPRESSION_DEVIATION_REQUIRED =
      "SDT compression deviation is required";
  public static final String SDT_COMPRESSION_DEVIATION_NEGATIVE =
      "SDT compression deviation cannot be negative";
  public static final String SDT_COMPRESSION_DEVIATION_FORMAT_ERROR =
      "SDT compression deviation formatting error";
  public static final String SDT_COMPRESSION_MAX_GREATER_THAN_MIN =
      "SDT compression maximum time needs to be greater than compression minimum time";
  public static final String SDT_COMPRESSION_TIME_NEGATIVE =
      "SDT compression %s time cannot be negative";
  public static final String SDT_COMPRESSION_TIME_FORMAT_ERROR =
      "SDT compression %s time formatting error";
  public static final String SDT_ENABLED_NO_COMPRESSION_TIME =
      "{} enabled SDT but did not set compression {} time";

  // ======================== Tag/Attribute ========================

  public static final String TIMESERIES_NO_TAG_ATTRIBUTE =
      "TimeSeries [%s] does not have any tag/attribute.";
  public static final String TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE =
      "TimeSeries [%s] does not have [%s] tag/attribute.";
  public static final String TIMESERIES_ALREADY_HAS_ATTRIBUTE =
      "TimeSeries [%s] already has the attribute [%s].";
  public static final String TIMESERIES_ALREADY_HAS_TAG =
      "TimeSeries [%s] already has the tag [%s].";
  public static final String TIMESERIES_NO_TAG_ATTRIBUTE_LOG =
      "TimeSeries [{}] does not have tag/attribute [{}]";
  public static final String TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE_FMT =
      "TimeSeries [%s] does not have tag/attribute [%s].";

  // ======================== TagManager Snapshot ========================

  public static final String FAILED_TO_DELETE_OLD_TAG_SNAPSHOT =
      "Failed to delete old snapshot {} while creating tagManager snapshot.";
  public static final String FAILED_TO_RENAME_TAG_SNAPSHOT =
      "Failed to rename {} to {} while creating tagManager snapshot.";
  public static final String FAILED_TO_DELETE_AFTER_RENAME_FAILURE =
      "Failed to delete {} after renaming failure.";
  public static final String FAILED_TO_CREATE_TAG_SNAPSHOT =
      "Failed to create tagManager snapshot due to {}";
  public static final String FAILED_TO_DELETE_AFTER_TAG_SNAPSHOT_FAILURE =
      "Failed to delete {} after creating tagManager snapshot failure.";
  public static final String FAILED_TO_DELETE_FILE = "Failed to delete {}.";
  public static final String FAILED_TO_DELETE_EXISTING_WHEN_LOADING =
      "Failed to delete existing {} when loading snapshot.";
  public static final String FAILED_TO_DELETE_EXISTING_WHEN_COPY_FAILURE =
      "Failed to delete existing {} when copying snapshot failure.";

  // ======================== TagLogFile ========================

  public static final String CREATE_SCHEMA_FOLDER = "create schema folder {}.";
  public static final String CREATE_SCHEMA_FOLDER_FAILED = "create schema folder {} failed.";

  // ======================== MemMTreeSnapshotUtil ========================

  public static final String FAILED_TO_DELETE_OLD_MTREE_SNAPSHOT =
      "Failed to delete old snapshot {} while creating mTree snapshot.";
  public static final String FAILED_TO_RENAME_MTREE_SNAPSHOT =
      "Failed to rename {} to {} while creating mTree snapshot.";
  public static final String FAILED_TO_CREATE_MTREE_SNAPSHOT =
      "Failed to create mTree snapshot due to {}";
  public static final String SERIALIZE_ERROR_INFO =
      "Error occurred during serializing MemMTree.";
  public static final String UNRECOGNIZED_MNODE_TYPE = "Unrecognized MNode type ";

  // ======================== View ========================

  public static final String IS_NO_VIEW = "[%s] is no view.";
  public static final String VIEW_NOT_SUPPORTED = "View is not supported.";
  public static final String VIEW_DOES_NOT_SUPPORT_ALIAS = "View doesn't support alias";
  public static final String CANNOT_CONSTRUCT_ABSTRACT_CLASS =
      "Can not construct abstract class.";

  // ======================== PBTree ========================

  public static final String TABLE_MODEL_NOT_SUPPORT_PBTREE =
      "TableModel does not support PBTree yet.";
  public static final String PBTREE_NOT_SUPPORT_ALTER_ENCODING =
      "PBTree does not support altering encoding and compressor yet.";
  public static final String NOT_IMPLEMENTED = "Not implemented";
  public static final String PBTREE_FILE_OVERWRITTEN =
      "PBTree File [{}] will be overwritten since already exists.";
  public static final String SCHEMA_FILE_WRONG_VERSION =
      "SchemaFile with wrong version, please check or upgrade.";
  public static final String NODE_NO_CHILD_IN_PBTREE =
      "Node [%s] has no child in pbtree file.";
  public static final String SCHEMA_FILE_INSPECTED = "SchemaFile[%s] had been inspected.";
  public static final String FAILED_TO_CREATE_SCHEMA_FILE_SNAPSHOT =
      "Failed to create SchemaFile snapshot due to {}";
  public static final String FAILED_TO_DELETE_OLD_PBTREE_SNAPSHOT =
      "Failed to delete old snapshot {} while creating pbtree file snapshot.";

  // ======================== PBTree Segment/Page ========================

  public static final String FAILED_TO_INSERT_RELOCATED_SEGMENT =
      "failed to insert buffer into relocated segment";
  public static final String FAILED_TO_UPDATE_RELOCATED_SEGMENT =
      "failed to update buffer upon relocated segment";
  public static final String ALIAS_INDEX_PAGE_EXTEND_CAPACITY =
      "AliasIndexPage can only extend to buffer with same capacity.";
  public static final String SEGMENTS_SPLIT_SAME_CAPACITY =
      "Segments only splits with same capacity.";
  public static final String SEGMENT_SPLIT_NO_RECORDS =
      "Segment can not be split with no records.";
  public static final String SEGMENT_SPLIT_ONLY_ONE_RECORD =
      "Segment can not be split with only one record.";
  public static final String INTERNAL_PAGE_EXTEND_CAPACITY =
      "InternalPage can only extend to buffer with same capacity.";
  public static final String INTERNAL_SEGMENT_SPLIT_NO_KEY =
      "Internal Segment cannot split without insert key";
  public static final String INTERNAL_SEGMENT_LESS_THAN_2_POINTERS =
      "Segment has less than 2 pointers can not be split.";
  public static final String LEAF_SEGMENT_EXTEND_SMALLER =
      "Leaf Segment cannot extend to a smaller buffer.";
  public static final String RECORD_CONFLICT_NAME_WITH_ALIAS =
      "Record [%s] has conflict name with alias of its siblings.";
  public static final String RECORD_CONFLICT_ALIAS =
      "Record [%s] has conflict alias [%s] with its siblings.";
  public static final String RECORD_NOT_EXISTED = "Record[key:%s] Not Existed.";
  public static final String SEGMENT_CACHE_MAP_INCONSISTENT =
      "Segment cache map inconsistent with segment list in page %d.";
  public static final String UNRECOGNIZED_NODE_TYPE = "Unrecognized node type: ";

  // ======================== PBTree PageManager ========================

  public static final String CHILD_SHALL_NOT_HAVE_SEGMENT_ADDRESS =
      "A child in newChildBuffer shall not have segmentAddress.";
  public static final String PAGE_INDEX_OUT_OF_RANGE = "Page index %d out of range.";
  public static final String ROOT_PAGE_SHALL_NOT_BE_MIGRATED =
      "Root page shall not be migrated.";
  public static final String SUBORDINATE_INDEX_NOT_ON_SINGLE_PAGE =
      "Subordinate index shall not build upon single page segment.";
  public static final String SUBORDINATE_INDEX_BROKEN =
      "File may be corrupted that subordinate index has broken.";
  public static final String DUPLICATE_PAGE_INSTANCES =
      "Duplicate page instances with identical index: {}";
  public static final String PAGE_LOCKED_TIMES = "Page [{}] had been locked {} times.";
  public static final String REENTRANT_WRITE_LOCKS_DETAIL =
      "Reentrant write locks on page {}, content detail:{}";
  public static final String REENTRANT_WRITE_LOCKS = "Reentrant write locks on page:{}";

  // ======================== PBTree Flush ========================

  public static final String IO_EXCEPTION_UPDATING_SG_MNODE =
      "IOException occurred during updating StorageGroupMNode {}";
  public static final String ERROR_DURING_MTREE_FLUSH =
      "Error occurred during MTree flush, current node is {}";

  // ======================== PBTree ReleaseFlushMonitor ========================

  public static final String RELEASE_TASK_MONITOR_INTERRUPTED =
      "ReleaseTaskMonitor thread is interrupted.";
  public static final String RELEASE_FLUSH_TASK_TIMEOUT =
      "Interrupt because the release task and flush task did not finish within {} milliseconds.";

  // ======================== PBTree PagePool ========================

  public static final String PAGE_CACHE_EVICTION_INTERRUPTED =
      "Interrupted during page cache eviction. Consider increasing cache size, reducing concurrency, or extending timeout";

  // ======================== ReadOnly MTreeStore ========================

  public static final String READ_ONLY_REENTRANT_MTREE_STORE = "ReadOnlyReentrantMTreeStore";

  // ======================== MNode ========================

  public static final String WRONG_MNODE_TYPE = "Wrong MNode Type";
  public static final String WRONG_NODE_TYPE = "Wrong node type";
  public static final String SHOULD_CALL_EXACT_SUB_CLASS = "Should call exact sub class!";
  public static final String VIEW_TABLE_NOT_ALLOWED = "View table is not allowed.";
  public static final String TABLE_DEVICE_NOT_UNDER_TREE_MODEL =
      "Table device shall not create under tree model";
  public static final String NO_SATISFIED_MNODE_FACTORY = "No satisfied MNodeFactory found";

  // ======================== Logfile ========================

  public static final String READ_LOG_LENGTH_NEGATIVE = "Read log length %s is negative.";
  public static final String PLAN_NOT_SUPPORT_DESERIALIZATION =
      "%s plan doesn't support deserialization.";
  public static final String PLAN_NOT_SUPPORT_SERIALIZATION =
      "%s plan doesn't support serialization.";
  public static final String SCHEMA_FILE_LOG_INCOMPLETE_ENTRY = "incomplete entry.";

  // ======================== Template ========================

  public static final String UNKNOWN_TEMPLATE_UPDATE_OPERATION_TYPE =
      "Unknown template update operation type";

  // ======================== InformationSchema ========================

  public static final String SYSTEM_VIEW_NOT_SUPPORT_SHOW_CREATE =
      "The system view does not support show create.";
  public static final String SYSTEM_DATABASE_NOT_SUPPORT_SHOW_CREATE =
      "The system database does not support show create.";

  // ======================== Traverser ========================

  // (uses e.getMessage(), no string literal needed)

  // ======================== BTreePageManager ========================

  // (uses e.getMessage(), no string literal needed)

  // ======================== Additional SchemaRegion ========================

  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_EMPTY =
      "SchemaRegionPlan of type %s doesn't support creating empty plan.";
  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_RECOVER_MEMORY =
      "SchemaRegionPlan of type %s doesn't support recover operation in SchemaRegionMemoryImpl.";
  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_RECOVER_PBTREE =
      "SchemaRegionPlan of type %s doesn't support recover operation in SchemaRegionPBTreeImpl.";
  public static final String PBTREE_NOT_SUPPORT_ALTER_DATA_TYPE =
      "PBTree does not support altering timeseries data type.";

  // ======================== Additional MTree ========================

  public static final String DEVICE_NUM_UPPER_LIMIT =
      "The number of devices has reached the upper limit";
  public static final String TIMESERIES_TYPE_NOT_COMPATIBLE =
      "The timeseries %s used new type %s is not compatible with the existing one %s";
  public static final String ALIAS_DUPLICATED =
      "The alias is duplicated with the name or alias of other measurement, alias: ";
  public static final String LOGICAL_VIEW_NODE_TYPE_ERROR =
      "Type of newMNode is not LogicalViewMNode! It's ";
  public static final String TEMPLATE_SHOULD_MOUNTED_ON_ANCESTOR =
      "There should be a template mounted on any ancestor of the node [%s] usingTemplate.";
  public static final String DESCENDANT_SHOULD_NOT_EXIST =
      "There should not exist descendant under this node %s";

  // ======================== Additional SchemaFile/Page ========================

  public static final String ADDING_CHILDREN_UNDER_TEMPLATE_NOT_ALLOWED =
      "Adding or updating children of device using template [%s] is NOT allowed.";
  public static final String CANNOT_FLUSH_NODE_NEGATIVE_ADDRESS =
      "Cannot flush any node with negative address [%s] except for DatabaseNode.";
  public static final String SEGMENTED_PAGE_SHARE_BUFFER =
      "SegmentedPage can share entire buffer slice only when it contains one MAX SIZE segment.";
  public static final String BYTEBUFFER_CORRUPTED_FOR_SCHEMA_PAGE =
      "ByteBuffer is corrupted or set to a wrong position to load as a SchemaPage.";
  public static final String NODE_NO_CHILD_IN_PBTREE_WITH_NAME =
      "Node[%s] has no child[%s] in pbtree file.";
  public static final String SINGLE_RECORD_TOO_LARGE =
      "Single record larger than half page is not supported in SchemaFile now.";
  public static final String PAGE_REPLACEMENT_ERROR =
      "Page[%d] replacement error: Different ref count or lock object.";
  public static final String NODE_NO_VALID_SEGMENT_ADDRESS =
      "Node [%s] has no valid segment address in pbtree file.";

  // ======================== Additional SchemaFileLog ========================

  public static final String COMMIT_MARK_WITHOUT_PREPARE = "COMMIT_MARK without PREPARE_MARK";
  public static final String EXTRANEOUS_BYTE_AFTER_PREPARE =
      "an extraneous byte rather than COMMIT_MARK after PREPARE_MARK";
  public static final String NOT_ENDED_BY_MARK =
      "not ended by COMMIT_MARK nor PREPARE_MARK.";

  // ======================== Additional MNodeContainer ========================

  public static final String DUPLICATE_NODE_IN_BUFFERS =
      "There shall not exist two node with the same name separately in newChildBuffer and updateChildBuffer";

  // ======================== Additional Logfile ========================

  public static final String FAILED_TO_CREATE_FILE_ALREADY_EXISTS =
      "Failed to create file %s because the named file already exists";

  // ======================== Additional View ========================

  public static final String VISIT_EXPRESSION_NOT_SUPPORTED =
      "visitExpression in TransformToExpressionVisitor is not supported.";

  // ======================== Additional Tag ========================

  public static final String BYTEBUFFER_SMALLER_THAN_TAG_SIZE =
      "ByteBuffer capacity is smaller than tagAttributeTotalSize, which is not allowed.";
  public static final String TIMESERIES_ALREADY_HAS_TAG_ATTRIBUTE_NAMED =
      "TimeSeries [%s] already has a tag/attribute named [%s].";

  // ======================== Additional Template ========================

  public static final String FAILED_TO_CREATE_TEMPLATE =
      "Failed to execute create device template {} in config node, status is {}.";
  public static final String CREATE_TEMPLATE_ERROR_PREFIX = "create template error -";
  public static final String CREATE_TEMPLATE_ERROR = "create template error.";
  public static final String GET_ALL_TEMPLATE_ERROR = "get all template error.";
  public static final String GET_TEMPLATE_INFO_ERROR = "get template info error.";
  public static final String FAILED_TO_SET_TEMPLATE =
      "Failed to execute set device template {} on path {} in config node, status is {}.";

  // ======================== Additional InformationSchema ========================

  public static final String INFORMATION_SCHEMA_READ_ONLY =
      "The database 'information_schema' can only be queried";

  // ======================== Additional GRASS/Updater ========================

  public static final String FAILED_TO_WRITE_ATTR_COMMIT =
      "Failed to write attribute commit message to region {}.";
  public static final String FAILED_TO_FETCH_DATANODE_LOCATIONS =
      "Failed to fetch dataNodeLocations, will retry.";

  // ======================== Additional ResourceByPathUtils ========================

  public static final String FAILED_TO_RESERVE_MEMORY_TVLIST =
      "Failed to reserve memory for TVList: ramSize {}, timestampsSize {}, arrayMemCost {}, rowCount {}, dataTypes {}";

  // ======================== Additional CachedMTreeStore ========================

  public static final String ERROR_DURING_PBTREE_CLEAR =
      "Error occurred during PBTree clear, {}";
  public static final String ERROR_DURING_MTREE_FLUSH_SCHEMA_REGION =
      "Error occurred during MTree flush, current SchemaRegionId is {}";
  public static final String ERROR_DURING_MTREE_FLUSH_SCHEMA_REGION_BECAUSE =
      "Error occurred during MTree flush, current SchemaRegionId is {} because {}";

  // ======================== Additional MemMTreeSnapshotUtil ========================

  public static final String DESERIALIZE_ERROR_INFO =
      "Error occurred during deserializing MemMTree.";

  // ======================== Additional MetaUtils ========================

  public static final String PATH_NO_LONGER_THAN_SG_LEVEL =
      "it is no longer than default sg level: ";
  public static final String PATH_DOES_NOT_START_WITH_ROOT = "it does not start with ";

  // ======================== FakeCRC32Deserializer ========================

  public static final String READ_LOG_LENGTH_NEGATIVE_LOG =
      "Read log length {} is negative.";

  // ======================== SchemaLogReader ========================

  public static final String FILE_CORRUPTED =
      "File {} is corrupted. The uncorrupted size is {}.";
  public static final String LOG_FILE_END_CORRUPTED_TRUNCATE =
      "The end of log file {} is corrupted. Start truncate it. The unbroken size is {}. The file size is {}.";
  public static final String FAIL_TO_TRUNCATE_LOG_FILE =
      "Fail to truncate log file to size {}";

  // ======================== SchemaRegionPlanDeserializer ========================

  public static final String CANNOT_DESERIALIZE_SCHEMA_REGION_PLAN =
      "Cannot deserialize SchemaRegionPlan from buffer";

  // ======================== MTreeBelowSGMemoryImpl ========================

  public static final String TIMESERIES_NUM_UPPER_LIMIT =
      "The number of timeseries has reached the upper limit";
  public static final String ALIAS_DUPLICATED_DETAIL =
      ", fullPath: ";
  public static final String ALIAS_DUPLICATED_OTHER_MEASUREMENT =
      ", otherMeasurement: ";
  public static final String START_CREATE_TABLE_DEVICE =
      "Start to create table device {}.{}";
  public static final String TABLE_DEVICE_ALREADY_EXISTS =
      "Table device {}.{} already exists";
  public static final String TABLE_DEVICE_CREATED =
      "Table device {}.{} created";

  // ======================== CachedMTreeStore / Scheduler ========================

  public static final String MTREE_FLUSH_COST =
      "It takes {}ms to flush MTree in SchemaRegion {}";

  // ======================== DataNodeTableCache ========================

  public static final String INIT_TABLE_CACHE_SUCCESS =
      "Init DataNodeTableCache successfully";
  public static final String PRE_UPDATE_TABLE_SUCCESS =
      "Pre-update table {}.{} successfully";
  public static final String PRE_RENAME_OLD_TABLE_SUCCESS =
      "Pre-rename old table {}.{} successfully";
  public static final String ROLLBACK_UPDATE_TABLE_SUCCESS =
      "Rollback-update table {}.{} successfully";
  public static final String ROLLBACK_RENAME_OLD_TABLE_SUCCESS =
      "Rollback renaming old table {}.{} successfully.";
  public static final String COMMIT_UPDATE_TABLE_SUCCESS_WITH_DETAIL =
      "Commit-update table {}.{} successfully, {}";
  public static final String COMMIT_UPDATE_TABLE_SUCCESS =
      "Commit-update table {}.{} successfully.";
  public static final String RENAME_OLD_TABLE_SUCCESS =
      "Rename old table {}.{} successfully.";
  public static final String INTERRUPTED_ACQUIRE_SEMAPHORE_GET_TABLES =
      "Interrupted when trying to acquire semaphore when trying to get tables from configNode, ignore.";
  public static final String UPDATE_TABLE_BY_FETCH_WITH_DETAIL =
      "Update table {}.{} by table fetch, {}";
  public static final String UPDATE_TABLE_BY_FETCH =
      "Update table {}.{} by table fetch.";
  public static final String COMPARE_TABLE_ADDED = "Added table: ";
  public static final String COMPARE_TABLE_REMOVED = "Removed table: ";
  public static final String COMPARE_TABLE_NAME = "Table name: ";
  public static final String COMPARE_TABLE_REMOVED_PROPS = " Removed props: ";
  public static final String COMPARE_TABLE_ADDED_PROPS = " Added props: ";
  public static final String COMPARE_TABLE_REMOVED_COLUMNS = " Removed column(s): ";
  public static final String COMPARE_TABLE_ADDED_COLUMNS = " Added column(s): ";
  public static final String COMPARE_TABLE_NOT_MODIFIED = " Not modified";

  // ======================== ClusterTemplateManager ========================

  public static final String ILLEGAL_PATH_LOG = "illegal path {}";

  private DataNodeSchemaMessages() {}
}
