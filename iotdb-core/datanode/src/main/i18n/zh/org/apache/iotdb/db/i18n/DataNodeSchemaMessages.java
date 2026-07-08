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

  // ======================== SchemaEngine 相关消息 ========================

  public static final String USED_SCHEMA_ENGINE_MODE = "使用的 schema 引擎模式：{}。";
  public static final String SCHEMA_REGION_RECOVERY_ERROR = "SchemaRegion 恢复过程中发生异常";
  public static final String CLEAR_SCHEMA_REGION_MAP = "已清空 schema region 映射表。";
  public static final String FAILED_TO_UPDATE_SUBTREE_MEASUREMENT_COUNT =
      "更新模板 {} 在 schemaRegion {} 中的子树测点计数失败";
  public static final String RECOVER_SPEND = "恢复 [{}] 耗时：{} ms";
  public static final String SCHEMA_REGION_FAILED_TO_RECOVER =
      "SchemaRegion [%d] 在 StorageGroup [%s] 中恢复失败。";
  public static final String SCHEMA_REGION_ALREADY_DELETED =
      "SchemaRegion(id = {}) 已被删除，已跳过";
  public static final String FAILED_TO_GET_TABLE_FOR_TIMESERIES_COUNT =
      "计算时间序列数量时获取表 {}.{} 失败，可能是集群正在重启或表正在被删除。";
  public static final String TREE_VIEW_TABLE_CANNOT_BE_WRITTEN_OR_DELETED =
      "表 %s.%s 是树模型视图，不能写入或删除";
  public static final String PEER_IS_SHUTTING_DOWN = "节点正在关闭中。";
  public static final String SCHEMA_REGION_DUPLICATED =
      "SchemaRegion [%s] 在 [%s] 和 [%s] 之间重复，前者已被恢复。";

  // ======================== MemSchemaEngineStatistics 相关消息 ========================

  public static final String CURRENT_SERIES_MEMORY_TOO_LARGE =
      "当前时间序列内存 {} 过大...";
  public static final String CURRENT_SERIES_MEMORY_BACK_TO_NORMAL =
      "当前时间序列内存 {} 已恢复正常水平，总时间序列数量为 {}。";
  public static final String WRONG_SCHEMA_ENGINE_STATISTICS_TYPE =
      "SchemaEngineStatistics 类型错误";

  // ======================== MemSchemaRegionStatistics 相关消息 ========================

  public static final String WRONG_SCHEMA_REGION_STATISTICS_TYPE =
      "SchemaRegionStatistics 类型错误";

  // ======================== SchemaRegionUtils 相关消息 ========================

  public static final String CANNOT_GET_FILES_IN_SCHEMA_REGION_DIR =
      "无法获取 schema region 目录 %s 中的文件";
  public static final String DELETE_SCHEMA_REGION_FILE = "删除 schema region 文件 {}";
  public static final String DELETE_SCHEMA_REGION_FILE_FAILED =
      "删除 schema region 文件 {} 失败。";
  public static final String FAILED_TO_DELETE_SCHEMA_REGION_FILE =
      "删除 schema region 文件 %s 失败";
  public static final String DELETE_SCHEMA_REGION_FOLDER = "删除 schema region 目录 {}";
  public static final String DELETE_SCHEMA_REGION_FOLDER_FAILED =
      "删除 schema region 目录 {} 失败。";
  public static final String FAILED_TO_DELETE_SCHEMA_REGION_FOLDER =
      "删除 schema region 目录 %s 失败";
  public static final String DELETE_DATABASE_SCHEMA_FOLDER = "删除数据库 schema 目录 {}";
  public static final String DELETE_DATABASE_SCHEMA_FOLDER_FAILED =
      "删除数据库 schema 目录 {} 失败";

  // ======================== SchemaRegionLoader 相关消息 ========================

  public static final String CLASS_NOT_SUBCLASS_OF_ISCHEMAREGION =
      "类 %s 不是 ISchemaRegion 的子类。";
  public static final String DUPLICATED_SCHEMA_REGION_IMPL =
      "存在重复的 SchemaRegion 实现，{} 和 {} 使用了相同的模式名称 [{}]";
  public static final String NO_SCHEMA_REGION_IMPL_WITH_TARGET_MODE =
      "未找到目标模式 {} 的 SchemaRegion 实现，使用默认模式 {}";
  public static final String SCHEMA_REGION_LOADER_INFO =
      "[SchemaRegionLoader]，schemaEngineMode：{}，currentMode：{}";

  // ======================== SchemaRegionPlanType 相关消息 ========================

  public static final String UNRECOGNIZED_SCHEMA_REGION_PLAN_TYPE =
      "无法识别的 SchemaRegionPlanType：";

  // ======================== SchemaRegion 初始化/目录 相关消息 ========================

  public static final String CREATE_DATABASE_SCHEMA_FOLDER = "创建数据库 schema 目录 {}";
  public static final String CREATE_DATABASE_SCHEMA_FOLDER_FAILED =
      "创建数据库 schema 目录 {} 失败。";
  public static final String CREATE_SCHEMA_REGION_FOLDER = "创建 schema region 目录 {}";
  public static final String CREATE_SCHEMA_REGION_FOLDER_FAILED =
      "创建 schema region 目录 {} 失败。";
  public static final String CANNOT_RECOVER_ALL_SCHEMA_INFO =
      "无法从 {} 恢复所有 schema 信息，将尽可能恢复";
  public static final String CANNOT_RECOVER_ALL_MTREE =
      "无法从 {} 文件恢复所有 MTree，将尽可能恢复";

  // ======================== SchemaRegion MLog 相关消息 ========================

  public static final String CANNOT_FORCE_MLOG = "无法将 {} 的 mlog 强制写入 schema region";
  public static final String SPEND_TIME_DESERIALIZE_MTREE =
      "从 mlog.bin 反序列化 MTree 耗时 {} ms，对应路径 {}";
  public static final String FAILED_TO_PARSE_MLOG = "解析 ";
  public static final String MLOG_BIN_SUFFIX = " mlog.bin 失败";
  public static final String PARSE_MLOG_ERROR = "在行号 {} 处解析 mlog 出错：";
  public static final String CANNOT_OPERATE_CMD = "无法执行命令 {}，错误：";
  public static final String MLOG_BIN_CORRUPTED =
      "mlog.bin 文件已损坏，请删除或修复该文件，然后重启 IoTDB";
  public static final String CANNOT_CLOSE_METADATA_LOG_WRITER =
      "无法关闭元数据日志写入器：";
  public static final String MLOG_RECOVERY_CHECK_POINT = "MLog 恢复检查点：{}";
  public static final String CANNOT_GET_MLOG_CHECKPOINT =
      "无法从 MLogDescription 文件获取检查点，原因：{}，使用默认值 0。";
  public static final String FAILED_TO_SKIP_MLOG = "跳过 {} 失败，schemaRegion 目录为 {}";
  public static final String UPDATE_MLOG_DESCRIPTION_FAILED = "更新 {} 失败，原因：{}";
  public static final String DIRECT_BUFFER_MEMORY_EXCEEDED =
      "直接缓冲区的总分配内存将达到 ";
  public static final String DIRECT_BUFFER_MEMORY_LIMIT = "，超过内存限制：";

  // ======================== SchemaRegion 快照相关消息 ========================

  public static final String FAILED_TO_CREATE_SNAPSHOT_NOT_INITIALIZED =
      "创建 schemaRegion {} 的快照失败，因为该 schemaRegion 尚未初始化。";
  public static final String START_CREATE_SNAPSHOT = "开始创建 schemaRegion {} 的快照";
  public static final String MTREE_SNAPSHOT_CREATION_COST =
      "schemaRegion {} 的 MTree 快照创建耗时 {}ms。";
  public static final String MTREE_SNAPSHOT_CREATION_COST_WITH_STATUS =
      "schemaRegion {} 的 MTree 快照创建耗时 {}ms，状态：{}";
  public static final String TAG_SNAPSHOT_CREATION_COST =
      "schemaRegion {} 的 Tag 快照创建耗时 {}ms。";
  public static final String TAG_SNAPSHOT_CREATION_COST_WITH_STATUS =
      "schemaRegion {} 的 Tag 快照创建耗时 {}ms，状态：{}";
  public static final String DEVICE_ATTR_SNAPSHOT_CREATION_COST =
      "schemaRegion {} 的设备属性快照创建耗时 {}ms，状态：{}";
  public static final String DEVICE_ATTR_UPDATER_SNAPSHOT_CREATION_COST =
      "schemaRegion {} 的设备属性远程更新器快照创建耗时 {}ms，状态：{}";
  public static final String SNAPSHOT_CREATION_COST =
      "schemaRegion {} 的快照创建耗时 {}ms。";
  public static final String SUCCESSFULLY_CREATE_SNAPSHOT =
      "成功创建 schemaRegion {} 的快照";
  public static final String START_LOADING_SNAPSHOT =
      "开始加载 schemaRegion {} 的快照";
  public static final String DEVICE_ATTR_SNAPSHOT_LOADING_COST =
      "schemaRegion {} 的设备属性快照加载耗时 {}ms。";
  public static final String DEVICE_ATTR_UPDATER_SNAPSHOT_LOADING_COST =
      "schemaRegion {} 的设备属性远程更新器快照加载耗时 {}ms。";
  public static final String TAG_SNAPSHOT_LOADING_COST =
      "schemaRegion {} 的 Tag 快照加载耗时 {}ms。";
  public static final String MTREE_SNAPSHOT_LOADING_COST =
      "schemaRegion {} 的 MTree 快照加载耗时 {}ms。";
  public static final String SNAPSHOT_LOADING_COST =
      "schemaRegion {} 的快照加载耗时 {}ms。";
  public static final String SUCCESSFULLY_LOAD_SNAPSHOT =
      "成功加载 schemaRegion {} 的快照";
  public static final String FAILED_TO_LOAD_SNAPSHOT =
      "加载 schemaRegion {} 的快照失败，原因：{}，将使用空的 schemaRegion";
  public static final String ERROR_DURING_INIT_SCHEMA_REGION =
      "初始化 schemaRegion {} 过程中发生错误";
  public static final String FAILED_TO_RECOVER_TAG_INDEX =
      "恢复 {} 的 tagIndex 失败，schemaRegion 为 {}。";
  public static final String FAILED_TO_READ_TAG_ATTRIBUTE =
      "读取 tag 和 attribute 信息失败：{}";

  // ======================== DeviceAttributeStore 相关消息 ========================

  public static final String FAILED_TO_DELETE_OLD_SNAPSHOT_DEVICE_ATTR =
      "创建设备属性快照时删除旧快照 {} 失败。";
  public static final String FAILED_TO_RENAME_SNAPSHOT_DEVICE_ATTR =
      "创建设备属性快照时将 {} 重命名为 {} 失败。";
  public static final String FAILED_TO_CREATE_DEVICE_ATTR_SNAPSHOT =
      "创建设备属性快照失败：{}";
  public static final String DEVICE_ATTR_SNAPSHOT_NOT_FOUND =
      "未找到设备属性快照 {}，视为从旧版本升级，使用空属性";
  public static final String LOAD_DEVICE_ATTR_SNAPSHOT_FAILED =
      "从 {} 加载设备属性快照失败";

  // ======================== DeviceAttributeCacheUpdater 相关消息 ========================

  public static final String FAILED_TO_DELETE_OLD_SNAPSHOT_UPDATER =
      "创建设备属性远程更新器快照时删除旧快照 {} 失败。";
  public static final String FAILED_TO_RENAME_SNAPSHOT_UPDATER =
      "创建设备属性远程更新器快照时将 {} 重命名为 {} 失败。";
  public static final String FAILED_TO_CREATE_UPDATER_SNAPSHOT =
      "创建设备属性远程更新器快照失败：{}";
  public static final String UPDATER_SNAPSHOT_NOT_FOUND =
      "未找到设备属性远程更新器快照 {}，视为从旧版本升级，将不更新远程节点";
  public static final String LOAD_UPDATER_SNAPSHOT_FAILED =
      "从 {} 加载设备属性远程更新器快照失败，继续...";
  public static final String REQUEST_MEMORY_SIZE_NEGATIVE =
      "requestMemory 大小不能为负数";
  public static final String RELEASE_MEMORY_SIZE_NEGATIVE =
      "releaseMemory 大小不能为负数";

  // ======================== MetaFormatUtils 相关消息 ========================

  public static final String ILLEGAL_NAME = "%s 是不合法的名称。";
  public static final String NAME_CONTAINS_UNSUPPORTED_CHAR =
      "名称 %s 包含不支持的字符。";
  public static final String DATABASE_NAME_ILLEGAL_CHARS =
      "数据库名称只能包含中英文字符、数字、反引号和下划线。%s";
  public static final String SDT_COMPRESSION_DEVIATION_REQUIRED =
      "SDT 压缩偏差为必填项";
  public static final String SDT_COMPRESSION_DEVIATION_NEGATIVE =
      "SDT 压缩偏差不能为负数";
  public static final String SDT_COMPRESSION_DEVIATION_FORMAT_ERROR =
      "SDT 压缩偏差格式错误";
  public static final String SDT_COMPRESSION_MAX_GREATER_THAN_MIN =
      "SDT 压缩最大时间必须大于最小时间";
  public static final String SDT_COMPRESSION_TIME_NEGATIVE =
      "SDT 压缩 %s 时间不能为负数";
  public static final String SDT_COMPRESSION_TIME_FORMAT_ERROR =
      "SDT 压缩 %s 时间格式错误";
  public static final String SDT_ENABLED_NO_COMPRESSION_TIME =
      "{} 已启用 SDT 但未设置压缩 {} 时间";

  // ======================== Tag/Attribute 相关消息 ========================

  public static final String TIMESERIES_NO_TAG_ATTRIBUTE =
      "时间序列 [%s] 没有任何 tag/attribute。";
  public static final String TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE =
      "时间序列 [%s] 没有 [%s] tag/attribute。";
  public static final String TIMESERIES_ALREADY_HAS_ATTRIBUTE =
      "时间序列 [%s] 已有 attribute [%s]。";
  public static final String TIMESERIES_ALREADY_HAS_TAG =
      "时间序列 [%s] 已有 tag [%s]。";
  public static final String TIMESERIES_NO_TAG_ATTRIBUTE_LOG =
      "时间序列 [{}] 没有 tag/attribute [{}]";
  public static final String TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE_FMT =
      "时间序列 [%s] 没有 tag/attribute [%s]。";

  // ======================== TagManager 快照相关消息 ========================

  public static final String FAILED_TO_DELETE_OLD_TAG_SNAPSHOT =
      "创建 tagManager 快照时删除旧快照 {} 失败。";
  public static final String FAILED_TO_RENAME_TAG_SNAPSHOT =
      "创建 tagManager 快照时将 {} 重命名为 {} 失败。";
  public static final String FAILED_TO_DELETE_AFTER_RENAME_FAILURE =
      "重命名失败后删除 {} 失败。";
  public static final String FAILED_TO_CREATE_TAG_SNAPSHOT =
      "创建 tagManager 快照失败：{}";
  public static final String FAILED_TO_DELETE_AFTER_TAG_SNAPSHOT_FAILURE =
      "创建 tagManager 快照失败后删除 {} 失败。";
  public static final String FAILED_TO_DELETE_FILE = "删除 {} 失败。";
  public static final String FAILED_TO_DELETE_EXISTING_WHEN_LOADING =
      "加载快照时删除已有的 {} 失败。";
  public static final String FAILED_TO_DELETE_EXISTING_WHEN_COPY_FAILURE =
      "复制快照失败时删除已有的 {} 失败。";

  // ======================== TagLogFile 相关消息 ========================

  public static final String CREATE_SCHEMA_FOLDER = "创建 schema 目录 {}。";
  public static final String CREATE_SCHEMA_FOLDER_FAILED = "创建 schema 目录 {} 失败。";

  // ======================== MemMTreeSnapshotUtil 相关消息 ========================

  public static final String FAILED_TO_DELETE_OLD_MTREE_SNAPSHOT =
      "创建 mTree 快照时删除旧快照 {} 失败。";
  public static final String FAILED_TO_RENAME_MTREE_SNAPSHOT =
      "创建 mTree 快照时将 {} 重命名为 {} 失败。";
  public static final String FAILED_TO_CREATE_MTREE_SNAPSHOT =
      "创建 mTree 快照失败：{}";
  public static final String SERIALIZE_ERROR_INFO =
      "序列化 MemMTree 过程中发生错误。";
  public static final String UNRECOGNIZED_MNODE_TYPE = "无法识别的 MNode 类型 ";

  // ======================== View 相关消息 ========================

  public static final String IS_NO_VIEW = "[%s] 不是视图。";
  public static final String VIEW_NOT_SUPPORTED = "不支持视图。";
  public static final String VIEW_DOES_NOT_SUPPORT_ALIAS = "视图不支持别名";
  public static final String CANNOT_CONSTRUCT_ABSTRACT_CLASS =
      "无法构造抽象类。";

  // ======================== PBTree 相关消息 ========================

  public static final String TABLE_MODEL_NOT_SUPPORT_PBTREE =
      "表模型尚不支持 PBTree。";
  public static final String PBTREE_NOT_SUPPORT_ALTER_ENCODING =
      "PBTree 尚不支持修改编码和压缩方式。";
  public static final String NOT_IMPLEMENTED = "尚未实现";
  public static final String PBTREE_FILE_OVERWRITTEN =
      "PBTree 文件 [{}] 已存在，将被覆盖。";
  public static final String SCHEMA_FILE_WRONG_VERSION =
      "SchemaFile 版本错误，请检查或升级。";
  public static final String NODE_NO_CHILD_IN_PBTREE =
      "节点 [%s] 在 pbtree 文件中没有子节点。";
  public static final String SCHEMA_FILE_INSPECTED = "SchemaFile[%s] 已被检查。";
  public static final String FAILED_TO_CREATE_SCHEMA_FILE_SNAPSHOT =
      "创建 SchemaFile 快照失败：{}";
  public static final String FAILED_TO_DELETE_OLD_PBTREE_SNAPSHOT =
      "创建 pbtree 文件快照时删除旧快照 {} 失败。";

  // ======================== PBTree Segment/Page 相关消息 ========================

  public static final String FAILED_TO_INSERT_RELOCATED_SEGMENT =
      "向重定位的 segment 插入缓冲区失败";
  public static final String FAILED_TO_UPDATE_RELOCATED_SEGMENT =
      "在重定位的 segment 上更新缓冲区失败";
  public static final String ALIAS_INDEX_PAGE_EXTEND_CAPACITY =
      "AliasIndexPage 只能扩展到相同容量的缓冲区。";
  public static final String SEGMENTS_SPLIT_SAME_CAPACITY =
      "Segment 只能以相同容量进行拆分。";
  public static final String SEGMENT_SPLIT_NO_RECORDS =
      "没有记录的 Segment 无法拆分。";
  public static final String SEGMENT_SPLIT_ONLY_ONE_RECORD =
      "只有一条记录的 Segment 无法拆分。";
  public static final String INTERNAL_PAGE_EXTEND_CAPACITY =
      "InternalPage 只能扩展到相同容量的缓冲区。";
  public static final String INTERNAL_SEGMENT_SPLIT_NO_KEY =
      "内部 Segment 没有插入键时无法拆分";
  public static final String INTERNAL_SEGMENT_LESS_THAN_2_POINTERS =
      "指针数少于 2 的 Segment 无法拆分。";
  public static final String LEAF_SEGMENT_EXTEND_SMALLER =
      "叶子 Segment 无法扩展到更小的缓冲区。";
  public static final String RECORD_CONFLICT_NAME_WITH_ALIAS =
      "记录 [%s] 的名称与其兄弟节点的别名冲突。";
  public static final String RECORD_CONFLICT_ALIAS =
      "记录 [%s] 的别名 [%s] 与其兄弟节点冲突。";
  public static final String RECORD_NOT_EXISTED = "记录[key:%s] 不存在。";
  public static final String SEGMENT_CACHE_MAP_INCONSISTENT =
      "页面 %d 中的 Segment 缓存映射与 Segment 列表不一致。";
  public static final String UNRECOGNIZED_NODE_TYPE = "无法识别的节点类型：";

  // ======================== PBTree PageManager 相关消息 ========================

  public static final String CHILD_SHALL_NOT_HAVE_SEGMENT_ADDRESS =
      "newChildBuffer 中的子节点不应有 segmentAddress。";
  public static final String PAGE_INDEX_OUT_OF_RANGE = "页面索引 %d 超出范围。";
  public static final String ROOT_PAGE_SHALL_NOT_BE_MIGRATED =
      "根页面不应被迁移。";
  public static final String SUBORDINATE_INDEX_NOT_ON_SINGLE_PAGE =
      "不应在单页面 segment 上构建从属索引。";
  public static final String SUBORDINATE_INDEX_BROKEN =
      "文件可能已损坏，从属索引已断裂。";
  public static final String DUPLICATE_PAGE_INSTANCES =
      "存在索引相同的重复页面实例：{}";
  public static final String PAGE_LOCKED_TIMES = "页面 [{}] 已被锁定 {} 次。";
  public static final String REENTRANT_WRITE_LOCKS_DETAIL =
      "页面 {} 上存在可重入写锁，内容详情：{}";
  public static final String REENTRANT_WRITE_LOCKS = "页面 {} 上存在可重入写锁";

  // ======================== PBTree Flush 相关消息 ========================

  public static final String IO_EXCEPTION_UPDATING_SG_MNODE =
      "更新 StorageGroupMNode {} 时发生 IO 异常";
  public static final String ERROR_DURING_MTREE_FLUSH =
      "MTree 刷写过程中发生错误，当前节点为 {}";

  // ======================== PBTree ReleaseFlushMonitor 相关消息 ========================

  public static final String RELEASE_TASK_MONITOR_INTERRUPTED =
      "ReleaseTaskMonitor 线程被中断。";
  public static final String RELEASE_FLUSH_TASK_TIMEOUT =
      "释放任务和刷写任务未在 {} 毫秒内完成，已中断。";

  // ======================== PBTree PagePool 相关消息 ========================

  public static final String PAGE_CACHE_EVICTION_INTERRUPTED =
      "页面缓存淘汰过程中被中断，请考虑增加缓存大小、降低并发或延长超时时间";

  // ======================== ReadOnly MTreeStore 相关消息 ========================

  public static final String READ_ONLY_REENTRANT_MTREE_STORE = "ReadOnlyReentrantMTreeStore";

  // ======================== MNode 相关消息 ========================

  public static final String WRONG_MNODE_TYPE = "错误的 MNode 类型";
  public static final String WRONG_NODE_TYPE = "错误的节点类型";
  public static final String SHOULD_CALL_EXACT_SUB_CLASS = "应调用具体的子类！";
  public static final String VIEW_TABLE_NOT_ALLOWED = "不允许使用视图表。";
  public static final String TABLE_DEVICE_NOT_UNDER_TREE_MODEL =
      "不应在树模型下创建表设备";
  public static final String NO_SATISFIED_MNODE_FACTORY = "未找到满足条件的 MNodeFactory";

  // ======================== Logfile 相关消息 ========================

  public static final String READ_LOG_LENGTH_NEGATIVE = "读取的日志长度 %s 为负数。";
  public static final String PLAN_NOT_SUPPORT_DESERIALIZATION =
      "%s 计划不支持反序列化。";
  public static final String PLAN_NOT_SUPPORT_SERIALIZATION =
      "%s 计划不支持序列化。";
  public static final String SCHEMA_FILE_LOG_INCOMPLETE_ENTRY = "不完整的日志条目。";

  // ======================== Template 相关消息 ========================

  public static final String UNKNOWN_TEMPLATE_UPDATE_OPERATION_TYPE =
      "未知的模板更新操作类型";

  // ======================== InformationSchema 相关消息 ========================

  public static final String SYSTEM_VIEW_NOT_SUPPORT_SHOW_CREATE =
      "系统视图不支持 show create。";
  public static final String SYSTEM_DATABASE_NOT_SUPPORT_SHOW_CREATE =
      "系统数据库不支持 show create。";

  // ======================== 附加 SchemaRegion 相关消息 ========================

  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_EMPTY =
      "类型为 %s 的 SchemaRegionPlan 不支持创建空计划。";
  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_RECOVER_MEMORY =
      "类型为 %s 的 SchemaRegionPlan 不支持在 SchemaRegionMemoryImpl 中执行恢复操作。";
  public static final String SCHEMA_REGION_PLAN_NOT_SUPPORT_RECOVER_PBTREE =
      "类型为 %s 的 SchemaRegionPlan 不支持在 SchemaRegionPBTreeImpl 中执行恢复操作。";
  public static final String PBTREE_NOT_SUPPORT_ALTER_DATA_TYPE =
      "PBTree 尚不支持修改时间序列数据类型。";

  // ======================== 附加 MTree 相关消息 ========================

  public static final String DEVICE_NUM_UPPER_LIMIT =
      "设备数量已达到上限";
  public static final String TIMESERIES_TYPE_NOT_COMPATIBLE =
      "时间序列 %s 使用的新类型 %s 与已有类型 %s 不兼容";
  public static final String ALIAS_DUPLICATED =
      "别名与其他测点的名称或别名重复，别名：";
  public static final String LOGICAL_VIEW_NODE_TYPE_ERROR =
      "newMNode 的类型不是 LogicalViewMNode！实际类型为 ";
  public static final String TEMPLATE_SHOULD_MOUNTED_ON_ANCESTOR =
      "使用模板的节点 [%s] 的祖先节点上应挂载模板。";
  public static final String DESCENDANT_SHOULD_NOT_EXIST =
      "节点 %s 下不应存在后代节点";

  // ======================== 附加 SchemaFile/Page 相关消息 ========================

  public static final String ADDING_CHILDREN_UNDER_TEMPLATE_NOT_ALLOWED =
      "不允许添加或更新使用模板 [%s] 的设备的子节点。";
  public static final String CANNOT_FLUSH_NODE_NEGATIVE_ADDRESS =
      "除 DatabaseNode 外，不能刷写地址为负 [%s] 的节点。";
  public static final String SEGMENTED_PAGE_SHARE_BUFFER =
      "SegmentedPage 仅在包含一个最大尺寸 segment 时才能共享整个缓冲区切片。";
  public static final String BYTEBUFFER_CORRUPTED_FOR_SCHEMA_PAGE =
      "ByteBuffer 已损坏或位置设置错误，无法加载为 SchemaPage。";
  public static final String NODE_NO_CHILD_IN_PBTREE_WITH_NAME =
      "节点 [%s] 在 pbtree 文件中没有子节点 [%s]。";
  public static final String SINGLE_RECORD_TOO_LARGE =
      "SchemaFile 目前不支持超过半页大小的单条记录。";
  public static final String PAGE_REPLACEMENT_ERROR =
      "页面 [%d] 替换错误：引用计数或锁对象不一致。";
  public static final String NODE_NO_VALID_SEGMENT_ADDRESS =
      "节点 [%s] 在 pbtree 文件中没有有效的 segment 地址。";

  // ======================== 附加 SchemaFileLog 相关消息 ========================

  public static final String COMMIT_MARK_WITHOUT_PREPARE = "存在 COMMIT_MARK 但缺少 PREPARE_MARK";
  public static final String EXTRANEOUS_BYTE_AFTER_PREPARE =
      "PREPARE_MARK 之后出现了非 COMMIT_MARK 的多余字节";
  public static final String NOT_ENDED_BY_MARK =
      "未以 COMMIT_MARK 或 PREPARE_MARK 结尾。";

  // ======================== 附加 MNodeContainer 相关消息 ========================

  public static final String DUPLICATE_NODE_IN_BUFFERS =
      "不应在 newChildBuffer 和 updateChildBuffer 中分别存在同名的两个节点";

  // ======================== 附加 Logfile 相关消息 ========================

  public static final String FAILED_TO_CREATE_FILE_ALREADY_EXISTS =
      "创建文件 %s 失败，因为同名文件已存在";

  // ======================== 附加 View 相关消息 ========================

  public static final String VISIT_EXPRESSION_NOT_SUPPORTED =
      "TransformToExpressionVisitor 中不支持 visitExpression。";

  // ======================== 附加 Tag 相关消息 ========================

  public static final String BYTEBUFFER_SMALLER_THAN_TAG_SIZE =
      "ByteBuffer 容量小于 tagAttributeTotalSize，不允许此操作。";
  public static final String TIMESERIES_ALREADY_HAS_TAG_ATTRIBUTE_NAMED =
      "时间序列 [%s] 已有名为 [%s] 的 tag/attribute。";

  // ======================== 附加 Template 相关消息 ========================

  public static final String FAILED_TO_CREATE_TEMPLATE =
      "在 ConfigNode 中执行创建设备模板 {} 失败，状态为 {}。";
  public static final String CREATE_TEMPLATE_ERROR_PREFIX = "创建模板出错 - ";
  public static final String CREATE_TEMPLATE_ERROR = "创建模板出错。";
  public static final String GET_ALL_TEMPLATE_ERROR = "获取所有模板出错。";
  public static final String GET_TEMPLATE_INFO_ERROR = "获取模板信息出错。";
  public static final String FAILED_TO_SET_TEMPLATE =
      "在 ConfigNode 中执行设置设备模板 {} 到路径 {} 失败，状态为 {}。";

  // ======================== 附加 InformationSchema 相关消息 ========================

  public static final String INFORMATION_SCHEMA_READ_ONLY =
      "数据库 'information_schema' 仅支持查询操作";

  // ======================== 附加 GRASS/Updater 相关消息 ========================

  public static final String FAILED_TO_WRITE_ATTR_COMMIT =
      "向 region {} 写入属性提交消息失败。";
  public static final String FAILED_TO_FETCH_DATANODE_LOCATIONS =
      "获取 DataNode 位置信息失败，将重试。";

  // ======================== 附加 ResourceByPathUtils 相关消息 ========================

  public static final String FAILED_TO_RESERVE_MEMORY_TVLIST =
      "为 TVList 预留内存失败：ramSize {}，timestampsSize {}，arrayMemCost {}，rowCount {}，dataTypes {}";

  // ======================== 附加 CachedMTreeStore 相关消息 ========================

  public static final String ERROR_DURING_PBTREE_CLEAR =
      "PBTree 清理过程中发生错误：{}";
  public static final String ERROR_DURING_MTREE_FLUSH_SCHEMA_REGION =
      "MTree 刷写过程中发生错误，当前 SchemaRegionId 为 {}";
  public static final String ERROR_DURING_MTREE_FLUSH_SCHEMA_REGION_BECAUSE =
      "MTree 刷写过程中发生错误，当前 SchemaRegionId 为 {}，原因：{}";

  // ======================== 附加 MemMTreeSnapshotUtil 相关消息 ========================

  public static final String DESERIALIZE_ERROR_INFO =
      "反序列化 MemMTree 过程中发生错误。";

  // ======================== 附加 MetaUtils 相关消息 ========================

  public static final String PATH_NO_LONGER_THAN_SG_LEVEL =
      "路径长度不超过默认 sg 层级：";
  public static final String PATH_DOES_NOT_START_WITH_ROOT = "路径不以 ";

  // ======================== FakeCRC32Deserializer 相关消息 ========================

  public static final String READ_LOG_LENGTH_NEGATIVE_LOG =
      "读取的日志长度 {} 为负数。";

  // ======================== SchemaLogReader 相关消息 ========================

  public static final String FILE_CORRUPTED =
      "文件 {} 已损坏，未损坏的大小为 {}。";
  public static final String LOG_FILE_END_CORRUPTED_TRUNCATE =
      "日志文件 {} 的末尾已损坏，开始截断。未损坏的大小为 {}，文件大小为 {}。";
  public static final String FAIL_TO_TRUNCATE_LOG_FILE =
      "截断日志文件到大小 {} 失败";

  // ======================== SchemaRegionPlanDeserializer 相关消息 ========================

  public static final String CANNOT_DESERIALIZE_SCHEMA_REGION_PLAN =
      "无法从缓冲区反序列化 SchemaRegionPlan";

  // ======================== MTreeBelowSGMemoryImpl 相关消息 ========================

  public static final String TIMESERIES_NUM_UPPER_LIMIT =
      "时间序列数量已达到上限";
  public static final String ALIAS_DUPLICATED_DETAIL =
      "，完整路径：";
  public static final String ALIAS_DUPLICATED_OTHER_MEASUREMENT =
      "，其他测点：";
  public static final String START_CREATE_TABLE_DEVICE =
      "开始创建表设备 {}.{}";
  public static final String TABLE_DEVICE_ALREADY_EXISTS =
      "表设备 {}.{} 已存在";
  public static final String TABLE_DEVICE_CREATED =
      "表设备 {}.{} 已创建";

  // ======================== CachedMTreeStore / Scheduler 相关消息 ========================

  public static final String MTREE_FLUSH_COST =
      "MTree 刷写耗时 {}ms，SchemaRegion 为 {}";

  // ======================== DataNodeTableCache 相关消息 ========================

  public static final String INIT_TABLE_CACHE_SUCCESS =
      "DataNodeTableCache 初始化成功";
  public static final String PRE_UPDATE_TABLE_SUCCESS =
      "预更新表 {}.{} 成功";
  public static final String PRE_RENAME_OLD_TABLE_SUCCESS =
      "预重命名旧表 {}.{} 成功";
  public static final String ROLLBACK_UPDATE_TABLE_SUCCESS =
      "回滚更新表 {}.{} 成功";
  public static final String ROLLBACK_RENAME_OLD_TABLE_SUCCESS =
      "回滚重命名旧表 {}.{} 成功。";
  public static final String COMMIT_UPDATE_TABLE_SUCCESS_WITH_DETAIL =
      "提交更新表 {}.{} 成功，{}";
  public static final String COMMIT_UPDATE_TABLE_SUCCESS =
      "提交更新表 {}.{} 成功。";
  public static final String RENAME_OLD_TABLE_SUCCESS =
      "重命名旧表 {}.{} 成功。";
  public static final String INTERRUPTED_ACQUIRE_SEMAPHORE_GET_TABLES =
      "尝试获取信号量以从 ConfigNode 获取表时被中断，已忽略。";
  public static final String UPDATE_TABLE_BY_FETCH_WITH_DETAIL =
      "通过表拉取更新表 {}.{}，{}";
  public static final String UPDATE_TABLE_BY_FETCH =
      "通过表拉取更新表 {}.{}。";
  public static final String COMPARE_TABLE_ADDED = "新增表：";
  public static final String COMPARE_TABLE_REMOVED = "已移除表：";
  public static final String COMPARE_TABLE_NAME = "表名：";
  public static final String COMPARE_TABLE_REMOVED_PROPS = " 已移除属性：";
  public static final String COMPARE_TABLE_ADDED_PROPS = " 新增属性：";
  public static final String COMPARE_TABLE_REMOVED_COLUMNS = " 已移除列：";
  public static final String COMPARE_TABLE_ADDED_COLUMNS = " 新增列：";
  public static final String COMPARE_TABLE_NOT_MODIFIED = " 未修改";

  // ======================== ClusterTemplateManager 相关消息 ========================

  public static final String ILLEGAL_PATH_LOG = "非法路径 {}";

  private DataNodeSchemaMessages() {}
  // ---------------------------------------------------------------------------
  // 补充日志消息
  // ---------------------------------------------------------------------------
  public static final String SCHEMA_LOG_FLUSHING_WORKING_MEMTABLE_ADD_CURRENT_QUERY_CONTEXT_TO_IMMUTABLE_7B7CD373 =
      "Flushing/Working MemTable - 将当前查询上下文添加到 immutable TVList 的查询列表";
  public static final String SCHEMA_LOG_FLUSHING_MEMTABLE_ADD_CURRENT_QUERY_CONTEXT_TO_MUTABLE_TVLIST_BEB0D766 =
      "Flushing MemTable - 将当前查询上下文添加到 mutable TVList 的查询列表";
  public static final String SCHEMA_LOG_WORKING_MEMTABLE_ADD_CURRENT_QUERY_CONTEXT_TO_MUTABLE_TVLIST_8C937414 =
      "Working MemTable - 当其已排序或没有其他查询时，将当前查询上下文添加到 mutable TVList 的查询列表";
  public static final String SCHEMA_LOG_WORKING_MEMTABLE_CLONE_MUTABLE_TVLIST_AND_REPLACE_OLD_TVLIST_FD1EAE22 =
      "Working MemTable - 克隆 mutable TVList 并替换 working MemTable 中的旧 TVList";
  public static final String PATH_DOES_NOT_START_WITH_FMT = "%s 不是以 %s 开头";
  public static final String ALIAS_DUPLICATED_FULLPATH_OTHER_MEASUREMENT_FMT =
      "别名与其他测点的名称或别名重复，别名：%s，完整路径：%s，其他测点：%s";
  public static final String TAG_ACTION_DELETE = "删除";
  public static final String TAG_ACTION_UPSERT = "更新插入";
  public static final String TAG_ACTION_DROP = "移除";
  public static final String TAG_ACTION_SET = "设置";
  public static final String TAG_ACTION_RENAME = "重命名";
  public static final String NESTED_LOGICAL_VIEW_UNSUPPORTED_FMT =
      "视图 [%s] 的源也是一个视图！不支持嵌套视图，请检查。";
  public static final String GET_SCHEMA_AS_LOGICAL_VIEW_SCHEMA_UNSUPPORTED =
      "DeviceUsingTemplateSchemaCache 不支持函数 getSchemaAsLogicalViewSchema。";
  public static final String DEVICE_TEMPLATE_ALREADY_ACTIVATED_ON_PATH_FMT =
      "路径 %s 上的设备模板已经被激活，无需重复激活。";
  public static final String SOME_TASK_DELETING_TIMESERIES_FMT =
      "某个任务正在删除时间序列 [%s]";
  public static final String PATH_ALREADY_EXIST_FMT = "路径 [%s] 已存在";
  public static final String MNODE_TYPE_MISMATCH_FMT = "MNode [%s] 不是 %s。";
  public static final String ALIAS_FOR_PATH_ALREADY_EXIST_FMT =
      "别名 [%s] 对应路径 [%s] 已存在";
  public static final String DATABASE_NOT_SET_FOR_SERIES_PATH_FMT =
      "%s，当前 seriesPath：[%s]";
  public static final String DATABASE_ALREADY_CREATED_AS_DATABASE_FMT =
      "%s 已被创建为数据库";
  public static final String SCHEMA_QUOTA_EXCEEDED_FMT =
      "当前元数据容量已超过集群配额。请检查 ConfigNode 上的配置，或删除一些已有的 %s 以满足配额限制。";
  public static final String DATABASE_QUOTA_EXCEEDED_FMT =
      "当前数据库数量已超过集群配额。集群允许的最大数据库数量为 %d，请检查 ConfigNode 上的 "
          + "database_limit_threshold 配置，或删除一些已有数据库以满足配额限制。";
  public static final String SERIES_OVERFLOW_FMT =
      "未使用设备模板的内存时间序列过多（当前内存：%s，序列数量：%s）。为优化内存，当设备具有相同时间序列时，更推荐使用设备模板。";
  public static final String ILLEGAL_PARAMETER_FAILED_CREATE_TIMESERIES_FMT =
      "%s。为路径 %s 创建时间序列失败";
  public static final String PATH_NOT_EXIST_WRONG_MESSAGE = "路径 [%s] 不存在";
  public static final String SOURCE_PATH_NOT_EXIST_WRONG_MESSAGE =
      "源路径 [%s] 对应视图 [%s] 不存在。";
  public static final String NORMAL_TIMESERIES_NOT_EXIST_WRONG_MESSAGE =
      "时间序列 [%s] 不存在或由设备模板表示";
  public static final String TEMPLATE_TIMESERIES_NOT_EXIST_WRONG_MESSAGE =
      "时间序列 [%s] 不存在或不是由设备模板表示";
  public static final String VIEW_NOT_EXIST_WRONG_MESSAGE = "视图 [%s] 不存在";
  public static final String PATH_LIST_ELLIPSIS_SEPARATOR = " ... ";
  public static final String REGISTERED_TYPE_STRING = "注册类型";
  public static final String NULL_VALUE = "null";
  public static final String DATA_TYPE_MISMATCH_REGISTERED_TYPE_FMT =
      "%s.%s 的数据类型不一致，%s %s，插入类型 %s，时间戳 %s，值 %s";
  public static final String DATA_TYPE_AND_VALUE_MISMATCH_FMT =
      "%s.%s 的数据类型和值不一致，插入类型 %s，时间戳 %s，值 %s";
  public static final String SCHEMA_DIR_CREATION_FAILED_FMT =
      "创建数据库 schema 文件夹 %s 失败。";
  public static final String MNODE_NOT_PINNED = "MNode 尚未被固定。";
  public static final String MNODE_NOT_CACHED = "MNode 尚未被缓存或已被淘汰。";
  public static final String TEMPLATE_IS_IN_USE_ON_FMT = "模板正在 %s 上使用";
  public static final String CANNOT_CREATE_TIMESERIES_TEMPLATE_SET_FMT =
      "无法创建时间序列 [%s]，因为设备模板 [%s] 已设置在路径 [%s] 上。";
  public static final String CANNOT_SET_DEVICE_TEMPLATE_TIMESERIES_UNDER_PATH_FMT =
      "无法将设备模板 [%s] 设置到路径 [%s]，因为路径 [%s] 下已有时间序列。";
  public static final String FAILED_CREATE_DUPLICATED_TEMPLATE_FMT =
      "为路径 %s 创建重复模板失败";
  public static final String TEMPLATE_ON_PATH_DIFFERENT_FROM_FMT =
      "%s 上的模板与 %s 不同";
  public static final String UNDEFINED_TEMPLATE_NAME_FMT = "未定义的模板名：%s";
  public static final String COLOSSAL_RECORD_FMT =
      "key [%s] 的记录过大，SchemaFile 无法存储，内容大小：%d";
  public static final String KEY_TOO_LARGE_FOR_INTERNAL_PAGE_FMT =
      "Key [%s] 过大，无法作为索引项存储在 InternalPage 中。";
  public static final String KEY_ALIAS_PAIR_TOO_LARGE_FMT =
      "Key-Alias 对 (%s, %s) 过大，SchemaFile 无法存储。";
  public static final String SEGMENT_OVERFLOW_FMT = "Segment 溢出：%d";
  public static final String SEGMENT_NOT_ENOUGH_SPACE = "Segment 空间不足";
  public static final String SEGMENT_NOT_ENOUGH_SPACE_AFTER_SPLIT_FMT =
      "Segment 即使 split 和 compact 后仍没有足够空间插入：%s";
  public static final String SEGMENT_NOT_FOUND_FMT =
      "Segment(index:%d) 在 page(index:%d) 中未找到。";
  public static final String SEGMENT_IS_NOT_LAST_FMT =
      "Segment(index:%d) 不是 page 中的最后一个 segment";
  public static final String NO_SPLITTABLE_SEGMENT_FOUND_FMT =
      "在 page [%s] 中未找到可 split 的 segment";
  public static final String PBTREE_FILE_LOG_CORRUPTED_FMT =
      "PBTreeFileLog [%s] 因 [%s] 损坏。";
  public static final String RECORD_DUPLICATED_FMT = "Segment 存在重复 record key：%s";
  public static final String PBTREE_FILE_NOT_EXISTS_FMT = "PBTree 文件 [%s] 不存在。";
  public static final String SCHEMA_PAGE_OVERFLOW_FMT =
      "pbtree 文件中的 Page [%s] 空间耗尽或包含过多 segment。";
  public static final String SOURCE_PATH_DELETED_FMT = "源路径 [%s] 已被删除";
  public static final String BROKEN_VIEW_UNMATCHED_FMT =
      "视图已损坏！源路径 [%s] 映射到不匹配的 %s 个路径：%s。";
  public static final String INSERT_NON_WRITABLE_VIEW_FMT =
      "无法向不是别名序列的视图插入数据。（视图路径：%s）";
  public static final String INSERT_NON_WRITABLE_VIEW =
      "无法向不是别名序列的视图插入数据。";
  public static final String DATABASE_NOT_SET = "未设置数据库";
  public static final String DUPLICATE_INSERTION_WRONG_MESSAGE =
      "插入非法，因为设备 [%s] 下的测点 [%s] 重复。";
  public static final String VIEW_IS_UNSUPPORTED_FMT = "视图不受支持，原因：%s";
  public static final String VIEW_CONTAINS_AGGREGATION_FUNCTION_FMT =
      "该视图包含名为 [%s] 的聚合函数";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext 不能为空";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator 不能为空";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_COMMA_50AD1C01 = ", ";
  public static final String MESSAGE_SKIP_ROLLBACK_RENAMING_OLD_TABLE_ARG_ARG_BECAUSE_IT_HAS_BEEN_HANDLED_664F2456 = "跳过回滚重命名旧表 {}.{}，因为该操作已被处理。";
  public static final String MESSAGE_SKIP_COMMIT_UPDATE_TABLE_ARG_ARG_BECAUSE_IT_HAS_BEEN_HANDLED_31362A1C = "跳过提交更新表 {}.{}，因为该操作已被处理。";

}
