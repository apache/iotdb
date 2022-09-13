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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.rpc.thrift.TPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.constant.DataNodeEndPoints;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class simulates the behaviour of configNode to manage the configs locally. The schema
 * configs include storage group, schema region and template. The data config is dataRegion.
 */
public class LocalConfigNode {

  private static final Logger logger = LoggerFactory.getLogger(LocalConfigNode.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final long STANDALONE_MOCK_TIME_SLOT_START_TIME = 0L;
  private volatile boolean initialized = false;

  private ScheduledExecutorService timedForceMLogThread;

  private final IStorageGroupSchemaManager storageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();
  private final TemplateManager templateManager = TemplateManager.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final LocalSchemaPartitionTable schemaPartitionTable =
      LocalSchemaPartitionTable.getInstance();

  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  private final LocalDataPartitionInfo dataPartitionInfo = LocalDataPartitionInfo.getInstance();

  private final SeriesPartitionExecutor executor =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());

  private final SyncService syncService = SyncService.getInstance();

  private IAuthorizer iAuthorizer;

  private LocalConfigNode() {
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.error("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error(e.getMessage());
    }
  }

  // region LocalSchemaConfigManager SingleTone
  private static class LocalSchemaConfigManagerHolder {
    private static final LocalConfigNode INSTANCE = new LocalConfigNode();

    private LocalSchemaConfigManagerHolder() {}
  }

  public static LocalConfigNode getInstance() {
    return LocalSchemaConfigManagerHolder.INSTANCE;
  }

  // endregion

  // region Interfaces for LocalSchemaConfigManager init, force and clear
  public synchronized void init() {
    if (initialized) {
      return;
    }

    try {

      templateManager.init();
      storageGroupSchemaManager.init();

      Map<PartialPath, List<SchemaRegionId>> recoveredLocalSchemaRegionInfo =
          schemaEngine.initForLocalConfigNode();
      schemaPartitionTable.init(recoveredLocalSchemaRegionInfo);

      if (config.getSyncMlogPeriodInMs() != 0) {
        timedForceMLogThread =
            IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
                "LocalConfigNode-TimedForceMLog-Thread");
        ScheduledExecutorUtil.unsafelyScheduleAtFixedRate(
            timedForceMLogThread,
            this::forceMlog,
            config.getSyncMlogPeriodInMs(),
            config.getSyncMlogPeriodInMs(),
            TimeUnit.MILLISECONDS);
      }

      // TODO: the judgment should be removed after old standalone removed
      if (config.isMppMode() && !config.isClusterMode()) {
        Map<String, List<DataRegionId>> recoveredLocalDataRegionInfo =
            storageEngine.getLocalDataRegionInfo();
        dataPartitionInfo.init(recoveredLocalDataRegionInfo);
      }
    } catch (MetadataException | IOException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }

    initialized = true;
  }

  public synchronized void clear() {
    if (!initialized) {
      return;
    }

    try {

      if (timedForceMLogThread != null) {
        timedForceMLogThread.shutdown();
        timedForceMLogThread = null;
      }

      schemaPartitionTable.clear();
      schemaEngine.clear();
      storageGroupSchemaManager.clear();
      templateManager.clear();

      dataPartitionInfo.clear();

    } catch (IOException e) {
      logger.error("Error occurred when clearing LocalConfigNode:", e);
    }

    initialized = false;
  }

  public synchronized void forceMlog() {
    if (!initialized) {
      return;
    }

    storageGroupSchemaManager.forceLog();
    templateManager.forceLog();
  }

  // endregion

  // region Interfaces for storage group management

  // region Interfaces for storage group write operation

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    storageGroupSchemaManager.setStorageGroup(storageGroup);
    for (SchemaRegionId schemaRegionId : schemaPartitionTable.setStorageGroup(storageGroup)) {
      schemaEngine.createSchemaRegion(storageGroup, schemaRegionId);
    }

    if (!config.isEnableMemControl()) {
      MemTableManager.getInstance().addOrDeleteStorageGroup(1);
    }
  }

  public void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {

    if (config.isMppMode() && !config.isClusterMode()) {
      deleteDataRegionsInStorageGroup(
          dataPartitionInfo.getDataRegionIdsByStorageGroup(storageGroup));
      dataPartitionInfo.deleteStorageGroup(storageGroup);
    }

    deleteSchemaRegionsInStorageGroup(
        storageGroup, schemaPartitionTable.getSchemaRegionIdsByStorageGroup(storageGroup));

    for (Template template : templateManager.getTemplateMap().values()) {
      templateManager.unmarkStorageGroup(template, storageGroup.getFullPath());
    }

    if (!config.isEnableMemControl()) {
      MemTableManager.getInstance().addOrDeleteStorageGroup(-1);
    }

    schemaPartitionTable.deleteStorageGroup(storageGroup);

    // delete storage group after all related resources have been cleared
    storageGroupSchemaManager.deleteStorageGroup(storageGroup);
  }

  private void deleteSchemaRegionsInStorageGroup(
      PartialPath storageGroup, List<SchemaRegionId> schemaRegionIdSet) throws MetadataException {
    for (SchemaRegionId schemaRegionId : schemaRegionIdSet) {
      schemaEngine.deleteSchemaRegion(schemaRegionId);
    }

    File sgDir = new File(config.getSchemaDir() + File.separator + storageGroup.getFullPath());
    if (sgDir.delete()) {
      logger.info("delete storage group folder {}", sgDir.getAbsolutePath());
    } else {
      if (sgDir.exists()) {
        logger.info("delete storage group folder {} failed.", sgDir.getAbsolutePath());
        throw new MetadataException(
            String.format("Failed to delete storage group folder %s", sgDir.getAbsolutePath()));
      }
    }
  }

  private void deleteDataRegionsInStorageGroup(List<DataRegionId> dataRegionIdSet) {
    for (DataRegionId dataRegionId : dataRegionIdSet) {
      storageEngine.deleteDataRegion(dataRegionId);
    }
  }

  /**
   * Delete storage groups of given paths from MTree.
   *
   * @param storageGroups list of paths to be deleted.
   */
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    for (PartialPath storageGroup : storageGroups) {
      deleteStorageGroup(storageGroup);
    }
  }

  private PartialPath ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      return getBelongedStorageGroup(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      try {
        setStorageGroup(storageGroupPath);
        return storageGroupPath;
      } catch (StorageGroupAlreadySetException storageGroupAlreadySetException) {
        if (storageGroupAlreadySetException.isHasChild()) {
          // if setStorageGroup failure is because of child, the deviceNode should not be created.
          // Timeseries can't be created under a deviceNode without storageGroup.
          throw storageGroupAlreadySetException;
        }

        // concurrent timeseries creation may result concurrent ensureStorageGroup
        // it's ok that the storageGroup has already been set
        return getBelongedStorageGroup(path);
      }
    }
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    if (config.isMppMode() && !config.isClusterMode()) {
      storageEngine.setTTL(dataPartitionInfo.getDataRegionIdsByStorageGroup(storageGroup), dataTTL);
    }
    storageGroupSchemaManager.setTTL(storageGroup, dataTTL);
  }

  // endregion

  // region Interfaces for storage group info query

  /**
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   */
  public boolean isStorageGroup(PartialPath path) {
    return storageGroupSchemaManager.isStorageGroup(path);
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    return storageGroupSchemaManager.checkStorageGroupByPath(path);
  }

  /**
   * Check whether the storage group of given path is set. The path may be a prefix path of some
   * storage group. Besides, the given path may be also beyond the MTreeAboveSG scope, then return
   * true if the covered part exists, which means there's storage group on this path. The rest part
   * will be checked by certain storage group subTree.
   *
   * @param path a full path or a prefix path
   */
  public boolean isStorageGroupAlreadySet(PartialPath path) {
    return storageGroupSchemaManager.isStorageGroupAlreadySet(path);
  }

  /**
   * To calculate the count of storage group for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupSchemaManager.getStorageGroupNum(pathPattern, isPrefixMatch);
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return storageGroupSchemaManager.getBelongedStorageGroup(path);
  }

  /**
   * Get the storage group that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all storage groups related to given path pattern
   */
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return storageGroupSchemaManager.getBelongedStorageGroups(pathPattern);
  }

  /**
   * Get all storage group matching given path pattern. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupSchemaManager.getMatchedStorageGroups(pathPattern, isPrefixMatch);
  }

  /** Get all storage group paths */
  public List<PartialPath> getAllStorageGroupPaths() {
    return storageGroupSchemaManager.getAllStorageGroupPaths();
  }

  /**
   * For a path, infer all storage groups it may belong to. The path can have wildcards. Resolve the
   * path or path pattern into StorageGroupName-FullPath pairs that FullPath matches the given path.
   *
   * <p>Consider the path into two parts: (1) the sub path which can not contain a storage group
   * name and (2) the sub path which is substring that begin after the storage group name.
   *
   * <p>(1) Suppose the part of the path can not contain a storage group name (e.g.,
   * "root".contains("root.sg") == false), then: For each one level wildcard *, only one level will
   * be inferred and the wildcard will be removed. For each multi level wildcard **, then the
   * inference will go on until the storage groups are found and the wildcard will be kept. (2)
   * Suppose the part of the path is a substring that begin after the storage group name. (e.g., For
   * "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part is "a.*.b.*"). For
   * this part, keep what it is.
   *
   * <p>Assuming we have three SGs: root.group1, root.group2, root.area1.group3 Eg1: for input
   * "root.**", returns ("root.group1", "root.group1.**"), ("root.group2", "root.group2.**")
   * ("root.area1.group3", "root.area1.group3.**") Eg2: for input "root.*.s1", returns
   * ("root.group1", "root.group1.s1"), ("root.group2", "root.group2.s1")
   *
   * <p>Eg3: for input "root.area1.**", returns ("root.area1.group3", "root.area1.group3.**")
   *
   * @param path can be a path pattern or a full path.
   * @return StorageGroupName-FullPath pairs
   * @apiNote :for cluster
   */
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    Map<String, List<PartialPath>> sgPathMap =
        storageGroupSchemaManager.groupPathByStorageGroup(path);
    if (logger.isDebugEnabled()) {
      logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    }
    return sgPathMap;
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    Map<PartialPath, Long> storageGroupsTTL = new HashMap<>();
    for (IStorageGroupMNode storageGroupMNode : getAllStorageGroupNodes()) {
      storageGroupsTTL.put(storageGroupMNode.getPartialPath(), storageGroupMNode.getDataTTL());
    }
    return storageGroupsTTL;
  }

  /**
   * To collect nodes in the given level for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All nodes start with the matched prefix path will be
   * collected. This method only count in nodes above storage group. Nodes below storage group,
   * including storage group node will be collected by certain SchemaRegion. The involved storage
   * groups will be collected to fetch schemaRegion.
   *
   * @param pathPattern a path pattern or a full path
   * @param nodeLevel the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException {
    return storageGroupSchemaManager.getNodesListInGivenLevel(
        pathPattern, nodeLevel, isPrefixMatch, filter);
  }

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain Storage Group.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [root.a, root.b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath pathPattern) throws MetadataException {
    return storageGroupSchemaManager.getChildNodePathInNextLevel(pathPattern);
  }

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain Storage Group.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [a, b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    return storageGroupSchemaManager.getChildNodeNameInNextLevel(pathPattern);
  }

  // endregion

  // region Interfaces for StorageGroupMNode Query

  /** Get storage group node by path. the give path don't need to be storage group path. */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    // used for storage engine auto create storage group
    ensureStorageGroup(path);
    return storageGroupSchemaManager.getStorageGroupNodeByPath(path);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return storageGroupSchemaManager.getAllStorageGroupNodes();
  }

  // endregion

  // endregion

  // region Interfaces for SchemaRegionId Management

  /**
   * Get the target SchemaRegionIds, which the given path belongs to. The path must be a fullPath
   * without wildcards, * or **. This method is the first step when there's a task on one certain
   * path, e.g., root.sg1 is a storage group and path = root.sg1.d1, return SchemaRegionId of
   * root.sg1. If there's no storage group on the given path, StorageGroupNotSetException will be
   * thrown.
   */
  public SchemaRegionId getBelongedSchemaRegionId(PartialPath path) throws MetadataException {
    PartialPath storageGroup = storageGroupSchemaManager.getBelongedStorageGroup(path);
    SchemaRegionId schemaRegionId = schemaPartitionTable.getSchemaRegionId(storageGroup, path);
    // Since the creation of storageGroup, schemaRegionId and schemaRegion is not atomic or locked,
    // any access concurrent with this creation may get null.
    // Thread A: create sg, allocate schemaRegionId, create schemaRegion
    // Thread B: access sg, access partitionTable to get schemaRegionId, access schemaEngine to get
    // schemaRegion
    // When A and B are running concurrently, B may get null while getting schemaRegionId or
    // schemaRegion. This means B must run after A ends.
    // To avoid this exception, please invoke getBelongedSchemaRegionIdWithAutoCreate according to
    // the scenario.
    if (schemaRegionId == null) {
      throw new MetadataException(
          String.format(
              "Storage group %s has not been prepared well. Schema region for %s has not been allocated or is not initialized.",
              storageGroup, path));
    }
    ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
    if (schemaRegion == null) {
      throw new MetadataException(
          String.format(
              "Storage group [%s] has not been prepared well. Schema region [%s] is not initialized.",
              storageGroup, schemaRegionId));
    }
    return schemaRegionId;
  }

  // This interface involves storage group and schema region auto creation
  public SchemaRegionId getBelongedSchemaRegionIdWithAutoCreate(PartialPath path)
      throws MetadataException {
    PartialPath storageGroup = ensureStorageGroup(path);
    SchemaRegionId schemaRegionId = schemaPartitionTable.getSchemaRegionId(storageGroup, path);
    if (schemaRegionId == null) {
      schemaPartitionTable.setStorageGroup(storageGroup);
      schemaRegionId = schemaPartitionTable.getSchemaRegionId(storageGroup, path);
    }
    ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
    if (schemaRegion == null) {
      schemaEngine.createSchemaRegion(storageGroup, schemaRegionId);
    }
    return schemaRegionId;
  }

  /**
   * Get the target SchemaRegionIds, which will be involved/covered by the given pathPattern. The
   * path may contain wildcards, * or **. This method is the first step when there's a task on
   * multiple paths represented by the given pathPattern. If isPrefixMatch, all storage groups under
   * the prefixPath that matches the given pathPattern will be collected.
   */
  public List<SchemaRegionId> getInvolvedSchemaRegionIds(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<SchemaRegionId> result = new ArrayList<>();
    for (PartialPath storageGroup :
        storageGroupSchemaManager.getInvolvedStorageGroups(pathPattern, isPrefixMatch)) {
      result.addAll(
          schemaPartitionTable.getInvolvedSchemaRegionIds(
              storageGroup, pathPattern, isPrefixMatch));
    }
    return result;
  }

  public List<SchemaRegionId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup) {
    return schemaPartitionTable.getSchemaRegionIdsByStorageGroup(storageGroup);
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    templateManager.createSchemaTemplate(plan);
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    if (templateManager.getTemplate(plan.getName()) == null) {
      throw new MetadataException(String.format("Template [%s] does not exist.", plan.getName()));
    }

    boolean isTemplateAppendable = true;

    Template template = templateManager.getTemplate(plan.getName());

    for (SchemaRegionId schemaRegionId : template.getRelatedSchemaRegion()) {
      if (!schemaEngine
          .getSchemaRegion(schemaRegionId)
          .isTemplateAppendable(template, plan.getMeasurements())) {
        isTemplateAppendable = false;
        break;
      }
    }

    if (!isTemplateAppendable) {
      throw new MetadataException(
          String.format(
              "Template [%s] cannot be appended for overlapping of new measurement and MTree",
              plan.getName()));
    }

    templateManager.appendSchemaTemplate(plan);
  }

  public void pruneSchemaTemplate(PruneTemplatePlan plan) throws MetadataException {
    if (templateManager.getTemplate(plan.getName()) == null) {
      throw new MetadataException(String.format("Template [%s] does not exist.", plan.getName()));
    }

    if (templateManager.getTemplate(plan.getName()).getRelatedSchemaRegion().size() > 0) {
      throw new MetadataException(
          String.format(
              "Template [%s] cannot be pruned since had been set before.", plan.getName()));
    }

    templateManager.pruneSchemaTemplate(plan);
  }

  public int countMeasurementsInTemplate(String templateName) throws MetadataException {
    try {
      return templateManager.getTemplate(templateName).getMeasurementsCount();
    } catch (UndefinedTemplateException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * @param templateName name of template to check
   * @param path full path to check
   * @return if path correspond to a measurement in template
   * @throws MetadataException
   */
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws MetadataException {
    return templateManager.getTemplate(templateName).isPathMeasurement(path);
  }

  public boolean isPathExistsInTemplate(String templateName, String path) throws MetadataException {
    return templateManager.getTemplate(templateName).isPathExistInTemplate(path);
  }

  public List<String> getMeasurementsInTemplate(String templateName, String path)
      throws MetadataException {
    return templateManager.getTemplate(templateName).getMeasurementsUnderPath(path);
  }

  public List<Pair<String, IMeasurementSchema>> getSchemasInTemplate(
      String templateName, String path) throws MetadataException {
    Set<Map.Entry<String, IMeasurementSchema>> rawSchemas =
        templateManager.getTemplate(templateName).getSchemaMap().entrySet();
    return rawSchemas.stream()
        .filter(e -> e.getKey().startsWith(path))
        .collect(
            ArrayList::new,
            (res, elem) -> res.add(new Pair<>(elem.getKey(), elem.getValue())),
            ArrayList::addAll);
  }

  public Set<String> getAllTemplates() {
    return templateManager.getAllTemplateName();
  }

  /**
   * Get all paths set designated template
   *
   * @param templateName designated template name, blank string for any template exists
   * @return paths set
   */
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    Set<String> result = new HashSet<>();
    if (templateName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      for (ISchemaRegion schemaRegion : schemaEngine.getAllSchemaRegions()) {
        result.addAll(schemaRegion.getPathsSetTemplate(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      }
    } else {
      for (SchemaRegionId schemaRegionId :
          templateManager.getTemplate(templateName).getRelatedSchemaRegion()) {
        result.addAll(
            schemaEngine.getSchemaRegion(schemaRegionId).getPathsSetTemplate(templateName));
      }
    }

    return result;
  }

  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    Set<String> result = new HashSet<>();
    if (templateName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      for (ISchemaRegion schemaRegion : schemaEngine.getAllSchemaRegions()) {
        result.addAll(schemaRegion.getPathsUsingTemplate(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      }
    } else {
      for (SchemaRegionId schemaRegionId :
          templateManager.getTemplate(templateName).getRelatedSchemaRegion()) {
        result.addAll(
            schemaEngine.getSchemaRegion(schemaRegionId).getPathsUsingTemplate(templateName));
      }
    }

    return result;
  }

  public void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException {
    String templateName = plan.getName();
    // check whether template exists
    if (!templateManager.getAllTemplateName().contains(templateName)) {
      throw new UndefinedTemplateException(templateName);
    }

    if (templateManager.getTemplate(plan.getName()).getRelatedSchemaRegion().size() > 0) {
      throw new MetadataException(
          String.format(
              "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
    }

    templateManager.dropSchemaTemplate(plan);
  }

  public synchronized void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    PartialPath path = new PartialPath(plan.getPrefixPath());
    try {
      schemaEngine
          .getSchemaRegion(getBelongedSchemaRegionIdWithAutoCreate(path))
          .setSchemaTemplate(plan);
    } catch (StorageGroupAlreadySetException e) {
      throw new MetadataException("Template should not be set above storageGroup");
    }
  }

  public synchronized void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    PartialPath path = new PartialPath(plan.getPrefixPath());
    try {
      schemaEngine.getSchemaRegion(getBelongedSchemaRegionId(path)).unsetSchemaTemplate(plan);
    } catch (StorageGroupNotSetException e) {
      throw new PathNotExistException(plan.getPrefixPath());
    }
  }

  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    PartialPath path = plan.getPrefixPath();
    try {
      schemaEngine
          .getSchemaRegion(getBelongedSchemaRegionIdWithAutoCreate(path))
          .setUsingSchemaTemplate(plan);
    } catch (StorageGroupNotSetException e) {
      throw new MetadataException(
          String.format(
              "Path [%s] has not been set any template.", plan.getPrefixPath().toString()));
    }
  }

  // endregion

  // region Interfaces for DataRegionId Management

  /**
   * Get the target DataRegionIds, which the given path belongs to. The path must be a fullPath
   * without wildcards, * or **. This method is the first step when there's a task on one certain
   * path, e.g., root.sg1 is a storage group and path = root.sg1.d1, return DataRegionId of
   * root.sg1. If there's no storage group on the given path, StorageGroupNotSetException will be
   * thrown.
   */
  public DataRegionId getBelongedDataRegionId(PartialPath path)
      throws MetadataException, DataRegionException {
    PartialPath storageGroup = storageGroupSchemaManager.getBelongedStorageGroup(path);
    DataRegionId dataRegionId = dataPartitionInfo.getDataRegionId(storageGroup, path);
    if (dataRegionId == null) {
      return null;
    }
    DataRegion dataRegion = storageEngine.getDataRegion(dataRegionId);
    if (dataRegion == null) {
      throw new DataRegionException(
          String.format(
              "Storage group %s has not been prepared well. Data region for %s is not initialized.",
              storageGroup, path));
    }
    return dataRegionId;
  }

  // This interface involves storage group and data region auto creation
  public DataRegionId getBelongedDataRegionIdWithAutoCreate(PartialPath devicePath)
      throws MetadataException, DataRegionException {
    PartialPath storageGroup = storageGroupSchemaManager.getBelongedStorageGroup(devicePath);
    DataRegionId dataRegionId = dataPartitionInfo.getDataRegionId(storageGroup, devicePath);
    if (dataRegionId == null) {
      dataPartitionInfo.registerStorageGroup(storageGroup);
      dataRegionId = dataPartitionInfo.allocateDataRegionForNewSlot(storageGroup, devicePath);
    }
    DataRegion dataRegion = storageEngine.getDataRegion(dataRegionId);
    if (dataRegion == null) {
      storageEngine.createDataRegion(dataRegionId, storageGroup.getFullPath(), Long.MAX_VALUE);
    }
    return dataRegionId;
  }

  public Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> getSchemaPartition(
      PathPatternTree patternTree) {

    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> partitionSlotsMap = new HashMap<>();
    patternTree.constructTree();
    List<PartialPath> partialPathList = patternTree.getAllPathPatterns();
    try {
      for (PartialPath path : partialPathList) {
        List<PartialPath> storageGroups = getBelongedStorageGroups(path);
        for (PartialPath storageGroupPath : storageGroups) {
          String storageGroup = storageGroupPath.getFullPath();
          SchemaRegionId schemaRegionId = getBelongedSchemaRegionId(storageGroupPath);
          ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
          Set<PartialPath> devices = schemaRegion.getMatchedDevices(storageGroupPath, true);
          for (PartialPath device : devices) {
            partitionSlotsMap
                .computeIfAbsent(storageGroup, key -> new HashMap<>())
                .put(
                    executor.getSeriesPartitionSlot(device.getFullPath()),
                    genStandaloneRegionReplicaSet(
                        TConsensusGroupType.SchemaRegion, schemaRegionId.getId()));
          }
        }
      }
      return partitionSlotsMap;
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> getOrCreateSchemaPartition(
      PathPatternTree patternTree) {
    List<String> devicePaths = patternTree.getAllDevicePatterns();
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> partitionSlotsMap = new HashMap<>();

    try {
      for (String devicePath : devicePaths) {
        // Only check devicePaths that without "*"
        if (!devicePath.contains("*")) {
          PartialPath device = new PartialPath(devicePath);
          PartialPath storageGroup = ensureStorageGroup(device);
          SchemaRegionId schemaRegionId = getBelongedSchemaRegionIdWithAutoCreate(device);
          partitionSlotsMap
              .computeIfAbsent(storageGroup.getFullPath(), key -> new HashMap<>())
              .put(
                  executor.getSeriesPartitionSlot(devicePath),
                  genStandaloneRegionReplicaSet(
                      TConsensusGroupType.SchemaRegion, schemaRegionId.getId()));
        }
      }
    } catch (MetadataException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateSchemaPartition():" + e.getMessage());
    }
    return partitionSlotsMap;
  }

  // region Interfaces for StandalonePartitionFetcher
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap)
      throws MetadataException, DataRegionException {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    for (Map.Entry<String, List<DataPartitionQueryParam>> sgEntry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      List<DataPartitionQueryParam> dataPartitionQueryParams = sgEntry.getValue();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          deviceToRegionsMap = new HashMap<>();
      for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
        String deviceId = dataPartitionQueryParam.getDevicePath();
        DataRegionId dataRegionId = getBelongedDataRegionId(new PartialPath(deviceId));
        // dataRegionId is null means the DataRegion is not created,
        // use an empty dataPartitionMap to init DataPartition
        if (dataRegionId != null) {
          Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionToRegionsMap =
              deviceToRegionsMap.getOrDefault(
                  executor.getSeriesPartitionSlot(deviceId), new HashMap<>());
          timePartitionToRegionsMap.put(
              new TTimePartitionSlot(STANDALONE_MOCK_TIME_SLOT_START_TIME),
              Collections.singletonList(
                  genStandaloneRegionReplicaSet(
                      TConsensusGroupType.DataRegion, dataRegionId.getId())));
          deviceToRegionsMap.put(
              executor.getSeriesPartitionSlot(deviceId), timePartitionToRegionsMap);
        }
      }
      if (!deviceToRegionsMap.isEmpty()) {
        dataPartitionMap.put(storageGroupName, deviceToRegionsMap);
      }
    }
    return new DataPartition(
        dataPartitionMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  private TRegionReplicaSet genStandaloneRegionReplicaSet(TConsensusGroupType type, int id) {
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(type, id));
    regionReplicaSet.setDataNodeLocations(
        Collections.singletonList(
            new TDataNodeLocation(
                IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
                new TEndPoint(),
                DataNodeEndPoints.LOCAL_HOST_INTERNAL_ENDPOINT,
                DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
                new TEndPoint(),
                new TEndPoint())));
    return regionReplicaSet;
  }

  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap)
      throws MetadataException, DataRegionException {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    for (Map.Entry<String, List<DataPartitionQueryParam>> sgEntry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      List<DataPartitionQueryParam> dataPartitionQueryParams = sgEntry.getValue();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          deviceToRegionsMap = new HashMap<>();
      for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
        // for each device
        String deviceId = dataPartitionQueryParam.getDevicePath();
        List<TTimePartitionSlot> timePartitionSlotList =
            dataPartitionQueryParam.getTimePartitionSlotList();
        for (TTimePartitionSlot timePartitionSlot : timePartitionSlotList) {
          DataRegionId dataRegionId =
              getBelongedDataRegionIdWithAutoCreate(new PartialPath(deviceId));
          Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionToRegionsMap =
              deviceToRegionsMap.getOrDefault(
                  executor.getSeriesPartitionSlot(deviceId), new HashMap<>());
          timePartitionToRegionsMap.put(
              timePartitionSlot,
              Collections.singletonList(
                  genStandaloneRegionReplicaSet(
                      TConsensusGroupType.DataRegion, dataRegionId.getId())));
          deviceToRegionsMap.put(
              executor.getSeriesPartitionSlot(deviceId), timePartitionToRegionsMap);
        }
      }
      dataPartitionMap.put(storageGroupName, deviceToRegionsMap);
    }
    return new DataPartition(
        dataPartitionMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  // endregion

  // author
  public void operatorPermission(AuthorStatement authorStatement) throws AuthException {
    AuthorOperator.AuthorType authorType =
        AuthorOperator.AuthorType.values()[authorStatement.getAuthorType().ordinal()];
    String userName = authorStatement.getUserName();
    String roleName = authorStatement.getRoleName();
    String password = authorStatement.getPassWord();
    String newPassword = authorStatement.getNewPassword();
    Set<Integer> permissions = AuthUtils.strToPermissions(authorStatement.getPrivilegeList());
    List<String> nodeNameList =
        authorStatement.getNodeNameList().stream()
            .map(PartialPath::getFullPath)
            .collect(Collectors.toList());
    switch (authorType) {
      case UPDATE_USER:
        iAuthorizer.updateUserPassword(userName, newPassword);
        break;
      case CREATE_USER:
        iAuthorizer.createUser(userName, password);
        break;
      case CREATE_ROLE:
        iAuthorizer.createRole(roleName);
        break;
      case DROP_USER:
        iAuthorizer.deleteUser(userName);
        break;
      case DROP_ROLE:
        iAuthorizer.deleteRole(roleName);
        break;
      case GRANT_ROLE:
        for (int i : permissions) {
          for (String path : nodeNameList) {
            iAuthorizer.grantPrivilegeToRole(roleName, path, i);
          }
        }
        break;
      case GRANT_USER:
        for (int i : permissions) {
          for (String path : nodeNameList) {
            iAuthorizer.grantPrivilegeToUser(userName, path, i);
          }
        }
        break;
      case GRANT_USER_ROLE:
        iAuthorizer.grantRoleToUser(roleName, userName);
        break;
      case REVOKE_USER:
        for (int i : permissions) {
          for (String path : nodeNameList) {
            iAuthorizer.revokePrivilegeFromUser(userName, path, i);
          }
        }
        break;
      case REVOKE_ROLE:
        for (int i : permissions) {
          for (String path : nodeNameList) {
            iAuthorizer.revokePrivilegeFromRole(roleName, path, i);
          }
        }
        break;
      case REVOKE_USER_ROLE:
        iAuthorizer.revokeRoleFromUser(roleName, userName);
        break;
      default:
        throw new AuthException("Unsupported operation " + authorType);
    }
  }

  public Map<String, List<String>> queryPermission(AuthorStatement authorStatement)
      throws AuthException {
    AuthorOperator.AuthorType authorType =
        AuthorOperator.AuthorType.values()[authorStatement.getAuthorType().ordinal()];
    switch (authorType) {
      case LIST_USER:
        return executeListRoleUsers(authorStatement);
      case LIST_ROLE:
        return executeListRoles(authorStatement);
      case LIST_USER_PRIVILEGE:
        return executeListUserPrivileges(authorStatement);
      case LIST_ROLE_PRIVILEGE:
        return executeListRolePrivileges(authorStatement);
      default:
        throw new AuthException("Unsupported operation " + authorType);
    }
  }

  public Map<String, List<String>> executeListRoleUsers(AuthorStatement authorStatement)
      throws AuthException {
    List<String> userList = iAuthorizer.listAllUsers();
    if (authorStatement.getRoleName() != null && !authorStatement.getRoleName().isEmpty()) {
      Role role;
      try {
        role = iAuthorizer.getRole(authorStatement.getRoleName());
        if (role == null) {
          throw new AuthException("No such role : " + authorStatement.getRoleName());
        }
      } catch (AuthException e) {
        throw new AuthException(e);
      }
      Iterator<String> itr = userList.iterator();
      while (itr.hasNext()) {
        User userObj = iAuthorizer.getUser(itr.next());
        if (userObj == null || !userObj.hasRole(authorStatement.getRoleName())) {
          itr.remove();
        }
      }
    }

    Map<String, List<String>> permissionInfo = new HashMap<>();
    permissionInfo.put(IoTDBConstant.COLUMN_USER, userList);
    return permissionInfo;
  }

  public Map<String, List<String>> executeListRoles(AuthorStatement authorStatement)
      throws AuthException {
    List<String> roleList = new ArrayList<>();
    if (authorStatement.getUserName() == null || authorStatement.getUserName().isEmpty()) {
      roleList.addAll(iAuthorizer.listAllRoles());
    } else {
      User user;
      try {
        user = iAuthorizer.getUser(authorStatement.getUserName());
        if (user == null) {
          throw new AuthException("No such user : " + authorStatement.getUserName());
        }
      } catch (AuthException e) {
        throw new AuthException(e);
      }
      for (String roleN : user.getRoleList()) {
        roleList.add(roleN);
      }
    }

    Map<String, List<String>> permissionInfo = new HashMap<>();
    permissionInfo.put(IoTDBConstant.COLUMN_ROLE, roleList);
    return permissionInfo;
  }

  public Map<String, List<String>> executeListRolePrivileges(AuthorStatement authorStatement)
      throws AuthException {
    Map<String, List<String>> permissionInfo = new HashMap<>();
    Role role;
    try {
      role = iAuthorizer.getRole(authorStatement.getRoleName());
      if (role == null) {
        throw new AuthException("No such role : " + authorStatement.getRoleName());
      }
    } catch (AuthException e) {
      throw new AuthException(e);
    }
    Set<String> rolePrivilegeSet = new HashSet<>();
    for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
      if (authorStatement.getNodeNameList().isEmpty()) {
        rolePrivilegeSet.add(pathPrivilege.toString());
        continue;
      }
      for (PartialPath path : authorStatement.getNodeNameList()) {
        if (AuthUtils.pathOrBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
          rolePrivilegeSet.add(pathPrivilege.toString());
        }
      }
    }

    permissionInfo.put(IoTDBConstant.COLUMN_PRIVILEGE, new ArrayList<>(rolePrivilegeSet));
    return permissionInfo;
  }

  public Map<String, List<String>> executeListUserPrivileges(AuthorStatement authorStatement)
      throws AuthException {
    Map<String, List<String>> permissionInfo = new HashMap<>();
    User user;
    try {
      user = iAuthorizer.getUser(authorStatement.getUserName());
      if (user == null) {
        throw new AuthException("No such user : " + authorStatement.getUserName());
      }
    } catch (AuthException e) {
      throw new AuthException(e);
    }
    List<String> userPrivilegesList = new ArrayList<>();

    if (IoTDBConstant.PATH_ROOT.equals(authorStatement.getUserName())) {
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        userPrivilegesList.add(privilegeType.toString());
      }
    } else {
      List<String> rolePrivileges = new ArrayList<>();
      Set<String> userPrivilegeSet = new HashSet<>();
      for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
        if (authorStatement.getNodeNameList().isEmpty()
            && !userPrivilegeSet.contains(pathPrivilege.toString())) {
          rolePrivileges.add("");
          userPrivilegeSet.add(pathPrivilege.toString());
          continue;
        }
        for (PartialPath path : authorStatement.getNodeNameList()) {
          if (AuthUtils.pathOrBelongsTo(path.getFullPath(), pathPrivilege.getPath())
              && !userPrivilegeSet.contains(pathPrivilege.toString())) {
            rolePrivileges.add("");
            userPrivilegeSet.add(pathPrivilege.toString());
          }
        }
      }
      userPrivilegesList.addAll(userPrivilegeSet);
      for (String roleN : user.getRoleList()) {
        Role role = iAuthorizer.getRole(roleN);
        if (roleN == null) {
          continue;
        }
        Set<String> rolePrivilegeSet = new HashSet<>();
        for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
          if (authorStatement.getNodeNameList().isEmpty()
              && !rolePrivilegeSet.contains(pathPrivilege.toString())) {
            rolePrivileges.add(roleN);
            rolePrivilegeSet.add(pathPrivilege.toString());
            continue;
          }
          for (PartialPath path : authorStatement.getNodeNameList()) {
            if (AuthUtils.pathOrBelongsTo(path.getFullPath(), pathPrivilege.getPath())
                && !rolePrivilegeSet.contains(pathPrivilege.toString())) {
              rolePrivileges.add(roleN);
              rolePrivilegeSet.add(pathPrivilege.toString());
            }
          }
        }
        userPrivilegesList.addAll(rolePrivilegeSet);
      }
      permissionInfo.put(IoTDBConstant.COLUMN_ROLE, rolePrivileges);
    }
    permissionInfo.put(IoTDBConstant.COLUMN_PRIVILEGE, userPrivilegesList);
    return permissionInfo;
  }

  public boolean login(String username, String password) throws AuthException {
    return iAuthorizer.login(username, password);
  }

  public boolean checkUserPrivileges(String username, String path, int permission)
      throws AuthException {
    return iAuthorizer.checkUserPrivileges(username, path, permission);
  }

  public TSStatus executeMergeOperation() {
    try {
      storageEngine.mergeAll();
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeFlushOperation(TFlushReq tFlushReq) {
    return storageEngine.operateFlush(tFlushReq);
  }

  public TSStatus executeClearCacheOperation() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeLoadConfigurationOperation() {
    try {
      IoTDBDescriptor.getInstance().loadHotModifiedProps();
    } catch (QueryProcessException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeSetSystemStatus(NodeStatus status) {
    try {
      CommonDescriptor.getInstance().getConfig().setNodeStatus(status);
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus createPipeSink(CreatePipeSinkStatement createPipeSinkStatement) {
    try {
      syncService.addPipeSink(createPipeSinkStatement);
    } catch (PipeSinkException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus dropPipeSink(String pipeSinkName) {
    try {
      syncService.dropPipeSink(pipeSinkName);
    } catch (PipeSinkException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public List<PipeSink> showPipeSink(String pipeSinkName) {
    boolean showAll = StringUtils.isEmpty(pipeSinkName);
    if (showAll) {
      return syncService.getAllPipeSink();
    } else {
      return Collections.singletonList(syncService.getPipeSink(pipeSinkName));
    }
  }

  public TSStatus createPipe(CreatePipeStatement createPipeStatement) {
    try {
      syncService.addPipe(createPipeStatement);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus startPipe(String pipeName) {
    try {
      syncService.startPipe(pipeName);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus stopPipe(String pipeName) {
    try {
      syncService.stopPipe(pipeName);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus dropPipe(String pipeName) {
    try {
      syncService.dropPipe(pipeName);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TShowPipeResp showPipe(String pipeName) {
    List<TPipeInfo> pipeInfos = SyncService.getInstance().showPipe(pipeName);
    return new TShowPipeResp().setPipeInfoList(pipeInfos).setStatus(StatusUtils.OK);
  }
}
