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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.SchemaResourceManager;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.sync.sender.manager.SchemaSyncManager;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

/**
 * This class simulates the behaviour of configNode to manage the configs locally. The schema
 * configs include storage group, schema region and template. The data config is dataRegion.
 */
public class LocalConfigNode {

  private static final Logger logger = LoggerFactory.getLogger(LocalConfigNode.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private volatile boolean initialized = false;

  private ScheduledExecutorService timedForceMLogThread;

  private IStorageGroupSchemaManager storageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();
  private TemplateManager templateManager = TemplateManager.getInstance();
  private SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private LocalSchemaPartitionTable schemaPartitionTable = LocalSchemaPartitionTable.getInstance();

  private StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  private LocalDataPartitionTable dataPartitionTable = LocalDataPartitionTable.getInstance();

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
      SchemaResourceManager.initSchemaResource();

      templateManager.init();
      storageGroupSchemaManager.init();

      Map<PartialPath, List<SchemaRegionId>> recoveredLocalSchemaRegionInfo = schemaEngine.init();
      schemaPartitionTable.init(recoveredLocalSchemaRegionInfo);
      dataPartitionTable.init(null);

      if (config.getSyncMlogPeriodInMs() != 0) {
        timedForceMLogThread =
            IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedForceMLogThread");

        timedForceMLogThread.scheduleAtFixedRate(
            this::forceMlog,
            config.getSyncMlogPeriodInMs(),
            config.getSyncMlogPeriodInMs(),
            TimeUnit.MILLISECONDS);
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
      SchemaResourceManager.clearSchemaResource();

      if (timedForceMLogThread != null) {
        timedForceMLogThread.shutdown();
        timedForceMLogThread = null;
      }

      schemaPartitionTable.clear();
      schemaEngine.clear();
      storageGroupSchemaManager.clear();
      templateManager.clear();

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
    schemaEngine.forceMlog();
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

    if (SchemaSyncManager.getInstance().isEnableSync()) {
      SchemaSyncManager.getInstance().syncMetadataPlan(new SetStorageGroupPlan(storageGroup));
    }

    if (!config.isEnableMemControl()) {
      MemTableManager.getInstance().addOrDeleteStorageGroup(1);
    }

    if (config.isMppMode() && !config.isClusterMode()) {
      for (DataRegionId dataRegionId : dataPartitionTable.setStorageGroup(storageGroup)) {
        try {
          storageEngine.createDataRegion(dataRegionId, storageGroup.getFullPath(), Long.MAX_VALUE);
        } catch (DataRegionException e) {
          // TODO (Fix exception type)
          throw new MetadataException(e);
        }
      }
    }
  }

  public void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {

    DeleteTimeSeriesPlan deleteTimeSeriesPlan =
        SchemaSyncManager.getInstance().isEnableSync()
            ? SchemaSyncManager.getInstance()
                .splitDeleteTimeseriesPlanByDevice(
                    storageGroup.concatNode(MULTI_LEVEL_PATH_WILDCARD))
            : null;

    deleteSchemaRegionsInStorageGroup(
        storageGroup, schemaPartitionTable.getSchemaRegionIdsByStorageGroup(storageGroup));

    for (Template template : templateManager.getTemplateMap().values()) {
      templateManager.unmarkStorageGroup(template, storageGroup.getFullPath());
    }
    if (SchemaSyncManager.getInstance().isEnableSync()) {
      SchemaSyncManager.getInstance().syncMetadataPlan(deleteTimeSeriesPlan);
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
  public Pair<Set<String>, Set<PartialPath>> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
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
    return schemaPartitionTable.getSchemaRegionId(storageGroup, path);
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

  public List<SchemaRegionId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup)
      throws MetadataException {
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
  public DataRegionId getBelongedDataRegionRegionId(PartialPath path)
      throws MetadataException, DataRegionException {
    PartialPath storageGroup = storageGroupSchemaManager.getBelongedStorageGroup(path);
    DataRegionId dataRegionId = dataPartitionTable.getDataRegionId(storageGroup, path);
    DataRegion dataRegion = storageEngine.getDataRegion(dataRegionId);
    if (dataRegion == null) {
      storageEngine.createDataRegion(dataRegionId, storageGroup.getFullPath(), Long.MAX_VALUE);
    }
    return dataPartitionTable.getDataRegionId(storageGroup, path);
  }

  public List<DataRegionId> getDataRegionIdsByStorageGroup(PartialPath storageGroup) {
    return dataPartitionTable.getDataRegionIdsByStorageGroup(storageGroup);
  }

  // endregion
}
