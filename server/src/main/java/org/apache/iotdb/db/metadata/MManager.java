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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.TimeseriesStatistics;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.SGMManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>MManager Singleton
 *   <li>Interfaces and Implementation of MManager initialization、snapshot、recover and clear
 *   <li>Interfaces and Implementation of Operating PhysicalPlans of Metadata
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces and Implementation for StorageGroup and TTL operation
 *   <li>Interfaces for get and auto create device
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for metadata count
 *         <li>Interfaces for level Node info Query
 *         <li>Interfaces for StorageGroup and TTL info Query
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces and methods for MNode query
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces only for Cluster module usage
 *   <li>Interfaces for lastCache operations
 *   <li>Interfaces and Implementation for InsertPlan process
 *   <li>Interfaces and Implementation for Template operations
 *   <li>TestOnly Interfaces
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
public class MManager {

  private static final Logger logger = LoggerFactory.getLogger(MManager.class);

  public static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean initialized;

  private ScheduledExecutorService timedForceMLogThread;

  private TimeseriesStatistics timeseriesStatistics = TimeseriesStatistics.getInstance();
  private IStorageGroupSchemaManager storageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();
  private TemplateManager templateManager = TemplateManager.getInstance();

  // region MManager Singleton
  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final MManager INSTANCE = new MManager();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
  }
  // endregion

  // region Interfaces and Implementation of MManager initialization、snapshot、recover and clear
  protected MManager() {
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.error("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }

    if (config.getSyncMlogPeriodInMs() != 0) {
      timedForceMLogThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedForceMLogThread");

      timedForceMLogThread.scheduleAtFixedRate(
          this::forceMlog,
          config.getSyncMlogPeriodInMs(),
          config.getSyncMlogPeriodInMs(),
          TimeUnit.MILLISECONDS);
    }
  }

  @SuppressWarnings("squid:S2093")
  public synchronized void init() {
    if (initialized) {
      return;
    }

    try {
      timeseriesStatistics.init();
      templateManager.init();
      storageGroupSchemaManager.init();

    } catch (IOException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }
    initialized = true;

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      startStatisticCounts();
    }
  }

  private void startStatisticCounts() {
    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            this,
            mManager -> {
              try {
                return mManager.getDevicesNum(new PartialPath("root.**"));
              } catch (MetadataException e) {
                logger.error("get deviceNum error", e);
              }
              return 0;
            },
            Tag.NAME.toString(),
            "device");

    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            this,
            mManager -> {
              try {
                return mManager.getStorageGroupNum(new PartialPath("root.**"), false);
              } catch (MetadataException e) {
                logger.error("get storageGroupNum error", e);
              }
              return 0;
            },
            Tag.NAME.toString(),
            "storageGroup");
  }

  public void forceMlog() {
    for (SGMManager sgmManager : storageGroupSchemaManager.getAllSGMManagers()) {
      sgmManager.forceMlog();
    }
  }

  /** function for clearing all metadata components */
  public synchronized void clear() {
    try {
      storageGroupSchemaManager.clear();
      templateManager.clear();
      timeseriesStatistics.clear();

      if (timedForceMLogThread != null) {
        timedForceMLogThread.shutdownNow();
        timedForceMLogThread = null;
      }

      initialized = false;
    } catch (IOException e) {
      logger.error("Error occurred when clearing MManager:", e);
    }
  }
  // endregion

  // region Interfaces and Implementation of operating PhysicalPlans of Metadata
  // This method is mainly used for Metadata Sync and  upgrade
  public void operation(PhysicalPlan plan) throws IOException, MetadataException {
    switch (plan.getOperatorType()) {
      case CREATE_TIMESERIES:
        CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
        createTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
        break;
      case CREATE_ALIGNED_TIMESERIES:
        CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
            (CreateAlignedTimeSeriesPlan) plan;
        createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        break;
      case DELETE_TIMESERIES:
        DeleteTimeSeriesPlan deleteTimeSeriesPlan = (DeleteTimeSeriesPlan) plan;
        // cause we only has one path for one DeleteTimeSeriesPlan
        deleteTimeseries(deleteTimeSeriesPlan.getPaths().get(0));
        break;
      case SET_STORAGE_GROUP:
        SetStorageGroupPlan setStorageGroupPlan = (SetStorageGroupPlan) plan;
        setStorageGroup(setStorageGroupPlan.getPath());
        break;
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupPlan deleteStorageGroupPlan = (DeleteStorageGroupPlan) plan;
        deleteStorageGroups(deleteStorageGroupPlan.getPaths());
        break;
      case TTL:
        SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
        setTTL(setTTLPlan.getStorageGroup(), setTTLPlan.getDataTTL());
        break;
      case CHANGE_ALIAS:
        ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        break;
      case CREATE_TEMPLATE:
        CreateTemplatePlan createTemplatePlan = (CreateTemplatePlan) plan;
        createSchemaTemplate(createTemplatePlan);
        break;
      case DROP_TEMPLATE:
        DropTemplatePlan dropTemplatePlan = (DropTemplatePlan) plan;
        dropSchemaTemplate(dropTemplatePlan);
        break;
      case APPEND_TEMPLATE:
        AppendTemplatePlan appendTemplatePlan = (AppendTemplatePlan) plan;
        appendSchemaTemplate(appendTemplatePlan);
        break;
      case PRUNE_TEMPLATE:
        PruneTemplatePlan pruneTemplatePlan = (PruneTemplatePlan) plan;
        pruneSchemaTemplate(pruneTemplatePlan);
        break;
      case SET_TEMPLATE:
        SetTemplatePlan setTemplatePlan = (SetTemplatePlan) plan;
        setSchemaTemplate(setTemplatePlan);
        break;
      case ACTIVATE_TEMPLATE:
        ActivateTemplatePlan activateTemplatePlan = (ActivateTemplatePlan) plan;
        setUsingSchemaTemplate(activateTemplatePlan);
        break;
      case AUTO_CREATE_DEVICE_MNODE:
        AutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan = (AutoCreateDeviceMNodePlan) plan;
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        break;
      case UNSET_TEMPLATE:
        UnsetTemplatePlan unsetTemplatePlan = (UnsetTemplatePlan) plan;
        unsetSchemaTemplate(unsetTemplatePlan);
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!timeseriesStatistics.isAllowToCreateNewSeries()) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    ensureStorageGroup(plan.getPath());
    storageGroupSchemaManager.getBelongedSGMManager(plan.getPath()).createTimeseries(plan, offset);
  }

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    try {
      createTimeseries(
          new CreateTimeSeriesPlan(path, dataType, encoding, compressor, props, null, null, null));
    } catch (PathAlreadyExistException | AliasAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore PathAlreadyExistException and AliasAlreadyExistException when Concurrent inserting"
                + " a non-exist time series {}",
            path);
      }
    }
  }

  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    if (!timeseriesStatistics.isAllowToCreateNewSeries()) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    ensureStorageGroup(plan.getPrefixPath());
    storageGroupSchemaManager
        .getBelongedSGMManager(plan.getPrefixPath())
        .createAlignedTimeSeries(plan);
  }

  private void ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      storageGroupSchemaManager.getBelongedStorageGroup(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      try {
        setStorageGroup(storageGroupPath);
      } catch (StorageGroupAlreadySetException storageGroupAlreadySetException) {
        // do nothing
        // concurrent timeseries creation may result concurrent ensureStorageGroup
        // it's ok that the storageGroup has already been set

        if (storageGroupAlreadySetException.isHasChild()) {
          // if setStorageGroup failure is because of child, the deviceNode should not be created.
          // Timeseries can't be created under a deviceNode without storageGroup.
          throw storageGroupAlreadySetException;
        }
      }
    }
  }

  /**
   * Delete all timeseries matching the given path pattern, may cross different storage group. If
   * using prefix match, the path pattern is used to match prefix path. All timeseries start with
   * the matched prefix path will be deleted.
   *
   * @param pathPattern path to be deleted
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<SGMManager> sgmManagers =
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch);
    if (sgmManagers.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }
    Set<String> failedNames = new HashSet<>();
    int deletedNum = 0;
    Pair<Integer, Set<String>> sgDeletionResult;
    for (SGMManager sgmManager : sgmManagers) {
      sgDeletionResult = sgmManager.deleteTimeseries(pathPattern, isPrefixMatch);
      deletedNum += sgDeletionResult.left;
      failedNames.addAll(sgDeletionResult.right);
    }

    if (deletedNum == 0 && failedNames.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }

    return failedNames.isEmpty() ? null : String.join(",", failedNames);
  }

  /**
   * Delete all timeseries matching the given path pattern, may cross different storage group
   *
   * @param pathPattern path to be deleted
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    return deleteTimeseries(pathPattern, false);
  }
  // endregion

  // region Interfaces and Implementation for StorageGroup and TTL operation
  // including sg set and delete, and ttl set

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    storageGroupSchemaManager.setStorageGroup(storageGroup);
    if (!config.isEnableMemControl()) {
      MemTableManager.getInstance().addOrDeleteStorageGroup(1);
    }
  }

  /**
   * Delete storage groups of given paths from MTree.
   *
   * @param storageGroups list of paths to be deleted.
   */
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    for (PartialPath storageGroup : storageGroups) {
      storageGroupSchemaManager.deleteStorageGroup(storageGroup);
      if (!config.isEnableMemControl()) {
        MemTableManager.getInstance().addOrDeleteStorageGroup(-1);
      }
    }
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    storageGroupSchemaManager.getSGMManagerByStorageGroupPath(storageGroup).setTTL(dataTTL);
  }
  // endregion

  // region Interfaces for get and auto create device
  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * @param path path
   * @param allowCreateSg The stand-alone version can create an sg at will, but the cluster version
   *     needs to make the Meta group aware of the creation of an SG, so an exception needs to be
   *     thrown here
   */
  protected IMNode getDeviceNodeWithAutoCreate(
      PartialPath path, boolean autoCreateSchema, boolean allowCreateSg, int sgLevel)
      throws IOException, MetadataException {
    try {
      return storageGroupSchemaManager
          .getBelongedSGMManager(path)
          .getDeviceNodeWithAutoCreate(path, autoCreateSchema);
    } catch (StorageGroupNotSetException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path.getFullPath());
      }
    }

    try {
      if (allowCreateSg) {
        PartialPath storageGroupPath = MetaUtils.getStorageGroupPathByLevel(path, sgLevel);
        setStorageGroup(storageGroupPath);
      } else {
        throw new StorageGroupNotSetException(path.getFullPath());
      }
    } catch (StorageGroupAlreadySetException e) {
      // Storage group may be set concurrently
      if (e.isHasChild()) {
        // If setStorageGroup failure is because of child, the deviceNode should not be created.
        // Timeseries can't be created under a deviceNode without storageGroup.
        throw e;
      }
    }

    return storageGroupSchemaManager
        .getBelongedSGMManager(path)
        .getDeviceNodeWithAutoCreate(path, autoCreateSchema);
  }

  protected IMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws MetadataException, IOException {
    return getDeviceNodeWithAutoCreate(
        path, config.isAutoCreateSchemaEnabled(), true, config.getDefaultStorageGroupLevel());
  }

  private void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    storageGroupSchemaManager.getBelongedSGMManager(plan.getPath()).autoCreateDeviceMNode(plan);
  }
  // endregion

  // region Interfaces for metadata info Query
  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {

    try {
      if (!storageGroupSchemaManager.isStorageGroupAlreadySet(path)) {
        return false;
      }
      try {
        return storageGroupSchemaManager.getBelongedSGMManager(path).isPathExist(path);
      } catch (StorageGroupNotSetException e) {
        // path exists above storage group
        return true;
      }
    } catch (MetadataException e) {
      return false;
    }
  }

  /** Get metadata in string */
  public String getMetadataInString() {
    return TIME_SERIES_TREE_HEADER + storageGroupSchemaManager.getMetadataInString();
  }

  // region Interfaces for metadata count

  public long getTotalSeriesNumber() {
    return timeseriesStatistics.getTotalSeriesNumber();
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    int count = 0;
    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch)) {
      count += sgmManager.getAllTimeseriesCount(pathPattern, isPrefixMatch);
    }
    return count;
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  /**
   * To calculate the count of devices for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    int num = 0;
    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch)) {
      num += sgmManager.getDevicesNum(pathPattern, isPrefixMatch);
    }
    return num;
  }

  /** To calculate the count of devices for given path pattern. */
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
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
   * To calculate the count of nodes in the given level for given path pattern. If using prefix
   * match, the path pattern is used to match prefix path. All nodes start with the matched prefix
   * path will be counted.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    Pair<Integer, List<SGMManager>> pair =
        storageGroupSchemaManager.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
    int count = pair.left;
    for (SGMManager sgmManager : pair.right) {
      count += sgmManager.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
    }
    return count;
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
      throws MetadataException {
    return getNodesCountInGivenLevel(pathPattern, level, false);
  }

  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    Map<PartialPath, Integer> result = new HashMap<>();
    Map<PartialPath, Integer> sgResult;
    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch)) {
      sgResult = sgmManager.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
      for (PartialPath path : sgResult.keySet()) {
        if (result.containsKey(path)) {
          result.put(path, result.get(path) + sgResult.get(path));
        } else {
          result.put(path, sgResult.get(path));
        }
      }
    }
    return result;
  }

  // endregion

  // region Interfaces for level Node info Query
  /**
   * Get all nodes matching the given path pattern in the given level. The level of the path should
   * match the nodeLevel. 1. The given level equals the path level without **, e.g. give path
   * root.*.d.* and the level should be 4. 2. The given level is greater than path level with **,
   * e.g. give path root.** and the level could be 2 or 3.
   *
   * @param pathPattern can be a pattern of a full path.
   * @param nodeLevel the level should match the level of the path
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    return getNodesListInGivenLevel(pathPattern, nodeLevel, null);
  }

  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
    Pair<List<PartialPath>, List<SGMManager>> pair =
        storageGroupSchemaManager.getNodesListInGivenLevel(pathPattern, nodeLevel, filter);
    List<PartialPath> result = pair.left;
    for (SGMManager sgmManager : pair.right) {
      result.addAll(sgmManager.getNodesListInGivenLevel(pathPattern, nodeLevel, filter));
    }
    return result;
  }

  /**
   * Get child node path in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException {
    Pair<Set<String>, List<SGMManager>> pair =
        storageGroupSchemaManager.getChildNodePathInNextLevel(pathPattern);
    Set<String> result = pair.left;
    for (SGMManager sgmManager : pair.right) {
      result.addAll(sgmManager.getChildNodePathInNextLevel(pathPattern));
    }
    return result;
  }

  /**
   * Get child node in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2] given path = root.sg.d1 return [s1,s2]
   *
   * @return All child nodes of given seriesPath.
   */
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    Pair<Set<String>, List<SGMManager>> pair =
        storageGroupSchemaManager.getChildNodeNameInNextLevel(pathPattern);
    Set<String> result = pair.left;
    for (SGMManager sgmManager : pair.right) {
      result.addAll(sgmManager.getChildNodeNameInNextLevel(pathPattern));
    }
    return result;
  }
  // endregion

  // region Interfaces for StorageGroup and TTL info Query
  /**
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    return storageGroupSchemaManager.isStorageGroup(path);
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    return storageGroupSchemaManager.checkStorageGroupByPath(path);
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
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    Map<PartialPath, Long> storageGroupsTTL = new HashMap<>();
    try {
      List<PartialPath> storageGroups = this.getAllStorageGroupPaths();
      for (PartialPath storageGroup : storageGroups) {
        long ttl = getStorageGroupNodeByStorageGroupPath(storageGroup).getDataTTL();
        storageGroupsTTL.put(storageGroup, ttl);
      }
    } catch (MetadataException e) {
      logger.error("get storage groups ttl failed.", e);
    }
    return storageGroupsTTL;
  }

  // endregion

  // region Interfaces for Entity/Device info Query

  /**
   * Get all devices that one of the timeseries, matching the given timeseries path pattern, belongs
   * to.
   *
   * @param timeseries a path pattern of the target timeseries
   * @return A HashSet instance which stores devices paths.
   */
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    return storageGroupSchemaManager
        .getBelongedSGMManager(timeseries)
        .getBelongedDevices(timeseries);
  }

  /**
   * Get all device paths matching the path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern the pattern of the target devices.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    Set<PartialPath> result = new TreeSet<>();
    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch)) {
      result.addAll(sgmManager.getMatchedDevices(pathPattern, isPrefixMatch));
    }
    return result;
  }

  /**
   * Get all device paths and according storage group paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult.
   */
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    List<ShowDevicesResult> result = new LinkedList<>();

    int limit = plan.getLimit();
    int offset = plan.getOffset();

    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(plan.getPath(), plan.isPrefixMatch())) {
      if (limit != 0 && plan.getLimit() == 0) {
        break;
      }
      result.addAll(sgmManager.getMatchedDevices(plan));
    }

    // reset limit and offset
    plan.setLimit(limit);
    plan.setOffset(offset);

    return result;
  }
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'. If using prefix match, the path pattern is used to match prefix path. All timeseries start
   * with the matched prefix path will be collected.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch).left;
  }

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   */
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern)
      throws MetadataException {
    return getMeasurementPaths(pathPattern, false);
  }

  /**
   * Similar to method getMeasurementPaths(), but return Path with alias and filter the result by
   * limit and offset. If using prefix match, the path pattern is used to match prefix path. All
   * timeseries start with the matched prefix path will be collected.
   *
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    List<MeasurementPath> measurementPaths = new LinkedList<>();
    Pair<List<MeasurementPath>, Integer> result;
    int resultOffset = 0;

    int tmpLimit = limit;
    int tmpOffset = offset;

    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(pathPattern, isPrefixMatch)) {
      if (limit != 0 && tmpLimit == 0) {
        break;
      }
      result =
          sgmManager.getMeasurementPathsWithAlias(pathPattern, tmpLimit, tmpOffset, isPrefixMatch);
      measurementPaths.addAll(result.left);
      resultOffset += result.right;
      if (limit != 0) {
        tmpOffset = Math.max(0, tmpOffset - result.right);
        tmpLimit -= result.left.size();
      }
    }

    result = new Pair<>(measurementPaths, resultOffset);
    return result;
  }

  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    List<ShowTimeSeriesResult> result = new LinkedList<>();

    /*
     There are two conditions and 4 cases.
     1. isOrderByHeat = false && limit = 0 : just collect all results from each storage group
     2. isOrderByHeat = false && limit != 0 : when finish the collection on one sg, the offset and limit should be decreased by the result taken from the current sg
     3. isOrderByHeat = true && limit = 0 : collect all result from each storage group and then sort
     4. isOrderByHeat = true && limit != 0 : set the limit' = offset + limit and offset' = 0,
     which means collect top limit' result from each sg and then sort them and collect the top limit results start from offset.
     It is ensured that the target result could be extracted from the top limit' results of each sg.
    */

    int limit = plan.getLimit();
    int offset = plan.getOffset();

    if (plan.isOrderByHeat() && limit != 0) {
      plan.setOffset(0);
      plan.setLimit(offset + limit);
    }

    for (SGMManager sgmManager :
        storageGroupSchemaManager.getInvolvedSGMManagers(plan.getPath(), plan.isPrefixMatch())) {
      if (limit != 0 && plan.getLimit() == 0) {
        break;
      }
      result.addAll(sgmManager.showTimeseries(plan, context));
    }

    Stream<ShowTimeSeriesResult> stream = result.stream();

    if (plan.isOrderByHeat()) {
      stream =
          stream.sorted(
              Comparator.comparingLong(ShowTimeSeriesResult::getLastTime)
                  .reversed()
                  .thenComparing(ShowResult::getName));
      if (limit != 0) {
        stream = stream.skip(offset).limit(limit);
      }
    }

    // reset limit and offset with the initial value
    plan.setLimit(limit);
    plan.setOffset(offset);

    return stream.collect(toList());
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
    if (fullPath.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }
    return getSeriesSchema(fullPath).getType();
  }

  /**
   * Get schema of paritialPath
   *
   * @param fullPath (may be ParitialPath or AlignedPath)
   * @return MeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    return getMeasurementMNode(fullPath).getSchema();
  }

  // attention: this path must be a device node
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    try {
      return storageGroupSchemaManager
          .getBelongedSGMManager(devicePath)
          .getAllMeasurementByDevicePath(devicePath);
    } catch (MetadataException e) {
      throw new PathNotExistException(devicePath.getFullPath());
    }
  }
  // endregion
  // endregion

  // region Interfaces and methods for MNode query
  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg],
   * return the MNode of root.sg Get storage group node by path. Give path like [root, sg, device],
   * MNodeTypeMismatchException will be thrown. If storage group is not set,
   * StorageGroupNotSetException will be thrown.
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return storageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(path);
  }

  /** Get storage group node by path. the give path don't need to be storage group path. */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    ensureStorageGroup(path);
    return storageGroupSchemaManager.getStorageGroupNodeByPath(path);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return storageGroupSchemaManager.getAllStorageGroupNodes();
  }

  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    return storageGroupSchemaManager.getBelongedSGMManager(path).getDeviceNode(path);
  }

  public IMeasurementMNode[] getMeasurementMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    return storageGroupSchemaManager
        .getBelongedSGMManager(deviceId)
        .getMeasurementMNodes(deviceId, measurements);
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    try {
      return storageGroupSchemaManager
          .getBelongedSGMManager(fullPath)
          .getMeasurementMNode(fullPath);
    } catch (StorageGroupNotSetException e) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
  }

  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws PathAlreadyExistException {
    IMNode result = deviceMNode.getChild(measurementName);
    if (result == null) {
      return null;
    }

    if (result.isMeasurement()) {
      return result.getAsMeasurementMNode();
    } else {
      throw new PathAlreadyExistException(
          deviceMNode.getFullPath() + PATH_SEPARATOR + measurementName);
    }
  }
  // endregion

  // region Interfaces for alias and tag/attribute operations
  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    storageGroupSchemaManager.getBelongedSGMManager(path).changeAlias(path, alias);
  }

  /**
   * upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager
        .getBelongedSGMManager(fullPath)
        .upsertTagsAndAttributes(alias, tagsMap, attributesMap, fullPath);
  }

  /**
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager
        .getBelongedSGMManager(fullPath)
        .addAttributes(attributesMap, fullPath);
  }

  /**
   * add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager.getBelongedSGMManager(fullPath).addTags(tagsMap, fullPath);
  }

  /**
   * drop tags or attributes of the timeseries
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager
        .getBelongedSGMManager(fullPath)
        .dropTagsOrAttributes(keySet, fullPath);
  }

  /**
   * set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager
        .getBelongedSGMManager(fullPath)
        .setTagsOrAttributesValue(alterMap, fullPath);
  }

  /**
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    storageGroupSchemaManager
        .getBelongedSGMManager(fullPath)
        .renameTagOrAttributeKey(oldKey, newKey, fullPath);
  }
  // endregion

  // region Interfaces only for Cluster module usage

  /**
   * Collect the timeseries schemas as IMeasurementSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas) {
    try {
      for (SGMManager sgmManager :
          storageGroupSchemaManager.getInvolvedSGMManagers(prefixPath, true)) {
        sgmManager.collectMeasurementSchema(prefixPath, measurementSchemas);
      }
    } catch (MetadataException ignored) {
      // do nothing
    }
  }

  /**
   * Collect the timeseries schemas as TimeseriesSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas) {
    try {
      for (SGMManager sgmManager :
          storageGroupSchemaManager.getInvolvedSGMManagers(prefixPath, true)) {
        sgmManager.collectTimeseriesSchema(prefixPath, timeseriesSchemas);
      }
    } catch (MetadataException ignored) {
      // do nothing
    }
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
   * if the path is in local mtree, nothing needed to do (because mtree is in the memory); Otherwise
   * cache the path to mRemoteSchemaCache
   */
  public void cacheMeta(
      PartialPath path, IMeasurementMNode measurementMNode, boolean needSetFullPath) {
    // do nothing
  }

  /**
   * StorageGroupFilter filters unsatisfied storage groups in metadata queries to speed up and
   * deduplicate.
   */
  @FunctionalInterface
  public interface StorageGroupFilter {

    boolean satisfy(String storageGroup);
  }
  // endregion

  // region Interfaces for lastCache operations
  /**
   * Update the last cache value of time series of given seriesPath.
   *
   * <p>MManager will use the seriesPath to search the node first and then process the lastCache in
   * the MeasurementMNode
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param seriesPath the PartialPath of full path from root to Measurement
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to update last cache for the {}, err:{}", seriesPath, e.getMessage());
      return;
    }

    LastCacheManager.updateLastCache(node, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /**
   * Update the last cache value in given MeasurementMNode. work.
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      IMeasurementMNode node,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    LastCacheManager.updateLastCache(node, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /**
   * Get the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param seriesPath the PartialPath of full path from root to Measurement
   * @return the last cache value
   */
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to get last cache for the {}, err:{}", seriesPath, e.getMessage());
      return null;
    }

    return LastCacheManager.getLastCache(node);
  }

  /**
   * Get the last cache value in given MeasurementMNode.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @return the last cache value
   */
  public TimeValuePair getLastCache(IMeasurementMNode node) {
    return LastCacheManager.getLastCache(node);
  }

  /**
   * Reset the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * @param seriesPath the PartialPath of full path from root to Measurement
   */
  public void resetLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to reset last cache for the {}, err:{}", seriesPath, e.getMessage());
      return;
    }

    LastCacheManager.resetLastCache(node);
  }

  /**
   * delete all the last cache value of any timeseries or aligned timeseries under the device
   *
   * <p>Invoking scenario (1) after upload tsfile
   *
   * @param deviceId path of device
   */
  public void deleteLastCacheByDevice(PartialPath deviceId) throws MetadataException {
    IMNode node = getDeviceNode(deviceId);
    if (node.isEntity()) {
      LastCacheManager.deleteLastCacheByDevice(node.getAsEntityMNode());
    }
  }

  /**
   * delete the last cache value of timeseries or subMeasurement of some aligned timeseries, which
   * is under the device and matching the originalPath
   *
   * <p>Invoking scenario (1) delete timeseries
   *
   * @param deviceId path of device
   * @param originalPath origin timeseries path
   * @param startTime startTime
   * @param endTime endTime
   */
  public void deleteLastCacheByDevice(
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)
      throws MetadataException {
    IMNode node = getDeviceNode(deviceId);
    if (node.isEntity()) {
      LastCacheManager.deleteLastCacheByDevice(
          node.getAsEntityMNode(), originalPath, startTime, endTime);
    }
  }
  // endregion

  // region Interfaces and Implementation for InsertPlan process
  /** get schema for device. Attention!!! Only support insertPlan */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    try {
      return storageGroupSchemaManager
          .getBelongedSGMManager(plan.getDevicePath())
          .getSeriesSchemasAndReadLockDevice(plan);
    } catch (StorageGroupNotSetException e) {
      if (config.isAutoCreateSchemaEnabled()) {
        ensureStorageGroup(plan.getDevicePath());
      } else {
        throw e;
      }

      return storageGroupSchemaManager
          .getBelongedSGMManager(plan.getDevicePath())
          .getSeriesSchemasAndReadLockDevice(plan);
    }
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

    for (PartialPath path : template.getRelatedStorageGroup()) {
      if (!storageGroupSchemaManager
          .getBelongedSGMManager(path)
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

    if (templateManager.getTemplate(plan.getName()).getRelatedStorageGroup().size() > 0) {
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
      for (SGMManager sgmManager : storageGroupSchemaManager.getAllSGMManagers()) {
        result.addAll(sgmManager.getPathsSetTemplate(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      }
    } else {
      for (PartialPath path : templateManager.getTemplate(templateName).getRelatedStorageGroup()) {
        result.addAll(
            storageGroupSchemaManager
                .getBelongedSGMManager(path)
                .getPathsSetTemplate(templateName));
      }
    }

    return result;
  }

  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    Set<String> result = new HashSet<>();
    if (templateName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      for (SGMManager sgmManager : storageGroupSchemaManager.getAllSGMManagers()) {
        result.addAll(sgmManager.getPathsUsingTemplate(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      }
    } else {
      for (PartialPath path : templateManager.getTemplate(templateName).getRelatedStorageGroup()) {
        result.addAll(
            storageGroupSchemaManager
                .getBelongedSGMManager(path)
                .getPathsUsingTemplate(templateName));
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

    if (templateManager.getTemplate(plan.getName()).getRelatedStorageGroup().size() > 0) {
      throw new MetadataException(
          String.format(
              "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
    }

    templateManager.dropSchemaTemplate(plan);
  }

  public synchronized void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    PartialPath path = new PartialPath(plan.getPrefixPath());
    try {
      ensureStorageGroup(path);
      storageGroupSchemaManager.getBelongedSGMManager(path).setSchemaTemplate(plan);
    } catch (StorageGroupAlreadySetException e) {
      throw new MetadataException("Template should not be set above storageGroup");
    }
  }

  public synchronized void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    try {
      storageGroupSchemaManager
          .getBelongedSGMManager(new PartialPath(plan.getPrefixPath()))
          .unsetSchemaTemplate(plan);
    } catch (StorageGroupNotSetException e) {
      throw new PathNotExistException(plan.getPrefixPath());
    }
  }

  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    try {
      storageGroupSchemaManager
          .getBelongedSGMManager(plan.getPrefixPath())
          .setUsingSchemaTemplate(plan);
    } catch (StorageGroupNotSetException e) {
      throw new MetadataException(
          String.format(
              "Path [%s] has not been set any template.", plan.getPrefixPath().toString()));
    }
  }

  IMNode setUsingSchemaTemplate(IMNode node) throws MetadataException {
    return storageGroupSchemaManager
        .getBelongedSGMManager(node.getPartialPath())
        .setUsingSchemaTemplate(node);
  }
  // endregion

  // region TestOnly Interfaces
  /**
   * To reduce the String number in memory, use the deviceId from MManager instead of the deviceId
   * read from disk
   *
   * @param devicePath read from disk
   * @return deviceId
   */
  @TestOnly
  public String getDeviceId(PartialPath devicePath) {
    String device = null;
    try {
      IMNode deviceNode = getDeviceNode(devicePath);
      device = deviceNode.getFullPath();
    } catch (MetadataException | NullPointerException e) {
      // Cannot get deviceId from MManager, return the input deviceId
    }
    return device;
  }

  /**
   * Attention!!!!!, this method could only be used for Tests involving multiple mmanagers. The
   * singleton of templateManager and tagManager will cause interference between mmanagers if one of
   * the mmanagers invoke init method or clear method
   *
   * <p>todo remove this method after delete or refactor the SlotPartitionTableTest in cluster
   * module
   */
  @TestOnly
  public void initForMultiMManagerTest() {
    templateManager = TemplateManager.getNewInstanceForTest();
    storageGroupSchemaManager = StorageGroupSchemaManager.getNewInstanceForTest();
    init();
  }

  @TestOnly
  public Template getTemplate(String templateName) throws MetadataException {
    try {
      return templateManager.getTemplate(templateName);
    } catch (UndefinedTemplateException e) {
      throw new MetadataException(e);
    }
  }
  // endregion
}
