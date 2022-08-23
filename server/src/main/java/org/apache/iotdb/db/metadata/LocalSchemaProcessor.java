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

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.rescon.SchemaStatisticsManager;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
 *   <li>SchemaProcessor Singleton
 *   <li>Interfaces and Implementation of Operating PhysicalPlans of Metadata
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces and Implementation for StorageGroup and TTL operation
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
 *   <li>Interfaces for Trigger
 *   <li>TestOnly Interfaces
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
public class LocalSchemaProcessor {

  private static final Logger logger = LoggerFactory.getLogger(LocalSchemaProcessor.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private LocalConfigNode configManager = LocalConfigNode.getInstance();
  private SchemaEngine schemaEngine = SchemaEngine.getInstance();

  // region SchemaProcessor Singleton
  private static class LocalSchemaProcessorHolder {

    private LocalSchemaProcessorHolder() {
      // allowed to do nothing
    }

    private static final LocalSchemaProcessor INSTANCE = new LocalSchemaProcessor();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static LocalSchemaProcessor getInstance() {
    return LocalSchemaProcessorHolder.INSTANCE;
  }

  protected LocalSchemaProcessor() {}
  // endregion

  // region methods in this region is only used for local schemaRegion management.

  /**
   * Get the target SchemaRegion, which the given path belongs to. The path must be a fullPath
   * without wildcards, * or **. This method is the first step when there's a task on one certain
   * path, e.g., root.sg1 is a storage group and path = root.sg1.d1, return SchemaRegion of
   * root.sg1. If there's no storage group on the given path, StorageGroupNotSetException will be
   * thrown.
   */
  private ISchemaRegion getBelongedSchemaRegion(PartialPath path) throws MetadataException {
    return schemaEngine.getSchemaRegion(configManager.getBelongedSchemaRegionId(path));
  }

  // This interface involves storage group auto creation
  private ISchemaRegion getBelongedSchemaRegionWithAutoCreate(PartialPath path)
      throws MetadataException {
    return schemaEngine.getSchemaRegion(
        configManager.getBelongedSchemaRegionIdWithAutoCreate(path));
  }

  /**
   * Get the target SchemaRegion, which will be involved/covered by the given pathPattern. The path
   * may contain wildcards, * or **. This method is the first step when there's a task on multiple
   * paths represented by the given pathPattern. If isPrefixMatch, all storage groups under the
   * prefixPath that matches the given pathPattern will be collected.
   */
  private List<ISchemaRegion> getInvolvedSchemaRegions(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<SchemaRegionId> schemaRegionIds =
        configManager.getInvolvedSchemaRegionIds(pathPattern, isPrefixMatch);
    List<ISchemaRegion> schemaRegions = new ArrayList<>();
    for (SchemaRegionId schemaRegionId : schemaRegionIds) {
      schemaRegions.add(schemaEngine.getSchemaRegion(schemaRegionId));
    }
    return schemaRegions;
  }

  private List<ISchemaRegion> getSchemaRegionsByStorageGroup(PartialPath storageGroup)
      throws MetadataException {
    List<SchemaRegionId> schemaRegionIds =
        configManager.getSchemaRegionIdsByStorageGroup(storageGroup);
    List<ISchemaRegion> schemaRegions = new ArrayList<>();
    for (SchemaRegionId schemaRegionId : schemaRegionIds) {
      schemaRegions.add(schemaEngine.getSchemaRegion(schemaRegionId));
    }
    return schemaRegions;
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
        for (PartialPath path : deleteTimeSeriesPlan.getPaths()) {
          deleteTimeseries(path);
        }
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

  private void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    getBelongedSchemaRegion(plan.getPath()).autoCreateDeviceMNode(plan);
  }
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    getBelongedSchemaRegionWithAutoCreate(plan.getPath()).createTimeseries(plan, offset);
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
    } catch (PathAlreadyExistException
        | AliasAlreadyExistException
        | MeasurementAlreadyExistException e) {
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
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    getBelongedSchemaRegionWithAutoCreate(plan.getPrefixPath()).createAlignedTimeSeries(plan);
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
    List<ISchemaRegion> schemaRegions = getInvolvedSchemaRegions(pathPattern, isPrefixMatch);
    if (schemaRegions.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }
    Set<String> failedNames = new HashSet<>();
    int deletedNum = 0;
    Pair<Integer, Set<String>> sgDeletionResult;
    for (ISchemaRegion schemaRegion : schemaRegions) {
      sgDeletionResult = schemaRegion.deleteTimeseries(pathPattern, isPrefixMatch);
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
    configManager.setStorageGroup(storageGroup);
  }

  /**
   * Delete storage groups of given paths from MTree.
   *
   * @param storageGroups list of paths to be deleted.
   */
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    configManager.deleteStorageGroups(storageGroups);
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    configManager.setTTL(storageGroup, dataTTL);
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
      if (!configManager.isStorageGroupAlreadySet(path)) {
        return false;
      }
      if (configManager.isStorageGroup(path)) {
        return true;
      }
      try {
        PartialPath storageGroup = configManager.getBelongedStorageGroup(path);
        for (ISchemaRegion schemaRegion : getSchemaRegionsByStorageGroup(storageGroup)) {
          if (schemaRegion.isPathExist(path)) {
            return true;
          }
        }
        return false;
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
    return "Doesn't support metadata Tree toString since v0.14";
  }

  // region Interfaces for metadata count

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    // todo this is for test assistance, refactor this to support massive timeseries
    if (pathPattern.getFullPath().equals("root.**")
        && TemplateManager.getInstance().getAllTemplateName().isEmpty()) {
      return (int) SchemaStatisticsManager.getInstance().getTotalSeriesNumber();
    }
    int count = 0;
    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      count += schemaRegion.getAllTimeseriesCount(pathPattern, isPrefixMatch);
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
    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      num += schemaRegion.getDevicesNum(pathPattern, isPrefixMatch);
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
    return configManager.getStorageGroupNum(pathPattern, isPrefixMatch);
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
    return getNodesListInGivenLevel(pathPattern, level, isPrefixMatch).size();
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
    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      sgResult = schemaRegion.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
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
    return getNodesListInGivenLevel(pathPattern, nodeLevel, false, filter);
  }

  private List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException {
    return getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch, null);
  }

  private List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch, StorageGroupFilter filter)
      throws MetadataException {
    Pair<List<PartialPath>, Set<PartialPath>> pair =
        configManager.getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch, filter);
    Set<PartialPath> result = new TreeSet<>(pair.left);
    for (PartialPath storageGroup : pair.right) {
      for (ISchemaRegion schemaRegion : getSchemaRegionsByStorageGroup(storageGroup)) {
        result.addAll(
            schemaRegion.getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch, filter));
      }
    }
    return new ArrayList<>(result);
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
  public Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    Pair<Set<TSchemaNode>, Set<PartialPath>> pair =
        configManager.getChildNodePathInNextLevel(pathPattern);
    Set<TSchemaNode> result = pair.left;
    for (PartialPath storageGroup : pair.right) {
      for (ISchemaRegion schemaRegion : getSchemaRegionsByStorageGroup(storageGroup)) {
        result.addAll(schemaRegion.getChildNodePathInNextLevel(pathPattern));
      }
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
    Pair<Set<String>, Set<PartialPath>> pair =
        configManager.getChildNodeNameInNextLevel(pathPattern);
    Set<String> result = pair.left;
    for (PartialPath storageGroup : pair.right) {
      for (ISchemaRegion schemaRegion : getSchemaRegionsByStorageGroup(storageGroup)) {
        result.addAll(schemaRegion.getChildNodeNameInNextLevel(pathPattern));
      }
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
    return configManager.isStorageGroup(path);
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    return configManager.checkStorageGroupByPath(path);
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
    return configManager.getBelongedStorageGroup(path);
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
    return configManager.getBelongedStorageGroups(pathPattern);
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
    return configManager.getMatchedStorageGroups(pathPattern, isPrefixMatch);
  }

  /** Get all storage group paths */
  public List<PartialPath> getAllStorageGroupPaths() {
    return configManager.getAllStorageGroupPaths();
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    return configManager.getStorageGroupsTTL();
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
    Set<PartialPath> result = new TreeSet<>();
    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(timeseries, false)) {
      result.addAll(schemaRegion.getBelongedDevices(timeseries));
    }
    return result;
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
    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      result.addAll(schemaRegion.getMatchedDevices(pathPattern, isPrefixMatch));
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

    Pair<List<ShowDevicesResult>, Integer> regionResult;
    for (ISchemaRegion schemaRegion :
        getInvolvedSchemaRegions(plan.getPath(), plan.isPrefixMatch())) {
      if (limit != 0 && plan.getLimit() == 0) {
        break;
      }
      regionResult = schemaRegion.getMatchedDevices(plan);
      result.addAll(regionResult.left);

      if (limit != 0) {
        plan.setLimit(plan.getLimit() - regionResult.left.size());
        plan.setOffset(Math.max(plan.getOffset() - regionResult.right, 0));
      }
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

    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      if (limit != 0 && tmpLimit == 0) {
        break;
      }
      result =
          schemaRegion.getMeasurementPathsWithAlias(
              pathPattern, tmpLimit, tmpOffset, isPrefixMatch);
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

    Pair<List<ShowTimeSeriesResult>, Integer> regionResult;
    for (ISchemaRegion schemaRegion :
        getInvolvedSchemaRegions(plan.getPath(), plan.isPrefixMatch())) {
      if (limit != 0 && plan.getLimit() == 0) {
        break;
      }
      regionResult = schemaRegion.showTimeseries(plan, context);
      result.addAll(regionResult.left);

      if (limit != 0) {
        plan.setLimit(plan.getLimit() - regionResult.left.size());
        plan.setOffset(Math.max(plan.getOffset() - regionResult.right, 0));
      }
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
      return getBelongedSchemaRegion(devicePath).getAllMeasurementByDevicePath(devicePath);
    } catch (MetadataException e) {
      throw new PathNotExistException(devicePath.getFullPath());
    }
  }
  // endregion
  // endregion

  // region Interfaces and methods for MNode query

  /** Get storage group node by path. the give path don't need to be storage group path. */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return configManager.getStorageGroupNodeByPath(path);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return configManager.getAllStorageGroupNodes();
  }

  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    return getBelongedSchemaRegion(path).getDeviceNode(path);
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    try {
      return getBelongedSchemaRegion(fullPath).getMeasurementMNode(fullPath);
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
      throws MetadataException {
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
  public void changeAlias(PartialPath path, String alias) throws MetadataException, IOException {
    getBelongedSchemaRegion(path).changeAlias(path, alias);
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
    getBelongedSchemaRegion(fullPath)
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
    getBelongedSchemaRegion(fullPath).addAttributes(attributesMap, fullPath);
  }

  /**
   * add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    getBelongedSchemaRegion(fullPath).addTags(tagsMap, fullPath);
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
    getBelongedSchemaRegion(fullPath).dropTagsOrAttributes(keySet, fullPath);
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
    getBelongedSchemaRegion(fullPath).setTagsOrAttributesValue(alterMap, fullPath);
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
    getBelongedSchemaRegion(fullPath).renameTagOrAttributeKey(oldKey, newKey, fullPath);
  }
  // endregion

  // region Interfaces only for Cluster module usage

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
    return configManager.groupPathByStorageGroup(path);
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
   * <p>SchemaProcessor will use the seriesPath to search the node first and then process the
   * lastCache in the MeasurementMNode
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
   * Get the last cache value of time series of given seriesPath. SchemaProcessor will use the
   * seriesPath to search the node.
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
   * Reset the last cache value of time series of given seriesPath. SchemaProcessor will use the
   * seriesPath to search the node.
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
    ISchemaRegion schemaRegion;
    if (config.isAutoCreateSchemaEnabled()) {
      schemaRegion = getBelongedSchemaRegionWithAutoCreate(plan.getDevicePath());
    } else {
      schemaRegion = getBelongedSchemaRegion(plan.getDevicePath());
    }

    return schemaRegion.getSeriesSchemasAndReadLockDevice(plan);
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    configManager.createSchemaTemplate(plan);
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    configManager.appendSchemaTemplate(plan);
  }

  public void pruneSchemaTemplate(PruneTemplatePlan plan) throws MetadataException {
    configManager.pruneSchemaTemplate(plan);
  }

  public int countMeasurementsInTemplate(String templateName) throws MetadataException {
    return configManager.countMeasurementsInTemplate(templateName);
  }

  /**
   * @param templateName name of template to check
   * @param path full path to check
   * @return if path correspond to a measurement in template
   * @throws MetadataException
   */
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws MetadataException {
    return configManager.isMeasurementInTemplate(templateName, path);
  }

  public boolean isPathExistsInTemplate(String templateName, String path) throws MetadataException {
    return configManager.isPathExistsInTemplate(templateName, path);
  }

  public List<String> getMeasurementsInTemplate(String templateName, String path)
      throws MetadataException {
    return configManager.getMeasurementsInTemplate(templateName, path);
  }

  public List<Pair<String, IMeasurementSchema>> getSchemasInTemplate(
      String templateName, String path) throws MetadataException {
    return configManager.getSchemasInTemplate(templateName, path);
  }

  public Set<String> getAllTemplates() {
    return configManager.getAllTemplates();
  }

  /**
   * Get all paths set designated template
   *
   * @param templateName designated template name, blank string for any template exists
   * @return paths set
   */
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    return configManager.getPathsSetTemplate(templateName);
  }

  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    return configManager.getPathsUsingTemplate(templateName);
  }

  public void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException {
    configManager.dropSchemaTemplate(plan);
  }

  public synchronized void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    configManager.setSchemaTemplate(plan);
  }

  public synchronized void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    configManager.unsetSchemaTemplate(plan);
  }

  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    configManager.setUsingSchemaTemplate(plan);
  }

  // endregion

  // region Interfaces for Trigger

  public IMNode getMNodeForTrigger(PartialPath fullPath) throws MetadataException {
    try {
      return getBelongedSchemaRegion(fullPath).getMNodeForTrigger(fullPath);
    } catch (StorageGroupNotSetException e) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
  }

  public void releaseMNodeAfterDropTrigger(IMNode imNode) throws MetadataException {
    getBelongedSchemaRegion(imNode.getPartialPath()).releaseMNodeAfterDropTrigger(imNode);
  }

  // endregion

  // region TestOnly Interfaces

  @TestOnly
  public void forceMlog() {
    configManager.forceMlog();
  }

  @TestOnly
  public long getTotalSeriesNumber() {
    return SchemaStatisticsManager.getInstance().getTotalSeriesNumber();
  }

  /**
   * To reduce the String number in memory, use the deviceId from SchemaProcessor instead of the
   * deviceId read from disk
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
      // Cannot get deviceId from SchemaProcessor, return the input deviceId
    }
    return device;
  }

  @TestOnly
  public Template getTemplate(String templateName) throws MetadataException {
    try {
      return TemplateManager.getInstance().getTemplate(templateName);
    } catch (UndefinedTemplateException e) {
      throw new MetadataException(e);
    }
  }
  // endregion
}
