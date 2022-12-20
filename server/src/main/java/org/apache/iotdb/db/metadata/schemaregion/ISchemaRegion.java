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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * This interface defines all interfaces and behaviours that one SchemaRegion should support and
 * implement.
 *
 * <p>The interfaces are divided as following:
 *
 * <ol>
 *   <li>Interfaces for initialization、recover and clear
 *   <li>Interfaces for schema region Info query and operation
 *   <li>Interfaces for Timeseries operation
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for metadata count
 *         <li>Interfaces for level Node info Query
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces and methods for MNode query
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces for InsertPlan process
 *   <li>Interfaces for Template operations
 * </ol>
 */
public interface ISchemaRegion {

  // region Interfaces for initialization、recover and clear
  void init() throws MetadataException;

  /** clear all metadata components of this schemaRegion */
  void clear();

  void forceMlog();
  // endregion

  // region Interfaces for schema region Info query and operation
  SchemaRegionId getSchemaRegionId();

  String getStorageGroupFullPath();

  // delete this schemaRegion and clear all resources
  void deleteSchemaRegion() throws MetadataException;

  boolean createSnapshot(File snapshotDir);

  void loadSnapshot(File latestSnapshotRootDir);
  // endregion

  // region Interfaces for Timeseries operation
  /**
   * Create timeseries.
   *
   * @param plan a plan describes how to create the timeseries.
   * @param offset
   * @throws MetadataException
   */
  void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException;

  /**
   * Create aligned timeseries.
   *
   * @param plan a plan describes how to create the timeseries.
   * @throws MetadataException
   */
  void createAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException;

  /**
   * Check whether measurement exists.
   *
   * @param devicePath the path of device that you want to check
   * @param measurementList a list of measurements that you want to check
   * @param aliasList a list of alias that you want to check
   * @return returns a map contains index of the measurements or alias that threw the exception, and
   *     exception details. The exceptions describe whether the measurement or alias exists. For
   *     example, a MeasurementAlreadyExistException means this measurement exists.
   */
  Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList);

  /**
   * Delete all timeseries matching the given path pattern. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * deleted.
   *
   * @param pathPattern path to be deleted
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return deletion failed Timeseries
   */
  @Deprecated
  Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Construct schema black list via setting matched timeseries to pre deleted.
   *
   * @param patternTree
   * @throws MetadataException
   * @return preDeletedNum. If there are intersections of patterns in the patternTree, there may be
   *     more than are actually pre-deleted.
   */
  long constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException;

  /**
   * Rollback schema black list via setting matched timeseries to not pre deleted.
   *
   * @param patternTree
   * @throws MetadataException
   */
  void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException;

  /**
   * Fetch schema black list (timeseries that has been pre deleted).
   *
   * @param patternTree
   * @throws MetadataException
   */
  Set<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree) throws MetadataException;

  /**
   * Delete timeseries in schema black list.
   *
   * @param patternTree
   * @throws MetadataException
   */
  void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException;
  // endregion

  // region Interfaces for metadata info Query
  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  @Deprecated
  boolean isPathExist(PartialPath path) throws MetadataException;

  // region Interfaces for metadata count
  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  @Deprecated
  long getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  long getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException;

  long getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, String key, String value, boolean isContains)
      throws MetadataException;

  /**
   * The measurements will be grouped by the node in given level and then counted for each group. If
   * no measurements found, but the path is contained in the group, then this path will also be
   * returned with measurements count zero.
   *
   * @param pathPattern
   * @param level the level you want to group by
   * @param isPrefixMatch using pathPattern as prefix matched path if set true
   * @return return a map from PartialPath to the count of matched measurements
   */
  Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException;

  Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      String key,
      String value,
      boolean isContains)
      throws MetadataException;

  /**
   * To calculate the count of devices for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   *
   * @param pathPattern
   * @param isPrefixMatch
   */
  long getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;
  // endregion

  // region Interfaces for level Node info Query
  /**
   * Get paths of nodes in given level and matching the pathPattern.
   *
   * @param pathPattern
   * @param nodeLevel
   * @param isPrefixMatch
   * @throws MetadataException
   * @return returns a list of PartialPath.
   */
  List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException;

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
  Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException;

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
  @Deprecated
  Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException;
  // endregion

  // region Interfaces for Entity/Device info Query
  /**
   * Get all devices that one of the timeseries, matching the given timeseries path pattern, belongs
   * to.
   *
   * @param timeseries a path pattern of the target timeseries
   * @return A HashSet instance which stores devices paths.
   */
  @Deprecated
  Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException;

  /**
   * Get all device paths matching the path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern the pattern of the target devices.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  @Deprecated
  Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Get all device paths and corresponding database paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult and the current offset of this region after traverse.
   */
  Pair<List<ShowDevicesResult>, Integer> getMatchedDevices(ShowDevicesPlan plan)
      throws MetadataException;
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
   * @param withTags whether returns tag kvs in the result list.
   */
  List<MeasurementPath> getMeasurementPaths(
      PartialPath pathPattern, boolean isPrefixMatch, boolean withTags) throws MetadataException;

  /**
   * Similar to method getMeasurementPaths(), but return Path with alias and filter the result by
   * limit and offset. If using prefix match, the path pattern is used to match prefix path. All
   * timeseries start with the matched prefix path will be collected.
   *
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException;

  List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException;

  /**
   * Show timeseries.
   *
   * @param plan
   * @param context
   * @throws MetadataException
   */
  Pair<List<ShowTimeSeriesResult>, Integer> showTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException;
  // endregion
  // endregion

  // region Interfaces and methods for MNode query
  IMNode getDeviceNode(PartialPath path) throws MetadataException;

  IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException;
  // endregion

  // region Interfaces for alias and tag/attribute operations
  @Deprecated
  void changeAlias(PartialPath path, String alias) throws MetadataException, IOException;

  /**
   * Upsert alias, tags and attributes key-value for the timeseries if the key has existed, just use
   * the new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  void upsertAliasAndTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or attributes already exist
   */
  void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags already exist
   */
  void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Drop tags or attributes of the timeseries. It will not throw exception even if the key does not
   * exist.
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags/attributes do not exist
   */
  void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or does not have tag/attribute or already has
   *     a tag/attribute named newKey
   */
  void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException;
  // endregion

  // region Interfaces for InsertPlan process

  DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean aligned)
      throws MetadataException;
  // endregion

  // region Interfaces for Template operations
  void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException;

  List<String> getPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException;

  long constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException;

  void rollbackSchemaBlackListWithTemplate(IRollbackPreDeactivateTemplatePlan plan)
      throws MetadataException;

  void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException;

  long countPathsUsingTemplate(int templateId, PathPatternTree patternTree)
      throws MetadataException;

  // endregion
}
