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
package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMTreeBelowSG {
  void clear();

  /**
   * Create MTree snapshot
   *
   * @param snapshotDir specify snapshot directory
   * @return false if failed to create snapshot; true if success
   */
  boolean createSnapshot(File snapshotDir);

  IMeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException;

  /**
   * Create aligned timeseries with full paths from root to one leaf node. Before creating
   * timeseries, the * database should be set first, throw exception otherwise
   *
   * @param devicePath device path
   * @param measurements measurements list
   * @param dataTypes data types list
   * @param encodings encodings list
   * @param compressors compressor
   */
  List<IMeasurementMNode> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException;

  /**
   * Check if measurements under device exists in MTree
   *
   * @param devicePath device full path
   * @param measurementList measurements list
   * @param aliasList alias of measurement
   * @return If all measurements not exists, return empty map. Otherwise, return a map whose key is
   *     index of measurement in list and value is exception.
   */
  Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList);

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  Pair<PartialPath, IMeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(PartialPath path)
      throws MetadataException;

  boolean isEmptyInternalMNode(IMNode node) throws MetadataException;

  /**
   * Get all pre-deleted timeseries matched by given pathPattern. For example, given path pattern
   * root.sg.*.s1 and pre-deleted timeseries root.sg.d1.s1, root.sg.d2.s1, then the result set is
   * {root.sg.d1.s1, root.sg.d2.s1}.
   *
   * @param pathPattern path pattern
   * @return all pre-deleted timeseries matched by given pathPattern
   */
  List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern) throws MetadataException;

  /**
   * Get all devices of pre-deleted timeseries matched by given pathPattern. For example, given path
   * pattern root.sg.*.s1 and pre-deleted timeseries root.sg.d1.s1, root.sg.d2.s1, then the result
   * set is {root.sg.d1, root.sg.d2}.
   *
   * @param pathPattern path pattern
   * @return all devices of pre-deleted timeseries matched by given pathPattern
   */
  Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException;

  void setAlias(IMeasurementMNode measurementMNode, String alias) throws MetadataException;

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  IMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException;

  IEntityMNode setToEntity(IMNode node) throws MetadataException;

  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  boolean isPathExist(PartialPath path) throws MetadataException;

  /**
   * Get all devices matching the given path pattern. If isPrefixMatch, then the devices under the
   * paths matching given path pattern will be collected too.
   *
   * @return a list contains all distinct devices names
   */
  Set<PartialPath> getDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  Pair<List<ShowDevicesResult>, Integer> getDevices(ShowDevicesPlan plan) throws MetadataException;

  Set<PartialPath> getDevicesByTimeseries(PartialPath timeseries) throws MetadataException;

  /**
   * Get all measurement paths matching the given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected and return.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Get all measurement paths matching the given path pattern
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   */
  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern) throws MetadataException;

  /**
   * Get all measurement paths matching the given path pattern If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected and return.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param limit the limit of query result.
   * @param offset the offset.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @param withTags whether returns all the tags of each timeseries as well.
   * @return Pair.left contains all the satisfied path Pair.right means the current offset or zero
   *     if we don't set offset.
   */
  Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException;

  /**
   * Fetch all measurement path
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param templateMap <TemplateId, Template>
   * @param withTags whether returns all the tags of each timeseries as well.
   * @return schema
   */
  List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException;

  /**
   * Get all measurement schema matching the given path pattern
   *
   * <p>result: [name, alias, database, dataType, encoding, compression, offset] and the current
   * offset
   */
  Pair<List<Pair<PartialPath, String[]>>, Integer> getAllMeasurementSchema(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException;

  /**
   * Get child node path in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException;

  /**
   * Get child node in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2]
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1.d1
   * return [s1, s2]
   *
   * @param pathPattern Path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException;

  /** Get all paths from root to the given level */
  List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException;

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param templateMap <TemplateId, Template>
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  int getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException;

  /**
   * Get the count of timeseries matching the given path by tag.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  int getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, List<String> timeseries, boolean hasTag)
      throws MetadataException;

  /**
   * Get the count of devices matching the given path. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  /**
   * Get the count of devices matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  int getDevicesNum(PartialPath pathPattern) throws MetadataException;

  /**
   * Get the count of nodes in the given level matching the given path. If using prefix match, the
   * path pattern is used to match prefix path. All timeseries start with the matched prefix path
   * will be counted.
   */
  int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException;

  Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException;

  Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      List<String> timeseries,
      boolean hasTag)
      throws MetadataException;

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  IMNode getNodeByPath(PartialPath path) throws MetadataException;

  IMeasurementMNode getMeasurementMNode(PartialPath path) throws MetadataException;

  List<IMeasurementMNode> getAllMeasurementMNode() throws MetadataException;

  /**
   * Get IMeasurementMNode by path pattern
   *
   * @param pathPattern full path or path pattern with wildcard
   * @return list of IMeasurementMNode
   */
  List<IMeasurementMNode> getMatchedMeasurementMNode(PartialPath pathPattern)
      throws MetadataException;

  void activateTemplate(PartialPath activatePath, Template template) throws MetadataException;

  List<String> getPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException;

  int countPathsUsingTemplate(PartialPath pathPattern, int templateId) throws MetadataException;
}
