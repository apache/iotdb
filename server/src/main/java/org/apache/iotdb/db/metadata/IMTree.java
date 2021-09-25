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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Operations on the hierarchical struct of the Metadata Tree is defined in this interface. */
public interface IMTree extends Serializable {

  /**
   * Create a timeseries with a full path from root to leaf node Before creating a timeseries, the
   * storage group should be set first, throw exception otherwise
   *
   * @param path timeseries path
   * @param dataType data type
   * @param encoding encoding
   * @param compressor compressor
   * @param props props
   * @param alias alias of measurement
   */
  MeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException;

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  IMNode getDeviceNodeWithAutoCreating(PartialPath deviceId, int sgLevel) throws MetadataException;

  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  boolean isPathExist(PartialPath path);

  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  void setStorageGroup(PartialPath path) throws MetadataException;

  /** Delete a storage group */
  List<MeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException;

  /**
   * Check whether path is storage group or not
   *
   * <p>e.g., path = root.a.b.sg. if nor a and b is StorageGroupMNode and sg is a StorageGroupMNode
   * path is a storage group
   *
   * @param path path
   * @apiNote :for cluster
   */
  boolean isStorageGroup(PartialPath path);

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  Pair<PartialPath, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(PartialPath path)
      throws MetadataException;

  /**
   * Get measurement schema for a given path. Path must be a complete Path from root to leaf node.
   */
  MeasurementSchema getSchema(PartialPath path) throws MetadataException;

  /**
   * Get node by path with storage group check If storage group is not set,
   * StorageGroupNotSetException will be thrown
   */
  IMNode getNodeByPathWithStorageGroupCheck(PartialPath path) throws MetadataException;

  IMNode getNodeByPathWithStorageGroupCheckAndMemoryLock(PartialPath path) throws MetadataException;

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], throw exception Get storage group node, if the give path is not a storage group, throw
   * exception
   */
  StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException;

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node, the give path don't need to be
   * storage group path.
   */
  StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException;

  void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException;

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  IMNode getNodeByPath(PartialPath path) throws MetadataException;

  IMNode getNodeByPathWithMemoryLock(PartialPath path) throws MetadataException;

  Map<String, IMNode> getChildrenOfNodeByPath(PartialPath path) throws MetadataException;

  IMNode getChildMNodeInDeviceWithMemoryLock(IMNode deviceNode, String childName)
      throws MetadataException;

  IMNode getNodeDeepClone(IMNode mNode) throws MetadataException;

  /**
   * Get all storage groups under the given path
   *
   * @return storage group list
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(PartialPath path) throws MetadataException;

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  List<PartialPath> getAllStorageGroupPaths();

  /**
   * Get the storage group that given path belonged to or under given path All related storage
   * groups refer two cases: 1. Storage groups with a prefix that is identical to path, e.g. given
   * path "root.sg1", storage group "root.sg1.sg2" and "root.sg1.sg3" will be added into result
   * list. 2. Storage group that this path belongs to, e.g. given path "root.sg1.d1", and it is in
   * storage group "root.sg1". Then we adds "root.sg1" into result list.
   *
   * @return a list contains all storage groups related to given path
   */
  List<PartialPath> searchAllRelatedStorageGroups(PartialPath path) throws MetadataException;

  /**
   * Get all storage group under given path
   *
   * @return a list contains all storage group names under give path
   */
  List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException;

  /** Get all storage group MNodes */
  List<StorageGroupMNode> getAllStorageGroupNodes();

  /**
   * Get storage group path by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException;

  /** Check whether the given path contains a storage group */
  boolean checkStorageGroupByPath(PartialPath path);

  /**
   * Get all timeseries under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException;

  /**
   * Get all timeseries paths under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   * @return Pair.left contains all the satisfied paths Pair.right means the current offset or zero
   *     if we don't set offset.
   */
  Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath prefixPath, int limit, int offset) throws MetadataException;

  /**
   * Get the count of timeseries under the given prefix path. if prefixPath contains '*', then not
   * throw PathNotExistException()
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException;

  /**
   * Get the count of devices under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getDevicesNum(PartialPath prefixPath) throws MetadataException;

  /**
   * Get the count of storage group under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getStorageGroupNum(PartialPath prefixPath) throws MetadataException;

  /** Get the count of nodes in the given level under the given prefix path. */
  int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException;

  /**
   * Get all time series schema under the given path order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException;

  /**
   * Get all time series schema under the given path
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException;

  List<Pair<PartialPath, String[]>> getAllMeasurementSchema(
      ShowTimeSeriesPlan plan, boolean removeCurrentOffset) throws MetadataException;

  /**
   * Get child node path in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param path The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException;

  /**
   * Get child node in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2]
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1.d1
   * return [s1, s2]
   *
   * @param path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException;

  /**
   * Get all devices under give path
   *
   * @return a list contains all distinct devices names
   */
  Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException;

  List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException;

  /** Get all paths from root to the given level. */
  List<PartialPath> getNodesList(PartialPath path, int nodeLevel) throws MetadataException;

  /** Get all paths from root to the given level */
  List<PartialPath> getNodesList(
      PartialPath path, int nodeLevel, MManager.StorageGroupFilter filter) throws MetadataException;

  Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException;

  int getMeasurementMNodeCount(PartialPath path) throws MetadataException;

  Collection<MeasurementMNode> collectMeasurementMNode(IMNode startingNode);

  void updateMNode(IMNode mNode) throws MetadataException;

  IMNode lockMNode(IMNode mNode) throws MetadataException;

  void unlockMNode(IMNode mNode) throws MetadataException;

  void serializeTo(String snapshotPath) throws IOException;

  void createSnapshot() throws IOException;

  void clear();
}
