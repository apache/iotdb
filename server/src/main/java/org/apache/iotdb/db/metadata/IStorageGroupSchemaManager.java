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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This class declares all the interfaces for storage group management.
public interface IStorageGroupSchemaManager {

  void init() throws MetadataException, IOException;

  void clear() throws IOException;

  /**
   * Set storage group of the given path to MTree.
   *
   * @param path storage group path
   */
  void setStorageGroup(PartialPath path) throws MetadataException;

  /**
   * Get the target SchemaRegion, which the given path belongs to. The path must be a fullPath
   * without wildcards, * or **. This method is the first step when there's a task on one certain
   * path, e.g., root.sg1 is a storage group and path = root.sg1.d1, return SchemaRegion of
   * root.sg1. If there's no storage group on the given path, StorageGroupNotSetException will be
   * thrown.
   */
  SchemaRegion getBelongedSchemaRegion(PartialPath path) throws MetadataException;

  /**
   * Get SchemaRegion, which the given path represented.
   *
   * @param path the path of the target storage group
   */
  SchemaRegion getSchemaRegionByStorageGroupPath(PartialPath path) throws MetadataException;

  /**
   * Get the target SchemaRegion, which will be involved/covered by the given pathPattern. The path
   * may contain wildcards, * or **. This method is the first step when there's a task on multiple
   * paths represented by the given pathPattern. If isPrefixMatch, all storage groups under the
   * prefixPath that matches the given pathPattern will be collected.
   */
  List<SchemaRegion> getInvolvedSchemaRegions(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<SchemaRegion> getAllSchemaRegions();

  /**
   * Delete storage groups of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3"
   */
  void deleteStorageGroup(PartialPath storageGroup) throws MetadataException;

  /** Check if the given path is storage group or not. */
  boolean isStorageGroup(PartialPath path);

  /** Check whether the given path contains a storage group */
  boolean checkStorageGroupByPath(PartialPath path);

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException;

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
  List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern) throws MetadataException;

  /**
   * Get all storage group matching given path pattern. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
  List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /** Get all storage group paths */
  List<PartialPath> getAllStorageGroupPaths();

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
  Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path) throws MetadataException;

  /**
   * To calculate the count of storage group for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg],
   * return the MNode of root.sg Get storage group node by path. Give path like [root, sg, device],
   * MNodeTypeMismatchException will be thrown. If storage group is not set,
   * StorageGroupNotSetException will be thrown.
   */
  IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException;

  /** Get storage group node by path. the give path don't need to be storage group path. */
  IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException;

  /** Get all storage group MNodes */
  List<IStorageGroupMNode> getAllStorageGroupNodes();

  /**
   * Check whether the storage group of given path is set. The path may be a prefix path of some
   * storage group. Besides, the given path may be also beyond the MTreeAboveSG scope, then return
   * true if the covered part exists, which means there's storage group on this path. The rest part
   * will be checked by certain storage group subTree.
   *
   * @param path a full path or a prefix path
   */
  boolean isStorageGroupAlreadySet(PartialPath path);

  /**
   * To calculate the count of nodes in the given level for given path pattern. If using prefix
   * match, the path pattern is used to match prefix path. All nodes start with the matched prefix
   * path will be counted. This method only count in nodes above storage group. Nodes below storage
   * group, including storage group node will be counted by certain SchemaRegion. The involved
   * storage groups will be collected to count nodes below storage group.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  Pair<Integer, List<SchemaRegion>> getNodesCountInGivenLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException;

  Pair<List<PartialPath>, List<SchemaRegion>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
      throws MetadataException;

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain SchemaRegion.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [root.a, root.b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Pair<Set<String>, List<SchemaRegion>> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException;

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above storage group. Nodes below storage group, including storage group node will be
   * counted by certain SchemaRegion.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [a, b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Pair<Set<String>, List<SchemaRegion>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException;

  /** Get metadata in string */
  String getMetadataInString();
}
