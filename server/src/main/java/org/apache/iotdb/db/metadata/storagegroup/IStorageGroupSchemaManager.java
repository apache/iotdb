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
package org.apache.iotdb.db.metadata.storagegroup;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This class declares all the interfaces for database management.
public interface IStorageGroupSchemaManager {

  void init() throws MetadataException, IOException;

  void forceLog();

  void clear() throws IOException;

  /**
   * create database of the given path to MTree.
   *
   * @param path database path
   */
  void setStorageGroup(PartialPath path) throws MetadataException;

  /** Delete databases of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3" */
  void deleteStorageGroup(PartialPath storageGroup) throws MetadataException;

  void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException;

  /** Check if the given path is database or not. */
  boolean isStorageGroup(PartialPath path);

  /** Check whether the given path contains a database */
  boolean checkStorageGroupByPath(PartialPath path);

  /**
   * Get database name by path
   *
   * <p>e.g., root.sg1 is a database and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return database in the given path
   */
  PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException;

  /**
   * Get the database that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all databases related to given path pattern
   */
  List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern) throws MetadataException;

  List<PartialPath> getInvolvedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /**
   * Get all database matching given path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores database paths matching given path pattern.
   */
  List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  /** Get all database paths */
  List<PartialPath> getAllStorageGroupPaths();

  /**
   * For a path, infer all databases it may belong to. The path can have wildcards. Resolve the path
   * or path pattern into StorageGroupName-FullPath pairs that FullPath matches the given path.
   *
   * <p>Consider the path into two parts: (1) the sub path which can not contain a database name and
   * (2) the sub path which is substring that begin after the database name.
   *
   * <p>(1) Suppose the part of the path can not contain a database name (e.g.,
   * "root".contains("root.sg") == false), then: For each one level wildcard *, only one level will
   * be inferred and the wildcard will be removed. For each multi level wildcard **, then the
   * inference will go on until the databases are found and the wildcard will be kept. (2) Suppose
   * the part of the path is a substring that begin after the database name. (e.g., For
   * "root.*.sg1.a.*.b.*" and "root.x.sg1" is a database, then this part is "a.*.b.*"). For this
   * part, keep what it is.
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
   * To calculate the count of database for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  /** Get database node by path. the give path don't need to be database path. */
  IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException;

  /** Get all database MNodes */
  List<IStorageGroupMNode> getAllStorageGroupNodes();

  /**
   * Different with LocalConfigNode.ensureStorageGroup, this method won't init storageGroup
   * resources and the input is the target database path.
   *
   * @param storageGroup database path
   */
  IStorageGroupMNode ensureStorageGroupByStorageGroupPath(PartialPath storageGroup)
      throws MetadataException;

  /**
   * Check whether the database of given path is set. The path may be a prefix path of some
   * database. Besides, the given path may be also beyond the MTreeAboveSG scope, then return true
   * if the covered part exists, which means there's database on this path. The rest part will be
   * checked by certain database subTree.
   *
   * @param path a full path or a prefix path
   */
  boolean isStorageGroupAlreadySet(PartialPath path);

  /**
   * To collect nodes in the given level for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All nodes start with the matched prefix path will be
   * collected. This method only count in nodes above database. Nodes below database, including
   * database node will be collected by certain SchemaRegion. The involved storage groups will be
   * collected to fetch schemaRegion.
   *
   * @param pathPattern a path pattern or a full path
   * @param nodeLevel the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException;

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above database. Nodes below database, including database node will be counted by certain
   * database.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [root.a, root.b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException;

  /**
   * Get child node path in the next level of the given path pattern. This method only count in
   * nodes above database. Nodes below database, including database node will be counted by certain
   * database.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.a.sg1.d1.s1, root.b.sg1.d1.s2, root.c.sg1.d2.s1] given path = root
   * return [a, b]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException;
}
