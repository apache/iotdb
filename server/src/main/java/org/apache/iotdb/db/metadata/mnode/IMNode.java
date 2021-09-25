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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.metadisk.cache.CacheEntry;
import org.apache.iotdb.db.metadata.metadisk.metafile.IPersistenceInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * This interface is the interface of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public interface IMNode extends Serializable {

  /** check whether the MNode has a child with the name or alias */
  boolean hasChild(String name);

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   */
  void addChild(String name, IMNode child);

  /**
   * Add a child to the current mnode.
   *
   * <p>This method will not take the child's name as one of the inputs and will also make this
   * Mnode be child node's parent. All is to reduce the probability of mistaken by users and be more
   * convenient for users to use. And the return of this method is used to conveniently construct a
   * chain of time series for users.
   *
   * @param child child's node
   * @return return the MNode already added
   */
  IMNode addChild(IMNode child);

  /** delete a child */
  void deleteChild(String name);

  /** delete the alias of a child */
  void deleteAliasChild(String alias);

  /** get the child with the name */
  IMNode getChild(String name);

  /**
   * get the count of all MeasurementMNode whose ancestor is current node Attention!!! the child
   * MNode may be not loaded, so it's better to use getMeasurementMNodeCount(PartialPath path)
   * provided by MTree
   */
  int getMeasurementMNodeCount();

  /** add an alias */
  boolean addAlias(String alias, IMNode child);

  /** get full path */
  String getFullPath();

  PartialPath getPartialPath();

  /** get parent */
  IMNode getParent();

  void setParent(IMNode parent);

  Map<String, IMNode> getChildren();

  Map<String, IMNode> getAliasChildren();

  void setChildren(Map<String, IMNode> children);

  void setAliasChildren(Map<String, IMNode> aliasChildren);

  String getName();

  void setName(String name);

  void serializeTo(MLogWriter logWriter) throws IOException;

  void replaceChild(String measurement, IMNode newChildNode);

  /** whether this is an instance of StorageGroupMNode */
  boolean isStorageGroup();

  /** whether this is an instance of MeasurementMNode */
  boolean isMeasurement();

  /**
   * whether this MNode's data has been loaded from the disk, to other words, whether this is an
   * instance of PersistenceMNode
   */
  boolean isLoaded();

  /** whether this mnode has been persisted to the disk */
  boolean isPersisted();

  /** get the information about the mnode on disk */
  IPersistenceInfo getPersistenceInfo();

  void setPersistenceInfo(IPersistenceInfo persistenceInfo);

  /** get the information about the mnode in cache */
  CacheEntry getCacheEntry();

  void setCacheEntry(CacheEntry cacheEntry);

  /** whether the mnode is cached in memory */
  boolean isCached();

  /**
   * when a mnode is going to be evicted from memory, get its evictionHolder which is simply a mnode
   * with only its persistence information
   */
  IMNode getEvictionHolder();

  /**
   * evict one child and replace it in children map with its evictionHolder to avoid
   * NullPointerException of ConcurrentHashmap
   */
  void evictChild(String name);

  /** whether this mnode has been locked in memory */
  boolean isLockedInMemory();

  /** whether the update on a node is necessary to sync to the mtree */
  boolean isDeleted();

  /** get a cloned instance of this mnode */
  IMNode clone();
}
