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
package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class REntityMNode extends RInternalMNode implements IEntityMNode {

  private transient volatile Map<String, IMeasurementMNode> aliasChildren = null;

  private volatile boolean isAligned = false;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public REntityMNode(String fullPath) {
    super(fullPath);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name))
        || (aliasChildren != null && aliasChildren.containsKey(name));
  }

  /** get the child with the name */
  @Override
  public IMNode getChild(String name) {
    IMNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    if (child != null) {
      return child;
    }
    return aliasChildren == null ? null : aliasChildren.get(name);
  }

  @Override
  public boolean addAlias(String alias, IMeasurementMNode child) {
    if (aliasChildren == null) {
      // double check, alias children volatile
      synchronized (this) {
        if (aliasChildren == null) {
          aliasChildren = new ConcurrentHashMap<>();
        }
      }
    }

    return aliasChildren.computeIfAbsent(alias, aliasName -> child) == child;
  }

  @Override
  public void deleteAliasChild(String alias) {
    // Don't do any update in the MNode related class, it won't persistent in memory or on disk
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, IMeasurementMNode> getAliasChildren() {
    if (aliasChildren == null) {
      return Collections.emptyMap();
    }
    return aliasChildren;
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren) {
    this.aliasChildren = aliasChildren;
  }

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean isAligned) {
    this.isAligned = isAligned;
  }

  @Override
  public ILastCacheContainer getLastCacheContainer(String measurementId) {
    return null;
  }

  @Override
  public Map<String, ILastCacheContainer> getTemplateLastCaches() {
    return null;
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}
}
