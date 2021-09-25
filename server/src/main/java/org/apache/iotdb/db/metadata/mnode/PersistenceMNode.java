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
import java.util.Collections;
import java.util.Map;

public class PersistenceMNode implements IPersistenceInfo, IMNode {

  /** offset in metafile */
  private long position;

  public PersistenceMNode() {}

  public PersistenceMNode(long position) {
    this.position = position;
  }

  public PersistenceMNode(IPersistenceInfo persistenceInfo) {
    position = persistenceInfo.getPosition();
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void setPosition(long position) {
    this.position = position;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public boolean isLoaded() {
    return false;
  }

  @Override
  public boolean isPersisted() {
    return true;
  }

  @Override
  public IPersistenceInfo getPersistenceInfo() {
    return this;
  }

  @Override
  public void setPersistenceInfo(IPersistenceInfo persistenceInfo) {
    if (persistenceInfo == null) {
      position = -1;
    } else {
      position = persistenceInfo.getPosition();
    }
  }

  @Override
  public IMNode getEvictionHolder() {
    return this;
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public void addChild(String name, IMNode child) {}

  @Override
  public IMNode addChild(IMNode child) {
    return null;
  }

  @Override
  public void deleteChild(String name) {}

  @Override
  public void deleteAliasChild(String alias) {}

  @Override
  public IMNode getChild(String name) {
    return null;
  }

  @Override
  public int getMeasurementMNodeCount() {
    return 0;
  }

  @Override
  public boolean addAlias(String alias, IMNode child) {
    return false;
  }

  @Override
  public String getFullPath() {
    return null;
  }

  @Override
  public PartialPath getPartialPath() {
    return null;
  }

  @Override
  public IMNode getParent() {
    return null;
  }

  @Override
  public void setParent(IMNode parent) {}

  @Override
  public Map<String, IMNode> getChildren() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, IMNode> getAliasChildren() {
    return Collections.emptyMap();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {}

  @Override
  public void setAliasChildren(Map<String, IMNode> aliasChildren) {}

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void setName(String name) {}

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}

  @Override
  public void replaceChild(String measurement, IMNode newChildNode) {}

  @Override
  public CacheEntry getCacheEntry() {
    return null;
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {}

  @Override
  public boolean isCached() {
    return false;
  }

  @Override
  public void evictChild(String name) {}

  @Override
  public boolean isLockedInMemory() {
    return false;
  }

  @Override
  public boolean isDeleted() {
    return position == -1;
  }

  @Override
  public IMNode clone() {
    return new PersistenceMNode(position);
  }
}
