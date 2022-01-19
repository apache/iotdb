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
package org.apache.iotdb.db.metadata.mtree.store.disk;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class CachedMNodeContainer implements ICachedMNodeContainer {

  private long segmentAddress = -1;

  private Map<String, IMNode> childCache = null;
  private Map<String, IMNode> newChildBuffer = null;
  private Map<String, IMNode> updatedChildBuffer = null;

  @Override
  public int size() {
    return getSize(childCache) + getSize(newChildBuffer) + getSize(updatedChildBuffer);
  }

  private int getSize(Map<String, IMNode> map) {
    return map == null ? 0 : map.size();
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(childCache) && isEmpty(newChildBuffer) && isEmpty(updatedChildBuffer);
  }

  private boolean isEmpty(Map<String, IMNode> map) {
    return map == null || map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return containsKey(childCache, key)
        || containsKey(newChildBuffer, key)
        || containsKey(updatedChildBuffer, key);
  }

  private boolean containsKey(Map<String, IMNode> map, Object key) {
    return map != null && map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return containsValue(childCache, value)
        || containsValue(newChildBuffer, value)
        || containsValue(updatedChildBuffer, value);
  }

  private boolean containsValue(Map<String, IMNode> map, Object value) {
    return map != null && map.containsValue(value);
  }

  @Override
  public IMNode get(Object key) {
    IMNode result = get(childCache, key);
    if (result != null) {
      return result;
    }
    result = get(newChildBuffer, key);
    if (result != null) {
      return result;
    }
    return get(updatedChildBuffer, key);
  }

  private IMNode get(Map<String, IMNode> map, Object key) {
    return map == null ? null : map.get(key);
  }

  @Nullable
  @Override
  public IMNode put(String key, IMNode value) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    return newChildBuffer.put(key, value);
  }

  @Override
  public IMNode remove(Object key) {
    IMNode result = remove(childCache, key);
    if (result == null) {
      result = remove(newChildBuffer, key);
    }
    if (result == null) {
      result = remove(updatedChildBuffer, key);
    }
    return result;
  }

  private IMNode remove(Map<String, IMNode> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public void putAll(@NotNull Map<? extends String, ? extends IMNode> m) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    newChildBuffer.putAll(m);
  }

  @Override
  public void clear() {
    childCache = null;
    newChildBuffer = null;
    updatedChildBuffer = null;
  }

  @NotNull
  @Override
  public Set<String> keySet() {
    Set<String> result = new TreeSet<>();
    result.addAll(keySet(childCache));
    result.addAll(keySet(newChildBuffer));
    result.addAll(keySet(updatedChildBuffer));
    return result;
  }

  private Set<String> keySet(Map<String, IMNode> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  @NotNull
  @Override
  public Collection<IMNode> values() {
    Collection<IMNode> result = new ArrayList<>();
    result.addAll(getValues(childCache));
    result.addAll(getValues(newChildBuffer));
    result.addAll(getValues(updatedChildBuffer));
    return result;
  }

  private Collection<IMNode> getValues(Map<String, IMNode> map) {
    return map == null ? Collections.emptyList() : map.values();
  }

  @NotNull
  @Override
  public Set<Entry<String, IMNode>> entrySet() {
    Set<Entry<String, IMNode>> result = new HashSet<>();
    result.addAll(entrySet(childCache));
    result.addAll(entrySet(newChildBuffer));
    result.addAll(entrySet(updatedChildBuffer));
    return result;
  }

  private Set<Entry<String, IMNode>> entrySet(Map<String, IMNode> map) {
    return map == null ? Collections.emptySet() : map.entrySet();
  }

  @Nullable
  @Override
  public IMNode replace(String key, IMNode value) {
    IMNode replacedOne = replace(childCache, key, value);
    if (replacedOne == null) {
      replacedOne = replace(newChildBuffer, key, value);
    }
    if (replacedOne == null) {
      replacedOne = replace(updatedChildBuffer, key, value);
    }
    return replacedOne;
  }

  private IMNode replace(Map<String, IMNode> map, String key, IMNode value) {
    return map == null ? null : map.replace(key, value);
  }

  @Override
  public long getSegmentAddress() {
    return segmentAddress;
  }

  @Override
  public void setSegmentAddress(long segmentAddress) {
    this.segmentAddress = segmentAddress;
  }

  @Override
  public boolean isVolatile() {
    return segmentAddress == -1;
  }

  @Override
  public boolean isFull() {
    return true;
  }

  @Override
  public boolean isExpelled() {
    return !isVolatile()
        && isEmpty(childCache)
        && isEmpty(newChildBuffer)
        && isEmpty(updatedChildBuffer);
  }

  @Override
  public Iterator<IMNode> getChildrenIterator() {
    return new CachedMNodeContainerIterator();
  }

  @Override
  public Map<String, IMNode> getChildCache() {
    return childCache == null ? Collections.emptyMap() : childCache;
  }

  @Override
  public Map<String, IMNode> getNewChildBuffer() {
    return newChildBuffer == null ? Collections.emptyMap() : newChildBuffer;
  }

  @Override
  public Map<String, IMNode> getUpdatedChildBuffer() {
    return updatedChildBuffer == null ? Collections.emptyMap() : updatedChildBuffer;
  }

  @Override
  public void loadChildrenFromDisk(Map<String, IMNode> children) {
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.putAll(children);
  }

  @Override
  public void addChildToCache(IMNode node) {
    String name = node.getName();
    if (containsKey(name)) {
      return;
    }
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.put(name, node);
  }

  @Override
  public void appendMNode(IMNode node) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    newChildBuffer.put(node.getName(), node);
  }

  @Override
  public void updateMNode(String name) {
    IMNode node = remove(childCache, name);
    if (node != null) {
      if (updatedChildBuffer == null) {
        updatedChildBuffer = new ConcurrentHashMap<>();
      }
      updatedChildBuffer.put(name, node);
    }
  }

  @Override
  public void moveMNodeToCache(String name) {
    IMNode node = remove(newChildBuffer, name);
    if (node == null) {
      node = remove(updatedChildBuffer, name);
    }
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.put(name, node);
  }

  @Override
  public void evictMNode(String name) {
    IMNode result = remove(childCache, name);
    if (result == null) {
      result = remove(newChildBuffer, name);
    }
    if (result == null) {
      remove(updatedChildBuffer, name);
    }
  }

  private class CachedMNodeContainerIterator implements Iterator<IMNode> {

    Iterator<IMNode> iterator;
    byte status = 0;

    CachedMNodeContainerIterator() {
      iterator = getChildCache().values().iterator();
    }

    @Override
    public boolean hasNext() {
      if (iterator.hasNext()) {
        return true;
      }
      while (!iterator.hasNext()) {
        if (!changeStatus()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public IMNode next() {
      return iterator.next();
    }

    private boolean changeStatus() {
      switch (status) {
        case 0:
          iterator = getNewChildBuffer().values().iterator();
          status = 1;
          return true;
        case 1:
          iterator = getUpdatedChildBuffer().values().iterator();
          status = 2;
          return true;
        default:
          return false;
      }
    }
  }
}
