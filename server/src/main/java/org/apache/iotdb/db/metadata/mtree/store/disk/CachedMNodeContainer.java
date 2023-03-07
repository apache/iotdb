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

import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.newnode.ICacheMNode;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptySet;

public class CachedMNodeContainer implements ICachedMNodeContainer {

  private long segmentAddress = -1;

  private Map<String, ICacheMNode> childCache = null;
  private Map<String, ICacheMNode> newChildBuffer = null;
  private Map<String, ICacheMNode> updatedChildBuffer = null;

  private static final IMNodeContainer<ICacheMNode> EMPTY_CONTAINER =
      new CachedMNodeContainer.EmptyContainer();

  public static IMNodeContainer<ICacheMNode> emptyMNodeContainer() {
    return EMPTY_CONTAINER;
  }

  @Override
  public int size() {
    return getSize(childCache) + getSize(newChildBuffer) + getSize(updatedChildBuffer);
  }

  private int getSize(Map<String, ICacheMNode> map) {
    return map == null ? 0 : map.size();
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(childCache) && isEmpty(newChildBuffer) && isEmpty(updatedChildBuffer);
  }

  private boolean isEmpty(Map<String, ICacheMNode> map) {
    return map == null || map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return containsKey(childCache, key)
        || containsKey(newChildBuffer, key)
        || containsKey(updatedChildBuffer, key);
  }

  private boolean containsKey(Map<String, ICacheMNode> map, Object key) {
    return map != null && map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return containsValue(childCache, value)
        || containsValue(newChildBuffer, value)
        || containsValue(updatedChildBuffer, value);
  }

  private boolean containsValue(Map<String, ICacheMNode> map, Object value) {
    return map != null && map.containsValue(value);
  }

  @Override
  public ICacheMNode get(Object key) {
    ICacheMNode result = get(childCache, key);
    if (result != null) {
      return result;
    }
    result = get(newChildBuffer, key);
    if (result != null) {
      return result;
    }
    return get(updatedChildBuffer, key);
  }

  private ICacheMNode get(Map<String, ICacheMNode> map, Object key) {
    return map == null ? null : map.get(key);
  }

  @Nullable
  @Override
  public synchronized ICacheMNode put(String key, ICacheMNode value) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    return newChildBuffer.put(key, value);
  }

  @Nullable
  @Override
  public synchronized ICacheMNode putIfAbsent(String key, ICacheMNode value) {

    ICacheMNode node = get(key);
    if (node == null) {
      if (newChildBuffer == null) {
        newChildBuffer = new ConcurrentHashMap<>();
      }
      node = newChildBuffer.put(key, value);
    }

    return node;
  }

  @Override
  public synchronized ICacheMNode remove(Object key) {
    ICacheMNode result = remove(childCache, key);
    if (result == null) {
      result = remove(newChildBuffer, key);
    }
    if (result == null) {
      result = remove(updatedChildBuffer, key);
    }
    return result;
  }

  private ICacheMNode remove(Map<String, ICacheMNode> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public synchronized void putAll(@Nonnull Map<? extends String, ? extends ICacheMNode> m) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    newChildBuffer.putAll(m);
  }

  @Override
  public synchronized void clear() {
    childCache = null;
    newChildBuffer = null;
    updatedChildBuffer = null;
  }

  @Nonnull
  @Override
  public Set<String> keySet() {
    Set<String> result = new TreeSet<>();
    result.addAll(keySet(childCache));
    result.addAll(keySet(newChildBuffer));
    result.addAll(keySet(updatedChildBuffer));
    return result;
  }

  private Set<String> keySet(Map<String, ICacheMNode> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  @Nonnull
  @Override
  public Collection<ICacheMNode> values() {
    Collection<ICacheMNode> result = new ArrayList<>();
    result.addAll(getValues(childCache));
    result.addAll(getValues(newChildBuffer));
    result.addAll(getValues(updatedChildBuffer));
    return result;
  }

  private Collection<ICacheMNode> getValues(Map<String, ICacheMNode> map) {
    return map == null ? Collections.emptyList() : map.values();
  }

  @Nonnull
  @Override
  public Set<Entry<String, ICacheMNode>> entrySet() {
    Set<Entry<String, ICacheMNode>> result = new HashSet<>();
    result.addAll(entrySet(childCache));
    result.addAll(entrySet(newChildBuffer));
    result.addAll(entrySet(updatedChildBuffer));
    return result;
  }

  private Set<Entry<String, ICacheMNode>> entrySet(Map<String, ICacheMNode> map) {
    return map == null ? Collections.emptySet() : map.entrySet();
  }

  @Nullable
  @Override
  public synchronized ICacheMNode replace(String key, ICacheMNode value) {
    ICacheMNode replacedOne = replace(childCache, key, value);
    if (replacedOne == null) {
      replacedOne = replace(newChildBuffer, key, value);
    }
    if (replacedOne == null) {
      replacedOne = replace(updatedChildBuffer, key, value);
    }
    return replacedOne;
  }

  private ICacheMNode replace(Map<String, ICacheMNode> map, String key, ICacheMNode value) {
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
  public boolean hasChildInNewChildBuffer(String name) {
    return containsKey(newChildBuffer, name);
  }

  @Override
  public boolean hasChildInBuffer(String name) {
    return containsKey(updatedChildBuffer, name) || containsKey(newChildBuffer, name);
  }

  @Override
  public Iterator<ICacheMNode> getChildrenIterator() {
    return new CachedMNodeContainerIterator();
  }

  @Override
  public Iterator<ICacheMNode> getChildrenBufferIterator() {
    return new BufferIterator();
  }

  @Override
  public Iterator<ICacheMNode> getNewChildBufferIterator() {
    return getNewChildBuffer().values().iterator();
  }

  @Override
  public Map<String, ICacheMNode> getChildCache() {
    return childCache == null ? Collections.emptyMap() : childCache;
  }

  @Override
  public Map<String, ICacheMNode> getNewChildBuffer() {
    return newChildBuffer == null ? Collections.emptyMap() : newChildBuffer;
  }

  @Override
  public Map<String, ICacheMNode> getUpdatedChildBuffer() {
    return updatedChildBuffer == null ? Collections.emptyMap() : updatedChildBuffer;
  }

  @Override
  public synchronized void loadChildrenFromDisk(Map<String, ICacheMNode> children) {
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.putAll(children);
  }

  @Override
  public synchronized void addChildToCache(ICacheMNode node) {
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
  public synchronized void appendMNode(ICacheMNode node) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    newChildBuffer.put(node.getName(), node);
  }

  @Override
  public synchronized void updateMNode(String name) {
    ICacheMNode node = remove(childCache, name);
    if (node != null) {
      if (updatedChildBuffer == null) {
        updatedChildBuffer = new ConcurrentHashMap<>();
      }
      updatedChildBuffer.put(name, node);
    }
  }

  @Override
  public synchronized void moveMNodeToCache(String name) {
    ICacheMNode node = remove(newChildBuffer, name);
    if (node == null) {
      node = remove(updatedChildBuffer, name);
    }
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.put(name, node);
  }

  @Override
  public synchronized void evictMNode(String name) {
    remove(childCache, name);
  }

  public synchronized String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("CachedMNodeContainer:{");
    builder.append("childCache:[");
    for (ICacheMNode node : getValues(childCache)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("newChildBuffer:[");
    for (ICacheMNode node : getValues(newChildBuffer)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("updateChildBuffer:[");
    for (ICacheMNode node : getValues(updatedChildBuffer)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("}");
    return builder.toString();
  }

  private class CachedMNodeContainerIterator implements Iterator<ICacheMNode> {

    Iterator<ICacheMNode> iterator;
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
    public ICacheMNode next() {
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

  private class BufferIterator implements Iterator<ICacheMNode> {
    Iterator<ICacheMNode> iterator;
    Iterator<ICacheMNode> newBufferIterator;
    Iterator<ICacheMNode> updateBufferIterator;
    byte status = 0;

    BufferIterator() {
      newBufferIterator = getNewChildBuffer().values().iterator();
      updateBufferIterator = getUpdatedChildBuffer().values().iterator();
      iterator = newBufferIterator;
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
    public ICacheMNode next() {
      return iterator.next();
    }

    private boolean changeStatus() {
      if (status == 0) {
        iterator = updateBufferIterator;
        status = 1;
        return true;
      }
      return false;
    }
  }

  private static class EmptyContainer extends AbstractMap<String, ICacheMNode>
      implements IMNodeContainer<ICacheMNode> {

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean containsKey(Object key) {
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return false;
    }

    @Override
    public ICacheMNode get(Object key) {
      return null;
    }

    @Override
    @NotNull
    public Set<String> keySet() {
      return emptySet();
    }

    @Override
    @NotNull
    public Collection<ICacheMNode> values() {
      return emptySet();
    }

    @NotNull
    public Set<Map.Entry<String, ICacheMNode>> entrySet() {
      return emptySet();
    }

    @Override
    public boolean equals(Object o) {
      return o == this;
    }
  }
}
