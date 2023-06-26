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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

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

  private Map<String, ICachedMNode> childCache = null;
  private Map<String, ICachedMNode> newChildBuffer = null;
  private Map<String, ICachedMNode> updatedChildBuffer = null;

  private static final IMNodeContainer<ICachedMNode> EMPTY_CONTAINER =
      new CachedMNodeContainer.EmptyContainer();

  public static IMNodeContainer<ICachedMNode> emptyMNodeContainer() {
    return EMPTY_CONTAINER;
  }

  @Override
  public int size() {
    return getSize(childCache) + getSize(newChildBuffer) + getSize(updatedChildBuffer);
  }

  private int getSize(Map<String, ICachedMNode> map) {
    return map == null ? 0 : map.size();
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(childCache) && isEmpty(newChildBuffer) && isEmpty(updatedChildBuffer);
  }

  private boolean isEmpty(Map<String, ICachedMNode> map) {
    return map == null || map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return containsKey(childCache, key)
        || containsKey(newChildBuffer, key)
        || containsKey(updatedChildBuffer, key);
  }

  private boolean containsKey(Map<String, ICachedMNode> map, Object key) {
    return map != null && map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return containsValue(childCache, value)
        || containsValue(newChildBuffer, value)
        || containsValue(updatedChildBuffer, value);
  }

  private boolean containsValue(Map<String, ICachedMNode> map, Object value) {
    return map != null && map.containsValue(value);
  }

  @Override
  public ICachedMNode get(Object key) {
    ICachedMNode result = get(childCache, key);
    if (result != null) {
      return result;
    }
    result = get(newChildBuffer, key);
    if (result != null) {
      return result;
    }
    return get(updatedChildBuffer, key);
  }

  private ICachedMNode get(Map<String, ICachedMNode> map, Object key) {
    return map == null ? null : map.get(key);
  }

  @Nullable
  @Override
  public synchronized ICachedMNode put(String key, ICachedMNode value) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    return newChildBuffer.put(key, value);
  }

  @Nullable
  @Override
  public synchronized ICachedMNode putIfAbsent(String key, ICachedMNode value) {

    ICachedMNode node = get(key);
    if (node == null) {
      if (newChildBuffer == null) {
        newChildBuffer = new ConcurrentHashMap<>();
      }
      node = newChildBuffer.put(key, value);
    }

    return node;
  }

  @Override
  public synchronized ICachedMNode remove(Object key) {
    ICachedMNode result = remove(childCache, key);
    if (result == null) {
      result = remove(newChildBuffer, key);
    }
    if (result == null) {
      result = remove(updatedChildBuffer, key);
    }
    return result;
  }

  private ICachedMNode remove(Map<String, ICachedMNode> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public synchronized void putAll(@Nonnull Map<? extends String, ? extends ICachedMNode> m) {
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

  private Set<String> keySet(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  @Nonnull
  @Override
  public Collection<ICachedMNode> values() {
    Collection<ICachedMNode> result = new ArrayList<>();
    result.addAll(getValues(childCache));
    result.addAll(getValues(newChildBuffer));
    result.addAll(getValues(updatedChildBuffer));
    return result;
  }

  private Collection<ICachedMNode> getValues(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptyList() : map.values();
  }

  @Nonnull
  @Override
  public Set<Entry<String, ICachedMNode>> entrySet() {
    Set<Entry<String, ICachedMNode>> result = new HashSet<>();
    result.addAll(entrySet(childCache));
    result.addAll(entrySet(newChildBuffer));
    result.addAll(entrySet(updatedChildBuffer));
    return result;
  }

  private Set<Entry<String, ICachedMNode>> entrySet(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptySet() : map.entrySet();
  }

  @Nullable
  @Override
  public synchronized ICachedMNode replace(String key, ICachedMNode value) {
    ICachedMNode replacedOne = replace(childCache, key, value);
    if (replacedOne == null) {
      replacedOne = replace(newChildBuffer, key, value);
    }
    if (replacedOne == null) {
      replacedOne = replace(updatedChildBuffer, key, value);
    }
    return replacedOne;
  }

  private ICachedMNode replace(Map<String, ICachedMNode> map, String key, ICachedMNode value) {
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
  public Iterator<ICachedMNode> getChildrenIterator() {
    return new CachedMNodeContainerIterator();
  }

  @Override
  public Iterator<ICachedMNode> getChildrenBufferIterator() {
    return new BufferIterator();
  }

  @Override
  public Iterator<ICachedMNode> getNewChildBufferIterator() {
    return getNewChildBuffer().values().iterator();
  }

  @Override
  public Map<String, ICachedMNode> getChildCache() {
    return childCache == null ? Collections.emptyMap() : childCache;
  }

  @Override
  public Map<String, ICachedMNode> getNewChildBuffer() {
    return newChildBuffer == null ? Collections.emptyMap() : newChildBuffer;
  }

  @Override
  public Map<String, ICachedMNode> getUpdatedChildBuffer() {
    return updatedChildBuffer == null ? Collections.emptyMap() : updatedChildBuffer;
  }

  @Override
  public synchronized void loadChildrenFromDisk(Map<String, ICachedMNode> children) {
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.putAll(children);
  }

  @Override
  public synchronized void addChildToCache(ICachedMNode node) {
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
  public synchronized void appendMNode(ICachedMNode node) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    newChildBuffer.put(node.getName(), node);
  }

  @Override
  public synchronized void updateMNode(String name) {
    ICachedMNode node = remove(childCache, name);
    if (node != null) {
      if (updatedChildBuffer == null) {
        updatedChildBuffer = new ConcurrentHashMap<>();
      }
      updatedChildBuffer.put(name, node);
    }
  }

  @Override
  public synchronized void moveMNodeToCache(String name) {
    ICachedMNode node = remove(newChildBuffer, name);
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
    for (ICachedMNode node : getValues(childCache)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("newChildBuffer:[");
    for (ICachedMNode node : getValues(newChildBuffer)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("updateChildBuffer:[");
    for (ICachedMNode node : getValues(updatedChildBuffer)) {
      builder.append(node.getName()).append(",");
    }
    builder.append("];");
    builder.append("}");
    return builder.toString();
  }

  private class CachedMNodeContainerIterator implements Iterator<ICachedMNode> {

    Iterator<ICachedMNode> iterator;
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
    public ICachedMNode next() {
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

  private class BufferIterator implements Iterator<ICachedMNode> {
    Iterator<ICachedMNode> iterator;
    Iterator<ICachedMNode> newBufferIterator;
    Iterator<ICachedMNode> updateBufferIterator;
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
    public ICachedMNode next() {
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

  private static class EmptyContainer extends AbstractMap<String, ICachedMNode>
      implements IMNodeContainer<ICachedMNode> {

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
    public ICachedMNode get(Object key) {
      return null;
    }

    @Override
    @NotNull
    public Set<String> keySet() {
      return emptySet();
    }

    @Override
    @NotNull
    public Collection<ICachedMNode> values() {
      return emptySet();
    }

    @NotNull
    public Set<Map.Entry<String, ICachedMNode>> entrySet() {
      return emptySet();
    }

    @Override
    public boolean equals(Object o) {
      return o == this;
    }
  }
}
