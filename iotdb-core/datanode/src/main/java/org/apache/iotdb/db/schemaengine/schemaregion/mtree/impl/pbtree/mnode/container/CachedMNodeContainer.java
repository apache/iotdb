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

import org.apache.iotdb.commons.schema.MergeSortIterator;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

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
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.MNodeChildBuffer.emptyMNodeChildBuffer;

public class CachedMNodeContainer implements ICachedMNodeContainer {

  private long segmentAddress = -1;

  private Map<String, ICachedMNode> childCache = null;
  private MNodeNewChildBuffer newChildBuffer = null;
  private MNodeUpdateChildBuffer updatedChildBuffer = null;

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
  public synchronized ICachedMNode get(Object key) {
    return internalGet(key);
  }

  private ICachedMNode internalGet(Object key) {
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
      newChildBuffer = new MNodeNewChildBuffer();
    }
    return newChildBuffer.put(key, value);
  }

  @Nullable
  @Override
  public synchronized ICachedMNode putIfAbsent(String key, ICachedMNode value) {
    ICachedMNode node = internalGet(key);
    if (node == null) {
      if (newChildBuffer == null) {
        newChildBuffer = new MNodeNewChildBuffer();
      }
      node = newChildBuffer.put(key, value);
    }
    return node;
  }

  @Override
  public synchronized ICachedMNode remove(Object key) {
    ICachedMNode result = removeFromMap(childCache, key);
    if (result == null) {
      result = removeFromMap(newChildBuffer, key);
    }
    if (result == null) {
      result = removeFromMap(updatedChildBuffer, key);
    }
    return result;
  }

  private ICachedMNode removeFromMap(Map<String, ICachedMNode> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public synchronized void putAll(@Nonnull Map<? extends String, ? extends ICachedMNode> m) {
    if (newChildBuffer == null) {
      newChildBuffer = new MNodeNewChildBuffer();
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
    throw new UnsupportedOperationException();
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
  public boolean hasChildInNewChildBuffer(String name) {
    return containsKey(newChildBuffer, name);
  }

  @Override
  public boolean hasChildInBuffer(String name) {
    return containsKey(updatedChildBuffer, name) || containsKey(newChildBuffer, name);
  }

  @Override
  public boolean hasChildrenInBuffer() {
    return !isEmpty(updatedChildBuffer) || !isEmpty(newChildBuffer);
  }

  @Override
  public Iterator<ICachedMNode> getChildrenIterator() {
    return new CachedMNodeContainerIterator((byte) 0);
  }

  @Override
  public Iterator<ICachedMNode> getChildrenBufferIterator() {
    return new BufferIterator();
  }

  @Override
  public Map<String, ICachedMNode> getChildCache() {
    return childCache == null ? Collections.emptyMap() : childCache;
  }

  @Override
  public IMNodeChildBuffer getNewChildBuffer() {
    return newChildBuffer == null ? emptyMNodeChildBuffer() : newChildBuffer;
  }

  @Override
  public IMNodeChildBuffer getUpdatedChildBuffer() {
    return updatedChildBuffer == null ? emptyMNodeChildBuffer() : updatedChildBuffer;
  }

  @Override
  public Map<String, ICachedMNode> getNewChildFlushingBuffer() {
    return getNewChildBuffer().getFlushingBuffer();
  }

  @Override
  public Map<String, ICachedMNode> getUpdatedChildFlushingBuffer() {
    return getUpdatedChildBuffer().getFlushingBuffer();
  }

  @Override
  public Map<String, ICachedMNode> getUpdatedChildReceivingBuffer() {
    return getUpdatedChildBuffer().getReceivingBuffer();
  }

  @Override
  public void transferAllBufferReceivingToFlushing() {
    getNewChildBuffer().transferReceivingBufferToFlushingBuffer();
    getUpdatedChildBuffer().transferReceivingBufferToFlushingBuffer();
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
      newChildBuffer = new MNodeNewChildBuffer();
    }
    newChildBuffer.put(node.getName(), node);
  }

  @Override
  public synchronized void updateMNode(String name) {
    ICachedMNode node = removeFromMap(childCache, name);
    if (node != null) {
      if (updatedChildBuffer == null) {
        updatedChildBuffer = new MNodeUpdateChildBuffer();
      }
      updatedChildBuffer.put(name, node);
    }
  }

  @Override
  public synchronized void moveMNodeFromNewChildBufferToCache(String name) {
    ICachedMNode node = getNewChildBuffer().removeFromFlushingBuffer(name);
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.put(name, node);
  }

  @Override
  public synchronized void moveMNodeFromUpdateChildBufferToCache(String name) {
    ICachedMNode node = getUpdatedChildBuffer().removeFromFlushingBuffer(name);
    if (childCache == null) {
      childCache = new ConcurrentHashMap<>();
    }
    childCache.put(name, node);
  }

  @Override
  public synchronized void evictMNode(String name) {
    removeFromMap(childCache, name);
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
    byte status;

    CachedMNodeContainerIterator(byte status) {
      this.status = status;
      changeStatus();
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
          iterator = getChildCache().values().iterator();
          status = 1;
          return true;
        case 1:
          iterator = getNewChildBuffer().getMNodeChildBufferIterator();
          status = 2;
          return true;
        case 2:
          iterator = getUpdatedChildBuffer().getMNodeChildBufferIterator();
          status = 3;
          return true;
        default:
          return false;
      }
    }
  }

  private class BufferIterator extends MergeSortIterator<ICachedMNode> {

    BufferIterator() {
      super(
          getNewChildBuffer().getMNodeChildBufferIterator(),
          getUpdatedChildBuffer().getMNodeChildBufferIterator());
    }

    protected int decide() {
      throw new IllegalStateException(
          "There shall not exist two node with the same name separately in newChildBuffer and updateChildBuffer");
    }

    protected int compare(ICachedMNode left, ICachedMNode right) {
      return left.getName().compareTo(right.getName());
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

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
