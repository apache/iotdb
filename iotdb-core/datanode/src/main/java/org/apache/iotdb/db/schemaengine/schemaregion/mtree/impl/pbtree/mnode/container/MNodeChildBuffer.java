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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptySet;

public abstract class MNodeChildBuffer implements IMNodeChildBuffer {

  protected Map<String, ICachedMNode> flushingBuffer; // Store old data nodes for disk flushing
  protected Map<String, ICachedMNode> receivingBuffer; // Store newly created or modified data nodes

  protected int totalSize =
      0; // The total size is the union size of flushingBuffer and receivingBuffer.

  private static final IMNodeChildBuffer EMPTY_BUFFER = new MNodeChildBuffer.EmptyBuffer();

  public static IMNodeChildBuffer emptyMNodeChildBuffer() {
    return EMPTY_BUFFER;
  }

  @Override
  public Iterator<ICachedMNode> getMNodeChildBufferIterator() {
    return new MNodeChildBufferIterator();
  }

  @Override
  public Map<String, ICachedMNode> getFlushingBuffer() {
    return flushingBuffer == null ? Collections.emptyMap() : flushingBuffer;
  }

  @Override
  public Map<String, ICachedMNode> getReceivingBuffer() {
    return receivingBuffer == null ? Collections.emptyMap() : receivingBuffer;
  }

  @Override
  public void transferReceivingBufferToFlushingBuffer() {
    if (flushingBuffer == null) {
      flushingBuffer = new ConcurrentHashMap<>();
    }
    if (receivingBuffer != null) {
      flushingBuffer.putAll(receivingBuffer);
      receivingBuffer.clear();
    }
  }

  @Override
  public int size() {
    return totalSize;
  }

  @Override
  public boolean isEmpty() {
    return totalSize == 0;
  }

  private boolean containKey(Map<String, ICachedMNode> map, Object key) {
    return map != null && map.containsKey(key);
  }

  @Override
  public boolean containsKey(Object key) {
    return containKey(flushingBuffer, key) || containKey(receivingBuffer, key);
  }

  private boolean containValue(Map<String, ICachedMNode> map, Object value) {
    return map != null && map.containsValue(value);
  }

  @Override
  public boolean containsValue(Object value) {
    return containValue(flushingBuffer, value) || containValue(receivingBuffer, value);
  }

  protected ICachedMNode get(Map<String, ICachedMNode> map, Object key) {
    return map != null ? map.get(key) : null;
  }

  @Override
  public synchronized ICachedMNode get(Object key) {
    ICachedMNode result = get(receivingBuffer, key);
    if (result != null) {
      return result;
    } else {
      return get(flushingBuffer, key);
    }
  }

  private ICachedMNode remove(Map<String, ICachedMNode> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public synchronized ICachedMNode remove(Object key) {
    // There are some duplicate keys in recevingBuffer and flushingBuffer.
    ICachedMNode result1 = remove(flushingBuffer, key);
    ICachedMNode result2 = remove(receivingBuffer, key);
    // If the first one is empty, then look at the second result; if the first one is not empty,
    // then the second one is either empty or the same as the first one
    ICachedMNode result = result1 != null ? result1 : result2;
    if (result != null) {
      totalSize--;
    }
    return result;
  }

  @Override
  public void clear() {
    if (receivingBuffer != null) {
      receivingBuffer.clear();
    }
    if (flushingBuffer != null) {
      flushingBuffer.clear();
    }
    totalSize = 0;
  }

  private Set<String> keySet(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  @Nonnull
  @Override
  public Set<String> keySet() {
    Set<String> result = new TreeSet<>();
    // This is a set structure. If there are duplicates, set will automatically remove them.
    result.addAll(keySet(receivingBuffer));
    result.addAll(keySet(flushingBuffer));
    return result;
  }

  private Collection<ICachedMNode> values(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptyList() : map.values();
  }

  @Nonnull
  @Override
  public Collection<ICachedMNode> values() {
    Collection<ICachedMNode> result = new HashSet<>();
    result.addAll(values(flushingBuffer));
    result.addAll(values(receivingBuffer));
    return result;
  }

  private Set<Entry<String, ICachedMNode>> entrySet(Map<String, ICachedMNode> map) {
    return map == null ? Collections.emptySet() : map.entrySet();
  }

  @Nonnull
  @Override
  public Set<Entry<String, ICachedMNode>> entrySet() {
    Set<Entry<String, ICachedMNode>> result = new HashSet<>();
    // HashSet will automatically remove duplicates
    result.addAll(entrySet(receivingBuffer));
    result.addAll(entrySet(flushingBuffer));
    return result;
  }

  @Nullable
  @Override
  public synchronized ICachedMNode replace(String key, ICachedMNode value) {
    throw new UnsupportedOperationException();
  }

  private Iterator<ICachedMNode> getSortedReceivingBuffer() {
    List<ICachedMNode> receivingBufferList = new ArrayList<>(getReceivingBuffer().values());
    receivingBufferList.sort(Comparator.comparing(ICachedMNode::getName));
    return receivingBufferList.iterator();
  }

  private Iterator<ICachedMNode> getSortedFlushingBuffer() {
    List<ICachedMNode> flushingBufferList = new ArrayList<>(getFlushingBuffer().values());
    flushingBufferList.sort(Comparator.comparing(ICachedMNode::getName));
    return flushingBufferList.iterator();
  }

  private class MNodeChildBufferIterator extends MergeSortIterator<ICachedMNode> {
    MNodeChildBufferIterator() {
      super(getSortedReceivingBuffer(), getSortedFlushingBuffer());
    }

    protected int decide() {
      return -1;
    }

    protected int compare(ICachedMNode left, ICachedMNode right) {
      return left.getName().compareTo(right.getName());
    }
  }

  private static class EmptyBuffer extends AbstractMap<String, ICachedMNode>
      implements IMNodeChildBuffer {

    @Override
    public Iterator<ICachedMNode> getMNodeChildBufferIterator() {
      return Collections.emptyIterator();
    }

    @Override
    public Map<String, ICachedMNode> getFlushingBuffer() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, ICachedMNode> getReceivingBuffer() {
      return Collections.emptyMap();
    }

    @Override
    public void transferReceivingBufferToFlushingBuffer() {
      // Do nothing
    }

    @Override
    public ICachedMNode removeFromFlushingBuffer(Object key) {
      return null;
    }

    @Nonnull
    @Override
    public Set<Entry<String, ICachedMNode>> entrySet() {
      return emptySet();
    }

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

    @Nonnull
    @Override
    public Set<String> keySet() {
      return emptySet();
    }

    @Nonnull
    @Override
    public Collection<ICachedMNode> values() {
      return emptySet();
    }
  }
}
