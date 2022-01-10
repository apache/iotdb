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

import org.apache.iotdb.db.metadata.mtree.store.disk.ISegment;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CachedMNodeContainer<E extends IMNode> implements IMNodeContainer<E>, ISegment {

  private long segmentAddress = -1;
  Map<String, E> childCache = null;
  Map<String, E> newChildBuffer = null;
  Map<String, E> updatedChildBuffer = null;

  @Override
  public int size() {
    return getSize(childCache) + getSize(newChildBuffer) + getSize(updatedChildBuffer);
  }

  private int getSize(Map<String, E> map) {
    return map == null ? 0 : map.size();
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(childCache) && isEmpty(newChildBuffer) && isEmpty(updatedChildBuffer);
  }

  private boolean isEmpty(Map<String, E> map) {
    return map == null || map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return containsKey(childCache, key)
        || containsKey(newChildBuffer, key)
        || containsKey(updatedChildBuffer, key);
  }

  private boolean containsKey(Map<String, E> map, Object key) {
    return map != null && map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return containsValue(childCache, value)
        || containsValue(newChildBuffer, value)
        || containsValue(updatedChildBuffer, value);
  }

  private boolean containsValue(Map<String, E> map, Object value) {
    return map != null && map.containsValue(value);
  }

  @Override
  public E get(Object key) {
    E result = get(childCache, key);
    if (result != null) {
      return result;
    }
    result = get(newChildBuffer, key);
    if (result != null) {
      return result;
    }
    return get(updatedChildBuffer);
  }

  private E get(Map<String, E> map, Object key) {
    return map == null ? null : map.get(key);
  }

  @Nullable
  @Override
  public E put(String key, E value) {
    if (newChildBuffer == null) {
      newChildBuffer = new ConcurrentHashMap<>();
    }
    return newChildBuffer.put(key, value);
  }

  @Override
  public E remove(Object key) {
    E result = remove(childCache, key);
    if (result != null) {
      return result;
    }
    result = remove(newChildBuffer, key);
    if (result != null) {
      return result;
    }
    return remove(updatedChildBuffer, key);
  }

  private E remove(Map<String, E> map, Object key) {
    return map == null ? null : map.remove(key);
  }

  @Override
  public void putAll(@NotNull Map<? extends String, ? extends E> m) {
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

  private Set<String> keySet(Map<String, E> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  @NotNull
  @Override
  public Collection<E> values() {
    Collection<E> result = new ArrayList<>();
    result.addAll(getValues(childCache));
    result.addAll(getValues(newChildBuffer));
    result.addAll(getValues(updatedChildBuffer));
    return result;
  }

  private Collection<E> getValues(Map<String, E> map) {
    return map == null ? Collections.emptyList() : map.values();
  }

  @NotNull
  @Override
  public Set<Entry<String, E>> entrySet() {
    Set<Entry<String, E>> result = new HashSet<>();
    result.addAll(entrySet(childCache));
    result.addAll(entrySet(newChildBuffer));
    result.addAll(entrySet(updatedChildBuffer));
    return result;
  }

  private Set<Entry<String, E>> entrySet(Map<String, E> map) {
    return map == null ? Collections.emptySet() : map.entrySet();
  }

  @Override
  public E getOrDefault(Object key, E defaultValue) {
    return IMNodeContainer.super.getOrDefault(key, defaultValue);
  }

  @Override
  public void forEach(BiConsumer<? super String, ? super E> action) {
    IMNodeContainer.super.forEach(action);
  }

  @Override
  public void replaceAll(BiFunction<? super String, ? super E, ? extends E> function) {
    IMNodeContainer.super.replaceAll(function);
  }

  @Nullable
  @Override
  public E putIfAbsent(String key, E value) {
    return IMNodeContainer.super.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return IMNodeContainer.super.remove(key, value);
  }

  @Override
  public boolean replace(String key, E oldValue, E newValue) {
    return IMNodeContainer.super.replace(key, oldValue, newValue);
  }

  @Nullable
  @Override
  public E replace(String key, E value) {
    return IMNodeContainer.super.replace(key, value);
  }

  @Override
  public E computeIfAbsent(
      String key, @NotNull Function<? super String, ? extends E> mappingFunction) {
    return IMNodeContainer.super.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public E computeIfPresent(
      String key, @NotNull BiFunction<? super String, ? super E, ? extends E> remappingFunction) {
    return IMNodeContainer.super.computeIfPresent(key, remappingFunction);
  }

  @Override
  public E compute(
      String key, @NotNull BiFunction<? super String, ? super E, ? extends E> remappingFunction) {
    return IMNodeContainer.super.compute(key, remappingFunction);
  }

  @Override
  public E merge(
      String key,
      @NotNull E value,
      @NotNull BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    return IMNodeContainer.super.merge(key, value, remappingFunction);
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
    return childCache == null && newChildBuffer == null && updatedChildBuffer == null;
  }

  @Override
  public Map<String, ? extends IMNode> getChildCache() {
    return childCache == null ? Collections.emptyMap() : childCache;
  }

  @Override
  public Map<String, ? extends IMNode> getNewChildBuffer() {
    return newChildBuffer == null ? Collections.emptyMap() : newChildBuffer;
  }

  @Override
  public Map<String, ? extends IMNode> getUpdatedChildBuffer() {
    return updatedChildBuffer == null ? Collections.emptyMap() : updatedChildBuffer;
  }
}
