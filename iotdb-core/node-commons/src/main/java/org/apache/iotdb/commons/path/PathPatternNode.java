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

package org.apache.iotdb.commons.path;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class PathPatternNode<V, S extends PathPatternNode.Serializer<V>> implements Accountable {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PathPatternNode.class);
  private final String name;
  private final Map<String, PathPatternNode<V, S>> children;
  private Set<V> valueSet;

  /**
   * Used only in PatternTreeMap to identify whether from root to the current node is a registered
   * path pattern. In PatternTreeMap, it can be replaced by the size of the valueSet
   */
  private boolean mark = false;

  private final S serializer;

  // Children names with wildcard, for accelerating wildcard searching
  // Here we do not include "*" or "**"
  // to ensure that the set is empty in most of the time, in order to save memory.
  private final Set<String> childrenNamesWithNonTrivialWildcard = new HashSet<>();

  public PathPatternNode(final String name, final S serializer) {
    this.name = name;
    this.children = new HashMap<>();
    this.serializer = serializer;
  }

  public PathPatternNode(
      final String name, final Supplier<? extends Set<V>> supplier, final S serializer) {
    this.name = name;
    this.children = new HashMap<>();
    this.valueSet = supplier.get();
    this.serializer = serializer;
  }

  public String getName() {
    return name;
  }

  public PathPatternNode<V, S> getChildren(final String nodeName) {
    return children.getOrDefault(nodeName, null);
  }

  // The nodeName must not contain any wildcards
  public List<PathPatternNode<V, S>> getMatchChildren(final String nodeName) {
    final List<PathPatternNode<V, S>> res = new ArrayList<>();
    if (children.containsKey(nodeName)) {
      res.add(children.get(nodeName));
    }
    if (children.containsKey(ONE_LEVEL_PATH_WILDCARD)) {
      res.add(children.get(ONE_LEVEL_PATH_WILDCARD));
    }
    if (children.containsKey(MULTI_LEVEL_PATH_WILDCARD)) {
      res.add(children.get(MULTI_LEVEL_PATH_WILDCARD));
    }
    childrenNamesWithNonTrivialWildcard.stream()
        .filter(path -> PathPatternUtil.isNodeMatch(path, nodeName))
        .map(children::get)
        .forEach(res::add);
    return res;
  }

  public Map<String, PathPatternNode<V, S>> getChildren() {
    return children;
  }

  public void addChild(final PathPatternNode<V, S> tmpNode) {
    final String nodeName = tmpNode.getName();
    if (PathPatternUtil.hasWildcard(nodeName)
        && !PathPatternUtil.isMultiLevelMatchWildcard(nodeName)
        && !ONE_LEVEL_PATH_WILDCARD.equals(nodeName)) {
      childrenNamesWithNonTrivialWildcard.add(nodeName);
    }
    children.put(nodeName, tmpNode);
  }

  public void deleteChild(final PathPatternNode<V, S> tmpNode) {
    children.remove(tmpNode.getName());
  }

  public void appendValue(final V value, final BiConsumer<V, Set<V>> remappingFunction) {
    remappingFunction.accept(value, valueSet);
  }

  public void deleteValue(final V value, final BiConsumer<V, Set<V>> remappingFunction) {
    remappingFunction.accept(value, valueSet);
  }

  public Collection<V> getValues() {
    return valueSet;
  }

  /**
   * @return {@code true} if from root to the current node is a registered path pattern.
   */
  public boolean isPathPattern() {
    return mark || isLeaf();
  }

  public boolean isLeaf() {
    return children.isEmpty();
  }

  public boolean isWildcard() {
    return PathPatternUtil.hasWildcard(name);
  }

  public boolean isMultiLevelWildcard() {
    return name.equals(MULTI_LEVEL_PATH_WILDCARD);
  }

  /** set true if from root to the current node is a registered path pattern. */
  public void markPathPattern(final boolean mark) {
    this.mark = mark;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final PathPatternNode<?, ?> that = (PathPatternNode<?, ?>) o;
    return mark == that.mark
        && Objects.equals(name, that.name)
        && Objects.equals(children, that.children)
        && Objects.equals(valueSet, that.valueSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, children, valueSet, mark);
  }

  /**
   * Serialize PathPatternNode
   *
   * <ul>
   *   <li>[required] 1 string: name. It specifies the name of node
   *   <li>[required] 1 int: nodeType or size of valueSet.
   *       <ul>
   *         <li>If this node is being used by PathPatternTree, it specifies the type of this node.
   *             -1 means from root to the current node is a registered path pattern. Otherwise -2.
   *         <li>If this node is being used by PatternTreeMap, it specifies the size of valueSet
   *             which will be serialized next.(>=0)
   *       </ul>
   *   <li>[optional] valueSet
   *   <li>[required] 1 int: children size.
   *   <li>[optional] children
   * </ul>
   */
  public void serialize(final ByteBuffer buffer) {
    ReadWriteIOUtils.write(name, buffer);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, buffer);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), buffer);
      for (final V value : valueSet) {
        serializer.write(value, buffer);
      }
    }
    ReadWriteIOUtils.write(children.size(), buffer);
    serializeChildren(buffer);
  }

  void serializeChildren(final ByteBuffer buffer) {
    for (final PathPatternNode<V, S> childNode : children.values()) {
      childNode.serialize(buffer);
    }
  }

  public void serialize(final PublicBAOS outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (final V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  public void serialize(final DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (final V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  void serializeChildren(final PublicBAOS outputStream) throws IOException {
    for (final PathPatternNode<V, S> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  void serializeChildren(final DataOutputStream outputStream) throws IOException {
    for (final PathPatternNode<V, S> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  void clear() {
    if (Objects.nonNull(valueSet)) {
      valueSet.clear();
    }
    children.clear();
  }

  public static <V, T extends PathPatternNode.Serializer<V>> PathPatternNode<V, T> deserializeNode(
      final ByteBuffer buffer, final T serializer, final Consumer<String> nodeNameProcessor) {
    final PathPatternNode<V, T> node =
        new PathPatternNode<>(ReadWriteIOUtils.readString(buffer), serializer);
    nodeNameProcessor.accept(node.name);
    final int typeOrValueSize = ReadWriteIOUtils.readInt(buffer);
    if (typeOrValueSize >= 0) {
      // measurement node in PatternTreeMap
      final Set<V> valueSet = new HashSet<>();
      for (int i = 0; i < typeOrValueSize; i++) {
        valueSet.add(serializer.read(buffer));
      }
      node.valueSet = valueSet;
    } else if (typeOrValueSize == -1) {
      // node in PathPatternTree
      node.markPathPattern(true);
    }
    int childrenSize = ReadWriteIOUtils.readInt(buffer);
    while (childrenSize > 0) {
      final PathPatternNode<V, T> tmpNode = deserializeNode(buffer, serializer, nodeNameProcessor);
      node.addChild(tmpNode);
      childrenSize--;
    }
    return node;
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + RamUsageEstimator.sizeOf(name)
        + RamUsageEstimator.sizeOfHashSet(valueSet)
        + RamUsageEstimator.sizeOfHashSet(childrenNamesWithNonTrivialWildcard)
        + RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            children,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY);
  }

  /**
   * Interface to support serialize and deserialize valueSet.
   *
   * @param <T> Type of value.
   */
  public interface Serializer<T> {

    void write(final T t, final ByteBuffer buffer);

    void write(final T t, final PublicBAOS stream) throws IOException;

    void write(final T t, final DataOutputStream stream) throws IOException;

    T read(final ByteBuffer buffer);
  }

  public static class VoidSerializer implements PathPatternNode.Serializer<Void> {

    private static class VoidSerializerHolder {
      private static final VoidSerializer INSTANCE = new VoidSerializer();

      private VoidSerializerHolder() {}
    }

    public static VoidSerializer getInstance() {
      return VoidSerializer.VoidSerializerHolder.INSTANCE;
    }

    private VoidSerializer() {}

    @Override
    public void write(final Void unused, final ByteBuffer buffer) {}

    @Override
    public void write(final Void unused, final PublicBAOS stream) {}

    @Override
    public void write(final Void unused, final DataOutputStream stream) {}

    @Override
    public Void read(final ByteBuffer buffer) {
      return null;
    }
  }
}
