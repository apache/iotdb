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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
import java.util.function.Supplier;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class PathPatternNode<V, VSerializer extends PathPatternNode.Serializer<V>> {

  private final String name;
  private final Map<String, PathPatternNode<V, VSerializer>> children;
  private Set<V> valueSet;
  /**
   * Used only in PatternTreeMap to identify whether from root to the current node is a registered
   * path pattern. In PatternTreeMap, it can be replaced by the size of the valueSet
   */
  private boolean mark = false;

  private final VSerializer serializer;

  public PathPatternNode(String name, VSerializer serializer) {
    this.name = name;
    this.children = new HashMap<>();
    this.serializer = serializer;
  }

  public PathPatternNode(String name, Supplier<? extends Set<V>> supplier, VSerializer serialize) {
    this.name = name;
    this.children = new HashMap<>();
    this.valueSet = supplier.get();
    this.serializer = serialize;
  }

  public String getName() {
    return name;
  }

  public PathPatternNode<V, VSerializer> getChildren(String nodeName) {
    return children.getOrDefault(nodeName, null);
  }

  public List<PathPatternNode<V, VSerializer>> getMatchChildren(String nodeName) {
    List<PathPatternNode<V, VSerializer>> res = new ArrayList<>();
    if (children.containsKey(nodeName)) {
      res.add(children.get(nodeName));
    }
    if (children.containsKey(ONE_LEVEL_PATH_WILDCARD)) {
      res.add(children.get(ONE_LEVEL_PATH_WILDCARD));
    }
    if (children.containsKey(MULTI_LEVEL_PATH_WILDCARD)) {
      res.add(children.get(MULTI_LEVEL_PATH_WILDCARD));
    }
    return res;
  }

  public Map<String, PathPatternNode<V, VSerializer>> getChildren() {
    return children;
  }

  public void addChild(PathPatternNode<V, VSerializer> tmpNode) {
    children.put(tmpNode.getName(), tmpNode);
  }

  public void deleteChild(PathPatternNode<V, VSerializer> tmpNode) {
    children.remove(tmpNode.getName());
  }

  public void appendValue(V value, BiConsumer<V, Set<V>> remappingFunction) {
    remappingFunction.accept(value, valueSet);
  }

  public void deleteValue(V value, BiConsumer<V, Set<V>> remappingFunction) {
    remappingFunction.accept(value, valueSet);
  }

  public Collection<V> getValues() {
    return valueSet;
  }

  /** @return true if from root to the current node is a registered path pattern. */
  public boolean isPathPattern() {
    return mark || isLeaf();
  }

  public boolean isLeaf() {
    return children.isEmpty();
  }

  public boolean isWildcard() {
    return name.equals(ONE_LEVEL_PATH_WILDCARD) || name.equals(MULTI_LEVEL_PATH_WILDCARD);
  }

  public boolean isMultiLevelWildcard() {
    return name.equals(MULTI_LEVEL_PATH_WILDCARD);
  }

  /** set true if from root to the current node is a registered path pattern. */
  public void markPathPattern(boolean mark) {
    this.mark = mark;
  }

  @TestOnly
  public boolean equalWith(PathPatternNode<V, VSerializer> that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    if (!Objects.equals(that.getName(), this.getName())) {
      return false;
    }
    if (that.isLeaf() != this.isLeaf()) {
      return false;
    }
    if (that.isPathPattern() != this.isPathPattern()) {
      return false;
    }
    if (that.getChildren().size() != this.getChildren().size()) {
      return false;
    }
    if (that.getValues() != null && !that.getValues().equals(this.getValues())) {
      return false;
    }
    for (Map.Entry<String, PathPatternNode<V, VSerializer>> entry : this.getChildren().entrySet()) {
      String nodeName = entry.getKey();
      if (that.getChildren(nodeName) == null
          || !that.getChildren(nodeName).equalWith(this.getChildren(nodeName))) {
        return false;
      }
    }
    return true;
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
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(name, buffer);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, buffer);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), buffer);
      for (V value : valueSet) {
        serializer.write(value, buffer);
      }
    }
    ReadWriteIOUtils.write(children.size(), buffer);
    serializeChildren(buffer);
  }

  void serializeChildren(ByteBuffer buffer) {
    for (PathPatternNode<V, VSerializer> childNode : children.values()) {
      childNode.serialize(buffer);
    }
  }

  public void serialize(PublicBAOS outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(mark ? -1 : -2, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    ReadWriteIOUtils.write(children.size(), outputStream);
    serializeChildren(outputStream);
  }

  void serializeChildren(PublicBAOS outputStream) throws IOException {
    for (PathPatternNode<V, VSerializer> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  void serializeChildren(DataOutputStream outputStream) throws IOException {
    for (PathPatternNode<V, VSerializer> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  public static <V, T extends PathPatternNode.Serializer<V>> PathPatternNode<V, T> deserializeNode(
      ByteBuffer buffer, T serializer) {
    PathPatternNode<V, T> node =
        new PathPatternNode<>(ReadWriteIOUtils.readString(buffer), serializer);
    int typeOrValueSize = ReadWriteIOUtils.readInt(buffer);
    if (typeOrValueSize >= 0) {
      // measurement node in PatternTreeMap
      Set<V> valueSet = new HashSet<>();
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
      PathPatternNode<V, T> tmpNode = deserializeNode(buffer, serializer);
      node.addChild(tmpNode);
      childrenSize--;
    }
    return node;
  }

  /**
   * Interface to support serialize and deserialize valueSet.
   *
   * @param <T> Type of value.
   */
  public interface Serializer<T> {

    void write(T t, ByteBuffer buffer);

    void write(T t, PublicBAOS stream) throws IOException;

    void write(T t, DataOutputStream stream) throws IOException;

    T read(ByteBuffer buffer);
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
    public void write(Void unused, ByteBuffer buffer) {}

    @Override
    public void write(Void unused, PublicBAOS stream) {}

    @Override
    public void write(Void unused, DataOutputStream stream) {}

    @Override
    public Void read(ByteBuffer buffer) {
      return null;
    }
  }
}
