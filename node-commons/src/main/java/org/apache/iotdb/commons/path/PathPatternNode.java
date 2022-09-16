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

public class PathPatternNode<V, Serializer extends PathPatternNode.Serializer<V>> {

  private final String name;
  private final Map<String, PathPatternNode<V, Serializer>> children;
  private Set<V> valueSet;
  private final Serializer serializer;

  public PathPatternNode(String name, Serializer serializer) {
    this.name = name;
    this.children = new HashMap<>();
    this.serializer = serializer;
  }

  public PathPatternNode(String name, Supplier<? extends Set<V>> supplier, Serializer serialize) {
    this.name = name;
    this.children = new HashMap<>();
    valueSet = supplier.get();
    this.serializer = serialize;
  }

  public String getName() {
    return name;
  }

  public PathPatternNode<V, Serializer> getChildren(String nodeName) {
    return children.getOrDefault(nodeName, null);
  }

  public List<PathPatternNode<V, Serializer>> getMatchChildren(String nodeName) {
    List<PathPatternNode<V, Serializer>> res = new ArrayList<>();
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

  public Map<String, PathPatternNode<V, Serializer>> getChildren() {
    return children;
  }

  public void addChild(PathPatternNode<V, Serializer> tmpNode) {
    children.put(tmpNode.getName(), tmpNode);
  }

  public void deleteChild(PathPatternNode<V, Serializer> tmpNode) {
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

  public boolean isLeaf() {
    return children.isEmpty();
  }

  public boolean isWildcard() {
    return name.equals(ONE_LEVEL_PATH_WILDCARD) || name.equals(MULTI_LEVEL_PATH_WILDCARD);
  }

  public boolean isMultiLevelWildcard() {
    return name.equals(MULTI_LEVEL_PATH_WILDCARD);
  }

  @TestOnly
  public boolean equalWith(PathPatternNode<V, Serializer> that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    if (!Objects.equals(that.getName(), this.getName())) {
      return false;
    }
    if (that.isLeaf() && this.isLeaf()) {
      return true;
    }
    if (that.getChildren().size() != this.getChildren().size()) {
      return false;
    }
    if (that.getValues() != null && !that.getValues().equals(this.getValues())) {
      return false;
    }
    for (Map.Entry<String, PathPatternNode<V, Serializer>> entry : this.getChildren().entrySet()) {
      String nodeName = entry.getKey();
      if (that.getChildren(nodeName) == null
          || !that.getChildren(nodeName).equalWith(this.getChildren(nodeName))) {
        return false;
      }
    }
    return true;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(children.size(), buffer);
    serializeChildren(buffer);
    // TODO
  }

  void serializeChildren(ByteBuffer buffer) {
    for (PathPatternNode<V, Serializer> childNode : children.values()) {
      childNode.serialize(buffer);
    }
  }

  public void serialize(PublicBAOS outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(0, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    serializeChildren(outputStream);
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
    if (valueSet == null) {
      ReadWriteIOUtils.write(0, outputStream);
    } else {
      ReadWriteIOUtils.write(valueSet.size(), outputStream);
      for (V value : valueSet) {
        serializer.write(value, outputStream);
      }
    }
    serializeChildren(outputStream);
  }

  void serializeChildren(PublicBAOS outputStream) throws IOException {
    for (PathPatternNode<V, Serializer> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  void serializeChildren(DataOutputStream outputStream) throws IOException {
    for (PathPatternNode<V, Serializer> childNode : children.values()) {
      childNode.serialize(outputStream);
    }
  }

  public static <V, T extends PathPatternNode.Serializer<V>> PathPatternNode<V, T> deserializeNode(
      ByteBuffer buffer, T serializer) {
    PathPatternNode<V, T> node =
        new PathPatternNode<>(ReadWriteIOUtils.readString(buffer), serializer);
    int childrenSize = ReadWriteIOUtils.readInt(buffer);
    while (childrenSize > 0) {
      PathPatternNode<V, T> tmpNode = deserializeNode(buffer, serializer);
      node.addChild(tmpNode);
      childrenSize--;
    }
    int valueSize = ReadWriteIOUtils.readInt(buffer);
    if (valueSize > 0) {
      Set<V> valueSet = new HashSet<>();
      for (int i = 0; i < valueSize; i++) {
        valueSet.add(serializer.read(buffer));
      }
      node.valueSet = valueSet;
    }
    return node;
  }

  public interface Serializer<T> {

    void write(T t, ByteBuffer buffer);

    void write(T t, PublicBAOS buffer);

    void write(T t, DataOutputStream buffer);

    T read(ByteBuffer buffer);
  }

  public static class StringSerializer implements PathPatternNode.Serializer<String> {

    private static class StringSerializerHolder {
      private static final StringSerializer INSTANCE = new StringSerializer();

      private StringSerializerHolder() {}
    }

    public static StringSerializer getInstance() {
      return StringSerializerHolder.INSTANCE;
    }

    private StringSerializer() {}

    @Override
    public void write(String s, ByteBuffer buffer) {
      ReadWriteIOUtils.write(s, buffer);
    }

    @Override
    public void write(String s, PublicBAOS buffer) {
      try {
        ReadWriteIOUtils.write(s, buffer);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void write(String s, DataOutputStream buffer) {
      try {
        ReadWriteIOUtils.write(s, buffer);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String read(ByteBuffer buffer) {
      return ReadWriteIOUtils.readString(buffer);
    }
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
    public void write(Void unused, PublicBAOS buffer) {}

    @Override
    public void write(Void unused, DataOutputStream buffer) {}

    @Override
    public Void read(ByteBuffer buffer) {
      return null;
    }
  }
}
