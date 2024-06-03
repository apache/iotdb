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

import org.apache.iotdb.commons.conf.IoTDBConstant;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@NotThreadSafe
public class PatternTreeMap<V, VSerializer extends PathPatternNode.Serializer<V>> {
  private final PathPatternNode<V, VSerializer> root;
  private final Supplier<? extends Set<V>> supplier;
  private final BiConsumer<V, Set<V>> appendFunction;
  private final BiConsumer<V, Set<V>> deleteFunction;
  private final VSerializer serializer;
  /**
   * Create PatternTreeMap.
   *
   * @param supplier provide type of set to store values on PathPatternNode
   * @param appendFunction define the merge logic of append value
   * @param deleteFunction define the split logic of delete value
   */
  public PatternTreeMap(
      Supplier<? extends Set<V>> supplier,
      BiConsumer<V, Set<V>> appendFunction,
      BiConsumer<V, Set<V>> deleteFunction,
      VSerializer serializer) {
    this.root = new PathPatternNode<>(IoTDBConstant.PATH_ROOT, supplier, serializer);
    this.supplier = supplier;
    this.appendFunction = appendFunction;
    this.deleteFunction = deleteFunction;
    this.serializer = serializer;
  }

  /**
   * Append key and value to PatternTreeMap.
   *
   * @param key PartialPath that can contain '*' or '**'
   * @param value The value to be appended
   */
  public void append(PartialPath key, V value) {
    if (appendFunction == null) {
      throw new UnsupportedOperationException();
    }
    String[] pathNodes = key.getNodes();
    PathPatternNode<V, VSerializer> curNode = root;
    for (int i = 1; i < pathNodes.length; i++) {
      PathPatternNode<V, VSerializer> nextNode = curNode.getChildren(pathNodes[i]);
      if (nextNode == null) {
        nextNode = new PathPatternNode<>(pathNodes[i], supplier, serializer);
        curNode.addChild(nextNode);
      }
      curNode = nextNode;
    }
    curNode.appendValue(value, appendFunction);
  }

  /**
   * Delete key and value to PatternTreeMap.
   *
   * @param key PartialPath that can contain '*' or '**'
   * @param value The value to be deleted
   */
  public void delete(PartialPath key, V value) {
    if (deleteFunction == null) {
      throw new UnsupportedOperationException();
    }
    deletePathNode(root, key.getNodes(), 0, value);
  }

  /**
   * Recursive method for deleting value.
   *
   * @param node current PathPatternNode
   * @param pathNodes pathNodes of key
   * @param pos current index of pathNodes
   * @param value the value to be deleted
   * @return true if current PathPatternNode can be removed
   */
  private boolean deletePathNode(
      PathPatternNode<V, VSerializer> node, String[] pathNodes, int pos, V value) {
    if (node == null) {
      return false;
    }
    if (pos == pathNodes.length - 1) {
      node.deleteValue(value, deleteFunction);
    } else {
      PathPatternNode<V, VSerializer> child = node.getChildren(pathNodes[pos + 1]);
      if (deletePathNode(child, pathNodes, pos + 1, value)) {
        node.deleteChild(child);
      }
    }
    return node.isLeaf() && node.getValues().isEmpty();
  }

  /**
   * Get value list related to PathPattern that overlapped with fullPath.
   *
   * @param fullPath full path without wildcard
   * @return de-duplicated value list
   */
  public List<V> getOverlapped(PartialPath fullPath) {
    Set<V> res = new HashSet<>();
    searchOverlapped(root, fullPath.getNodes(), 0, res);
    return new ArrayList<>(res);
  }

  /**
   * Recursive method for search overlapped pattern.
   *
   * @param node current PathPatternNode
   * @param pathNodes pathNodes of key
   * @param pos current index of pathNodes
   * @param resultSet result set
   */
  private void searchOverlapped(
      PathPatternNode<V, VSerializer> node, String[] pathNodes, int pos, Set<V> resultSet) {
    if (pos == pathNodes.length - 1) {
      resultSet.addAll(node.getValues());
      return;
    }
    if (node.isMultiLevelWildcard()) {
      searchOverlapped(node, pathNodes, pos + 1, resultSet);
    }
    for (PathPatternNode<V, VSerializer> child : node.getMatchChildren(pathNodes[pos + 1])) {
      searchOverlapped(child, pathNodes, pos + 1, resultSet);
    }
  }

  /**
   * Get a list of value lists related to PathPattern that overlapped with measurements under the
   * same device.
   *
   * @param devicePath device path without wildcard
   * @param measurements list of measurements
   * @return de-duplicated value list
   */
  public List<List<V>> getOverlapped(PartialPath devicePath, List<String> measurements) {
    List<Set<V>> resultSet = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      resultSet.add(new HashSet<>());
    }
    searchOverlapped(root, devicePath.getNodes(), 0, measurements, resultSet);
    List<List<V>> res = new ArrayList<>();
    for (Set<V> set : resultSet) {
      res.add(new ArrayList<>(set));
    }
    return res;
  }

  /**
   * Recursive method for search overlapped pattern.
   *
   * @param node current PathPatternNode
   * @param deviceNodes pathNodes of device
   * @param pos current index of deviceNodes
   * @param measurements list of measurements under device
   * @param resultSet result set
   */
  private void searchOverlapped(
      PathPatternNode<V, VSerializer> node,
      String[] deviceNodes,
      int pos,
      List<String> measurements,
      List<Set<V>> resultSet) {
    if (pos == deviceNodes.length - 1) {
      for (int i = 0; i < measurements.size(); i++) {
        for (PathPatternNode<V, VSerializer> child : node.getMatchChildren(measurements.get(i))) {
          resultSet.get(i).addAll(child.getValues());
        }
        if (node.isMultiLevelWildcard()) {
          resultSet.get(i).addAll(node.getValues());
        }
      }
      return;
    }
    if (node.isMultiLevelWildcard()) {
      searchOverlapped(node, deviceNodes, pos + 1, measurements, resultSet);
    }
    for (PathPatternNode<V, VSerializer> child : node.getMatchChildren(deviceNodes[pos + 1])) {
      searchOverlapped(child, deviceNodes, pos + 1, measurements, resultSet);
    }
  }
}
