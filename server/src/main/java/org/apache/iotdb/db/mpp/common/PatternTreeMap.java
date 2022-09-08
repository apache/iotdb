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
package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

// List<String>, String
// Modif
public class PatternTreeMap<V> {
  private PatternTreeMapNode<V> root;
  //  private Map<PartialPath, Collection<V>> map;
  private BiConsumer<V, Collection<V>> remappingFunction;

  public PatternTreeMap(BiConsumer<V, Collection<V>> remappingFunction) {
    this.root = new PatternTreeMapNode<>(SQLConstant.ROOT);
    //    this.map = new ConcurrentHashMap<>();
    this.remappingFunction = remappingFunction;
  }

  public PatternTreeMap() {
    this((newValue, set) -> set.add(newValue));
  }

  public void appendPathPattern(PartialPath pathPattern, V value) {
    String[] pathNodes = pathPattern.getNodes();
    PathPatternNode curNode = root;
    for (int i = 1; i < pathNodes.length; i++) {
      PathPatternNode nextNode = curNode.getChildren(pathNodes[i]);
      if (nextNode == null) {
        nextNode = new PatternTreeMapNode<>(pathNodes[i]);
        curNode.addChild(nextNode);
      }
      curNode = nextNode;
    }
    ((PatternTreeMapNode<V>)curNode).appendValue(value, remappingFunction);
  }

  public void deletePathPattern(PartialPath pathPattern, V value) {
    deletePathNode(root, pathPattern.getNodes(), 0);
  }

  private boolean deletePathNode(PathPatternNode node, String[] pathNodes, int pos) {
    if (node == null) {
      return false;
    } else if (pos == pathNodes.length - 1) {
      return true;
    }
    PathPatternNode child = node.getChildren(pathNodes[pos + 1]);
    if (deletePathNode(child, pathNodes, pos + 1)) {
      node.deleteChild(child);
      return true;
    }
    return false;
  }

  public List<V> getOverlappedPathPatterns(PartialPath fullPath) {
    List<V> res = new ArrayList<>();
    get(root, fullPath.getNodes(), 0, res, false);
    return res;
  }

  /**
   * @param node 当前节点
   * @param pathNodes
   * @param pos 当前pathNodes[pos]
   * @param resultList
   */
  public void get(PathPatternNode node, String[] pathNodes, int pos, List<V> resultList, boolean fromMultiWildCard) {
    if (pos == pathNodes.length - 1) {
      resultList.addAll(((PatternTreeMapNode<V>)node).getValues());
      return;
    }
    if (node.isMultiLevelWildcard()&&!fromMultiWildCard) {
      for (int i = pos + 1; i < pathNodes.length; i++) {
        get(node, pathNodes, i, resultList, true);
      }
    }
    for (PathPatternNode child : node.getMatchChildren(pathNodes[pos + 1])) {
      get(child, pathNodes, pos + 1, resultList,false);
    }
  }


  class PatternTreeMapNode<V> extends PathPatternNode{
    
    private final Set<V> valueSet = new HashSet<>();
    
    public PatternTreeMapNode(String name){
      super(name);
    }

    public void appendValue(V value, BiConsumer<V, Collection<V>> remappingFunction) {
      remappingFunction.accept(value, valueSet);
    }

    public Set<V> getValues() {
      return valueSet;
    }

  }
}
