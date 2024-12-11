/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.schema.ttl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** TTL Cache Tree, which is a prefix B+ tree with each node storing TTL. */
@NotThreadSafe
public class TTLCache {

  private final CacheNode ttlCacheTree;
  public static final long NULL_TTL = -1;

  private int ttlCount;

  public TTLCache() {
    ttlCacheTree = new CacheNode(IoTDBConstant.PATH_ROOT);
    ttlCacheTree.addChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD, Long.MAX_VALUE);
    ttlCount = 1;
  }

  /**
   * Put ttl into cache tree.
   *
   * @param nodes should be prefix path or specific device path without wildcard
   */
  public void setTTL(String[] nodes, long ttl) {
    int start = nodes[0].equals("root") ? 1 : 0;
    if (nodes.length <= start || ttl < 0) {
      return;
    }
    CacheNode current = ttlCacheTree;
    for (int i = start; i < nodes.length; i++) {
      CacheNode child = current.getChild(nodes[i]);
      if (child == null) {
        child = current.addChild(nodes[i], NULL_TTL);
      }
      current = child;
    }
    if (current.ttl == NULL_TTL) {
      ttlCount++;
    }
    current.ttl = ttl;
  }

  /**
   * Unset ttl and remove all useless nodes whose subtree at them contains no valid ttl. If the path
   * to be removed is internal node, then just reset its ttl. Else, find the sub path and remove it.
   *
   * @param nodes path to be removed
   */
  public TSStatus unsetTTL(String[] nodes) {
    int start = nodes[0].equals("root") ? 1 : 0;
    if (nodes.length <= start) {
      return new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode())
          .setMessage(String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes));
    } else if (nodes.length == start + 1) {
      // if path equals to root.**, then unset it to configured ttl
      if (nodes[0].equals(IoTDBConstant.PATH_ROOT)
          && nodes[1].equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        ttlCacheTree.getChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD).ttl = Long.MAX_VALUE;
        return StatusUtils.OK;
      }
    }
    CacheNode current = ttlCacheTree;
    int index = 0;
    boolean hasNonDefaultTTL;
    CacheNode parentOfSubPathToBeRemoved = null;
    for (int i = start; i < nodes.length; i++) {
      hasNonDefaultTTL = !current.getChildren().isEmpty() || current.ttl != NULL_TTL;
      CacheNode child = current.getChild(nodes[i]);
      if (child == null) {
        // there is no matching path on ttl cache tree
        return new TSStatus(TSStatusCode.PATH_NOT_EXIST.getStatusCode())
            .setMessage(
                "Not TTL rule"
                    + " set for "
                    + String.join(IoTDBConstant.PATH_SEPARATOR + "", nodes));
      }
      if (hasNonDefaultTTL) {
        parentOfSubPathToBeRemoved = current;
        index = i;
      }
      current = child;
    }
    // currently, current node is the leaf node of the path to be removed
    if (current.ttl != NULL_TTL) {
      ttlCount--;
    }

    if (!current.getChildren().isEmpty()) {
      // node to be removed is internal node, then just reset its ttl
      current.ttl = NULL_TTL;
      return StatusUtils.OK;
    }

    // node to be removed is leaf node, then remove corresponding node of this path from cache tree
    if (parentOfSubPathToBeRemoved != null) {
      parentOfSubPathToBeRemoved.removeChild(nodes[index]);
    }
    return StatusUtils.OK;
  }

  /**
   * Get ttl from cache tree. Return the TTL of the node closest to the path leaf node that has a
   * TTL which is not NULL_TTL.
   *
   * @param nodes should be prefix path or specific device path without wildcard
   */
  public long getClosestTTL(String[] nodes) {
    long ttl = ttlCacheTree.ttl;
    CacheNode current = ttlCacheTree;
    int start = nodes[0].equals("root") ? 1 : 0;
    for (int i = start; i < nodes.length; i++) {
      CacheNode child = current.getChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD);
      ttl = child != null ? child.ttl : ttl;
      current = current.getChild(nodes[i]);
      if (current == null) {
        break;
      }
    }
    ttl = current != null && current.ttl != NULL_TTL ? current.ttl : ttl;
    return ttl;
  }

  /**
   * Return the ttl of path. If the path does not exist, it means that the TTL is not set, and
   * return NULL_TTL.
   */
  public long getLastNodeTTL(String[] nodes) {
    CacheNode node = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      node = node.getChild(nodes[i]);
      if (node == null) {
        return NULL_TTL;
      }
    }
    return node.ttl;
  }

  /**
   * @return key is path contains wildcard between each node
   */
  public Map<String, Long> getAllPathTTL() {
    Map<String, Long> result = new HashMap<>();
    for (Map.Entry<String[], Long> entry : getAllTTLs().entrySet()) {
      result.put(
          String.join(String.valueOf(IoTDBConstant.PATH_SEPARATOR), entry.getKey()),
          entry.getValue());
    }
    return result;
  }

  public Map<String[], Long> getAllTTLs() {
    Map<String[], Long> result = new HashMap<>();
    List<String> pathNodes = new ArrayList<>();
    pathNodes.add(IoTDBConstant.PATH_ROOT);
    dfsCacheTree(result, pathNodes, ttlCacheTree);
    return result;
  }

  private void dfsCacheTree(
      Map<String[], Long> pathTTLMap, List<String> pathNodes, CacheNode node) {
    if (node.ttl != NULL_TTL) {
      pathTTLMap.put(pathNodes.toArray(new String[0]), node.ttl);
    }
    for (Map.Entry<String, CacheNode> entry : node.getChildren().entrySet()) {
      pathNodes.add(entry.getKey());
      dfsCacheTree(pathTTLMap, pathNodes, entry.getValue());
      pathNodes.remove(pathNodes.size() - 1);
    }
  }

  public int getTtlCount() {
    return ttlCount;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    Map<String, Long> allPathTTLMap = getAllPathTTL();
    ReadWriteIOUtils.write(allPathTTLMap.size(), outputStream);
    for (Map.Entry<String, Long> entry : allPathTTLMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    outputStream.flush();
  }

  public void deserialize(InputStream bufferedInputStream)
      throws IOException, IllegalPathException {
    int size = ReadWriteIOUtils.readInt(bufferedInputStream);
    while (size > 0) {
      String path = ReadWriteIOUtils.readString(bufferedInputStream);
      long ttl = ReadWriteIOUtils.readLong(bufferedInputStream);
      setTTL(PathUtils.splitPathToDetachedNodes(path), ttl);
      size--;
    }
  }

  public void clear() {
    ttlCacheTree.removeAllChildren();
    ttlCacheTree.addChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD, Long.MAX_VALUE);
  }

  static class CacheNode {
    public String name;
    public long ttl;
    private final Map<String, CacheNode> children;

    public CacheNode(String name, long ttl) {
      children = new HashMap<>();
      this.name = name;
      this.ttl = ttl;
    }

    public CacheNode(String name) {
      children = new HashMap<>();
      this.name = name;
      this.ttl = NULL_TTL;
    }

    public CacheNode addChild(String name, long ttl) {
      CacheNode newNode = new CacheNode(name, ttl);
      children.put(name, newNode);
      return newNode;
    }

    public void removeChild(String name) {
      children.remove(name);
    }

    public void removeAllChildren() {
      children.clear();
    }

    public CacheNode getChild(String name) {
      if (name.startsWith("root.")) {
        name = name.substring("root.".length());
      }
      return children.get(name);
    }

    public Map<String, CacheNode> getChildren() {
      return children;
    }
  }
}
