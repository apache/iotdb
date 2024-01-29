package org.apache.iotdb.commons.schema.ttl;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@NotThreadSafe
public class TTLCache {

  private final CacheNode ttlCacheTree;
  public static final long NULL_TTL = -1;

  private int ttlCount;

  public TTLCache() {
    ttlCacheTree = new CacheNode(IoTDBConstant.PATH_ROOT);
    long defaultTTL =
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs(),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    defaultTTL = defaultTTL <= 0 ? Long.MAX_VALUE : defaultTTL;
    ttlCacheTree.addChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD, defaultTTL);
    ttlCount = 1;
  }

  /**
   * Put ttl into cache tree.
   *
   * @param nodes should be prefix path or specific device path without wildcard
   * @return has added new node with ttl or not. Returns true only if the original node does not
   *     exist or the node's ttl is NULL_TTL. If the original node exists and ttl is not NULL_TTL,
   *     return false.
   */
  public void setTTL(String[] nodes, long ttl) {
    if (nodes.length < 2) {
      return;
    }
    CacheNode parent = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      CacheNode child = parent.getChild(nodes[i]);
      if (child == null) {
        parent.addChild(nodes[i], NULL_TTL);
        child = parent.getChild(nodes[i]);
      }
      parent = child;
    }
    if (parent.ttl == NULL_TTL) {
      ttlCount++;
    }
    parent.ttl = ttl;
  }

  /**
   * If the path to be removed is internal node, then just reset its ttl. Else, find the sub path
   * and remove it. Eg: The original ttl cache tree is as following: root / \ ** db / \ a b \ device
   * <br>
   * If unset ttl to root.db.b, then just reset node b ttl to NULL_TTL <br>
   * If unset ttl to root.db.b.device, then remove sub path d.device
   *
   * @param nodes path to be removed
   */
  public void unsetTTL(String[] nodes) {
    if (nodes.length < 2) {
      return;
    } else if (nodes.length == 2) {
      // if path equals to root.**, then unset it to configured ttl
      if (nodes[0].equals(IoTDBConstant.PATH_ROOT)
          && nodes[1].equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        ttlCacheTree.getChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD).ttl =
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs();
        return;
      }
    }
    CacheNode parent = ttlCacheTree;
    int index = 0;
    boolean flag;
    CacheNode parentOfSubPathToBeRemoved = null;
    for (int i = 1; i < nodes.length; i++) {
      flag = !parent.getChildren().isEmpty() || parent.ttl != NULL_TTL;
      CacheNode child = parent.getChild(nodes[i]);
      if (child == null) {
        // there is no matching path on ttl cache tree
        return;
      }
      if (flag) {
        parentOfSubPathToBeRemoved = parent;
        index = i;
      }
      parent = child;
    }
    // currently, parent is the leaf node of the path to be removed
    if (parent.ttl != NULL_TTL) {
      ttlCount--;
    }

    if (!parent.getChildren().isEmpty()) {
      // node to be removed is internal node, then just reset its ttl
      parent.ttl = NULL_TTL;
      return;
    }

    // node to be removed is leaf node, then remove corresponding node of this path from cache tree
    if (parentOfSubPathToBeRemoved != null) {
      parentOfSubPathToBeRemoved.removeChild(nodes[index]);
    }
  }

  /**
   * Get ttl from cache tree.
   *
   * @param nodes should be prefix path or specific device path without wildcard
   */
  public long getTTL(String[] nodes) {
    long ttl = ttlCacheTree.ttl;
    CacheNode parent = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      CacheNode child = parent.getChild(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD);
      ttl = child != null ? child.ttl : ttl;
      parent = parent.getChild(nodes[i]);
      if (parent == null) {
        break;
      }
    }
    ttl = parent != null && parent.ttl != NULL_TTL ? parent.ttl : ttl;
    return ttl;
  }

  public Map<String, Long> getAllTTLUnderOneNode(String[] nodes) {
    Map<String, Long> pathTTLMap = new HashMap<>();
    CacheNode node = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      node = node.getChild(nodes[i]);
      if (node == null) {
        return pathTTLMap;
      }
    }

    // get all ttl under current node
    dfsCacheTree(pathTTLMap, new StringBuilder(new PartialPath(nodes).getFullPath()), node);
    return pathTTLMap;
  }

  public long getNodeTTL(String[] nodes) {
    CacheNode node = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      node = node.getChild(nodes[i]);
      if (node == null) {
        return NULL_TTL;
      }
    }
    return node.ttl;
  }

  public Map<String, Long> getAllPathTTL() {
    Map<String, Long> result = new LinkedHashMap<>();
    dfsCacheTree(result, new StringBuilder(IoTDBConstant.PATH_ROOT), ttlCacheTree);
    return result;
  }

  private void dfsCacheTree(Map<String, Long> pathTTLMap, StringBuilder path, CacheNode node) {
    if (node.ttl != NULL_TTL) {
      pathTTLMap.put(path.toString(), node.ttl);
    }
    int idx = path.length();
    for (Map.Entry<String, CacheNode> entry : node.getChildren().entrySet()) {
      dfsCacheTree(
          pathTTLMap,
          path.append(IoTDBConstant.PATH_SEPARATOR).append(entry.getValue().name),
          entry.getValue());
      path.delete(idx, path.length());
    }
  }

  public int getTtlCount() {
    return ttlCount;
  }

  public void serialize(BufferedOutputStream outputStream) throws IOException {
    Map<String, Long> allPathTTLMap = getAllPathTTL();
    ReadWriteIOUtils.write(allPathTTLMap.size(), outputStream);
    for (Map.Entry<String, Long> entry : allPathTTLMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    outputStream.flush();
  }

  public void deserialize(BufferedInputStream bufferedInputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(bufferedInputStream);
    while (size > 0) {
      String path = ReadWriteIOUtils.readString(bufferedInputStream);
      long ttl = ReadWriteIOUtils.readLong(bufferedInputStream);
      setTTL(Objects.requireNonNull(path).split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), ttl);
      size--;
    }
  }

  public void clear() {
    ttlCacheTree.removeAllChildren();
    ttlCacheTree.addChild(
        IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD,
        CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
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

    public void addChild(CacheNode childNode) {
      children.put(childNode.name, childNode);
    }

    public void addChild(String name, long ttl) {
      children.put(name, new CacheNode(name, ttl));
    }

    public void removeChild(String name) {
      children.remove(name);
    }

    public void removeAllChildren() {
      children.clear();
    }

    public CacheNode getChild(String name) {
      return children.get(name);
    }

    public Map<String, CacheNode> getChildren() {
      return children;
    }
  }
}
