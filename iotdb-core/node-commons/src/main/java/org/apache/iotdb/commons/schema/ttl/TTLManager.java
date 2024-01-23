package org.apache.iotdb.commons.schema.ttl;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TTLManager {

  private final CacheNode ttlCacheTree;
  public static final long NULL_TTL = -1;

  public static final String INFINITE = "INF";

  private static final String SEPARATOR = ",";

  private static final String STRING_ENCODING = "utf-8";

  public TTLManager() {
    ttlCacheTree = new CacheNode(IoTDBConstant.PATH_ROOT);
    ttlCacheTree.addChild(
        IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD,
        CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
  }

  /**
   * Put ttl into cache tree.
   *
   * @param nodes should be prefix path or specific device path without wildcard
   */
  public void setTTL(String[] nodes, long ttl) {
    CacheNode parent = ttlCacheTree;
    for (int i = 1; i < nodes.length; i++) {
      CacheNode child = parent.getChild(nodes[i]);
      if (child == null) {
        parent.addChild(nodes[i], NULL_TTL);
        child = parent.getChild(nodes[i]);
      }
      parent = child;
    }
    parent.ttl = ttl;
  }

  public boolean unsetTTL(String[] nodes) {
    CacheNode parent = ttlCacheTree;
    int index = 0;
    boolean nextToBeRemoved;
    CacheNode targetNode = null;
    for (int i = 1; i < nodes.length; i++) {
      nextToBeRemoved = !parent.getChildren().isEmpty() || parent.ttl != NULL_TTL;
      CacheNode child = parent.getChild(nodes[i]);
      if (child == null) {
        return false;
      }
      if (nextToBeRemoved) {
        targetNode = parent;
        index = i;
      }
      parent = child;
    }

    // remove corresponding nodes of this path from cache tree
    if (targetNode != null) {
      targetNode.removeChild(nodes[index]);
    }
    return true;
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
      Map<String, CacheNode> children = parent.getChildren();
      CacheNode child = children.get(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD);
      ttl = child != null ? child.ttl : ttl;
      parent = children.get(nodes[i]);
      if (parent == null) {
        break;
      }
    }
    ttl = parent != null && parent.ttl != NULL_TTL ? parent.ttl : ttl;
    return ttl;
  }

  public Map<String, Long> getAllPathTTL() {
    Map<String, Long> result = new HashMap<>();
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
      setTTL(path.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), ttl);
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
    private Map<String, CacheNode> children;

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
