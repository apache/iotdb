package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.CachedStringPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class MNode implements IMNode {

  private static Map<String, String> cachedPathPool =
      CachedStringPool.getInstance().getCachedPool();

  /** Name of the MNode */
  protected String name;

  protected IMNode parent;

  /** from root to this node, only be set when used once for InternalMNode */
  protected String fullPath;

  /** Constructor of MNode. */
  public MNode(IMNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public IMNode getParent() {
    return parent;
  }

  @Override
  public void setParent(IMNode parent) {
    this.parent = parent;
  }

  /**
   * get partial path of this node
   *
   * @return partial path
   */
  @Override
  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    IMNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  /** get full path */
  @Override
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath();
      String cachedFullPath = cachedPathPool.get(fullPath);
      if (cachedFullPath == null) {
        cachedPathPool.put(fullPath, fullPath);
      } else {
        fullPath = cachedFullPath;
      }
    }
    return fullPath;
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    IMNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.getName());
    }
    return builder.toString();
  }

  @Override
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MNode mNode = (MNode) o;
    if (fullPath == null) {
      return Objects.equals(getFullPath(), mNode.getFullPath());
    } else {
      return Objects.equals(fullPath, mNode.fullPath);
    }
  }

  @Override
  public int hashCode() {
    if (fullPath == null) {
      return Objects.hash(getFullPath());
    } else {
      return Objects.hash(fullPath);
    }
  }

  @Override
  public String toString() {
    return this.getName();
  }
}
