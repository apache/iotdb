package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rocksdb.RockDBConstants;
import org.apache.iotdb.db.metadata.rocksdb.RocksDBMNodeType;
import org.apache.iotdb.db.metadata.rocksdb.RocksDBReadWriteHandler;
import org.apache.iotdb.db.metadata.rocksdb.RocksDBUtils;
import org.apache.iotdb.db.metadata.template.Template;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public abstract class RMNode implements IMNode {
  /** from root to this node, only be set when used once for InternalMNode */
  protected String fullPath;

  protected RocksDBReadWriteHandler readWriteHandler;

  protected IMNode parent;

  protected IMNode child;

  private static final Logger logger = LoggerFactory.getLogger(RMNode.class);

  /** Constructor of MNode. */
  public RMNode(String fullPath) {
    this.fullPath = fullPath.intern();
    try {
      readWriteHandler = RocksDBReadWriteHandler.getInstance();
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
    }
  }

  @Override
  public String getName() {
    return fullPath;
  }

  @Override
  public void setName(String name) {
    // Do noting
  }

  @Override
  public IMNode getParent() {
    if (parent != null) {
      return parent;
    }
    String parentName = fullPath.substring(0, fullPath.lastIndexOf(RockDBConstants.PATH_SEPARATOR));
    byte[] value = null;
    for (RocksDBMNodeType type : RocksDBMNodeType.values()) {
      String parentInnerName =
          RocksDBUtils.convertPartialPathToInner(
              parentName, RocksDBUtils.getLevelByPartialPath(parentName), type.getValue());
      try {
        value = readWriteHandler.get(null, (type + parentInnerName).getBytes());
      } catch (RocksDBException e) {
        logger.error("Failed to get parent node.", e);
      }
      if (value != null) {
        switch (type.getValue()) {
          case RockDBConstants.NODE_TYPE_SG:
            parent = new RStorageGroupMNode(parentName, value);
          case RockDBConstants.NODE_TYPE_INTERNAL:
            parent = new RInternalMNode(parentName);
          case RockDBConstants.NODE_TYPE_ENTITY:
            parent = new REntityMNode(parentName);
        }
      }
    }
    return parent;
  }

  @Override
  public void setParent(IMNode parent) {
    this.parent = parent;
  }

  @Override
  public boolean hasChild(String name) {
    // TODO: query to find if has children
    return false;
  }

  @Override
  public IMNode getChild(String name) {
    if (!hasChild(name)) {
      return null;
    }
    // TODO: query by types
    return null;
  }

  @Override
  public Map<String, IMNode> getChildren() {
    return null;
  }

  /**
   * get partial path of this node
   *
   * @return partial path
   */
  @Override
  public PartialPath getPartialPath() {
    try {
      return new PartialPath(fullPath);
    } catch (IllegalPathException ignored) {
      return null;
    }
  }

  /** get full path */
  @Override
  public String getFullPath() {
    return fullPath;
  }

  @Override
  public void setFullPath(String fullPath) {
    new UnsupportedOperationException();
  }

  @Override
  public boolean isEmptyInternal() {
    // TODO: implement it
    return false;
  }

  @Override
  public boolean isUseTemplate() {
    return false;
  }

  @Override
  public IStorageGroupMNode getAsStorageGroupMNode() {
    if (isStorageGroup()) {
      return (IStorageGroupMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IEntityMNode getAsEntityMNode() {
    if (isEntity()) {
      return (IEntityMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
  }

  @Override
  public IMeasurementMNode getAsMeasurementMNode() {
    if (isMeasurement()) {
      return (IMeasurementMNode) this;
    } else {
      throw new UnsupportedOperationException("Wrong MNode Type");
    }
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
    return Objects.equals(fullPath, mNode.getFullPath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath);
  }

  @Override
  public String toString() {
    return this.fullPath;
  }

  // unsupported exception
  @Override
  public void addChild(String name, IMNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IMNode addChild(IMNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Template getUpperTemplate() {
    return null;
  }

  @Override
  public Template getSchemaTemplate() {
    return null;
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {
    throw new UnsupportedOperationException();
  }
  // end
}
