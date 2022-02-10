package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

import java.io.IOException;
import java.util.Map;

public class REntityMNode extends RMNode implements IEntityMNode {

  private volatile boolean isAligned = false;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public REntityMNode(String fullPath) {
    super(fullPath);
  }

  @Override
  public boolean addAlias(String alias, IMeasurementMNode child) {
    return false;
  }

  @Override
  public void deleteAliasChild(String alias) {}

  @Override
  public Map<String, IMeasurementMNode> getAliasChildren() {
    return null;
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren) {}

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean isAligned) {
    this.isAligned = isAligned;
  }

  @Override
  public ILastCacheContainer getLastCacheContainer(String measurementId) {
    return null;
  }

  @Override
  public Map<String, ILastCacheContainer> getTemplateLastCaches() {
    return null;
  }

  @Override
  public Map<String, IMNode> getChildren() {
    return null;
  }

  @Override
  public boolean isStorageGroup() {
    return false;
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}
}
