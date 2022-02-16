package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;

import java.io.IOException;

public class RStorageGroupMNode extends RMNode implements IStorageGroupMNode {

  private long dataTTL;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RStorageGroupMNode(String fullPath, long dataTTL) {
    super(fullPath);
    this.dataTTL = dataTTL;
  }

  @Override
  public boolean isStorageGroup() {
    return true;
  }

  @Override
  public boolean isEntity() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return false;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {}

  @Override
  public long getDataTTL() {
    return dataTTL;
  }

  @Override
  public void setDataTTL(long dataTTL) {}
}
