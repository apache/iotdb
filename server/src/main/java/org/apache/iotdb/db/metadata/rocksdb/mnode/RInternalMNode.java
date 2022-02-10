package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;

public class RInternalMNode extends RMNode implements IMNode {
  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RInternalMNode(String fullPath) {
    super(fullPath);
  }

  @Override
  public boolean isStorageGroup() {
    return false;
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
}
