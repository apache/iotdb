package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.metafile.PersistenceInfo;

public class PersistenceMNode implements PersistenceInfo {

  /** offset in metafile */
  private long position;

  public PersistenceMNode() {}

  public PersistenceMNode(long position) {
    this.position = position;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void setPosition(long position) {
    this.position = position;
  }
}
