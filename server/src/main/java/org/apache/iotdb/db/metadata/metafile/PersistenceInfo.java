package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.PersistenceMNode;

public interface PersistenceInfo {

  static PersistenceInfo createPersistenceInfo(long position){
    return new PersistenceMNode(position);
  }

  long getPosition();

  void setPosition(long position);
}
