package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;

public interface PersistenceInfo extends MNode {

  long getPosition();

  void setPosition(long position);
}
