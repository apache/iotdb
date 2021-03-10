package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;

public interface MetaFileWriter {

  void writeMNode(MNode mNode) throws IOException;
}
