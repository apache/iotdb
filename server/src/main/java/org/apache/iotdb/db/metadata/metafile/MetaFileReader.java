package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;

public interface MetaFileReader {

  MNode readMNode(PartialPath path) throws IOException;
}
