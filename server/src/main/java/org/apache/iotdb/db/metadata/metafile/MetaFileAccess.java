package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;
import java.util.Collection;

public interface MetaFileAccess {

  MNode read(PartialPath path) throws IOException;

  MNode read(long position, boolean isMeasurement) throws IOException;

  void readData(MNode mNode) throws IOException;

  void write(MNode mNode) throws IOException;

  void write(Collection<MNode> mNodes) throws IOException;

  void remove(PartialPath path) throws IOException;

  void remove(long position) throws IOException;

  void close() throws IOException;

  void sync() throws IOException;
}
