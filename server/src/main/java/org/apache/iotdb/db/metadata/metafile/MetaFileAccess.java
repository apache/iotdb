package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;

public interface MetaFileAccess {

  MNode read(PartialPath path) throws IOException;

  MNode read(long position, boolean isMeasurement) throws IOException;

  void write(MNode mNode) throws IOException;

  void remove(PartialPath path) throws IOException;

  void remove(long position) throws IOException;

  void close() throws IOException;

  void sync() throws IOException;
}
