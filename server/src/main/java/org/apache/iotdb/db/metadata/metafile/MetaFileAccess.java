package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;

public interface MetaFileAccess {

  long length() throws IOException;

  MNode read(PartialPath path) throws IOException;

  MNode read(long position) throws IOException;

  void write(MNode mNode) throws IOException;

  void remove(PartialPath path) throws IOException;

  void remove(long position) throws IOException;

  void close() throws IOException;

  void sync() throws IOException;
}
