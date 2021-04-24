package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;
import java.util.Collection;

/** this interface provides mnode IO operation on a file/disk */
public interface MetaFileAccess {

  MNode readRoot() throws IOException;

  MNode read(PersistenceInfo persistenceInfo) throws IOException;

  void write(MNode mNode) throws IOException;

  void write(Collection<MNode> mNodes) throws IOException;

  void remove(PersistenceInfo persistenceInfo) throws IOException;

  void close() throws IOException;

  void sync() throws IOException;
}
