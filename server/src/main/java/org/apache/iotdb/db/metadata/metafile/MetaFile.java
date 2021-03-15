package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;

public class MetaFile implements MetaFileAccess {
  @Override
  public long length() throws IOException {
    return 0;
  }

  @Override
  public MNode read(PartialPath path) throws IOException {
    return null;
  }

  @Override
  public MNode read(long position) throws IOException {
    return null;
  }

  @Override
  public void write(MNode mNode) throws IOException {}

  @Override
  public void remove(PartialPath path) throws IOException {}

  @Override
  public void remove(long position) throws IOException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void sync() throws IOException {}
}
