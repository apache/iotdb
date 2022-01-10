package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public interface ISchemaFile {
  void write(IMNode node);
  void write(Collection<IMNode> nodes);
  void delete(IMNode node);
  void close() throws IOException;
  IMNode read(PartialPath path);
  Iterator<IMNode> getChildren(IMNode parent);

}
