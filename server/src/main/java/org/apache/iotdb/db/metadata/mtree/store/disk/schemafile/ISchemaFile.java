package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public interface ISchemaFile {

  void write(IMNode node) throws MetadataException, IOException;

  void write(Collection<IMNode> nodes);

  void delete(IMNode node);

  void close() throws IOException;

  IMNode read(IMNode parent, String childName) throws MetadataException, IOException;

  Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException;




}
