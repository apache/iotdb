package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.util.Iterator;

public interface ISchemaFile {

  /**
   * Get the storage group node, with its segment address of 0.
   *
   * @return node instance
   * @throws MetadataException
   */
  IMNode init() throws MetadataException;

  /**
   * Only storage group node along with its descendents could be flushed into schema file.
   *
   * @param node
   * @throws MetadataException
   * @throws IOException
   */
  void writeMNode(IMNode node) throws MetadataException, IOException;

  void delete(IMNode node) throws IOException, MetadataException;

  void close() throws IOException;

  IMNode getChildNode(IMNode parent, String childName) throws MetadataException, IOException;

  Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException;
}
