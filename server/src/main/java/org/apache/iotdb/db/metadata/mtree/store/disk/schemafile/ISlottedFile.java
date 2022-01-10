package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.nio.ByteBuffer;

/**
 * This interface interacts with segment bytebuffer.
 */
public interface ISlottedFile {

  /**
   * write a record of node to this segment
   * @param key node name for generally
   * @param buffer content of the record
   * @return spare space after insert
   */
  int insertRecord(String key, ByteBuffer buffer);

  int insertRecords(String[] keys, ByteBuffer[] buffers);

  /**
   * @param key node name usually
   * @return node content
   */
  ByteBuffer getRecord(String key);

  /**
   * Records are always sync with buffer, but header and key-address list are not.
   * This method sync these values to the buffer.
   * @return synchronized buffer
   */
  ByteBuffer sync2Buffer();

  void deleteSegment();
}
