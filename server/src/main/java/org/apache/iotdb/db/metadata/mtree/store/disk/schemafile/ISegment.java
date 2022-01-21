package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.nio.ByteBuffer;
import java.util.Queue;

/** This interface interacts with segment bytebuffer. */
public interface ISegment {
  int SEG_HEADER_SIZE = 25; // in bytes
  /**
   * write a record of node to this segment
   *
   * @param key node name for generally
   * @param buffer content of the record
   * @return spare space after insert
   */
  int insertRecord(String key, ByteBuffer buffer) throws RecordDuplicatedException;

  int insertRecords(String[] keys, ByteBuffer[] buffers);

  int updateRecord(String key, ByteBuffer buffer) throws SegmentOverflowException;

  int removeRecord(String key);

  IMNode getRecordAsIMNode(String key);

  boolean hasRecordKey(String key);

  Queue<IMNode> getAllRecords();

  /**
   * Records are always sync with buffer, but header and key-address list are not. This method sync
   * these values to the buffer.
   */
  void syncBuffer();

  short size();

  short getSpareSize();

  void delete();

  long getPrevSegAddress();

  long getNextSegAddress();

  void setPrevSegAddress(long prevSegAddress);

  void setNextSegAddress(long nextSegAddress);

  /**
   * This method write content to a ByteBuffer, which has larger capacity than itself
   *
   * @param newBuffer target buffer
   */
  void extendsTo(ByteBuffer newBuffer);

  String toString();
}
