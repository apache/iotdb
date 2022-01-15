package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.SegmentOverflowException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

/**
 * This interface interacts with segment bytebuffer.
 */
public interface ISegment {
  int SEG_HEADER_SIZE = 25;  // in bytes
  /**
   * write a record of node to this segment
   * @param key node name for generally
   * @param buffer content of the record
   * @return spare space after insert
   */
  int insertRecord(String key, ByteBuffer buffer) throws RecordDuplicatedException;

  int insertRecords(String[] keys, ByteBuffer[] buffers);

  int updateRecord(String key, ByteBuffer buffer) throws SegmentOverflowException;

  int removeRecord(String key);

  /**
   * Danger of memory leak, [[DEPRECATED]]
   * @param key node name usually
   * @return node content
   */
  ByteBuffer getRecord(String key);

  /**
   * Records are always sync with buffer, but header and key-address list are not.
   * This method sync these values to the buffer.
   */
  void syncBuffer();

  short size();

  void delete();

  long getPrevSegAddress();

  long getNextSegAddress();

  void setPrevSegAddress(long prevSegAddress);

  void setNextSegAddress(long nextSegAddress);

  /**
   * This method write content to a ByteBuffer, which has larger capacity than itself
   * @param newBuffer target buffer
   */
  void extendsTo(ByteBuffer newBuffer);

  String toString();

  static boolean isDeleted(ByteBuffer buffer) {
    buffer.clear();
    buffer.position(ISegment.SEG_HEADER_SIZE - 1);
    boolean res = ReadWriteIOUtils.readBool(buffer);
    buffer.clear();
    return res;
  }

  /**
   * If buffer position is set to begin of the segment, this methods extract length of it.
   * @param buffer
   * @return
   */
  static short getSegBufLen(ByteBuffer buffer) {
    short res = ReadWriteIOUtils.readShort(buffer);
    buffer.position(buffer.position() - 2);
    return res;
  }
}
