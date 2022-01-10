package org.apache.iotdb.db.metadata.schemafile;


import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

/**
 * This class initiate a segment object with corresponding bytes.
 * Implements add, get, remove records methods.
 * Acts like a wrapper of bytebuffer.
 * */
public class SlottedFile implements ISlottedFile{
  static final int SEG_HEADER = 128;

  byte[] content;

  int recordNum;
  boolean delFlag;
  long parRecord, lastSegAddr, nextSegAddr;

  /**
   * init SlottedFile with an existed buffer
   * */
  public SlottedFile(ByteBuffer buffer) {
    content = buffer.array();
    recordNum = ReadWriteIOUtils.readInt(buffer);
    delFlag = ReadWriteIOUtils.readBool(buffer);
    parRecord = ReadWriteIOUtils.readLong(buffer);
    lastSegAddr = ReadWriteIOUtils.readLong(buffer);
    nextSegAddr = ReadWriteIOUtils.readLong(buffer);
  }

  /**
   * init blank buffer and initiate SlottedFile
   * @param buffer buffer of the page
   * @param start offset to begin this segment
   * @param length length of this segment
   */
  public SlottedFile(ByteBuffer buffer, short start, short length) {
    
  }


}
