package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;


import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class initiate a segment object with corresponding bytes.
 * Implements add, get, remove records methods.
 * Acts like a wrapper of a bytebuffer which reflects a segment.
 * */
public class SlottedFile implements ISlottedFile{
  static final int SEG_HEADER = 10;  // in bytes

  // members load from buffer
  ByteBuffer buffer;
  short length, freeAddr, recordNum, pairLength;
  boolean delFlag;
  // TODO: accelerate access by these members
  long parRecord, lastSegAddr, nextSegAddr;

  // reconstruct from key-address pair buffer
  List<Pair<String, Short>> keyAddressList;

  /**
   * init SlottedFile with a buffer, which contains all information about this segment
   *
   * Segment Structure:
   * 10 byte: header
   *   1 short: length, segment length
   *   1 short: freeAddr, address of last free space
   *   1 short: recordNum, record counts
   *   1 short: pairLength, length of key-address
   *   1 bit: delFlag, delete flag
   *
   * var length: key-address pairs
   * ... empty space ...
   * var length: records
   * */
  public SlottedFile(ByteBuffer buffer) {
    this.buffer = buffer;
    length = ReadWriteIOUtils.readShort(buffer);
    if (length == 0) {
      // blank segment
      length = (short) buffer.capacity();
      freeAddr = (short) buffer.capacity();
      recordNum = 0;
      pairLength = 0;
      delFlag = false;
      // parRecord = lastSegAddr = nextSegAddr = 0L;

      keyAddressList = new ArrayList<>();
    } else {
      freeAddr = ReadWriteIOUtils.readShort(buffer);
      recordNum = ReadWriteIOUtils.readShort(buffer);
      pairLength = ReadWriteIOUtils.readShort(buffer);
      delFlag = ReadWriteIOUtils.readBool(buffer);

      // parRecord = ReadWriteIOUtils.readLong(buffer);
      // lastSegAddr = ReadWriteIOUtils.readLong(buffer);
      // nextSegAddr = ReadWriteIOUtils.readLong(buffer);

      buffer.position(SlottedFile.SEG_HEADER);
      buffer.limit(SlottedFile.SEG_HEADER + pairLength);
      ByteBuffer pairBuffer = buffer.slice();
      reconstructKeyAddress(pairBuffer);
    }
  }

  // region interface impl

  @Override
  public int insertRecord(String key, ByteBuffer buf) {
    // TODO: durability assurance yet to finish
    int recordStartAddr = freeAddr - buf.capacity();

    for (int idx = 0; idx < keyAddressList.size(); idx++) {
      if ((keyAddressList.get(idx).left.compareTo(key) > 0) || (idx == keyAddressList.size() - 1 )) {
        keyAddressList.add(idx, new Pair<>(key, (short) recordStartAddr));

        pairLength += key.getBytes().length + 2;
        break;
      }
    }

    this.buffer.position(recordStartAddr);
    this.buffer.put(buf);

    this.freeAddr = (short) recordStartAddr;
    this.recordNum ++;

    return recordStartAddr - pairLength - SlottedFile.SEG_HEADER;
  }

  @Override
  public int insertRecords(String[] keys, ByteBuffer[] buffers) {
    return -1;
  }

  @Override
  public ByteBuffer getRecord(String key) {
    short targetAddr;
    for (Pair<String, Short> stringShortPair : keyAddressList) {
      if (key.compareTo(stringShortPair.left) == 0) {
        targetAddr = stringShortPair.right;

        this.buffer.position(targetAddr);
        short recLen = ReadWriteIOUtils.readShort(buffer);

        this.buffer.position(targetAddr);
        this.buffer.limit(targetAddr + recLen);
        return this.buffer.slice();
      }
    }
    return null;
  }

  @Override
  public ByteBuffer sync2Buffer() {
    ByteBuffer prefBuffer = ByteBuffer.allocate(SlottedFile.SEG_HEADER + pairLength);

    ReadWriteIOUtils.write(length, prefBuffer);
    ReadWriteIOUtils.write(freeAddr, prefBuffer);
    ReadWriteIOUtils.write(recordNum, prefBuffer);
    ReadWriteIOUtils.write(pairLength, prefBuffer);
    ReadWriteIOUtils.write(delFlag, prefBuffer);

    prefBuffer.position(SlottedFile.SEG_HEADER);

    for (Pair<String, Short> pair: keyAddressList) {
      ReadWriteIOUtils.write(pair.left, prefBuffer);
      ReadWriteIOUtils.write(pair.right, prefBuffer);
    }

    this.buffer.position(0);
    this.buffer.put(prefBuffer);
    return this.buffer;
  }

  @Override
  public void deleteSegment() {
    this.delFlag = true;
  }

  // endregion

  private void reconstructKeyAddress(ByteBuffer pairBuffer) {
    keyAddressList = new ArrayList<>(recordNum);
    for (int idx = 0; idx < keyAddressList.size(); idx++) {
      String key = ReadWriteIOUtils.readString(pairBuffer);
      Short address = ReadWriteIOUtils.readShort(pairBuffer);
      keyAddressList.set(idx, new Pair<>(key, address));

      pairLength += key.getBytes().length + 2;
    }
  }

}
