package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;


import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.SegmentOverflowException;
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
public class Segment implements ISegment {
  static ByteBuffer PRE_ALL_BUF = null;  // to pre-allocate segment instantly

  // members load from buffer
  final ByteBuffer buffer;
  short length, freeAddr, recordNum, pairLength;
  boolean delFlag;
  long parRecord, prevSegAddress, nextSegAddress;

  // reconstruct from key-address pair buffer
  List<Pair<String, Short>> keyAddressList;

  /**
   * Init SlottedFile with a buffer, which contains all information about this segment
   *
   * For a page no more than 16 kib, a signed short is enough to index all bytes inside a segment.
   * Segment Structure:
   * 25 byte: header
   *   1 short: length, segment length
   *   1 short: freeAddr, start offset of records
   *   1 short: recordNum, amount of records in this segment
   *   1 short: pairLength, length of key-address in bytes
   *   1 long (8 bytes): prevSegIndex, previous segment index
   *   1 long (8 bytes): nextSegIndex, next segment index
   *   1 bit: delFlag, delete flag
   *   (--- check sum, parent record address, max/min record key may be contained further ---)
   *
   * var length: key-address pairs, begin at 25 bytes offset, length of pairLength
   * ... empty space ...
   * var length: records
   * */
  public Segment(ByteBuffer buffer, boolean override) {
    this.buffer = buffer;
    // determine from 3 kind: BLANK, EXISTED, CRACKED
    if (override) {
      // blank segment
      length = (short) buffer.capacity();
      freeAddr = (short) buffer.capacity();
      recordNum = 0;
      pairLength = 0;
      prevSegAddress = 0;
      nextSegAddress = 0;
      delFlag = false;
      // parRecord = lastSegAddr = nextSegAddr = 0L;

      keyAddressList = new ArrayList<>();
    } else {
      length = ReadWriteIOUtils.readShort(buffer);
      freeAddr = ReadWriteIOUtils.readShort(buffer);
      recordNum = ReadWriteIOUtils.readShort(buffer);
      pairLength = ReadWriteIOUtils.readShort(buffer);
      delFlag = ReadWriteIOUtils.readBool(buffer);

      // parRecord = ReadWriteIOUtils.readLong(buffer);
      prevSegAddress = ReadWriteIOUtils.readLong(buffer);
      nextSegAddress = ReadWriteIOUtils.readLong(buffer);

      buffer.position(Segment.SEG_HEADER_SIZE);
      buffer.limit(Segment.SEG_HEADER_SIZE + pairLength);
      ByteBuffer pairBuffer = buffer.slice();
      buffer.clear();  // reconstruction finished, reset buffer position and limit
      reconstructKeyAddress(pairBuffer);
    }
  }

  public Segment(ByteBuffer buffer) {
    this(buffer, true);
  }

  public Segment(int size) {
    this(ByteBuffer.allocate(size));
  }

  public static ISegment initAsSegment(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    return new Segment(buffer, true);
  }

  public static ISegment loadAsSegment(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    return new Segment(buffer, false);
  }

  // region interface impl

  /**
   * check whether enough space, notice that pairLength including 3 parts:
   *   [var length] key string itself,
   *   [int, 4 bytes] length of key string,
   *   [short, 2 bytes] key address
   * @return -1 for segment overflow, otherwise for spare space
   */
  @Override
  public int insertRecord(String key, ByteBuffer buf) throws RecordDuplicatedException {
    int recordStartAddr = freeAddr - buf.capacity();

    int newPairLength = pairLength + key.getBytes().length + 4 + 2;
    if (recordStartAddr < Segment.SEG_HEADER_SIZE + newPairLength) {
      return -1;
    }
    pairLength = (short) newPairLength;

    int tarIdx = 0;
    // TODO: implement binary search further
    for (int idx = 0; idx < keyAddressList.size(); idx++) {
      if (keyAddressList.get(idx).left.compareTo(key) == 0) {
        throw new RecordDuplicatedException(key);
      }
      if (keyAddressList.get(idx).left.compareTo(key) > 0) {
        tarIdx = idx;
        break;
      }
      if (idx == keyAddressList.size() - 1) {
        tarIdx = idx + 1;
      }
    }
    keyAddressList.add(tarIdx, new Pair<>(key, (short) recordStartAddr));

    buf.clear();
    this.buffer.position(recordStartAddr);
    this.buffer.put(buf);

    this.freeAddr = (short) recordStartAddr;
    this.recordNum ++;

    return recordStartAddr - pairLength - Segment.SEG_HEADER_SIZE;
  }

  @Override
  public int insertRecords(String[] keys, ByteBuffer[] buffers) {
    return -1;
  }

  @Override
  public ByteBuffer getRecord(String key) {
    short targetAddr;
    int idx = getRecordIndexByKey(key);
    if (idx >= 0) {
      targetAddr = keyAddressList.get(idx).right;
      this.buffer.clear();
      this.buffer.position(targetAddr);
      short len = RecordUtils.getRecordLength(this.buffer);
      this.buffer.limit(targetAddr + len);
      return this.buffer.slice();
    }
    return null;
  }

  /**
   * @param key
   * @param buffer
   * @return index of keyAddressList, -1 for not found, exception for space run out
   * @throws SegmentOverflowException if segment runs out of memory
   */
  @Override
  public int updateRecord(String key, ByteBuffer buffer) throws SegmentOverflowException {
    int idx = getRecordIndexByKey(key);
    if (idx < 0) {
      return -1;
    }

    this.buffer.clear();
    buffer.clear();
    this.buffer.position(keyAddressList.get(idx).right);
    short oriLen = RecordUtils.getRecordLength(this.buffer);
    short newLen = (short)buffer.capacity();
    if (oriLen >= newLen) {
      // update in place
      buffer.clear();
      this.buffer.limit(this.buffer.position() + oriLen);
      this.buffer.put(buffer);
    } else {
      // allocate new space for record, modify key-address list, freeAddr
      if (ISegment.SEG_HEADER_SIZE + pairLength + newLen > freeAddr) {
        // not enough space
        throw new SegmentOverflowException(idx);
      }

      freeAddr = (short)(freeAddr - newLen);
      keyAddressList.get(idx).right = freeAddr;
      // it will not mark old record as expired
      this.buffer.position(freeAddr);
      this.buffer.limit(freeAddr + newLen);
      this.buffer.put(buffer);
    }

    return idx;
  }

  @Override
  public int removeRecord(String key) {
    // just modify keyAddressList, if the last record to delete, modify freeAddr
    int idx = getRecordIndexByKey(key);
    if (idx < 0) {
      return -1;
    }

    if (keyAddressList.get(idx).right == freeAddr) {
      this.buffer.clear();
      this.buffer.position(freeAddr);
      short len = RecordUtils.getRecordLength(this.buffer);
      freeAddr += len;
    }
    pairLength -= 2;
    pairLength -= key.getBytes().length;
    keyAddressList.remove(idx);

    return idx;
  }

  @Override
  public void syncBuffer() {
    ByteBuffer prefBuffer = ByteBuffer.allocate(Segment.SEG_HEADER_SIZE + pairLength);

    ReadWriteIOUtils.write(length, prefBuffer);
    ReadWriteIOUtils.write(freeAddr, prefBuffer);
    ReadWriteIOUtils.write(recordNum, prefBuffer);
    ReadWriteIOUtils.write(pairLength, prefBuffer);
    ReadWriteIOUtils.write(prevSegAddress, prefBuffer);
    ReadWriteIOUtils.write(nextSegAddress, prefBuffer);
    ReadWriteIOUtils.write(delFlag, prefBuffer);

    prefBuffer.position(Segment.SEG_HEADER_SIZE);

    for (Pair<String, Short> pair: keyAddressList) {
      ReadWriteIOUtils.write(pair.left, prefBuffer);
      ReadWriteIOUtils.write(pair.right, prefBuffer);
    }

    prefBuffer.clear();
    this.buffer.clear();
    this.buffer.put(prefBuffer);
  }

  @Override
  public void delete() {
    this.delFlag = true;
    this.buffer.clear();
    this.buffer.position(SEG_HEADER_SIZE - 1);
    ReadWriteIOUtils.write(true, this.buffer);
  }

  @Override
  public short size(){
    return length;
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) {
    short sizeGap = (short)(newBuffer.capacity() - length);

    this.buffer.clear();
    newBuffer.clear();

    if (sizeGap < 0) {
      return;
    }
    if (sizeGap == 0) {
      newBuffer.put(this.buffer);
      this.buffer.clear();
      newBuffer.clear();
      return;
    }

    ReadWriteIOUtils.write((short)newBuffer.capacity(), newBuffer);
    ReadWriteIOUtils.write((short)(freeAddr + sizeGap), newBuffer);
    ReadWriteIOUtils.write(recordNum, newBuffer);
    ReadWriteIOUtils.write(pairLength, newBuffer);
    ReadWriteIOUtils.write(prevSegAddress, newBuffer);
    ReadWriteIOUtils.write(nextSegAddress, newBuffer);
    ReadWriteIOUtils.write(delFlag, newBuffer);

    newBuffer.position(ISegment.SEG_HEADER_SIZE);
    for(Pair<String, Short> pair : keyAddressList) {
      ReadWriteIOUtils.write(pair.left, newBuffer);
      ReadWriteIOUtils.write((short)(pair.right + sizeGap), newBuffer);
    }

    this.buffer.clear();
    this.buffer.position(freeAddr);
    this.buffer.limit(length);
    newBuffer.position(freeAddr + sizeGap);
    newBuffer.put(this.buffer);
    newBuffer.clear();
    this.buffer.clear();
  }

  // endregion

  public ByteBuffer getBufferCopy() {
    syncBuffer();
    ByteBuffer newBuffer = ByteBuffer.allocate(this.buffer.capacity());
    this.buffer.clear();
    newBuffer.put(this.buffer);
    newBuffer.clear();
    return newBuffer;
  }

  public long getPrevSegAddress() {
    return prevSegAddress;
  }

  public long getNextSegAddress() {
    return nextSegAddress;
  }

  public void setPrevSegAddress(long prevSegAddress) {
    this.prevSegAddress = prevSegAddress;
  }

  public void setNextSegAddress(long nextSegAddress) {
    this.nextSegAddress = nextSegAddress;
  }

  public static void preAllocate(ByteBuffer buf) {
    short size = (short) buf.capacity();
    if (Segment.PRE_ALL_BUF == null) {
      // construct a buffer as header without segment length (2 bytes)
      Segment.PRE_ALL_BUF = ByteBuffer.allocate(Segment.SEG_HEADER_SIZE - 2);
      ReadWriteIOUtils.write((short)0, Segment.PRE_ALL_BUF);
      ReadWriteIOUtils.write((short)0, Segment.PRE_ALL_BUF);
      ReadWriteIOUtils.write((short)0, Segment.PRE_ALL_BUF);
      ReadWriteIOUtils.write(0L, Segment.PRE_ALL_BUF);
      ReadWriteIOUtils.write(0L, Segment.PRE_ALL_BUF);
      ReadWriteIOUtils.write(false, Segment.PRE_ALL_BUF);
    }
    Segment.PRE_ALL_BUF.clear();
    buf.clear();
    ReadWriteIOUtils.write(size, buf);
    buf.put(Segment.PRE_ALL_BUF);
  }

  /**
   * To decouple search implementation from other methods
   * @param key Record Key
   * @return index of record, -1 for not found
   */
  private int getRecordIndexByKey(String key) {
    int head = 0;
    int tail = keyAddressList.size() - 1;
    if (key.compareTo(keyAddressList.get(head).left) < 0
    || key.compareTo(keyAddressList.get(tail).left)> 0) {
      return -1;
    }
    if (key.compareTo(keyAddressList.get(head).left) == 0) {
      return head;
    }
    if (key.compareTo(keyAddressList.get(tail).left) == 0) {
      return tail;
    }
    int pivot = (head + tail) / 2;
    while (key.compareTo(keyAddressList.get(pivot).left) != 0) {
      if (head == tail) {
        return -1;
      }
      if (key.compareTo(keyAddressList.get(pivot).left) < 0) {
        tail = pivot;
      } else if (key.compareTo(keyAddressList.get(pivot).left) > 0) {
        head = pivot;
      }
      pivot = (head + tail) / 2;
    }
    return pivot;

  }

  private void reconstructKeyAddress(ByteBuffer pairBuffer) {
    keyAddressList = new ArrayList<>();
    for (int idx = 0; idx < recordNum; idx++) {
      String key = ReadWriteIOUtils.readString(pairBuffer);
      Short address = ReadWriteIOUtils.readShort(pairBuffer);
      keyAddressList.add(new Pair<>(key, address));
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("");
    builder.append(String.format("[spare begin: %d, records offset: %d, pairs:",
        this.pairLength + Segment.SEG_HEADER_SIZE, this.freeAddr));
    for (Pair<String, Short> pair : keyAddressList) {
      builder.append(String.format("(%s, %d),", pair.left, pair.right));
    }
    builder.append("]");
    return builder.toString();
  }

}
