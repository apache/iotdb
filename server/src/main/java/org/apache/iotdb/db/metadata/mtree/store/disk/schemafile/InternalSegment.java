/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Work like an internal node within a B+ tree constitute of segments. <br>
 * Content of this segment is pointers to other segments, and keys suggesting information.
 */
public class InternalSegment implements ISegment<Integer, Integer> {
  public static int COMPOUND_POINT_LENGTH = 8;

  // members load from buffer
  private final ByteBuffer buffer;
  private short freeAddr, pointNum, spareSpace;
  private long prevSegAddress, nextSegAddress;
  private boolean delFlag;

  transient String lastKey = null, penulKey = null; // to assess trend of insertion

  /**
   * Notice that internal or leaf segment are both full-page segment, their address can be noted by
   * a 32-bits page index though. So a long variable is able to carry a segment address and an
   * offset indicating the key inside internal segment, which named compoundPointers in this
   * segment.
   *
   * <p>A compound pointer has the same structure as GlobalSegmentAddress, but different meaning.
   *
   * <p><b>Compound pointers will not be deserialized as any collections since it may contains
   * massive entries, and binary search and insert will be conducted DIRECTLY on {@link
   * #buffer}.</b>
   *
   * <p>Logically, pointers and keys are like: <br>
   * P_0, K_1, P_1, K_2, P_2, ... K_n-1, P_n-1<br>
   * Obviously Pi and Ki share the index, and P0 has no corresponding key for which it should be
   * initiated when constructed.
   *
   * <p><b>Internal Segment Structure:</b>
   *
   * <p>25 byte: header
   *
   * <ul>
   *   <li>1 short: identical flag, always -1, to be different from {@link Segment}
   *   <li>1 short: freeAddr, start offset of keys
   *   <li>1 short: pointNum, amount of compound pointers in this segment
   *   <li>1 short: spareSpace, accurate spare space size of this segment
   *   <li>1 long (8 bytes): prevSegIndex, points to parent segment
   *   <li>1 long (8 bytes): nextSegIndex, points to sibling segment
   *   <li>1 bit: delFlag, delete flag
   * </ul>
   *
   * (--- checksum, parent record address, max/min record key may be contained further ---)
   * <li>var length: compoundPointers, begin at 25 bytes offset, each occupies 8 bytes <br>
   *     ... spare space ...
   * <li>var length: keys, search code within the b+ tree
   */
  private InternalSegment(ByteBuffer buf, boolean override, int p0) {
    this.buffer = buf;
    this.buffer.clear();
    if (override) {
      this.freeAddr = (short) this.buffer.capacity();
      this.pointNum = 1;
      this.spareSpace = (short) (freeAddr - ISegment.SEG_HEADER_SIZE - COMPOUND_POINT_LENGTH);
      this.delFlag = false;
      this.prevSegAddress = -1;
      this.nextSegAddress = -1;
      flushBufferHeader();

      this.buffer.position(SEG_HEADER_SIZE);
      ReadWriteIOUtils.write(compoundPointer(p0, Short.MIN_VALUE), this.buffer);
    } else {
      ReadWriteIOUtils.readShort(this.buffer); // read flag bytes
      this.freeAddr = ReadWriteIOUtils.readShort(this.buffer);
      this.pointNum = ReadWriteIOUtils.readShort(this.buffer);
      this.spareSpace = ReadWriteIOUtils.readShort(this.buffer);
      this.prevSegAddress = ReadWriteIOUtils.readLong(this.buffer);
      this.nextSegAddress = ReadWriteIOUtils.readLong(this.buffer);
      this.delFlag = ReadWriteIOUtils.readBool(this.buffer);
    }
  }

  public static ISegment<Integer, Integer> initInternalSegment(ByteBuffer buffer, int p0) {
    return new InternalSegment(buffer, true, p0);
  }

  public static ISegment<Integer, Integer> loadInternalSegment(ByteBuffer buffer) {
    return new InternalSegment(buffer, false, -1);
  }

  private synchronized void flushBufferHeader() {
    this.buffer.clear();
    ReadWriteIOUtils.write((short) -1, this.buffer);
    ReadWriteIOUtils.write(freeAddr, this.buffer);
    ReadWriteIOUtils.write(pointNum, this.buffer);
    ReadWriteIOUtils.write(spareSpace, this.buffer);
    ReadWriteIOUtils.write(this.prevSegAddress, this.buffer);
    ReadWriteIOUtils.write(this.nextSegAddress, this.buffer);
    ReadWriteIOUtils.write(this.delFlag, this.buffer);
  }

  // region Interface Implementation
  /**
   * Insert B+Tree entry for {@link InternalSegment}.
   *
   * @param key
   * @param pointer address of a segment which has minimum key as first parameter
   * @return spare space if succeeds, -1 if overflow
   */
  @Override
  public int insertRecord(String key, Integer pointer) {
    int pos = getIndexByKey(key);
    // key already exists
    if (pos != 0 && getKeyByIndex(pos).equals(key)) {
      return spareSpace;
    }

    if (!spaceSpareFor(key)) {
      return -1;
    }

    if (SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * (pointNum + 1) + 4 + key.getBytes().length
        > freeAddr) {
      compactKeys();
    }

    // append key
    this.buffer.clear();
    this.freeAddr = (short) (this.freeAddr - key.getBytes().length - 4);
    this.buffer.position(freeAddr);
    ReadWriteIOUtils.write(key, this.buffer);

    int migNum = pointNum - pos - 1;
    if (migNum > 0) {
      // move compound pointers
      ByteBuffer buf = ByteBuffer.allocate(migNum * COMPOUND_POINT_LENGTH);
      this.buffer.limit(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * pointNum);
      this.buffer.position(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * (pos + 1));
      buf.put(this.buffer);

      this.buffer.position(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * (pos + 1));
      ReadWriteIOUtils.write(compoundPointer(pointer, freeAddr), this.buffer);

      buf.clear();
      this.buffer.limit(this.buffer.limit() + COMPOUND_POINT_LENGTH);
      this.buffer.put(buf);
    } else {
      // append compound pointer
      this.buffer.clear();
      this.buffer.position(SEG_HEADER_SIZE + pointNum * COMPOUND_POINT_LENGTH);
      ReadWriteIOUtils.write(compoundPointer(pointer, freeAddr), this.buffer);
    }

    spareSpace -= (key.getBytes().length + 4 + COMPOUND_POINT_LENGTH);
    pointNum++;

    penulKey = lastKey;
    lastKey = key;
    return spareSpace;
  }

  /**
   * Notice that split segment must be right sibling to the original one.
   *
   * @param key key occurs split
   * @param tPk boxed point occurs split
   * @param dstBuffer split segment buffer
   * @return search key for split segment
   */
  public synchronized String splitByKey(String key, Integer tPk, ByteBuffer dstBuffer)
      throws MetadataException {
    if (dstBuffer.capacity() != this.buffer.capacity()) {
      throw new MetadataException("Segments only split with same capacity.");
    }

    if (this.pointNum < 2) {
      throw new MetadataException("Segment has less than 2 pointers can not be split.");
    }

    int pk = tPk;
    // whether to implement inclined split
    boolean monotonic =
        SchemaFile.INCLINED_SPLIT
            && (lastKey != null)
            && (penulKey != null)
            && ((key.compareTo(lastKey)) * (lastKey.compareTo(penulKey)) > 0);

    // search key for split segment
    String searchKey = null;

    // this method BREAKS envelop of the passing in buffer to be more efficient
    // attributes for dstBuffer
    short freeAddr, pointNum, spareSpace;
    long prevSegAddress = this.prevSegAddress, nextSegAddress = this.nextSegAddress;
    boolean delFlag = false;

    dstBuffer.clear();
    this.buffer.clear();

    int pos = getIndexByKey(key);
    // bulk split will not compact buffer immediately, thus save some time
    if (SchemaFile.BULK_SPLIT && pos == 0 && monotonic) {
      // bulk way migrates all keys while k1, which is then the search key, is unnecessary
      freeAddr = this.freeAddr;
      dstBuffer.position(this.freeAddr);
      this.buffer.position(this.freeAddr);
      dstBuffer.put(this.buffer);

      // migrate p1 to p_n-1, and p1 is unnecessary to be modified
      dstBuffer.position(SEG_HEADER_SIZE);
      this.buffer.position(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH);
      this.buffer.limit(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * this.pointNum);
      dstBuffer.put(this.buffer);

      // k1 is the search key for split segment
      searchKey = getKeyByIndex(1);
      pointNum = (short) (this.pointNum - 1);
      spareSpace =
          (short)
              (freeAddr
                  - SEG_HEADER_SIZE
                  - COMPOUND_POINT_LENGTH * pointNum
                  + searchKey.getBytes().length
                  + 4);

      // only key in this.buffer
      this.pointNum = 2;
      this.freeAddr = (short) (this.buffer.capacity() - key.getBytes().length - 4);
      this.spareSpace =
          (short) (this.freeAddr - SEG_HEADER_SIZE - this.pointNum * COMPOUND_POINT_LENGTH);

      this.buffer.clear();
      this.buffer.position(this.freeAddr);
      ReadWriteIOUtils.write(key, this.buffer);
      this.buffer.position(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH);
      ReadWriteIOUtils.write(compoundPointer(pk, this.freeAddr), this.buffer);
    } else if (SchemaFile.BULK_SPLIT && pos == this.pointNum - 1 && monotonic) {
      // only p_n-1 and key will be written into split segment
      freeAddr = (short) (dstBuffer.capacity() - key.getBytes().length - 4);
      dstBuffer.position(freeAddr);
      ReadWriteIOUtils.write(key, dstBuffer);
      dstBuffer.position(SEG_HEADER_SIZE);
      ReadWriteIOUtils.write(getPointerByIndex(this.pointNum - 1), dstBuffer);
      ReadWriteIOUtils.write(compoundPointer(pk, freeAddr), dstBuffer);

      pointNum = 2;
      spareSpace = (short) (freeAddr - SEG_HEADER_SIZE - pointNum * COMPOUND_POINT_LENGTH);

      // remove k_n-1 and p_n-1 from this.buffer
      String removedKey = getKeyByIndex(this.pointNum - 1);
      searchKey = removedKey;
      this.pointNum -= 1;
      this.spareSpace += (short) (removedKey.getBytes().length + 4 + COMPOUND_POINT_LENGTH);
    } else {
      // supposing splitPos is an index of a virtual array of ordered keys
      // the virtual array includes the insert, indexed from 1 to n (n for this.pointNum)

      int splitPos;
      // ensure that trending direction always gets more space
      if (monotonic) {
        splitPos =
            key.compareTo(lastKey) > 0
                ? Math.max(pos, (this.pointNum + 1) / 2)
                : Math.min(pos, (this.pointNum + 1) / 2);
      } else {
        splitPos = (this.pointNum + 1) / 2;
      }

      // since an edge key cannot be split, it shall not be 1 or n
      if (splitPos <= 1 || splitPos == this.pointNum) {
        splitPos = splitPos <= 1 ? 2 : this.pointNum - 1;
      }

      // prepare to migrate split segment
      ByteBuffer tempPtrBuffer = ByteBuffer.allocate(COMPOUND_POINT_LENGTH * (this.pointNum + 1));
      freeAddr = (short) dstBuffer.capacity();
      pointNum = 0;

      // ptr and key to be migrated
      long mPtr;
      String mKey;
      int ai;
      for (int vi = splitPos; vi <= this.pointNum; vi++) {
        if (vi == pos + 1) {
          // directly points to the new key, do nothing
          mPtr = compoundPointer(pk, Short.MIN_VALUE);
          mKey = key;
        } else {
          // vi for virtual index of the above virtual array, ai for actual index of existed keys
          ai = vi > pos ? vi - 1 : vi;
          mPtr = getPointerByIndex(ai);
          mKey = getKeyByIndex(ai);
          // this.pointNum --;
          this.spareSpace -= COMPOUND_POINT_LENGTH + mKey.getBytes().length + 4;
        }

        pointNum++;
        // mPtr has an invalid offset, needs correction except that stores as first ptr
        if (vi == splitPos) {
          // split key will not be migrated
          searchKey = mKey;
          ReadWriteIOUtils.write(mPtr, tempPtrBuffer);
        } else {
          freeAddr -= mKey.getBytes().length + 4;
          dstBuffer.position(freeAddr);
          ReadWriteIOUtils.write(mKey, dstBuffer);
          ReadWriteIOUtils.write(compoundPointer(pageIndex(mPtr), freeAddr), tempPtrBuffer);
        }
      }
      tempPtrBuffer.flip();
      dstBuffer.position(SEG_HEADER_SIZE);
      dstBuffer.put(tempPtrBuffer);
      spareSpace = (short) (freeAddr - SEG_HEADER_SIZE - COMPOUND_POINT_LENGTH * pointNum);

      // compact this buffer
      if (pos < splitPos - 1) {
        this.pointNum -= pointNum;
        compactKeys();
        // need to be inserted
        if (insertRecord(key, pk) < 0) {
          throw new SegmentOverflowException(key);
        }
      } else {
        // one of split segment ptr comes from new key
        this.pointNum -= pointNum - 1;
        compactKeys();
      }
    }

    dstBuffer.clear();
    ReadWriteIOUtils.write((short) -1, dstBuffer);
    ReadWriteIOUtils.write(freeAddr, dstBuffer);
    ReadWriteIOUtils.write(pointNum, dstBuffer);
    ReadWriteIOUtils.write(spareSpace, dstBuffer);
    ReadWriteIOUtils.write(prevSegAddress, dstBuffer);
    ReadWriteIOUtils.write(nextSegAddress, dstBuffer);
    ReadWriteIOUtils.write(delFlag, dstBuffer);
    this.flushBufferHeader();

    penulKey = null;
    lastKey = null;
    return searchKey;
  }

  @Override
  public int updateRecord(String key, Integer updPtr)
      throws SegmentOverflowException, RecordDuplicatedException {
    return 0;
  }

  @Override
  public int removeRecord(String key) {
    return 0;
  }

  /**
   * Get page index which may contain passing key.
   *
   * @param key search key.
   * @return page index.
   */
  @Override
  public Integer getRecordByKey(String key) {
    return pageIndex(getPointerByIndex(getIndexByKey(key)));
  }

  @Override
  public boolean hasRecordKey(String key) {
    // has exactly the key passing in
    int pos = getIndexByKey(key);
    return (pos != 0) && key.equals(getKeyByIndex(pos));
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return false;
  }

  @Override
  public Queue<Integer> getAllRecords() {
    Queue<Integer> res = new ArrayDeque<>(this.pointNum);
    for (int i = 0; i < this.pointNum; i++) {
      res.add(pageIndex(getPointerByIndex(i)));
    }
    return res;
  }

  @Override
  public void syncBuffer() {
    this.flushBufferHeader();
  }

  @Override
  public short size() {
    return (short) this.buffer.capacity();
  }

  @Override
  public short getSpareSize() {
    return this.spareSpace;
  }

  @Override
  public void delete() {
    this.delFlag = true;
    flushBufferHeader();
  }

  @Override
  public long getPrevSegAddress() {
    return this.prevSegAddress;
  }

  @Override
  public long getNextSegAddress() {
    return this.nextSegAddress;
  }

  @Override
  public void setPrevSegAddress(long prevSegAddress) {
    this.prevSegAddress = prevSegAddress;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    this.nextSegAddress = nextSegAddress;
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) throws MetadataException {
    if (newBuffer.capacity() != this.buffer.capacity()) {
      throw new MetadataException("Internal Segment can only extend to buffer with same capacity.");
    }

    flushBufferHeader();
    this.buffer.clear();
    newBuffer.clear();

    newBuffer.put(this.buffer);
  }

  @Override
  public String inspect() {
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    StringBuilder builder = new StringBuilder("");
    builder.append(
        String.format(
            "[length: %d, total_ptrs: %d, spare_size:%d,",
            this.buffer.capacity(), this.pointNum, this.spareSpace));
    bufferR.clear();
    builder.append(String.format("(MIN_POINT, %s),", pageIndex(getPointerByIndex(0))));

    int i = 1;
    while (i < pointNum) {
      builder.append(
          String.format(
              "(%s, %s, %s),",
              getKeyByIndex(i), keyOffset(getPointerByIndex(i)), pageIndex(getPointerByIndex(i))));
      i++;
    }
    builder.append("]");
    return builder.toString();
  }

  @Override
  public String toString() {
    return inspect();
  }

  @Override
  public boolean isInternalSegment() {
    return true;
  }

  // endregion

  // region Segment Space Management

  private void compactKeys() {
    ByteBuffer tempBuffer = ByteBuffer.allocate(this.buffer.capacity() - this.freeAddr);
    tempBuffer.position(tempBuffer.capacity());
    this.freeAddr = (short) this.buffer.capacity();
    String key;
    int accSiz = 0;
    for (int i = 1; i < this.pointNum; i++) {
      // this.buffer will not be overridden immediately
      key = getKeyByIndex(i);
      accSiz += key.getBytes().length + 4;
      this.freeAddr = (short) (this.buffer.capacity() - accSiz);

      // for lowest 2 bytes denote key offset
      this.buffer.position(SEG_HEADER_SIZE + COMPOUND_POINT_LENGTH * i + 6);
      ReadWriteIOUtils.write(this.freeAddr, this.buffer);

      // write tempBuffer backward
      tempBuffer.position(tempBuffer.capacity() - accSiz);
      ReadWriteIOUtils.write(key, tempBuffer);
    }
    tempBuffer.position(tempBuffer.capacity() - accSiz);
    this.buffer.position(this.freeAddr);
    this.buffer.put(tempBuffer);
    this.spareSpace =
        (short) (this.freeAddr - SEG_HEADER_SIZE - COMPOUND_POINT_LENGTH * this.pointNum);
  }

  private boolean spaceSpareFor(String key) {
    // 4 byte to indicate string buffer, 8 bytes for compound pointer
    return this.spareSpace >= (key.getBytes().length + 4 + COMPOUND_POINT_LENGTH);
  }

  /**
   * Find suitable position to find or insert key. Notice that index ranges from 0 to pointNum-1.
   *
   * @param key to be searched or inserted.
   * @return position where the key is the biggest one smaller or equals to parameter.
   */
  private int getIndexByKey(String key) {
    // TODO: before leaf node implement cascade delete,
    //  RecordDuplicatedException will only be thrown from leaf node

    // notice that pointNum always bigger than 2 in a valid Internal Segment
    if (pointNum == 1 || key.compareTo(getKeyByIndex(1)) < 0) {
      return 0;
    } else if (key.compareTo(getKeyByIndex(pointNum - 1)) >= 0) {
      return pointNum - 1;
    }

    int head = 1;
    int tail = pointNum - 1;
    int pivot = (head + tail) / 2;

    // breaking condition: pivot smaller than key, but (pivot+1) bigger than key
    while (!((key.compareTo(getKeyByIndex(pivot)) >= 0)
        && (key.compareTo(getKeyByIndex(pivot + 1)) < 0))) {
      if (key.compareTo(getKeyByIndex(pivot)) < 0) {
        tail = pivot;
      } else if (key.compareTo(getKeyByIndex(pivot + 1)) == 0) {
        return pivot + 1;
      } else if (key.compareTo(getKeyByIndex(pivot + 1)) > 0) {
        head = pivot;
      }

      // it can be proved that pivot <= n-2
      pivot = (head + tail) / 2;
    }
    return pivot;
  }

  // endregion

  // region Internal Segment Utils

  /**
   * CompoundPointer structure (from high bits to low):
   *
   * <ul>
   *   <li>16 bits: reserved
   *   <li>32 bits: page index, which indicate segment actually
   *   <li>16 bits: key offset, which denote keys in corresponding segment
   * </ul>
   */
  private long compoundPointer(int pageIndex, short offset) {
    return SchemaFile.getGlobalIndex(pageIndex, offset);
  }

  private int pageIndex(long point) {
    return SchemaFile.getPageIndex(point);
  }

  private short keyOffset(long point) {
    return SchemaFile.getSegIndex(point);
  }

  private long getPointerByIndex(int index) {
    if (index < 0 || index >= pointNum) {
      // TODO: check whether reasonable to throw an unchecked
      throw new IndexOutOfBoundsException();
    }
    synchronized (this.buffer) {
      this.buffer.limit(this.buffer.capacity());
      this.buffer.position(ISegment.SEG_HEADER_SIZE + index * COMPOUND_POINT_LENGTH);
      return ReadWriteIOUtils.readLong(this.buffer);
    }
  }

  private String getKeyByOffset(short offset) {
    synchronized (this.buffer) {
      this.buffer.limit(this.buffer.capacity());
      this.buffer.position(offset);
      return ReadWriteIOUtils.readString(this.buffer);
    }
  }

  // TODO: performance leveraging. without constructing collection for keys,
  //  get a key with logical index occurs 2 synchronized buffer access
  private String getKeyByIndex(int index) {
    if (index <= 0 || index >= pointNum) {
      throw new IndexOutOfBoundsException();
    }
    return getKeyByOffset(keyOffset(getPointerByIndex(index)));
  }

  // endregion
}
