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

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.BULK_SPLIT;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.COMP_POINTER_OFFSET_DIGIT;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.COMP_PTR_OFFSET_MASK;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.INTERNAL_PAGE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.INTERNAL_SPLIT_VALVE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_HEADER_SIZE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_INDEX_MASK;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.SEG_INDEX_MASK;

/**
 * A page which acts like a segment, manages index entry of the b+ tree constructed by MNode with
 * massive children. <br>
 * Notice that {@link #spareOffset} in this class means start offset of keys.
 */
public class InternalPage extends SchemaPage implements ISegment<Integer, Integer> {

  protected static int COMPOUND_POINT_LENGTH = 8;

  private long firstLeaf;
  private int subIndexPage;

  private transient String penultKey, lastKey;

  /**
   * <b>Compound pointers will not be deserialized as any Java Objects since it may contains massive
   * entries (maybe more than 500), and binary search and insert will be conducted DIRECTLY on
   * {@link #pageBuffer}.</b>
   *
   * <p>Logically, pointers and keys are like: <br>
   * P_0, K_1, P_1, K_2, P_2, ... K_n-1, P_n-1<br>
   * Obviously Pi and Ki share the index, and P0 has no corresponding key for which it should be
   * initiated when constructed. By addition, n above is denoted by {@link #memberNum}.
   *
   * <p><b>Page Header Structure as in {@linkplain ISchemaPage}.</b>
   *
   * <p>Page Body Structure:
   *
   * <ul>
   *   <li>8 bytes * memberNum: compound pointer as {@linkplain #compoundPointer} mentioned.
   *   <li>... spare space...
   *   <li>var length * (memberNum-1): keys corresponding to the pointers
   * </ul>
   */
  public InternalPage(ByteBuffer pageBuffer) {
    super(pageBuffer);
    firstLeaf = ReadWriteIOUtils.readLong(pageBuffer);
    subIndexPage = ReadWriteIOUtils.readInt(pageBuffer);
  }

  @Override
  public int insertRecord(String key, Integer pointer) throws RecordDuplicatedException {
    // TODO: remove debug parameter INTERNAL_SPLIT_VALVE
    if (spareSize < COMPOUND_POINT_LENGTH + 4 + key.getBytes().length + INTERNAL_SPLIT_VALVE) {
      return -1;
    }

    // check whether key already exists
    int pos = getIndexByKey(key);
    if (pos != 0 && getKeyByIndex(pos).equals(key)) {
      return spareSize;
    }

    if (PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * (memberNum + 1) + 4 + key.getBytes().length
        > spareOffset) {
      compactKeys();
    }

    // append key
    this.pageBuffer.clear();
    this.spareOffset = (short) (this.spareOffset - key.getBytes().length - 4);
    this.pageBuffer.position(spareOffset);
    ReadWriteIOUtils.write(key, this.pageBuffer);

    int migNum = memberNum - pos - 1;
    if (migNum > 0) {
      // move compound pointers
      ByteBuffer buf = ByteBuffer.allocate(migNum * COMPOUND_POINT_LENGTH);
      this.pageBuffer.limit(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * memberNum);
      this.pageBuffer.position(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * (pos + 1));
      buf.put(this.pageBuffer);

      this.pageBuffer.position(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * (pos + 1));
      ReadWriteIOUtils.write(compoundPointer(pointer, spareOffset), this.pageBuffer);

      buf.clear();
      this.pageBuffer.limit(this.pageBuffer.limit() + COMPOUND_POINT_LENGTH);
      this.pageBuffer.put(buf);
    } else {
      // append compound pointer
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(PAGE_HEADER_SIZE + memberNum * COMPOUND_POINT_LENGTH);
      ReadWriteIOUtils.write(compoundPointer(pointer, spareOffset), this.pageBuffer);
    }

    spareSize -= (key.getBytes().length + 4 + COMPOUND_POINT_LENGTH);
    memberNum++;

    penultKey = lastKey;
    lastKey = key;
    return spareSize;
  }

  @Override
  public int updateRecord(String key, Integer buffer)
      throws SegmentOverflowException, RecordDuplicatedException {
    return 0;
  }

  @Override
  public int removeRecord(String key) {
    return 0;
  }

  @Override
  public Integer getRecordByKey(String key) {
    return pageIndex(getPointerByIndex(getIndexByKey(key)));
  }

  @Override
  public Integer getRecordByAlias(String alias) {
    return null;
  }

  @Override
  public boolean hasRecordKey(String key) {
    int pos = getIndexByKey(key);
    return (pos != 0) && key.equals(getKeyByIndex(pos));
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return false;
  }

  @Override
  public Queue<Integer> getAllRecords() throws MetadataException {
    Queue<Integer> res = new ArrayDeque<>(this.memberNum);
    for (int i = 0; i < this.memberNum; i++) {
      res.add(pageIndex(getPointerByIndex(i)));
    }
    return res;
  }

  @Override
  public void syncBuffer() {
    syncPageBuffer();
  }

  @Override
  public short size() {
    return (short) this.pageBuffer.capacity();
  }

  @Override
  public short getSpareSize() {
    return spareSize;
  }

  @Override
  public void delete() {}

  @Override
  public long getNextSegAddress() {
    return firstLeaf;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    firstLeaf = nextSegAddress;
  }

  @Override
  public int getSubIndex() {
    return subIndexPage;
  }

  @Override
  public void setSubIndex(int pid) {
    this.subIndexPage = pid;
    syncPageBuffer();
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) throws MetadataException {
    if (newBuffer.capacity() != this.pageBuffer.capacity()) {
      throw new MetadataException("InternalPage can only extend to buffer with same capacity.");
    }

    syncPageBuffer();
    this.pageBuffer.clear();
    newBuffer.clear();

    newBuffer.put(this.pageBuffer);
  }

  @Override
  public String splitByKey(String key, Integer tPk, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {
    // TODO: initiation and registration methods in SchemaFile
    if (dstBuffer.capacity() != this.pageBuffer.capacity()) {
      throw new MetadataException("Segments only split with same capacity.");
    }

    if (key == null || tPk == null) {
      throw new MetadataException("Internal Segment cannot split without insert key");
    }

    if (this.memberNum < 2) {
      throw new MetadataException("Segment has less than 2 pointers can not be split.");
    }

    int pk = tPk;
    // whether to implement inclined split
    boolean monotonic =
        inclineSplit
            && (lastKey != null)
            && (penultKey != null)
            && ((key.compareTo(lastKey)) * (lastKey.compareTo(penultKey)) > 0);

    // search key for split segment
    String searchKey = null;

    // this method BREAKS envelop of the passing in buffer to be more efficient
    // attributes for dstBuffer
    short spareOffset, memberNum, spareSize;
    long firstLeaf = this.firstLeaf;
    int subIdx = this.subIndexPage;

    int pos = getIndexByKey(key);

    dstBuffer.clear();
    this.pageBuffer.clear();
    // bulk split will not compact buffer immediately, thus save some time
    if (BULK_SPLIT && pos == 0 && monotonic) {
      // insert key is the smallest key, migrate all existed keys
      spareOffset = this.spareOffset;
      dstBuffer.position(this.spareOffset);
      this.pageBuffer.position(this.spareOffset);
      dstBuffer.put(this.pageBuffer);

      // migrate p1 to p_n-1, offset of each pointer shall not be modified
      dstBuffer.position(PAGE_HEADER_SIZE);
      this.pageBuffer.position(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH);
      this.pageBuffer.limit(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * this.memberNum);
      dstBuffer.put(this.pageBuffer);

      // k1 is the search key for split segment, and not valid key in split segment
      searchKey = getKeyByIndex(1);
      memberNum = (short) (this.memberNum - 1);
      spareSize =
          (short)
              (spareOffset
                  - PAGE_HEADER_SIZE
                  - COMPOUND_POINT_LENGTH * memberNum
                  + searchKey.getBytes().length
                  + 4);

      // only key in this.pageBuffer
      this.memberNum = 2;
      this.spareOffset = (short) (this.pageBuffer.capacity() - key.getBytes().length - 4);
      this.spareSize =
          (short) (this.spareOffset - PAGE_HEADER_SIZE - this.memberNum * COMPOUND_POINT_LENGTH);

      this.pageBuffer.clear();
      this.pageBuffer.position(this.spareOffset);
      ReadWriteIOUtils.write(key, this.pageBuffer);
      this.pageBuffer.position(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH);
      ReadWriteIOUtils.write(compoundPointer(pk, this.spareOffset), this.pageBuffer);
    } else if (BULK_SPLIT && pos == this.memberNum - 1 && monotonic) {
      // only p_n-1 and key will be written into split segment
      spareOffset = (short) (dstBuffer.capacity() - key.getBytes().length - 4);
      dstBuffer.position(spareOffset);
      ReadWriteIOUtils.write(key, dstBuffer);
      dstBuffer.position(PAGE_HEADER_SIZE);
      ReadWriteIOUtils.write(getPointerByIndex(this.memberNum - 1), dstBuffer);
      ReadWriteIOUtils.write(compoundPointer(pk, spareOffset), dstBuffer);

      memberNum = 2;
      spareSize = (short) (spareOffset - PAGE_HEADER_SIZE - memberNum * COMPOUND_POINT_LENGTH);

      // remove k_n-1 and p_n-1 from this.pageBuffer
      String removedKey = getKeyByIndex(this.memberNum - 1);
      searchKey = removedKey;
      this.memberNum -= 1;
      this.spareSize += (short) (removedKey.getBytes().length + 4 + COMPOUND_POINT_LENGTH);
    } else {
      // supposing splitPos is an index of a virtual array of ordered keys
      // the virtual array includes the insert, indexed from 1 to n (n for this.memberNum)

      int splitPos;
      // insert key always belongs to the bigger part
      if (monotonic) {
        splitPos =
            key.compareTo(lastKey) > 0
                ? Math.max(pos, (this.memberNum + 1) / 2)
                : Math.min(pos + 1, (this.memberNum + 1) / 2);
      } else {
        splitPos = (this.memberNum + 1) / 2;
      }

      // since an edge key cannot be split, it shall not be 1 or n
      if (splitPos <= 1 || splitPos == this.memberNum) {
        splitPos = splitPos <= 1 ? 2 : this.memberNum - 1;
      }

      // prepare to migrate split segment
      ByteBuffer tempPtrBuffer = ByteBuffer.allocate(COMPOUND_POINT_LENGTH * (this.memberNum + 1));
      spareOffset = (short) dstBuffer.capacity();
      memberNum = 0;

      // ptr and key to be migrated
      long mPtr;
      String mKey;
      int ai;
      for (int vi = splitPos; vi <= this.memberNum; vi++) {
        if (vi == pos + 1) {
          // directly points to the new key, do nothing
          // offset of mPtr always be corrected below, MIN_VALUE as placeholder
          mPtr = compoundPointer(pk, Short.MIN_VALUE);
          mKey = key;
        } else {
          // vi for virtual index of the above virtual array, ai for actual index of existed keys
          ai = vi > pos ? vi - 1 : vi;
          mPtr = getPointerByIndex(ai);
          mKey = getKeyByIndex(ai);
          // this.spareSize and this.memNumber will always be corrected below during compaction,
          //  therefore unnecessary to count here
        }

        memberNum++;
        // mPtr has an invalid offset, needs correction except that stores as first ptr
        if (vi == splitPos) {
          // split key will not be migrated
          searchKey = mKey;
          ReadWriteIOUtils.write(mPtr, tempPtrBuffer);
        } else {
          spareOffset -= mKey.getBytes().length + 4;
          dstBuffer.position(spareOffset);
          ReadWriteIOUtils.write(mKey, dstBuffer);
          ReadWriteIOUtils.write(compoundPointer(pageIndex(mPtr), spareOffset), tempPtrBuffer);
        }
      }
      tempPtrBuffer.flip();
      dstBuffer.position(PAGE_HEADER_SIZE);
      dstBuffer.put(tempPtrBuffer);
      spareSize = (short) (spareOffset - PAGE_HEADER_SIZE - COMPOUND_POINT_LENGTH * memberNum);

      // compact this buffer
      if (pos < splitPos - 1) {
        this.memberNum -= memberNum;
        compactKeys();
        // need to be inserted
        if (insertRecord(key, pk) < 0) {
          throw new SegmentOverflowException(key);
        }
      } else {
        // one of split segment ptr comes from new key
        this.memberNum -= memberNum - 1;
        compactKeys();
      }
    }

    dstBuffer.clear();
    ReadWriteIOUtils.write(INTERNAL_PAGE, dstBuffer);
    ReadWriteIOUtils.write(-1, dstBuffer);
    ReadWriteIOUtils.write(spareOffset, dstBuffer);
    ReadWriteIOUtils.write(spareSize, dstBuffer);
    ReadWriteIOUtils.write(memberNum, dstBuffer);
    ReadWriteIOUtils.write(firstLeaf, dstBuffer);
    ReadWriteIOUtils.write(subIdx, dstBuffer);

    this.syncPageBuffer();

    penultKey = null;
    lastKey = null;
    return searchKey;
  }

  @Override
  public synchronized void syncPageBuffer() {
    super.syncPageBuffer();
    ReadWriteIOUtils.write(firstLeaf, pageBuffer);
    ReadWriteIOUtils.write(subIndexPage, pageBuffer);
  }

  @Override
  public String inspect() {
    ByteBuffer bufferR = this.pageBuffer.asReadOnlyBuffer();
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "page_id:%d, spare_offset:%d, spare_size:%d%n", pageIndex, spareOffset, spareSize));
    builder.append(
        String.format(
            "[IndexEntrySegment, total_ptrs:%d, spare_size:%d, sub_index:%d, ",
            this.memberNum, this.spareSize, this.subIndexPage));
    bufferR.clear();
    builder.append(String.format("(MIN_POINT, %s),", pageIndex(getPointerByIndex(0))));

    for (int i = 1; i < memberNum; i++) {
      builder.append(
          String.format(
              "(%s, %s, %s),",
              getKeyByIndex(i), keyOffset(getPointerByIndex(i)), pageIndex(getPointerByIndex(i))));
    }
    builder.append("]\n");
    return builder.toString();
  }

  @Override
  public String toString() {
    return inspect();
  }

  @Override
  public ISegment<Integer, Integer> getAsInternalPage() {
    return this;
  }

  @Override
  public ByteBuffer resetBuffer(int ptr) {
    memberNum = 1;
    spareSize = (short) (this.pageBuffer.capacity() - PAGE_HEADER_SIZE - COMPOUND_POINT_LENGTH);
    spareOffset = (short) this.pageBuffer.capacity();
    firstLeaf = ptr;

    this.pageBuffer.clear();
    this.pageBuffer.position(PAGE_HEADER_SIZE);
    ReadWriteIOUtils.write(compoundPointer(ptr, (short) 0), this.pageBuffer);
    syncPageBuffer();
    this.pageBuffer.clear();

    return this.pageBuffer.slice();
  }

  // region Compound Pointer Utility

  private void compactKeys() {
    ByteBuffer tempBuffer = ByteBuffer.allocate(this.pageBuffer.capacity() - this.spareOffset);
    tempBuffer.position(tempBuffer.capacity());
    this.spareOffset = (short) this.pageBuffer.capacity();
    String key;
    int accSiz = 0;
    for (int i = 1; i < this.memberNum; i++) {
      // this.pageBuffer will not be overridden immediately
      key = getKeyByIndex(i);
      accSiz += key.getBytes().length + 4;
      this.spareOffset = (short) (this.pageBuffer.capacity() - accSiz);

      // for lowest 2 bytes denote key offset, FIXME: '+6' is dependent on encoding of
      // ReadWriteIOUtils
      this.pageBuffer.position(PAGE_HEADER_SIZE + COMPOUND_POINT_LENGTH * i + 6);
      ReadWriteIOUtils.write(this.spareOffset, this.pageBuffer);

      // write tempBuffer backward
      tempBuffer.position(tempBuffer.capacity() - accSiz);
      ReadWriteIOUtils.write(key, tempBuffer);
    }
    tempBuffer.position(tempBuffer.capacity() - accSiz);
    this.pageBuffer.position(this.spareOffset);
    this.pageBuffer.put(tempBuffer);
    this.spareSize =
        (short) (this.spareOffset - PAGE_HEADER_SIZE - COMPOUND_POINT_LENGTH * this.memberNum);
  }

  /**
   * Find suitable position to find or insert key. Notice that index ranges from 0 to memberNum-1.
   *
   * @param key to be searched or inserted.
   * @return position where the key is the biggest one smaller or equals to parameter.
   */
  private int getIndexByKey(String key) {
    // TODO: before leaf node implement cascade delete,
    //  RecordDuplicatedException will only be thrown from leaf node

    // notice that memberNum always bigger than 2 in a valid Internal Segment
    if (memberNum == 1 || key.compareTo(getKeyByIndex(1)) < 0) {
      return 0;
    } else if (key.compareTo(getKeyByIndex(memberNum - 1)) >= 0) {
      return memberNum - 1;
    }

    int head = 1;
    int tail = memberNum - 1;
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

  /**
   * CompoundPointer structure (from high bits to low):
   *
   * <ul>
   *   <li>16 bits: reserved
   *   <li>32 bits: page index, which points to target content
   *   <li>16 bits: key offset, which denotes where key is in this page/segment.
   * </ul>
   */
  private long compoundPointer(int pageIndex, short offset) {
    return (((PAGE_INDEX_MASK & pageIndex) << COMP_POINTER_OFFSET_DIGIT)
        | (offset & SEG_INDEX_MASK));
  }

  private int pageIndex(long pointer) {
    return (int)
        ((pointer & (PAGE_INDEX_MASK << COMP_POINTER_OFFSET_DIGIT)) >> COMP_POINTER_OFFSET_DIGIT);
  }

  private short keyOffset(long pointer) {
    return (short) (pointer & COMP_PTR_OFFSET_MASK);
  }

  private long getPointerByIndex(int index) {
    if (index < 0 || index >= memberNum) {
      // TODO: check whether reasonable to throw an unchecked
      throw new IndexOutOfBoundsException();
    }
    synchronized (pageBuffer) {
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(PAGE_HEADER_SIZE + index * COMPOUND_POINT_LENGTH);
      return ReadWriteIOUtils.readLong(this.pageBuffer);
    }
  }

  private String getKeyByOffset(short offset) {
    synchronized (this.pageBuffer) {
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(offset);
      return ReadWriteIOUtils.readString(this.pageBuffer);
    }
  }

  private String getKeyByIndex(int index) {
    if (index <= 0 || index >= memberNum) {
      throw new IndexOutOfBoundsException();
    }
    return getKeyByOffset(keyOffset(getPointerByIndex(index)));
  }
  // endregion

}
