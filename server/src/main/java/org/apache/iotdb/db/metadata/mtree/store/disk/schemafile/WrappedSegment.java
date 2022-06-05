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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * This class initiate a segment object with corresponding bytes. Implements add, get, remove
 * records methods. <br>
 * Act like a wrapper of a bytebuffer which reflects a segment. <br>
 * And itself is wrapped inside a SchemaPage.
 */
public class WrappedSegment implements ISegment<ByteBuffer, IMNode> {

  // members load from buffer
  private final ByteBuffer buffer;
  private short length, freeAddr, recordNum, pairLength;
  private boolean delFlag;
  private long prevSegAddress, nextSegAddress;

  // reconstruct from key-address pair buffer
  private List<Pair<String, Short>> keyAddressList;

  // reconstruct every initiation after keyAddressList but not write into buffer
  private List<Pair<String, String>> aliasKeyList;

  // assess monotonic
  String penuKey = null, lastKey = null;

  /**
   * Init Segment with a buffer, which contains all information about this segment
   *
   * <p>For a page no more than 16 kib, a signed short is enough to index all bytes inside a
   * segment.
   *
   * <p><b>Segment Structure:</b>
   * <li>25 byte: header
   * <li>1 short: length, segment length
   * <li>1 short: freeAddr, start offset of records
   * <li>1 short: recordNum, amount of records in this segment
   * <li>1 short: pairLength, length of key-address in bytes
   * <li>1 long (8 bytes): prevSegIndex, previous segment index
   * <li>1 long (8 bytes): nextSegIndex, next segment index
   * <li>1 bit: delFlag, delete flag <br>
   *     (--- checksum, parent record address, max/min record key may be contained further ---)
   * <li>var length: key-address pairs, begin at 25 bytes offset, length of pairLength <br>
   *     ... empty space ...
   * <li>var length: records
   */
  public WrappedSegment(ByteBuffer buffer, boolean override) throws RecordDuplicatedException {
    this.buffer = buffer;
    this.buffer.clear();
    if (override) {
      // blank segment
      length = (short) buffer.capacity();
      freeAddr = (short) buffer.capacity();
      recordNum = 0;
      pairLength = 0;

      // these two address need to be initiated as same as in childrenContainer
      prevSegAddress = -1;
      nextSegAddress = -1;
      delFlag = false;
      // parRecord = lastSegAddr = nextSegAddr = 0L;

      keyAddressList = new ArrayList<>();
      aliasKeyList = new ArrayList<>();
    } else {
      length = ReadWriteIOUtils.readShort(buffer);
      freeAddr = ReadWriteIOUtils.readShort(buffer);
      recordNum = ReadWriteIOUtils.readShort(buffer);
      pairLength = ReadWriteIOUtils.readShort(buffer);

      // parRecord = ReadWriteIOUtils.readLong(buffer);
      prevSegAddress = ReadWriteIOUtils.readLong(buffer);
      nextSegAddress = ReadWriteIOUtils.readLong(buffer);
      delFlag = ReadWriteIOUtils.readBool(buffer);

      buffer.position(SchemaPage.SEG_HEADER_SIZE);
      buffer.limit(SchemaPage.SEG_HEADER_SIZE + pairLength);
      ByteBuffer pairBuffer = buffer.slice();
      buffer.clear(); // reconstruction finished, reset buffer position and limit
      reconstructKeyAddress(pairBuffer);
      reconstructAliasAddressList();
    }
  }

  public WrappedSegment(ByteBuffer buffer) throws RecordDuplicatedException {
    this(buffer, true);
  }

  public WrappedSegment(int size) throws RecordDuplicatedException {
    this(ByteBuffer.allocate(size));
  }

  public static ISegment<ByteBuffer, IMNode> initAsSegment(ByteBuffer buffer)
      throws RecordDuplicatedException {
    if (buffer == null) {
      return null;
    }
    return new WrappedSegment(buffer, true);
  }

  public static ISegment<ByteBuffer, IMNode> loadAsSegment(ByteBuffer buffer)
      throws RecordDuplicatedException {
    if (buffer == null) {
      return null;
    }
    return new WrappedSegment(buffer, false);
  }

  private void reconstructKeyAddress(ByteBuffer pairBuffer) {
    keyAddressList = new ArrayList<>();
    for (int idx = 0; idx < recordNum; idx++) {
      String key = ReadWriteIOUtils.readString(pairBuffer);
      Short address = ReadWriteIOUtils.readShort(pairBuffer);
      keyAddressList.add(new Pair<>(key, address));
    }
  }

  private void reconstructAliasAddressList() throws RecordDuplicatedException {
    aliasKeyList = new ArrayList<>();
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    bufferR.clear();
    for (Pair<String, Short> p : keyAddressList) {
      if (p.right >= 0) {
        bufferR.position(p.right);
        if (RecordUtils.getRecordType(bufferR) == RecordUtils.MEASUREMENT_TYPE) {
          String alias = RecordUtils.getRecordAlias(bufferR);
          if (alias != null) {
            aliasKeyList.add(binaryInsertPairList(aliasKeyList, alias), new Pair<>(alias, p.left));
          }
        }
      }
    }
  }

  // region Interface Implementation

  @Override
  public synchronized int insertRecord(String key, ByteBuffer buf)
      throws RecordDuplicatedException {
    buf.clear();

    int recordStartAddr = freeAddr - buf.capacity();

    int newPairLength = pairLength + key.getBytes().length + 4 + 2;
    if (recordStartAddr < SchemaPage.SEG_HEADER_SIZE + newPairLength) {
      return -1;
    }
    pairLength = (short) newPairLength;

    int tarIdx = binaryInsertPairList(keyAddressList, key);

    // update aliasKeyList
    if (RecordUtils.getRecordType(buf) == RecordUtils.MEASUREMENT_TYPE) {
      String alias = RecordUtils.getRecordAlias(buf);
      if (alias != null && !alias.equals("")) {
        aliasKeyList.add(binaryInsertPairList(aliasKeyList, alias), new Pair<>(alias, key));
      }
    }

    buf.clear();
    this.buffer.clear();
    this.buffer.position(recordStartAddr);
    this.buffer.put(buf);
    keyAddressList.add(tarIdx, new Pair<>(key, (short) recordStartAddr));
    this.freeAddr = (short) recordStartAddr;
    this.recordNum++;

    penuKey = lastKey;
    lastKey = key;
    return recordStartAddr - pairLength - SchemaPage.SEG_HEADER_SIZE;
  }

  @Override
  public synchronized String splitByKey(
      String key, ByteBuffer recBuf, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {
    if (this.buffer.capacity() != dstBuffer.capacity()) {
      throw new MetadataException("Segments only splits with same capacity.");
    }

    if (key == null && keyAddressList.size() == 1) {
      throw new MetadataException("Segment can not be split with only one record.");
    }

    // notice that key can be null here
    boolean monotonic =
        penuKey != null
            && key != null
            && lastKey != null
            && inclineSplit
            && (key.compareTo(lastKey)) * (lastKey.compareTo(penuKey)) > 0;

    int n = keyAddressList.size();

    // actual index of key just smaller than the insert, -2 for null key
    int pos = key != null ? binaryInsertPairList(keyAddressList, key) - 1 : -2;

    int sp; // virtual index to split
    if (monotonic) {
      // new entry into part with more space
      sp = key.compareTo(lastKey) > 0 ? Math.max(pos + 1, n / 2) : Math.min(pos + 2, n / 2);
    } else {
      sp = n / 2;
    }

    // little different from InternalSegment, only the front edge key can not split
    sp = sp <= 0 ? 1 : sp;

    // prepare header for dstBuffer
    short length = this.length,
        freeAddr = (short) dstBuffer.capacity(),
        recordNum = 0,
        pairLength = 0;
    boolean delFlag = false;
    long prevSegAddress = this.prevSegAddress, nextSegAddress = this.nextSegAddress;

    int recSize, keySize;
    String mKey, sKey = null;
    ByteBuffer srcBuf;
    int aix; // aix for actual index on keyAddressList
    n = key == null ? n - 1 : n; // null key
    // TODO: implement bulk split further
    for (int ix = sp; ix <= n; ix++) {
      if (ix == pos + 1) {
        // migrate newly insert
        srcBuf = recBuf;
        recSize = recBuf.capacity();
        mKey = key;
        keySize = 4 + mKey.getBytes().length;

        recBuf.clear();
      } else {
        srcBuf = this.buffer;
        // pos equals -2 if key is null
        aix = (ix > pos) && (pos != -2) ? ix - 1 : ix;
        mKey = keyAddressList.get(aix).left;
        keySize = 4 + mKey.getBytes().length;

        // prepare on this.buffer
        this.buffer.clear();
        this.buffer.position(keyAddressList.get(aix).right);
        recSize = RecordUtils.getRecordLength(this.buffer);
        this.buffer.limit(this.buffer.position() + recSize);

        this.recordNum--;
      }

      if (ix == sp) {
        // search key is the first key in split segment
        sKey = mKey;
      }

      freeAddr -= recSize;
      dstBuffer.position(freeAddr);
      dstBuffer.put(srcBuf);

      dstBuffer.position(SchemaPage.SEG_HEADER_SIZE + pairLength);
      ReadWriteIOUtils.write(mKey, dstBuffer);
      ReadWriteIOUtils.write(freeAddr, dstBuffer);

      recordNum++;
      pairLength += keySize + 2;
    }

    // compact and update status
    this.keyAddressList = this.keyAddressList.subList(0, this.recordNum);
    this.aliasKeyList.clear();
    compactRecords();
    reconstructAliasAddressList();
    if (sp > pos + 1 && key != null) {
      // new insert shall be in this
      if (insertRecord(key, recBuf) < 0) {
        throw new SegmentOverflowException(key);
      }
    }

    // flush dstBuffer header
    dstBuffer.clear();
    ReadWriteIOUtils.write(length, dstBuffer);
    ReadWriteIOUtils.write(freeAddr, dstBuffer);
    ReadWriteIOUtils.write(recordNum, dstBuffer);
    ReadWriteIOUtils.write(pairLength, dstBuffer);
    ReadWriteIOUtils.write(prevSegAddress, dstBuffer);
    ReadWriteIOUtils.write(nextSegAddress, dstBuffer);
    ReadWriteIOUtils.write(delFlag, dstBuffer);

    penuKey = null;
    lastKey = null;
    return sKey;
  }

  @Override
  public IMNode getRecordByKey(String key) throws MetadataException {
    // index means order for target node in keyAddressList, NOT aliasKeyList
    int index = getRecordIndexByKey(key);

    if (index < 0) {
      return null;
    }

    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer(); // for concurrent read
    short offset = getOffsetByKeyIndex(index);
    roBuffer.clear();
    roBuffer.position(offset);
    short len = RecordUtils.getRecordLength(roBuffer);
    roBuffer.limit(offset + len);

    return RecordUtils.buffer2Node(keyAddressList.get(index).left, roBuffer);
  }

  @Override
  public IMNode getRecordByAlias(String alias) throws MetadataException {
    int ix = getRecordIndexByAlias(alias);

    if (ix < 0) {
      return null;
    }

    ByteBuffer rBuffer = this.buffer.asReadOnlyBuffer();
    short offset = getOffsetByKeyIndex(ix);
    rBuffer.clear().position(offset).limit(offset + RecordUtils.getRecordLength(rBuffer));
    return RecordUtils.buffer2Node(keyAddressList.get(ix).left, rBuffer);
  }

  @Override
  public boolean hasRecordKey(String key) {
    return getRecordIndexByKey(key) > -1;
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return getRecordIndexByAlias(alias) > -1;
  }

  @Override
  public Queue<IMNode> getAllRecords() throws MetadataException {
    Queue<IMNode> res = new ArrayDeque<>(keyAddressList.size());
    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer();
    roBuffer.clear();
    for (Pair<String, Short> p : keyAddressList) {
      roBuffer.limit(roBuffer.capacity());
      roBuffer.position(p.right);
      short len = RecordUtils.getRecordLength(roBuffer);
      roBuffer.limit(p.right + len);
      res.add(RecordUtils.buffer2Node(p.left, roBuffer));
    }
    return res;
  }

  @Override
  public int updateRecord(String key, ByteBuffer uBuffer)
      throws SegmentOverflowException, RecordDuplicatedException {

    int idx = getRecordIndexByKey(key);
    if (idx < 0) {
      return -1;
    }

    this.buffer.clear();
    uBuffer.clear();
    this.buffer.position(keyAddressList.get(idx).right);

    String oriAlias = null;
    if (RecordUtils.getRecordType(this.buffer) == RecordUtils.MEASUREMENT_TYPE) {
      oriAlias = RecordUtils.getRecordAlias(this.buffer);
    }

    short oriLen = RecordUtils.getRecordLength(this.buffer);
    short newLen = (short) uBuffer.capacity();
    if (oriLen >= newLen) {
      // update in place
      this.buffer.limit(this.buffer.position() + oriLen);
      this.buffer.put(uBuffer);
    } else {
      // allocate new space for record, modify key-address list, freeAddr
      if (SchemaPage.SEG_HEADER_SIZE + pairLength + newLen > freeAddr) {
        // not enough space
        throw new SegmentOverflowException(idx);
      }

      freeAddr = (short) (freeAddr - newLen);
      // it will not mark old record as expired
      this.buffer.position(freeAddr);
      this.buffer.limit(freeAddr + newLen);
      keyAddressList.get(idx).right = freeAddr;
      this.buffer.put(uBuffer);
    }

    // update alias-key list accordingly
    if (oriAlias != null) {
      aliasKeyList.remove(binarySearchPairList(aliasKeyList, oriAlias));

      uBuffer.clear();
      String alias = RecordUtils.getRecordAlias(uBuffer);
      if (alias != null) {
        aliasKeyList.add(binaryInsertPairList(aliasKeyList, alias), new Pair<>(alias, key));
      }
    }

    return idx;
  }

  @Override
  public int removeRecord(String key) {
    int idx = getRecordIndexByKey(key);

    // deletion only seeks for name of IMNode
    if (idx < 0) {
      return -1;
    }

    this.buffer.clear();
    this.buffer.position(keyAddressList.get(idx).right);

    // free address pointer forwards if last record removed
    if (keyAddressList.get(idx).right == freeAddr) {
      short len = RecordUtils.getRecordLength(this.buffer);
      freeAddr += len;
    }

    // update alias-key list accordingly
    if (RecordUtils.getRecordType(this.buffer) == RecordUtils.MEASUREMENT_TYPE) {
      String alias = RecordUtils.getRecordAlias(this.buffer);
      if (alias != null && !alias.equals("")) {
        aliasKeyList.remove(binarySearchPairList(aliasKeyList, alias));
      }
    }

    // TODO: compact segment further as well
    recordNum--;
    pairLength -= 2;
    pairLength -= (key.getBytes().length + 4);
    keyAddressList.remove(idx);

    return idx;
  }

  @Override
  public void syncBuffer() {
    ByteBuffer prefBuffer = ByteBuffer.allocate(SchemaPage.SEG_HEADER_SIZE + pairLength);

    ReadWriteIOUtils.write(length, prefBuffer);
    ReadWriteIOUtils.write(freeAddr, prefBuffer);
    ReadWriteIOUtils.write(recordNum, prefBuffer);
    ReadWriteIOUtils.write(pairLength, prefBuffer);
    ReadWriteIOUtils.write(prevSegAddress, prefBuffer);
    ReadWriteIOUtils.write(nextSegAddress, prefBuffer);
    ReadWriteIOUtils.write(delFlag, prefBuffer);

    prefBuffer.position(SchemaPage.SEG_HEADER_SIZE);

    for (Pair<String, Short> pair : keyAddressList) {
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
    this.buffer.position(SchemaPage.SEG_HEADER_SIZE - 1);
    ReadWriteIOUtils.write(true, this.buffer);
  }

  @Override
  public short size() {
    return length;
  }

  @Override
  public short getSpareSize() {
    return (short) (freeAddr - pairLength - SchemaPage.SEG_HEADER_SIZE);
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) throws MetadataException {
    short sizeGap = (short) (newBuffer.capacity() - length);

    if (sizeGap < 0) {
      throw new MetadataException("Leaf Segment cannot extend to a smaller buffer.");
    }

    this.buffer.clear();
    newBuffer.clear();

    if (sizeGap == 0) {
      this.syncBuffer();
      this.buffer.clear();
      newBuffer.put(this.buffer);
      this.buffer.clear();
      newBuffer.clear();
      return;
    }

    ReadWriteIOUtils.write((short) newBuffer.capacity(), newBuffer);
    ReadWriteIOUtils.write((short) (freeAddr + sizeGap), newBuffer);
    ReadWriteIOUtils.write(recordNum, newBuffer);
    ReadWriteIOUtils.write(pairLength, newBuffer);
    ReadWriteIOUtils.write(prevSegAddress, newBuffer);
    ReadWriteIOUtils.write(nextSegAddress, newBuffer);
    ReadWriteIOUtils.write(delFlag, newBuffer);

    newBuffer.position(SchemaPage.SEG_HEADER_SIZE);
    for (Pair<String, Short> pair : keyAddressList) {
      ReadWriteIOUtils.write(pair.left, newBuffer);
      ReadWriteIOUtils.write((short) (pair.right + sizeGap), newBuffer);
    }

    this.buffer.clear();
    this.buffer.position(freeAddr);
    this.buffer.limit(length);
    newBuffer.position(freeAddr + sizeGap);
    newBuffer.put(this.buffer);
    newBuffer.clear();
    this.buffer.clear();
  }

  @Override
  public long getNextSegAddress() {
    return nextSegAddress;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    this.nextSegAddress = nextSegAddress;
  }

  @Override
  public ByteBuffer resetBuffer(int ptr) {
    freeAddr = (short) this.buffer.capacity();
    recordNum = 0;
    pairLength = 0;
    prevSegAddress = -1;
    nextSegAddress = -1;
    keyAddressList.clear();
    syncBuffer();
    this.buffer.clear();
    return this.buffer.slice();
  }

  // endregion

  // region Segment & Record Buffer Operation

  public ByteBuffer getBufferCopy() {
    syncBuffer();
    ByteBuffer newBuffer = ByteBuffer.allocate(this.buffer.capacity());
    this.buffer.clear();
    newBuffer.put(this.buffer);
    newBuffer.clear();
    return newBuffer;
  }

  protected void updateRecordSegAddr(String key, long newSegAddr) {
    int index = getRecordIndexByKey(key);
    short offset = getOffsetByKeyIndex(index);

    this.buffer.clear();
    this.buffer.position(offset);
    RecordUtils.updateSegAddr(this.buffer, newSegAddr);
  }

  private void compactRecords() {
    // compact by existed item on keyAddressList
    ByteBuffer tempBuf = ByteBuffer.allocate(this.buffer.capacity() - this.freeAddr);
    int accSiz = 0;
    this.pairLength = 0;
    for (Pair<String, Short> pair : keyAddressList) {
      this.pairLength += pair.left.getBytes().length + 4 + 2;
      this.buffer.clear();
      this.buffer.position(pair.right);
      this.buffer.limit(pair.right + RecordUtils.getRecordLength(this.buffer));

      accSiz += this.buffer.remaining();
      pair.right = (short) (this.buffer.capacity() - accSiz);

      tempBuf.position(tempBuf.capacity() - accSiz);
      tempBuf.put(this.buffer);
    }
    tempBuf.clear();
    tempBuf.position(tempBuf.capacity() - accSiz);
    this.freeAddr = (short) (this.buffer.capacity() - accSiz);

    this.buffer.clear();
    this.buffer.position(this.freeAddr);
    this.buffer.put(tempBuf);

    syncBuffer();
  }

  // endregion

  // region Getters of Record Index or Offset
  /**
   * To decouple search implementation from other methods Rather than offset of the target key,
   * index could be used to update or remove on keyAddressList
   *
   * @param key Record Key
   * @return index of record, -1 for not found
   */
  private int getRecordIndexByKey(String key) {
    return binarySearchPairList(keyAddressList, key);
  }

  /**
   * Notice it figures index of the record within {@link #keyAddressList} whose alias is target
   * parameter.
   *
   * @param alias
   * @return index of the record within {@link #keyAddressList}
   */
  private int getRecordIndexByAlias(String alias) {
    int aliasIndex = binarySearchPairList(aliasKeyList, alias);
    if (aliasIndex < 0) {
      return -1;
    }
    return binarySearchPairList(keyAddressList, aliasKeyList.get(aliasIndex).right);
  }

  private short getOffsetByKeyIndex(int index) {
    return keyAddressList.get(index).right;
  }

  private <T> int binarySearchPairList(List<Pair<String, T>> list, String key) {
    int head = 0;
    int tail = list.size() - 1;
    if (tail < 0
        || key.compareTo(list.get(head).left) < 0
        || key.compareTo(list.get(tail).left) > 0) {
      return -1;
    }
    if (key.compareTo(list.get(head).left) == 0) {
      return head;
    }
    if (key.compareTo(list.get(tail).left) == 0) {
      return tail;
    }
    int pivot = (head + tail) / 2;
    while (key.compareTo(list.get(pivot).left) != 0) {
      if (head == tail || pivot == head || pivot == tail) {
        return -1;
      }
      if (key.compareTo(list.get(pivot).left) < 0) {
        tail = pivot;
      } else if (key.compareTo(list.get(pivot).left) > 0) {
        head = pivot;
      }
      pivot = (head + tail) / 2;
    }
    return pivot;
  }

  /**
   * @return target index the record with passing in key should be inserted
   * @throws RecordDuplicatedException
   */
  private <T> int binaryInsertPairList(List<Pair<String, T>> list, String key)
      throws RecordDuplicatedException {
    if (list.size() == 0) {
      return 0;
    }

    int tarIdx = 0;
    int head = 0;
    int tail = list.size() - 1;

    if (list.get(head).left.compareTo(key) == 0 || list.get(tail).left.compareTo(key) == 0) {
      throw new RecordDuplicatedException(key);
    }

    if (key.compareTo(list.get(head).left) < 0) {
      return 0;
    }

    if (key.compareTo(list.get(tail).left) > 0) {
      return list.size();
    }

    int pivot;
    while (head != tail) {
      pivot = (head + tail) / 2;
      // notice pivot always smaller than list.size()-1
      if (list.get(pivot).left.compareTo(key) == 0
          || list.get(pivot + 1).left.compareTo(key) == 0) {
        throw new RecordDuplicatedException(key);
      }

      if (list.get(pivot).left.compareTo(key) < 0 && list.get(pivot + 1).left.compareTo(key) > 0) {
        return pivot + 1;
      }

      if (pivot == head || pivot == tail) {
        if (list.get(head).left.compareTo(key) > 0) {
          return head;
        }
        if (list.get(tail).left.compareTo(key) < 0) {
          return tail + 1;
        }
      }

      // impossible for pivot.cmp > 0 and (pivot+1).cmp < 0
      if (list.get(pivot).left.compareTo(key) > 0) {
        tail = pivot;
      }

      if (list.get(pivot + 1).left.compareTo(key) < 0) {
        head = pivot;
      }
    }
    return tarIdx;
  }

  // endregion

  @Override
  public String toString() {
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    StringBuilder builder = new StringBuilder("");
    builder.append(
        String.format(
            "[size: %d, K-AL size: %d, spare:%d,",
            this.length,
            keyAddressList.size(),
            freeAddr - pairLength - SchemaPage.SEG_HEADER_SIZE));
    bufferR.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      bufferR.position(pair.right);
      if (RecordUtils.getRecordType(bufferR) == 0 || RecordUtils.getRecordType(bufferR) == 1) {
        builder.append(
            String.format(
                "(%s -> %s),",
                pair.left,
                RecordUtils.getRecordSegAddr(bufferR) == -1
                    ? -1
                    : Long.toHexString(RecordUtils.getRecordSegAddr(bufferR))));
      } else if (RecordUtils.getRecordType(bufferR) == 4) {
        builder.append(String.format("(%s, %s),", pair.left, RecordUtils.getRecordAlias(bufferR)));
      } else {
        throw new BufferUnderflowException();
      }
    }
    builder.append("]");
    return builder.toString();
  }

  @Override
  public String inspect() {
    if (!SchemaFile.DETAIL_SKETCH) {
      return "";
    }

    // Internal/Entity presents as (name, is_aligned, child_segment_address)
    // Measurement presents as (name, data_type, encoding, compressor, alias_if_exist)
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    StringBuilder builder = new StringBuilder("");
    builder.append(
        String.format(
            "[length: %d, total_records: %d, spare_size:%d,",
            this.length,
            keyAddressList.size(),
            freeAddr - pairLength - SchemaPage.SEG_HEADER_SIZE));
    bufferR.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      bufferR.position(pair.right);
      if (RecordUtils.getRecordType(bufferR) == 0 || RecordUtils.getRecordType(bufferR) == 1) {
        builder.append(
            String.format(
                "(%s, %s, %s),",
                pair.left,
                RecordUtils.getAlignment(bufferR) ? "aligned" : "not_aligned",
                RecordUtils.getRecordSegAddr(bufferR) == -1
                    ? -1
                    : Long.toHexString(RecordUtils.getRecordSegAddr(bufferR))));
      } else if (RecordUtils.getRecordType(bufferR) == 4) {
        byte[] schemaBytes = RecordUtils.getSchemaBytes(bufferR);
        builder.append(
            String.format(
                "(%s, %s, %s, %s, %s),",
                pair.left,
                TSDataType.values()[schemaBytes[0]],
                TSEncoding.values()[schemaBytes[1]],
                CompressionType.values()[schemaBytes[2]],
                RecordUtils.getRecordAlias(bufferR)));
      } else {
        throw new BufferUnderflowException();
      }
    }
    builder.append("]");
    return builder.toString();
  }

  /**
   * If buffer position is set to begin of the segment, this methods extract length of it.
   *
   * @param buffer
   * @return
   */
  static short getSegBufLen(ByteBuffer buffer) {
    short res = ReadWriteIOUtils.readShort(buffer);
    buffer.position(buffer.position() - 2);
    return res;
  }

  // region Test Only Methods

  @TestOnly
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

  @TestOnly
  public List<Pair<String, Short>> getKeyOffsetList() {
    return keyAddressList;
  }

  // endregion
}
