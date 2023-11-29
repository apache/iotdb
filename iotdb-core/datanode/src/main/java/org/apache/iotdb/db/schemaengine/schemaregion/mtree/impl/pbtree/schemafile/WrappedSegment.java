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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig.SEG_HEADER_SIZE;

/**
 * This class initiate a segment object with corresponding bytes. Implements add, get, remove
 * records which is serialized ICacheMNode. <br>
 * Act like a wrapper of a bytebuffer which reflects a segment. <br>
 * And itself is wrapped inside a SchemaPage.
 */
public class WrappedSegment extends Segment<ICachedMNode> {

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
    super(buffer, override);
  }

  public WrappedSegment(ByteBuffer buffer) throws RecordDuplicatedException {
    this(buffer, true);
  }

  @TestOnly
  public WrappedSegment(int size) throws RecordDuplicatedException {
    this(ByteBuffer.allocate(size));
  }

  public static ISegment<ByteBuffer, ICachedMNode> initAsSegment(ByteBuffer buffer)
      throws RecordDuplicatedException {
    if (buffer == null) {
      return null;
    }
    return new WrappedSegment(buffer, true);
  }

  public static ISegment<ByteBuffer, ICachedMNode> loadAsSegment(ByteBuffer buffer)
      throws RecordDuplicatedException {
    if (buffer == null) {
      return null;
    }
    return new WrappedSegment(buffer, false);
  }

  // region Interface Implementation

  @Override
  public synchronized int insertRecord(String key, ByteBuffer buf)
      throws RecordDuplicatedException {
    buf.clear();

    // key and body store adjacently
    byte[] ikBytes = key.getBytes();
    int recordStartAddr = freeAddr - buf.capacity() - 4 - ikBytes.length;

    int newPairLength = pairLength + 2;
    if (recordStartAddr < SchemaFileConfig.SEG_HEADER_SIZE + newPairLength) {
      return -1;
    }
    pairLength = (short) newPairLength;

    int tarIdx = binaryInsertOnKeys(key);

    // fixme EXPENSIVE cross check of duplication between name and alias
    if (aliasFlag && getIndexByAlias(key) != -1) {
      throw new RecordDuplicatedException(
          String.format("Record [%s] has conflict name with alias of its siblings.", key));
    }

    // check alias-key duplication, set flag if necessary
    String alias = RecordUtils.getRecordAlias(buf);
    if (alias != null && !alias.equals("")) {
      if (binarySearchOnKeys(alias) >= 0 || getIndexByAlias(alias) != -1) {
        throw new RecordDuplicatedException(
            String.format("Record [%s] has conflict alias [%s] with its siblings.", key, alias));
      }
      aliasFlag = true;
    }

    buf.clear();
    this.buffer.clear();
    this.buffer.position(recordStartAddr);
    ReadWriteIOUtils.write(key, this.buffer);
    this.buffer.put(buf);

    this.buffer.clear().position(SchemaFileConfig.SEG_HEADER_SIZE + tarIdx * 2);
    int shiftOffsets = recordNum - tarIdx;
    if (shiftOffsets > 0) {
      short[] shifts = new short[shiftOffsets];
      this.buffer.asShortBuffer().get(shifts);
      this.buffer.position(SchemaFileConfig.SEG_HEADER_SIZE + tarIdx * 2);
      this.buffer.putShort((short) recordStartAddr);
      this.buffer.asShortBuffer().put(shifts);
    } else {
      this.buffer.putShort((short) recordStartAddr);
    }

    this.freeAddr = (short) recordStartAddr;
    this.recordNum++;

    penuKey = lastKey;
    lastKey = key;
    return recordStartAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE;
  }

  private int getIndexByAlias(String target) {
    ByteBuffer checkBuffer = this.buffer.asReadOnlyBuffer();
    short[] offsets = new short[recordNum];

    checkBuffer
        .position(SchemaFileConfig.SEG_HEADER_SIZE)
        .limit(SchemaFileConfig.SEG_HEADER_SIZE + pairLength);
    checkBuffer.asShortBuffer().get(offsets);

    checkBuffer.clear();
    int keySize;
    for (int i = 0; i < offsets.length; i++) {
      checkBuffer.position(offsets[i]);
      keySize = checkBuffer.getInt();
      checkBuffer.position(checkBuffer.position() + keySize);
      if (target.equals(RecordUtils.getRecordAlias(checkBuffer))) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public synchronized String splitByKey(
      String key, ByteBuffer recBuf, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {
    String sk = super.splitByKey(key, recBuf, dstBuffer, inclineSplit);
    return sk;
  }

  @Override
  public ICachedMNode getRecordByKey(String key) throws MetadataException {
    // index means order for target node in keyAddressList, NOT aliasKeyList
    int index = binarySearchOnKeys(key);

    if (index < 0) {
      return null;
    }

    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer(); // for concurrent read
    short offset = getOffsetByIndex(index);
    roBuffer.clear();
    roBuffer.position(offset + key.getBytes().length + 4);
    short len = RecordUtils.getRecordLength(roBuffer);
    roBuffer.limit(offset + key.getBytes().length + 4 + len);

    return RecordUtils.buffer2Node(key, roBuffer);
  }

  @Override
  public ICachedMNode getRecordByAlias(String alias) throws MetadataException {
    int ix = getIndexByAlias(alias);

    if (ix < 0) {
      return null;
    }

    ByteBuffer rBuffer = this.buffer.asReadOnlyBuffer();
    short offset = getOffsetByIndex(ix);
    rBuffer.position(offset);
    String key = ReadWriteIOUtils.readString(rBuffer);
    rBuffer.limit(rBuffer.position() + RecordUtils.getRecordLength(rBuffer));
    return RecordUtils.buffer2Node(key, rBuffer);
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return getIndexByAlias(alias) > -1;
  }

  @Override
  public Queue<ICachedMNode> getAllRecords() throws MetadataException {
    Queue<ICachedMNode> res = new ArrayDeque<>(recordNum);
    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer();

    short[] offsets = new short[recordNum];
    roBuffer.position(SchemaFileConfig.SEG_HEADER_SIZE);
    roBuffer.asShortBuffer().get(offsets);

    roBuffer.clear();

    short len;
    String key;
    for (short offset : offsets) {
      roBuffer.clear();
      roBuffer.position(offset);
      key = ReadWriteIOUtils.readString(roBuffer);
      len = RecordUtils.getRecordLength(roBuffer);
      roBuffer.limit(roBuffer.position() + len);
      res.add(RecordUtils.buffer2Node(key, roBuffer));
    }
    return res;
  }

  @Override
  public int updateRecord(String key, ByteBuffer uBuffer)
      throws SegmentOverflowException, RecordDuplicatedException {

    int idx = binarySearchOnKeys(key);
    if (idx < 0) {
      return -1;
    }

    this.buffer.clear();
    uBuffer.clear();
    this.buffer.position(getOffsetByIndex(idx) + 4 + key.getBytes().length);

    short oriLen = RecordUtils.getRecordLength(this.buffer);
    short newLen = (short) uBuffer.capacity();
    if (oriLen >= newLen) {
      // update in place
      this.buffer.limit(this.buffer.position() + oriLen);
      this.buffer.put(uBuffer);
    } else {
      // allocate new space for record, update offset array, freeAddr
      if (SchemaFileConfig.SEG_HEADER_SIZE + pairLength + newLen + 4 + key.getBytes().length
          > freeAddr) {
        // not enough space
        throw new SegmentOverflowException(idx);
      }

      freeAddr = (short) (freeAddr - newLen - 4 - key.getBytes().length);
      // it will not mark old record as expired
      this.buffer.position(freeAddr);
      ReadWriteIOUtils.write(key, this.buffer);
      this.buffer.put(uBuffer);

      this.buffer.position(SchemaFileConfig.SEG_HEADER_SIZE + 2 * idx);
      this.buffer.putShort(freeAddr);
    }

    return idx;
  }

  @Override
  public int removeRecord(String key) {
    int idx = binarySearchOnKeys(key);

    // deletion only seeks for name of ICacheMNode
    if (idx < 0) {
      return -1;
    }

    // restore space immediately if the last record removed
    short offset = getOffsetByIndex(idx);
    if (offset == freeAddr) {
      this.buffer.position(offset + 4 + key.getBytes().length);
      short len = RecordUtils.getRecordLength(this.buffer);
      freeAddr += len + 4 + key.getBytes().length;
    }

    // shift offsets forward
    if (idx != recordNum) {
      int shift = recordNum - idx;
      this.buffer.position(SchemaFileConfig.SEG_HEADER_SIZE + idx * 2);
      ShortBuffer lb = this.buffer.asReadOnlyBuffer().asShortBuffer();
      lb.get();
      while (shift != 0) {
        this.buffer.putShort(lb.get());
        shift--;
      }
    }

    recordNum--;
    pairLength -= 2;

    return idx;
  }

  @Override
  protected short getRecordLength() {
    return RecordUtils.getRecordLength(this.buffer);
  }
  // endregion

  // region Segment & Record Buffer Operation

  protected void updateRecordSegAddr(String key, long newSegAddr) {
    int index = binarySearchOnKeys(key);
    short offset = getOffsetByIndex(index);

    this.buffer.clear();
    this.buffer.position(offset + 4 + key.getBytes().length);
    RecordUtils.updateSegAddr(this.buffer, newSegAddr);
  }

  // endregion

  @Override
  public String toString() {
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    StringBuilder builder = new StringBuilder("");
    List<Pair<String, Short>> keyAddressList = getKeyOffsetList();
    builder.append(
        String.format(
            "[size: %d, K-AL size: %d, spare:%d,",
            this.length,
            keyAddressList.size(),
            freeAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE));
    bufferR.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      bufferR.position(pair.right + 4 + pair.left.getBytes().length);
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
    if (!SchemaFileConfig.DETAIL_SKETCH) {
      return "";
    }

    List<Pair<String, Short>> keyAddressList = getKeyOffsetList();

    // Internal/Entity presents as (name, is_aligned, child_segment_address)
    // Measurement presents as (name, data_type, encoding, compressor, alias_if_exist)
    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    StringBuilder builder = new StringBuilder("");
    builder.append(
        String.format(
            "[length: %d, total_records: %d, spare_size:%d,",
            this.length,
            keyAddressList.size(),
            freeAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE));
    bufferR.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      bufferR.position(pair.right + pair.left.getBytes().length + 4);
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
        byte[] schemaBytes = RecordUtils.getMeasStatsBytes(bufferR);
        builder.append(
            String.format(
                "(%s, %s, %s, %s, %s),",
                pair.left,
                TSDataType.values()[schemaBytes[0]],
                TSEncoding.values()[schemaBytes[1]],
                CompressionType.deserialize(schemaBytes[2]),
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
  public ByteBuffer getBufferCopy() {
    syncBuffer();
    ByteBuffer newBuffer = ByteBuffer.allocate(this.buffer.capacity());
    this.buffer.clear();
    newBuffer.put(this.buffer);
    newBuffer.clear();
    return newBuffer;
  }

  @TestOnly
  public ByteBuffer getRecord(String key) {
    short targetAddr;
    int idx = binarySearchOnKeys(key);
    if (idx >= 0) {
      targetAddr = getOffsetByIndex(idx);
      this.buffer.clear();
      this.buffer.position(targetAddr);
      this.buffer.position(targetAddr + 4 + this.buffer.getInt());
      short len = RecordUtils.getRecordLength(this.buffer);
      this.buffer.limit(this.buffer.position() + len);
      return this.buffer.slice();
    }
    return null;
  }

  @TestOnly
  public List<Pair<String, Short>> getKeyOffsetList() {
    short[] offsets = new short[recordNum];
    List<Pair<String, Short>> res = new ArrayList<>();

    ByteBuffer buffer = this.buffer.asReadOnlyBuffer();
    buffer.position(SchemaFileConfig.SEG_HEADER_SIZE);
    buffer.asShortBuffer().get(offsets);

    buffer.clear();
    for (short offset : offsets) {
      buffer.position(offset);
      res.add(new Pair<>(ReadWriteIOUtils.readString(buffer), offset));
    }

    return res;
  }

  @TestOnly
  public short[] getOffsets() {
    ByteBuffer buf = this.buffer.asReadOnlyBuffer();
    short[] ofs = new short[recordNum];
    buf.clear().position(SEG_HEADER_SIZE);
    buf.asShortBuffer().get(ofs);
    return ofs;
  }

  // endregion
}
