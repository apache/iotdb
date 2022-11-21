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
 * records which is serialized IMNode. <br>
 * Act like a wrapper of a bytebuffer which reflects a segment. <br>
 * And itself is wrapped inside a SchemaPage.
 */
public class WrappedSegment extends Segment<IMNode> {

  // reconstruct every initiation after keyAddressList but not write into buffer
  private List<Pair<String, String>> aliasKeyList;

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

    if (override) {
      aliasKeyList = new ArrayList<>();
    } else {
      reconstructAliasAddressList();
    }
  }

  public WrappedSegment(ByteBuffer buffer) throws RecordDuplicatedException {
    this(buffer, true);
  }

  @TestOnly
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

  private void reconstructAliasAddressList() throws RecordDuplicatedException {
    if (aliasKeyList != null) {
      aliasKeyList.clear();
    } else {
      aliasKeyList = new ArrayList<>();
    }

    ByteBuffer bufferR = this.buffer.asReadOnlyBuffer();
    bufferR.clear();
    for (Pair<String, Short> p : keyAddressList) {
      if (p.right >= 0) {
        bufferR.position(p.right);
        String alias = RecordUtils.getRecordAlias(bufferR);
        if (alias != null) {
          aliasKeyList.add(binaryInsertPairList(aliasKeyList, alias), new Pair<>(alias, p.left));
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
    if (recordStartAddr < SchemaFileConfig.SEG_HEADER_SIZE + newPairLength) {
      return -1;
    }
    pairLength = (short) newPairLength;

    int tarIdx = binaryInsertPairList(keyAddressList, key);

    // update aliasKeyList
    String alias = RecordUtils.getRecordAlias(buf);
    if (alias != null && !alias.equals("")) {
      aliasKeyList.add(binaryInsertPairList(aliasKeyList, alias), new Pair<>(alias, key));
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
    return recordStartAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE;
  }

  @Override
  public synchronized String splitByKey(
      String key, ByteBuffer recBuf, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {
    String sk = super.splitByKey(key, recBuf, dstBuffer, inclineSplit);
    reconstructAliasAddressList();
    return sk;
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

    String oriAlias = RecordUtils.getRecordAlias(this.buffer);

    short oriLen = RecordUtils.getRecordLength(this.buffer);
    short newLen = (short) uBuffer.capacity();
    if (oriLen >= newLen) {
      // update in place
      this.buffer.limit(this.buffer.position() + oriLen);
      this.buffer.put(uBuffer);
    } else {
      // allocate new space for record, modify key-address list, freeAddr
      if (SchemaFileConfig.SEG_HEADER_SIZE + pairLength + newLen > freeAddr) {
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
    String alias = RecordUtils.getRecordAlias(this.buffer);
    if (alias != null && !alias.equals("")) {
      aliasKeyList.remove(binarySearchPairList(aliasKeyList, alias));
    }

    // TODO: compact segment further as well
    recordNum--;
    pairLength -= 2;
    pairLength -= (key.getBytes().length + 4);
    keyAddressList.remove(idx);

    return idx;
  }

  @Override
  protected short getRecordLength() {
    return RecordUtils.getRecordLength(this.buffer);
  }
  // endregion

  // region Segment & Record Buffer Operation

  protected void updateRecordSegAddr(String key, long newSegAddr) {
    int index = getRecordIndexByKey(key);
    short offset = getOffsetByKeyIndex(index);

    this.buffer.clear();
    this.buffer.position(offset);
    RecordUtils.updateSegAddr(this.buffer, newSegAddr);
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
            freeAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE));
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
    if (!SchemaFileConfig.DETAIL_SKETCH) {
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
            freeAddr - pairLength - SchemaFileConfig.SEG_HEADER_SIZE));
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
