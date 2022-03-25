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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

/**
 * This class initiate a segment object with corresponding bytes. Implements add, get, remove
 * records methods. <br>
 * Act like a wrapper of a bytebuffer which reflects a segment.
 */
public class Segment implements ISegment {
  private static final Logger logger = LoggerFactory.getLogger(Segment.class);

  // control concurrency of the segment executing read and write
  // ReentrantReadWriteLock segWriteLock;

  // members load from buffer
  final ByteBuffer buffer;
  short length, freeAddr, recordNum, pairLength;
  boolean delFlag;
  long prevSegAddress, nextSegAddress;

  // reconstruct from key-address pair buffer
  List<Pair<String, Short>> keyAddressList;

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
  public Segment(ByteBuffer buffer, boolean override) {
    this.buffer = buffer;
    // determine from 3 kind: BLANK, EXISTED, CRACKED
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
    } else {
      length = ReadWriteIOUtils.readShort(buffer);
      freeAddr = ReadWriteIOUtils.readShort(buffer);
      recordNum = ReadWriteIOUtils.readShort(buffer);
      pairLength = ReadWriteIOUtils.readShort(buffer);

      // parRecord = ReadWriteIOUtils.readLong(buffer);
      prevSegAddress = ReadWriteIOUtils.readLong(buffer);
      nextSegAddress = ReadWriteIOUtils.readLong(buffer);
      delFlag = ReadWriteIOUtils.readBool(buffer);

      buffer.position(Segment.SEG_HEADER_SIZE);
      buffer.limit(Segment.SEG_HEADER_SIZE + pairLength);
      ByteBuffer pairBuffer = buffer.slice();
      buffer.clear(); // reconstruction finished, reset buffer position and limit
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

  private void reconstructKeyAddress(ByteBuffer pairBuffer) {
    keyAddressList = new ArrayList<>();
    for (int idx = 0; idx < recordNum; idx++) {
      String key = ReadWriteIOUtils.readString(pairBuffer);
      Short address = ReadWriteIOUtils.readShort(pairBuffer);
      keyAddressList.add(new Pair<>(key, address));
    }
  }

  // region Interface Implementation

  /**
   * check whether enough space, notice that pairLength including 3 parts: [var length] key string
   * itself, [int, 4 bytes] length of key string, [short, 2 bytes] key address
   *
   * @return -1 for segment overflow, otherwise for spare space
   */
  @Override
  public int insertRecord(String key, ByteBuffer buf) throws RecordDuplicatedException {
    try {
      buf.clear();
      bufferChecker(buf);
    } catch (BufferUnderflowException | BufferOverflowException e) {
      logger.error(
          String.format(
              "Buffer broken when insert, key:%s, buffer:%s", key, Arrays.toString(buf.array())));
      e.printStackTrace();
      throw e;
    }

    int recordStartAddr = freeAddr - buf.capacity();

    int newPairLength = pairLength + key.getBytes().length + 4 + 2;
    if (recordStartAddr < Segment.SEG_HEADER_SIZE + newPairLength) {
      return -1;
    }
    pairLength = (short) newPairLength;

    int tarIdx = 0;

    int head = 0;
    int tail = keyAddressList.size() - 1;

    if (tail == -1) {
      // no element
      tarIdx = 0;
    } else if (tail == 0) {
      // only one element
      if (keyAddressList.get(0).left.compareTo(key) == 0) {
        throw new RecordDuplicatedException(key);
      }
      tarIdx = keyAddressList.get(0).left.compareTo(key) > 0 ? 0 : 1;
    } else if (keyAddressList.get(head).left.compareTo(key) == 0
        || keyAddressList.get(tail).left.compareTo(key) == 0) {
      throw new RecordDuplicatedException(key);
    } else if (keyAddressList.get(head).left.compareTo(key) > 0) {
      tarIdx = 0;
    } else if (keyAddressList.get(tail).left.compareTo(key) < 0) {
      tarIdx = keyAddressList.size();
    } else {
      while (head != tail) {
        int pivot = (head + tail) / 2;
        if (pivot == keyAddressList.size() - 1) {
          tarIdx = pivot;
          break;
        }

        if (keyAddressList.get(pivot).left.compareTo(key) == 0
            || keyAddressList.get(pivot + 1).left.compareTo(key) == 0) {
          throw new RecordDuplicatedException(key);
        }

        if (keyAddressList.get(pivot).left.compareTo(key) < 0
            && keyAddressList.get(pivot + 1).left.compareTo(key) > 0) {
          tarIdx = pivot + 1;
          break;
        }

        if (pivot == head || pivot == tail) {
          if (keyAddressList.get(head).left.compareTo(key) > 0) {
            tarIdx = head;
          }
          if (keyAddressList.get(tail).left.compareTo(key) < 0) {
            tarIdx = tail + 1;
          }
          break;
        }

        // impossible for pivot.cmp > 0 and (pivot+1).cmp < 0
        if (keyAddressList.get(pivot).left.compareTo(key) > 0) {
          tail = pivot;
        }

        if (keyAddressList.get(pivot + 1).left.compareTo(key) < 0) {
          head = pivot;
        }
      }
    }

    buf.clear();
    this.buffer.position(recordStartAddr);
    bufferMonitor(buf);
    // this.buffer.put(buf);
    keyAddressList.add(tarIdx, new Pair<>(key, (short) recordStartAddr));
    this.freeAddr = (short) recordStartAddr;
    this.recordNum++;

    return recordStartAddr - pairLength - Segment.SEG_HEADER_SIZE;
  }

  @Override
  public IMNode getRecordAsIMNode(String key) throws MetadataException {
    int index =
        getRecordIndexByKey(key) >= 0 ? getRecordIndexByKey(key) : getRecordIndexByAlias(key);
    if (index < 0) {
      return null;
    }

    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer(); // for concurrent read
    short offset = getOffsetByIndex(index);
    roBuffer.clear();
    roBuffer.position(offset);
    short len = RecordUtils.getRecordLength(roBuffer);
    roBuffer.limit(offset + len);

    try {
      return RecordUtils.buffer2Node(keyAddressList.get(index).left, roBuffer);
    } catch (BufferUnderflowException | BufferOverflowException e) {
      logger.error(
          String.format(
              "Get record[key:%s] failed, start offset:%d, end offset:%d, buffer cap:%d",
              key, offset, offset + len, roBuffer.capacity()));
      logger.error(
          String.format(
              "Buffer content: %s",
              Arrays.toString(
                  Arrays.copyOfRange(
                      roBuffer.array(), offset, Math.min(offset + len, roBuffer.capacity())))));
      logger.error(e.toString());
      throw e;
    }
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
    Queue<IMNode> res = new ArrayDeque<>();
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

  /**
   * @param key
   * @param buffer
   * @return index of keyAddressList, -1 for not found, exception for space run out
   * @throws SegmentOverflowException if segment runs out of memory
   */
  @Override
  public int updateRecord(String key, ByteBuffer buffer) throws SegmentOverflowException {
    try {
      buffer.clear();
      bufferChecker(buffer);
    } catch (BufferUnderflowException | BufferOverflowException e) {
      logger.error(
          String.format(
              "Buffer broken when update, key:%s, buffer:%s",
              key, Arrays.toString(buffer.array())));
      e.printStackTrace();
      throw e;
    }

    int idx = getRecordIndexByKey(key);
    if (idx < 0) {
      return -1;
    }

    this.buffer.clear();
    buffer.clear();
    this.buffer.position(keyAddressList.get(idx).right);
    short oriLen = RecordUtils.getRecordLength(this.buffer);
    short newLen = (short) buffer.capacity();
    if (oriLen >= newLen) {
      // update in place
      this.buffer.limit(this.buffer.position() + oriLen);
      bufferMonitor(buffer);
      // this.buffer.put(buffer);
    } else {
      // allocate new space for record, modify key-address list, freeAddr
      if (ISegment.SEG_HEADER_SIZE + pairLength + newLen > freeAddr) {
        // not enough space
        throw new SegmentOverflowException(idx);
      }

      freeAddr = (short) (freeAddr - newLen);
      // it will not mark old record as expired
      this.buffer.position(freeAddr);
      this.buffer.limit(freeAddr + newLen);
      bufferMonitor(buffer);
      keyAddressList.get(idx).right = freeAddr;
      // this.buffer.put(buffer);
    }

    return idx;
  }

  @Override
  public int removeRecord(String key) {
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

    // TODO: compact segment further as well
    recordNum--;
    pairLength -= 2;
    pairLength -= (key.getBytes().length + 4);
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

    for (Pair<String, Short> pair : keyAddressList) {
      ReadWriteIOUtils.write(pair.left, prefBuffer);
      ReadWriteIOUtils.write(pair.right, prefBuffer);
    }

    prefBuffer.clear();
    this.buffer.clear();
    bufferMonitor(prefBuffer);
    // this.buffer.put(prefBuffer);
  }

  @Override
  public void delete() {
    this.delFlag = true;
    this.buffer.clear();
    this.buffer.position(SEG_HEADER_SIZE - 1);
    ReadWriteIOUtils.write(true, this.buffer);
  }

  @Override
  public short size() {
    return length;
  }

  @Override
  public short getSpareSize() {
    return (short) (freeAddr - pairLength - SEG_HEADER_SIZE);
  }

  /**
   * This method will write info into a buffer equal or larger to existed one. There is no need to
   * call sync before this method, since it will flush header and key-offset list directly.
   *
   * @param newBuffer target buffer
   */
  @Override
  public void extendsTo(ByteBuffer newBuffer) {
    short sizeGap = (short) (newBuffer.capacity() - length);

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

    ReadWriteIOUtils.write((short) newBuffer.capacity(), newBuffer);
    ReadWriteIOUtils.write((short) (freeAddr + sizeGap), newBuffer);
    ReadWriteIOUtils.write(recordNum, newBuffer);
    ReadWriteIOUtils.write(pairLength, newBuffer);
    ReadWriteIOUtils.write(prevSegAddress, newBuffer);
    ReadWriteIOUtils.write(nextSegAddress, newBuffer);
    ReadWriteIOUtils.write(delFlag, newBuffer);

    newBuffer.position(ISegment.SEG_HEADER_SIZE);
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
  public long getPrevSegAddress() {
    return prevSegAddress;
  }

  @Override
  public long getNextSegAddress() {
    return nextSegAddress;
  }

  @Override
  public void setPrevSegAddress(long prevSegAddress) {
    this.prevSegAddress = prevSegAddress;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    this.nextSegAddress = nextSegAddress;
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
    short offset = getOffsetByIndex(index);

    if (couldBeRootSegment()) {
      RuntimeException re = bufferIntegrity();
      if (re != null) {
        logger.error(
            String.format(
                "Buffer broken before update record segment addr, key: %s, seg addr:%s",
                key, newSegAddr));
        throw re;
      }
    }
    this.buffer.clear();
    this.buffer.position(offset);
    RecordUtils.updateSegAddr(this.buffer, newSegAddr);

    if (couldBeRootSegment()) {
      RuntimeException re = bufferIntegrity();
      if (re != null) {
        logger.error(
            String.format(
                "Buffer broken after update record segment addr, key: %s, seg addr:%s",
                key, newSegAddr));
        throw re;
      }
    }
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
    int head = 0;
    int tail = keyAddressList.size() - 1;
    if (tail < 0
        || key.compareTo(keyAddressList.get(head).left) < 0
        || key.compareTo(keyAddressList.get(tail).left) > 0) {
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
      if (head == tail || pivot == head || pivot == tail) {
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

  private int getRecordIndexByAlias(String alias) {
    int debuggerI = 0;
    ByteBuffer roBuffer = this.buffer.asReadOnlyBuffer();
    try {
      for (int i = 0; i < keyAddressList.size(); i++) {
        debuggerI = i;
        roBuffer.clear();
        roBuffer.position(keyAddressList.get(i).right);
        if (RecordUtils.getRecordType(roBuffer) == 4) {
          String tarAlias = RecordUtils.getRecordAlias(roBuffer);
          if (tarAlias != null && tarAlias.equals(alias)) {
            return i;
          }
        }
      }
    } catch (BufferUnderflowException | BufferOverflowException e) {
      logger.error(
          String.format(
              "Target alias: %s, segment size:%d, K-AL size: %d, pair of crashed index:%s",
              alias, this.length, keyAddressList.size(), keyAddressList.get(debuggerI)));
      logger.error(
          String.format(
              "Snippet of crashed record: %s",
              Arrays.toString(
                  Arrays.copyOfRange(
                      roBuffer.array(),
                      keyAddressList.get(debuggerI).right,
                      keyAddressList.get(debuggerI).right + 30))));
      e.printStackTrace();
      throw e;
    }
    return -1;
  }

  private short getOffsetByIndex(int index) {
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
            this.length, keyAddressList.size(), freeAddr - pairLength - SEG_HEADER_SIZE));
    bufferR.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      bufferR.position(pair.right);
      try {
        if (RecordUtils.getRecordType(bufferR) == 0 || RecordUtils.getRecordType(bufferR) == 1) {
          builder.append(
              String.format("(%s -> %d),", pair.left, RecordUtils.getRecordSegAddr(bufferR)));
        } else if (RecordUtils.getRecordType(bufferR) == 4) {
          builder.append(
              String.format("(%s, %s),", pair.left, RecordUtils.getRecordAlias(bufferR)));
        } else {
          logger.error(String.format("Record Broken at: %s", pair));
          throw new BufferUnderflowException();
        }
      } catch (BufferUnderflowException | BufferOverflowException e) {
        logger.error(String.format("Segment broken with string: %s", builder.toString()));
        logger.error(
            String.format(
                "Broken record bytes: %s",
                Arrays.toString(Arrays.copyOfRange(bufferR.array(), pair.right, pair.right + 30))));
        throw e;
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

  private synchronized void bufferMonitor(ByteBuffer buf) {
    int pos = this.buffer.position();
    int lmt = this.buffer.limit();
    if (couldBeRootSegment() && bufferIntegrity() != null) {
      logger.error(String.format("Buffer broken before put:%s", Arrays.toString(buf.array())));
      throw new BufferUnderflowException();
    }

    this.buffer.position(pos);
    this.buffer.limit(lmt);
    this.buffer.put(buf);

    if (couldBeRootSegment() && bufferIntegrity() != null) {
      logger.error(String.format("Buffer broken after put:%s", Arrays.toString(buf.array())));
      throw new BufferUnderflowException();
    }
  }

  private RuntimeException bufferIntegrity() {
    if (couldBeRootSegment()) {
      // only check for root page
      try {
        this.buffer.clear();
        for (Pair<String, Short> pair : keyAddressList) {
          this.buffer.position(pair.right);

          byte type = RecordUtils.getRecordType(this.buffer);
          if (type != 0 && type != 1 && type != 4) {
            logger.error(String.format("[Buffer Broken]Type error at: %s", pair));
            throw new BufferUnderflowException();
          }

          short recLen = RecordUtils.getRecordLength(this.buffer);
          if (type == 0 || type == 1) {
            if (recLen != 16) {
              logger.error(String.format("[Buffer Broken]Length error at: %s", pair));
              throw new BufferUnderflowException();
            }
          }
        }
      } catch (BufferUnderflowException | BufferOverflowException e) {
        return e;
      }
    }
    return null;
  }

  private boolean couldBeRootSegment() {
    return ((this.length == SchemaFile.SEG_MAX_SIZ) && (this.prevSegAddress < 0));
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
  public ByteBuffer getInnerBuffer() {
    return this.buffer;
  }

  @TestOnly
  public List<Pair<String, Short>> getKeyOffsetList() {
    return keyAddressList;
  }

  @TestOnly
  private void bufferChecker(ByteBuffer buf) {
    byte type = RecordUtils.getRecordType(buf);

    if (((type != 0) && (type != 1) && (type != 4)) || RecordUtils.getRecordLength(buf) <= 0) {
      throw new BufferOverflowException();
    }

    if ((type == 0) || (type == 1)) {
      if (RecordUtils.getRecordLength(buf) != 16) {
        throw new BufferOverflowException();
      }
    }
  }

  // endregion
}
