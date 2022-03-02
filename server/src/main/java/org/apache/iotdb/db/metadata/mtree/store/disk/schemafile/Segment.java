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

import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * This class initiate a segment object with corresponding bytes. Implements add, get, remove
 * records methods. Act like a wrapper of a bytebuffer which reflects a segment.
 */
public class Segment implements ISegment {
  static ByteBuffer PRE_ALL_BUF = null; // to pre-allocate segment instantly

  // members load from buffer
  final ByteBuffer buffer;
  short length, freeAddr, recordNum, pairLength;
  boolean delFlag;
  long parRecord, prevSegAddress, nextSegAddress;

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

  // region interface impl

  /**
   * check whether enough space, notice that pairLength including 3 parts: [var length] key string
   * itself, [int, 4 bytes] length of key string, [short, 2 bytes] key address
   *
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

    keyAddressList.add(tarIdx, new Pair<>(key, (short) recordStartAddr));

    buf.clear();
    this.buffer.position(recordStartAddr);
    this.buffer.put(buf);

    this.freeAddr = (short) recordStartAddr;
    this.recordNum++;

    return recordStartAddr - pairLength - Segment.SEG_HEADER_SIZE;
  }

  @Override
  public int insertRecords(String[] keys, ByteBuffer[] buffers) {
    return -1;
  }

  @Override
  public IMNode getRecordAsIMNode(String key) {
    int index = getRecordIndexByKey(key);
    if (index < 0) {
      return null;
    }

    short offset = getOffsetByIndex(index);
    this.buffer.clear();
    this.buffer.position(offset);
    short len = RecordUtils.getRecordLength(this.buffer);
    this.buffer.limit(offset + len);

    return RecordUtils.buffer2Node(key, this.buffer);
  }

  @Override
  public boolean hasRecordKey(String key) {
    for (Pair<String, Short> p : keyAddressList) {
      if (p.left.equals(key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Queue<IMNode> getAllRecords() {
    Queue<IMNode> res = new ArrayDeque<>();
    this.buffer.clear();
    for (Pair<String, Short> p : keyAddressList) {
      this.buffer.limit(this.buffer.capacity());
      this.buffer.position(p.right);
      short len = RecordUtils.getRecordLength(this.buffer);
      this.buffer.limit(p.right + len);
      res.add(RecordUtils.buffer2Node(p.left, this.buffer));
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
      this.buffer.put(buffer);
    } else {
      // allocate new space for record, modify key-address list, freeAddr
      if (ISegment.SEG_HEADER_SIZE + pairLength + newLen > freeAddr) {
        // not enough space
        throw new SegmentOverflowException(idx);
      }

      freeAddr = (short) (freeAddr - newLen);
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

    // TODO: compact segment further as well
    recordNum--;
    pairLength -= 2;
    pairLength -= key.getBytes().length + 4;
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

  // endregion

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
    this.buffer.clear();
    this.buffer.position(offset);
    RecordUtils.updateSegAddr(this.buffer, newSegAddr);
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
    if (key.compareTo(keyAddressList.get(head).left) < 0
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

  private short getOffsetByIndex(int index) {
    return keyAddressList.get(index).right;
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
    builder.append(
        String.format(
            "[size: %d, K-AL size: %d, spare:%d,",
            this.length, keyAddressList.size(), freeAddr - pairLength - SEG_HEADER_SIZE));
    this.buffer.clear();
    for (Pair<String, Short> pair : keyAddressList) {
      this.buffer.position(pair.right);
      if (RecordUtils.getRecordType(buffer) < 3) {
        builder.append(
            String.format("(%s -> %d),", pair.left, RecordUtils.getRecordSegAddr(buffer)));
      } else {
        builder.append(String.format("(%s, %s),", pair.left, RecordUtils.getRecordAlias(buffer)));
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
}
