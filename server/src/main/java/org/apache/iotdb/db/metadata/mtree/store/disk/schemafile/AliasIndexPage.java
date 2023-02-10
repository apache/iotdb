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

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.ALIAS_PAGE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig.PAGE_HEADER_SIZE;

/**
 * This page stores measurement alias as key and name as record, aiming to provide a mapping from
 * alias to name so that secondary index could be built upon efficient structure.
 *
 * <p>Since it is not necessary to shift size of it, extending a {@linkplain SchemaPage} might be
 * more reasonable for this structure.
 *
 * <p>TODO: another abstract class for this and InternalPage is expected
 */
public class AliasIndexPage extends SchemaPage implements ISegment<String, String> {

  private static final int OFFSET_LEN = 2;
  long nextPage;
  String penultKey = null, lastKey = null;

  /**
   * <b>Page Header Structure: (19 bytes used, 13 bytes reserved)</b> As in {@linkplain
   * ISchemaPage}.
   *
   * <p>Page Body Structure:
   *
   * <ul>
   *   <li>2 bytes * memberNum: offset of (alias, name) pair, to locate string content
   *   <li>... spare space...
   *   <li>var length(4 byte, var length, 4 bytes, var length): (alias, name) pairs, ordered by
   *       alias
   * </ul>
   */
  protected AliasIndexPage(ByteBuffer pageBuffer) {
    super(pageBuffer);
    nextPage = ReadWriteIOUtils.readLong(this.pageBuffer);
  }

  // region Interface Implementation

  @Override
  public int insertRecord(String alias, String name) throws RecordDuplicatedException {
    if (spareSize < OFFSET_LEN + 8 + alias.getBytes().length + name.getBytes().length) {
      return -1;
    }

    // check whether key already exists
    int pos = getIndexByKey(alias);
    if (memberNum > 0 && pos < memberNum && getKeyByIndex(pos).equals(alias)) {
      // not insert duplicated alias
      return spareSize;
    }

    if (PAGE_HEADER_SIZE
            + OFFSET_LEN * (memberNum + 1)
            + 8
            + alias.getBytes().length
            + name.getBytes().length
        > spareOffset) {
      compactKeys();
    }

    // append key
    this.pageBuffer.clear();
    this.spareOffset =
        (short) (this.spareOffset - alias.getBytes().length - name.getBytes().length - 8);
    this.pageBuffer.position(spareOffset);
    ReadWriteIOUtils.write(alias, this.pageBuffer);
    ReadWriteIOUtils.write(name, this.pageBuffer);

    int migNum = memberNum - pos;
    if (migNum > 0) {
      // move compound pointers
      ByteBuffer buf = ByteBuffer.allocate(migNum * OFFSET_LEN);
      this.pageBuffer.limit(PAGE_HEADER_SIZE + OFFSET_LEN * memberNum);
      this.pageBuffer.position(PAGE_HEADER_SIZE + OFFSET_LEN * pos);
      buf.put(this.pageBuffer);

      this.pageBuffer.position(PAGE_HEADER_SIZE + OFFSET_LEN * pos);
      ReadWriteIOUtils.write(spareOffset, this.pageBuffer);

      buf.clear();
      this.pageBuffer.limit(this.pageBuffer.limit() + OFFSET_LEN);
      this.pageBuffer.put(buf);
    } else {
      // append compound pointer
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(PAGE_HEADER_SIZE + memberNum * OFFSET_LEN);
      ReadWriteIOUtils.write(spareOffset, this.pageBuffer);
    }

    spareSize -= (alias.getBytes().length + name.getBytes().length + 8 + OFFSET_LEN);
    memberNum++;

    penultKey = lastKey;
    lastKey = alias;
    return spareSize;
  }

  @Override
  public int updateRecord(String key, String buffer)
      throws SegmentOverflowException, RecordDuplicatedException {
    return -1;
  }

  @Override
  public int removeRecord(String key) {
    if (memberNum <= 0) {
      return spareSize;
    }

    int pos = getIndexByKey(key);
    if (!key.equals(getKeyByIndex(pos))) {
      return spareSize;
    }

    int migNum = memberNum - pos - 1;
    if (migNum > 0) {
      ByteBuffer temBuf = ByteBuffer.allocate(migNum * OFFSET_LEN);
      this.pageBuffer.clear().limit(PAGE_HEADER_SIZE + OFFSET_LEN * memberNum);
      this.pageBuffer.position(PAGE_HEADER_SIZE + pos * OFFSET_LEN + OFFSET_LEN);
      temBuf.put(this.pageBuffer).clear();

      this.pageBuffer.clear().position(PAGE_HEADER_SIZE + pos * OFFSET_LEN);
      this.pageBuffer.put(temBuf);
    }
    memberNum--;
    spareSize += OFFSET_LEN + 8 + key.getBytes().length + getNameByIndex(pos).getBytes().length;

    return spareSize;
  }

  @Override
  public String getRecordByKey(String key) throws MetadataException {
    int pos = getIndexByKey(key);

    if (memberNum <= 0 || pos > memberNum) {
      return null;
    }

    // to tell whether pos is position to insert
    if (pos == 0 || pos == memberNum) {
      pos = pos == memberNum ? pos - 1 : pos;
    }

    return key.equals(getKeyByIndex(pos)) ? getNameByIndex(pos) : null;
  }

  @Override
  public String getRecordByAlias(String alias) throws MetadataException {
    return getRecordByKey(alias);
  }

  @Override
  public boolean hasRecordKey(String key) {
    int idx = getIndexByKey(key);
    return memberNum > 0 && idx < memberNum && key.equals(getKeyByIndex(idx));
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return hasRecordKey(alias);
  }

  @Override
  public Queue<String> getAllRecords() throws MetadataException {
    Queue<String> res = new ArrayDeque<>(memberNum);
    for (int i = 0; i < memberNum; i++) {
      res.add(getNameByIndex(i));
    }
    return res;
  }

  @Override
  public void syncPageBuffer() {
    super.syncPageBuffer();
    ReadWriteIOUtils.write(this.nextPage, this.pageBuffer);
  }

  @Override
  public void syncBuffer() {
    super.syncPageBuffer();
    ReadWriteIOUtils.write(this.nextPage, this.pageBuffer);
  }

  @Override
  public short size() {
    return (short) pageBuffer.capacity();
  }

  @Override
  public short getSpareSize() {
    return spareSize;
  }

  @Override
  public void delete() {}

  @Override
  public long getNextSegAddress() {
    return nextPage;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    nextPage = nextSegAddress;
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) throws MetadataException {
    if (newBuffer.capacity() != this.pageBuffer.capacity()) {
      throw new MetadataException("AliasIndexPage can only extend to buffer with same capacity.");
    }

    syncPageBuffer();
    this.pageBuffer.clear();
    newBuffer.clear();

    newBuffer.put(this.pageBuffer);
  }

  @Override
  public String splitByKey(String key, String entry, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {

    if (this.pageBuffer.capacity() != dstBuffer.capacity()) {
      throw new MetadataException("Segments only splits with same capacity.");
    }

    if (memberNum == 0) {
      throw new MetadataException("Segment can not be split with no records.");
    }

    if (key == null && memberNum <= 1) {
      throw new MetadataException("Segment can not be split with only one record.");
    }

    // notice that key can be null here
    boolean monotonic =
        penultKey != null
            && key != null
            && lastKey != null
            && inclineSplit
            && (key.compareTo(lastKey)) * (lastKey.compareTo(penultKey)) > 0;

    int n = memberNum;

    // actual index of key just smaller than the insert, -2 for null key
    int pos = key != null ? getIndexByKey(key) : -2;

    int sp; // virtual index to split
    if (monotonic) {
      // new entry into part with more space
      sp = key.compareTo(lastKey) > 0 ? Math.max(pos, n / 2) : Math.min(pos, n / 2);
    } else {
      sp = n / 2;
    }

    // little different from InternalSegment, only the front edge key can not split
    sp = sp <= 0 ? 1 : sp;

    // this method BREAKS envelop of the passing in buffer to be more efficient
    // attributes for dstBuffer
    short memberNum = 0,
        spareOffset = (short) dstBuffer.capacity(),
        spareSize = (short) (spareOffset - PAGE_HEADER_SIZE);
    long nextSegAddress = this.nextPage;
    dstBuffer.clear();

    short offset;
    int recSize;
    String mKey, sKey = null;
    int aix; // aix for actual index on keyAddressList
    n = key == null ? n - 1 : n; // null key
    // TODO: implement bulk split further
    for (int ix = sp; ix <= n; ix++) {
      if (ix == pos) {
        // migrate newly insert
        mKey = key;
        recSize = 8 + entry.getBytes().length + mKey.getBytes().length;

        spareOffset -= recSize;
        dstBuffer.position(spareOffset);
        ReadWriteIOUtils.write(mKey, dstBuffer);
        ReadWriteIOUtils.write(entry, dstBuffer);

      } else {
        // pos equals -2 if key is null
        aix = (ix > pos) && (pos != -2) ? ix - 1 : ix;
        mKey = getKeyByIndex(aix);

        offset = getOffsetByIndex(aix);
        pageBuffer.clear().position(offset);
        recSize = ReadWriteIOUtils.readInt(pageBuffer);
        pageBuffer.position(pageBuffer.position() + recSize);
        recSize += ReadWriteIOUtils.readInt(pageBuffer) + 8;
        pageBuffer.position(offset);
        pageBuffer.limit(offset + recSize);

        spareOffset -= recSize;
        dstBuffer.position(spareOffset);
        dstBuffer.put(this.pageBuffer);
      }
      dstBuffer.position(PAGE_HEADER_SIZE + memberNum * OFFSET_LEN);
      ReadWriteIOUtils.write(spareOffset, dstBuffer);

      if (ix == sp) {
        // search key is the first key in split segment
        sKey = mKey;
      }
      memberNum++;
      spareSize -= recSize + 2;
    }

    this.memberNum -= memberNum;
    if (key != null && sp <= pos) {
      // extra key has inserted into split buffer
      this.memberNum++;
    }

    // compact and update status
    compactKeys();
    if (key != null && sp > pos) {
      // new insert shall be in this
      if (insertRecord(key, entry) < 0) {
        throw new SegmentOverflowException(key);
      }
    }

    // flush dstBuffer header
    dstBuffer.clear();
    ReadWriteIOUtils.write(ALIAS_PAGE, dstBuffer);
    ReadWriteIOUtils.write(-1, dstBuffer);
    ReadWriteIOUtils.write(spareOffset, dstBuffer);
    ReadWriteIOUtils.write(spareSize, dstBuffer);
    ReadWriteIOUtils.write(memberNum, dstBuffer);
    ReadWriteIOUtils.write(nextPage, dstBuffer);

    penultKey = null;
    lastKey = null;
    return sKey;
  }

  @Override
  public ByteBuffer resetBuffer(int ptr) {
    return null;
  }

  @Override
  public String inspect() {
    syncPageBuffer();
    ByteBuffer bufferR = this.pageBuffer.asReadOnlyBuffer();
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "page_id:%d, total_seg:%d, spare_before:%d%n", pageIndex, 1, spareOffset));
    builder.append(
        String.format(
            "[AliasIndexSegment, total_alias: %d, spare_size:%d,", this.memberNum, this.spareSize));
    bufferR.clear();

    for (int i = 0; i < memberNum; i++) {
      builder.append(String.format("(%s, %s),", getKeyByIndex(i), getNameByIndex(i)));
    }
    builder.append("]\n");
    return builder.toString();
  }

  @Override
  public String toString() {
    return inspect();
  }

  @Override
  public ISegment<String, String> getAsAliasIndexPage() {
    return this;
  }

  // endregion

  private void compactKeys() {
    ByteBuffer tempBuffer = ByteBuffer.allocate(this.pageBuffer.capacity() - this.spareOffset);
    tempBuffer.position(tempBuffer.capacity());
    this.spareOffset = (short) this.pageBuffer.capacity();
    String key, name;
    int accSiz = 0;
    for (int i = 0; i < this.memberNum; i++) {
      // this.pageBuffer will not be overridden immediately
      key = getKeyByIndex(i);
      name = getNameByIndex(i);
      accSiz += key.getBytes().length + name.getBytes().length + 8;
      this.spareOffset = (short) (this.pageBuffer.capacity() - accSiz);

      this.pageBuffer.position(PAGE_HEADER_SIZE + OFFSET_LEN * i);
      ReadWriteIOUtils.write(this.spareOffset, this.pageBuffer);

      // write tempBuffer backward
      tempBuffer.position(tempBuffer.capacity() - accSiz);
      ReadWriteIOUtils.write(key, tempBuffer);
      ReadWriteIOUtils.write(name, tempBuffer);
    }
    tempBuffer.position(tempBuffer.capacity() - accSiz);
    this.pageBuffer.position(this.spareOffset);
    this.pageBuffer.put(tempBuffer);
    this.spareSize = (short) (this.spareOffset - PAGE_HEADER_SIZE - OFFSET_LEN * this.memberNum);
  }

  /**
   * Binary search implementation.
   *
   * @param key to be searched or inserted.
   * @return the position of the smallest key larger than or equal to the passing in.
   */
  private int getIndexByKey(String key) {

    if (memberNum == 0 || key.compareTo(getKeyByIndex(0)) <= 0) {
      return 0;
    } else if (key.compareTo(getKeyByIndex(memberNum - 1)) >= 0) {
      return memberNum;
    }

    int head = 0;
    int tail = memberNum - 1;

    int pivot = 0;
    String pk;
    while (head != tail) {
      pivot = (head + tail) / 2;
      pk = getKeyByIndex(pivot);
      if (key.compareTo(pk) == 0) {
        return pivot;
      }

      if (key.compareTo(pk) > 0) {
        head = pivot;
      } else {
        tail = pivot;
      }

      if (head == tail - 1) {
        // notice that getKey(tail) always greater than key
        return key.compareTo(getKeyByIndex(head)) <= 0 ? head : tail;
      }
    }

    return pivot;
  }

  private short getOffsetByIndex(int index) {
    if (index < 0 || index >= memberNum) {
      // TODO: check whether reasonable to throw an unchecked
      throw new IndexOutOfBoundsException();
    }
    synchronized (pageBuffer) {
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(PAGE_HEADER_SIZE + index * OFFSET_LEN);
      return ReadWriteIOUtils.readShort(this.pageBuffer);
    }
  }

  private String getKeyByOffset(short offset) {
    synchronized (this.pageBuffer) {
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(offset);
      return ReadWriteIOUtils.readString(this.pageBuffer);
    }
  }

  private String getNameByOffset(short offset) {
    synchronized (this.pageBuffer) {
      this.pageBuffer.limit(this.pageBuffer.capacity());
      this.pageBuffer.position(offset);
      this.pageBuffer.position(offset + 4 + ReadWriteIOUtils.readInt(this.pageBuffer));
      return ReadWriteIOUtils.readString(this.pageBuffer);
    }
  }

  private String getKeyByIndex(int index) {
    if (index < 0 || index > memberNum) {
      throw new IndexOutOfBoundsException();
    }
    return getKeyByOffset(getOffsetByIndex(index));
  }

  private String getNameByIndex(int index) {
    if (index < 0 || index >= memberNum) {
      throw new IndexOutOfBoundsException();
    }
    return getNameByOffset(getOffsetByIndex(index));
  }
}
