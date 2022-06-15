package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * This page stores measurement alias as key and name as record, aiming to provide a mapping
 * from alias to name so that secondary index could be built upon efficient structure.
 *
 * <p>Since it is not necessary to shift size of it, extending a {@linkplain SchemaPage}
 * might be more reasonable for this structure.
 *
 * <p>TODO: another abstract class for this and InternalPage is expected
 * */
public class AliasIndexPage extends SchemaPage implements ISegment<String, String> {

  private static int OFFSET_LEN = 2;
  long nextPage;
  String penultKey = null, lastKey = null;

  /**
   * <b>Page Header Structure: (19 bytes used, 13 bytes reserved)</b>
   * As in {@linkplain ISchemaPage}.
   *
   * <p>Page Body Structure:
   * <ul>
   *   <li>2 bytes * memberNum: offset of (alias, name) pair, to locate string content
   *   <li>... spare space...
   *   <li>var length(4 byte, var length, 4 bytes, var length): (alias, name) pairs, ordered by alias
   * </ul>
   *
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
    if (getKeyByIndex(pos).equals(alias)) {
      // not insert duplicated alias
      return spareSize;
    }

    if (SchemaPage.PAGE_HEADER_SIZE
        + OFFSET_LEN * (memberNum + 1)
        + 8
        + alias.getBytes().length
        + name.getBytes().length
        > spareOffset) {
      compactKeys();
    }

    // append key
    this.pageBuffer.clear();
    this.spareOffset = (short) (this.spareOffset
        - alias.getBytes().length
        - name.getBytes().length
        - 8);
    this.pageBuffer.position(spareOffset);
    ReadWriteIOUtils.write(alias, this.pageBuffer);
    ReadWriteIOUtils.write(name, this.pageBuffer);

    int migNum = memberNum - pos;
    if (migNum > 0) {
      // move compound pointers
      ByteBuffer buf = ByteBuffer.allocate(migNum * OFFSET_LEN);
      this.pageBuffer.limit(SchemaPage.PAGE_HEADER_SIZE + OFFSET_LEN * memberNum);
      this.pageBuffer.position(SchemaPage.PAGE_HEADER_SIZE + OFFSET_LEN * pos);
      buf.put(this.pageBuffer);

      this.pageBuffer.position(SchemaPage.PAGE_HEADER_SIZE + OFFSET_LEN * pos);
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
  public int updateRecord(String key, String buffer) throws SegmentOverflowException, RecordDuplicatedException {
    return -1;
  }

  @Override
  public int removeRecord(String key) {
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
    memberNum --;
    spareSize += OFFSET_LEN + 8 + key.getBytes().length + getNameByIndex(pos).getBytes().length;

    return spareSize;
  }

  @Override
  public String getRecordByKey(String key) throws MetadataException {
    int pos = getIndexByKey(key);
    if (!key.equals(getKeyByIndex(pos))) {
      return null;
    }
    return getNameByIndex(pos);
  }

  @Override
  public String getRecordByAlias(String alias) throws MetadataException {
    return getRecordByKey(alias);
  }

  @Override
  public boolean hasRecordKey(String key) {
    return key.equals(getKeyByIndex(getIndexByKey(key)));
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
  public void delete() {

  }

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
  public String splitByKey(String key, String entry, ByteBuffer dstBuffer, boolean inclineSplit) throws MetadataException {
    return null;
  }

  @Override
  public ByteBuffer resetBuffer(int ptr) {
    return null;
  }

  @Override
  public String inspect() {
    return null;
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
    for (int i = 1; i < this.memberNum; i++) {
      // this.pageBuffer will not be overridden immediately
      key = getKeyByIndex(i);
      name = getNameByIndex(i);
      accSiz += key.getBytes().length + name.getBytes().length + 8;
      this.spareOffset = (short) (this.pageBuffer.capacity() - accSiz);

      this.pageBuffer.position(SchemaPage.PAGE_HEADER_SIZE + OFFSET_LEN * i);
      ReadWriteIOUtils.write(this.spareOffset, this.pageBuffer);

      // write tempBuffer backward
      tempBuffer.position(tempBuffer.capacity() - accSiz);
      ReadWriteIOUtils.write(key, tempBuffer);
      ReadWriteIOUtils.write(name, tempBuffer);
    }
    tempBuffer.position(tempBuffer.capacity() - accSiz);
    this.pageBuffer.position(this.spareOffset);
    this.pageBuffer.put(tempBuffer);
    this.spareSize =
        (short)
            (this.spareOffset
                - SchemaPage.PAGE_HEADER_SIZE
                - OFFSET_LEN * this.memberNum);
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
      this.pageBuffer.position(SchemaPage.PAGE_HEADER_SIZE + index * OFFSET_LEN);
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
    if (index <= 0 || index >= memberNum) {
      throw new IndexOutOfBoundsException();
    }
    return getKeyByOffset(getOffsetByIndex(index));
  }

  private String getNameByIndex(int index) {
    if (index <= 0 || index >= memberNum) {
      throw new IndexOutOfBoundsException();
    }
    return getNameByOffset(getOffsetByIndex(index));
  }
}
