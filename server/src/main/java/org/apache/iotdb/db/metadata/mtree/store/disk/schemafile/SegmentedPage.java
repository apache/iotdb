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
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentedPage extends SchemaPage implements ISegmentedPage {

  // segment address array inside a page, map segmentIndex -> segmentOffset
  // if only one full-page segment inside, it still stores the offset
  private final transient List<Short> segOffsetLst;

  // maintains leaf segment instance inside this page, lazily instantiated
  // map segmentIndex -> segmentInstance
  private final transient Map<Short, ISegment<ByteBuffer, IMNode>> segCacheMap;

  /**
   * This class is aimed to manage space inside one page.
   *
   * <p>A segment inside a page has 3 representation: index, offset and instance. <br>
   *
   * <ul>
   *   <li>Index is meant to decouple file-wide indexing with in-page compaction
   *   <li>Offset is meant for in-page indexing
   *   <li>Segment instance is meant for records manipulations
   * </ul>
   *
   * <b>Page Header Structure as in {@linkplain ISchemaPage}.</b>
   *
   * <p>Page Body Structure:
   *
   * <ul>
   *   <li>var length * memberNum: {@linkplain WrappedSegment} contains serialized IMNodes.
   *   <li>... spare space...
   *   <li>2 bytes * memberNum: offset of segments, using marking deletion as {@linkplain
   *       #deleteSegment} mentioned.
   * </ul>
   */
  public SegmentedPage(ByteBuffer pageBuffer) {
    super(pageBuffer);
    segCacheMap = new ConcurrentHashMap<>();
    segOffsetLst = new ArrayList<>();

    pageBuffer.position(pageBuffer.capacity() - SchemaFileConfig.SEG_OFF_DIG * memberNum);
    for (int idx = 0; idx < memberNum; idx++) {
      segOffsetLst.add(ReadWriteIOUtils.readShort(pageBuffer));
    }
  }

  // region Interface Implementation

  @Override
  public long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment<ByteBuffer, IMNode> tarSeg = getSegment(segIdx);

    if (tarSeg.insertRecord(key, buffer) < 0) {
      // relocate inside page, if not enough space for new size segment, throw exception
      tarSeg =
          relocateSegment(
              tarSeg, segIdx, SchemaFile.reEstimateSegSize(tarSeg.size() + buffer.capacity()));
    } else {
      return 0L;
    }

    if (tarSeg.insertRecord(key, buffer) < 0) {
      throw new MetadataException("failed to insert buffer into new segment");
    }

    return 0L;
  }

  @Override
  public IMNode read(short segIdx, String key) throws MetadataException {
    return getSegment(segIdx).getRecordByKey(key);
  }

  @Override
  public IMNode readByAlias(short segIdx, String alias) throws MetadataException {
    return getSegment(segIdx).getRecordByAlias(alias);
  }

  @Override
  public void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment<ByteBuffer, IMNode> seg = getSegment(segIdx);
    try {
      if (seg.updateRecord(key, buffer) < 0) {
        throw new MetadataException("Record to update not found.");
      }
    } catch (SegmentOverflowException e) {
      // relocate large enough to include buffer, throw up if page overflow
      seg =
          relocateSegment(
              seg, segIdx, SchemaFile.reEstimateSegSize(seg.size() + buffer.capacity()));

      // retry and throw if failed again
      if (seg.updateRecord(key, buffer) < 0) {
        throw new MetadataException(
            String.format("Unknown reason for key [%s] not found in page [%d].", key, pageIndex));
      }
    }
  }

  @Override
  public Queue<IMNode> getChildren(short segId) throws MetadataException {
    return getSegment(segId).getAllRecords();
  }

  @Override
  public void removeRecord(short segId, String key) throws SegmentNotFoundException {
    getSegment(segId).removeRecord(key);
  }

  /**
   * Implementing marking deletion will not modify {@linkplain #memberNum} nor truncate {@linkplain
   * #segOffsetLst}.
   */
  @Override
  public synchronized void deleteSegment(short segId) throws SegmentNotFoundException {
    spareSize += getSegmentSize(segId);
    getSegment(segId).delete();
    segCacheMap.remove(segId);
    segOffsetLst.set(segId, (short) -1);
  }

  @Override
  public void purgeSegments() {
    segCacheMap.clear();
    segOffsetLst.clear();
    memberNum = 0;
    spareOffset = SchemaFileConfig.PAGE_HEADER_SIZE;
    spareSize = (short) (pageBuffer.capacity() - SchemaFileConfig.PAGE_HEADER_SIZE);
  }

  @Override
  public int validSegments() {
    return memberNum;
  }

  @Override
  public short getSpareSize() {
    return spareSize;
  }

  @Override
  public boolean isCapableForSegSize(short size) {
    return spareSize >= size + SchemaFileConfig.SEG_OFF_DIG;
  }

  @Override
  public short getSegmentSize(short segId) throws SegmentNotFoundException {
    return getSegment(segId).size();
  }

  @Override
  public void getPageBuffer(ByteBuffer dst) {
    this.pageBuffer.clear();
    dst.put(this.pageBuffer);
  }

  @Override
  public synchronized short allocNewSegment(short size) throws MetadataException {
    ISegment<ByteBuffer, IMNode> newSeg = WrappedSegment.initAsSegment(allocSpareBufferSlice(size));

    if (newSeg == null) {
      compactSegments();
      newSeg = WrappedSegment.initAsSegment(allocSpareBufferSlice(size));
    }

    if (newSeg == null) {
      throw new SchemaPageOverflowException(pageIndex);
    }

    return registerNewSegment(newSeg);
  }

  @Override
  public long transplantSegment(ISegmentedPage srcPage, short segId, short newSegSize)
      throws MetadataException {
    if (!isCapableForSegSize(newSegSize)) {
      throw new SchemaPageOverflowException(pageIndex);
    }
    if (maxAppendableSegmentSize() < newSegSize) {
      compactSegments();
    }

    ByteBuffer newBuf = ByteBuffer.allocate(newSegSize);
    srcPage.extendsSegmentTo(newBuf, segId);

    newBuf.clear();
    this.pageBuffer.clear();
    this.pageBuffer.position(spareOffset);
    this.pageBuffer.put(newBuf);

    this.pageBuffer.position(spareOffset);
    this.pageBuffer.limit(spareOffset + newSegSize);
    ISegment<ByteBuffer, IMNode> newSeg = WrappedSegment.loadAsSegment(this.pageBuffer.slice());

    // registerNewSegment will modify page status considering the new segment
    return SchemaFile.getGlobalIndex(pageIndex, registerNewSegment(newSeg));
  }

  @Override
  public void extendsSegmentTo(ByteBuffer dstBuffer, short segId) throws MetadataException {
    getSegment(segId).extendsTo(dstBuffer);
  }

  @Override
  public void setNextSegAddress(short segId, long address) throws SegmentNotFoundException {
    getSegment(segId).setNextSegAddress(address);
  }

  @Override
  public long getNextSegAddress(short segId) throws SegmentNotFoundException {
    return getSegment(segId).getNextSegAddress();
  }

  @Override
  public String splitWrappedSegment(
      String key, ByteBuffer recBuf, ISchemaPage dstPage, boolean inclineSplit)
      throws MetadataException {
    // only full page leaf segment can be split
    if (segOffsetLst.size() != 1
        || segOffsetLst.get(0) != SchemaFileConfig.PAGE_HEADER_SIZE
        || getSegment((short) 0).size() != SchemaFileConfig.SEG_MAX_SIZ) {
      throw new SegmentNotFoundException(pageIndex);
    }

    return getSegment((short) 0)
        .splitByKey(key, recBuf, dstPage.getEntireSegmentSlice(), inclineSplit);
  }

  @Override
  public String inspect() throws SegmentNotFoundException {
    syncPageBuffer();
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "page_id:%d, total_seg:%d, spare_from:%d, spare_size:%d%n",
                pageIndex, memberNum, spareOffset, spareSize));
    for (short idx = 0; idx < segOffsetLst.size(); idx++) {
      short offset = segOffsetLst.get(idx);
      if (offset < 0) {
        builder.append(String.format("seg_id:%d deleted, offset:%d%n", idx, offset));
      } else {
        ISegment<?, ?> seg = getSegment(idx);
        builder.append(
            String.format(
                "seg_id:%d, offset:%d, address:%s, next_seg:%s, %s%n",
                idx,
                offset,
                Long.toHexString(SchemaFile.getGlobalIndex(pageIndex, idx)),
                seg.getNextSegAddress() == -1 ? -1 : Long.toHexString(seg.getNextSegAddress()),
                seg.inspect()));
      }
    }
    return builder.toString();
  }

  @Override
  public synchronized void syncPageBuffer() {
    super.syncPageBuffer();
    for (Map.Entry<Short, ISegment<ByteBuffer, IMNode>> entry : segCacheMap.entrySet()) {
      entry.getValue().syncBuffer();
    }

    pageBuffer.position(SchemaFileConfig.PAGE_LENGTH - memberNum * SchemaFileConfig.SEG_OFF_DIG);
    for (short offset : segOffsetLst) {
      ReadWriteIOUtils.write(offset, pageBuffer);
    }
  }

  @Override
  public ISegmentedPage getAsSegmentedPage() {
    return this;
  }

  @Override
  public ByteBuffer getEntireSegmentSlice() throws MetadataException {
    if (segOffsetLst.size() != 1
        || segOffsetLst.get(0) != SchemaFileConfig.PAGE_HEADER_SIZE
        || spareSize != 0
        || spareOffset != this.pageBuffer.capacity() - SchemaFileConfig.SEG_OFF_DIG) {
      throw new MetadataException(
          "SegmentedPage can share entire buffer slice only when it contains one MAX SIZE segment.");
    }
    // buffer may be modified, segment instance shall be abolished
    segCacheMap.clear();
    synchronized (this.pageBuffer) {
      this.pageBuffer.position(SchemaFileConfig.PAGE_HEADER_SIZE);
      this.pageBuffer.limit(this.pageBuffer.capacity() - SchemaFileConfig.SEG_OFF_DIG);
      return this.pageBuffer.slice();
    }
  }
  // endregion

  // region Segment Getters
  /**
   * Retrieve leaf segment instance by index, instantiated and add to cache map if not yet.
   *
   * @param index index rather than offset of the segment
   * @return null if InternalSegment, otherwise instance
   */
  private ISegment<ByteBuffer, IMNode> getSegment(short index) throws SegmentNotFoundException {
    if (segOffsetLst.size() <= index || segOffsetLst.get(index) < 0) {
      throw new SegmentNotFoundException(pageIndex, index);
    }

    synchronized (segCacheMap) {
      if (segCacheMap.containsKey(index)) {
        return segCacheMap.get(index);
      }
    }

    // duplicate for concurrent event
    ByteBuffer bufferR = this.pageBuffer.duplicate();
    bufferR.clear();
    bufferR.position(getSegmentOffset(index));
    bufferR.limit(bufferR.position() + WrappedSegment.getSegBufLen(bufferR));

    ISegment<ByteBuffer, IMNode> res;
    try {
      res = WrappedSegment.loadAsSegment(bufferR.slice());
    } catch (RecordDuplicatedException e) {
      throw new SegmentNotFoundException(e.getMessage());
    }

    synchronized (segCacheMap) {
      if (segCacheMap.containsKey(index)) {
        return segCacheMap.get(index);
      }

      segCacheMap.put(index, res);
      return res;
    }
  }

  private short getSegmentOffset(short index) throws SegmentNotFoundException {
    if (index >= segOffsetLst.size() || segOffsetLst.get(index) < 0) {
      throw new SegmentNotFoundException(pageIndex, index);
    }
    return segOffsetLst.get(index);
  }

  // endregion

  // region Page Space Management

  /**
   * Allocate a new segment to extend specified segment, modify cache map and list.
   *
   * <p><b> The new segment could be allocated from spare space or rearranged space.</b>
   *
   * @param seg original segment instance
   * @param segIdx original segment index
   * @param newSize target segment size
   * @return reallocated segment instance
   * @throws SchemaPageOverflowException if this page has no enough space
   */
  private ISegment<ByteBuffer, IMNode> relocateSegment(
      ISegment<?, ?> seg, short segIdx, short newSize) throws MetadataException {
    if (seg.size() == SchemaFileConfig.SEG_MAX_SIZ || getSpareSize() + seg.size() < newSize) {
      throw new SchemaPageOverflowException(pageIndex);
    }

    // try to allocate space directly from spareOffset or rearrange and extend in place
    ByteBuffer newBuffer = allocSpareBufferSlice(newSize);
    if (newBuffer == null) {
      rearrangeSegments(segIdx);
      return extendSegmentInPlace(segIdx, seg.size(), newSize);
    }

    // allocate buffer slice successfully
    seg.extendsTo(newBuffer);
    ISegment<ByteBuffer, IMNode> newSeg = WrappedSegment.loadAsSegment(newBuffer);

    // since this buffer is allocated from pageSpareOffset, new spare offset can simply add size up
    segOffsetLst.set(segIdx, spareOffset);
    segCacheMap.put(segIdx, newSeg);
    spareOffset += newSeg.size();
    spareSize -= (short) (newSize - seg.size());

    return newSeg;
  }

  private short maxAppendableSegmentSize() {
    return (short)
        (pageBuffer.limit() - SchemaFileConfig.SEG_OFF_DIG * (memberNum + 1) - spareOffset);
  }

  /**
   * This method will allocate DIRECTLY from {@link #spareOffset} and return corresponding
   * ByteBuffer slice. It will not update {@link #segOffsetLst} nor {@link #segCacheMap}, since no
   * segment initiated here.
   *
   * @param size target size of the ByteBuffer
   * @return ByteBuffer return null if {@linkplain SchemaPageOverflowException} to improve
   *     efficiency
   */
  private ByteBuffer allocSpareBufferSlice(short size) {
    // check whether enough space to be directly allocate
    if (this.pageBuffer.capacity() - spareOffset - memberNum * SchemaFileConfig.SEG_OFF_DIG
        < size + SchemaFileConfig.SEG_OFF_DIG) {
      // since this may occur frequently, throw exception here may be inefficient
      return null;
    }

    pageBuffer.clear();
    pageBuffer.position(spareOffset);
    pageBuffer.limit(spareOffset + size);

    return pageBuffer.slice();
  }

  /**
   * To compact segments further, set deleted segments offset to -1 It modifies pageSpareOffset if
   * more space released. Over-write stash segments with existed segments.
   */
  private void compactSegments() {
    this.rearrangeSegments((short) -1);
  }

  /**
   * Compact segments and move target segment (id at idx) to the tail of segments.<br>
   * Since this method may overwrite segment buffer, all existed buffer instance shall be abolished.
   */
  private synchronized void rearrangeSegments(short idx) {
    // all segment instance shall be abolished
    syncPageBuffer();
    segCacheMap.clear();

    ByteBuffer mirrorPage = ByteBuffer.allocate(this.pageBuffer.capacity());
    this.pageBuffer.clear();
    mirrorPage.put(this.pageBuffer);
    this.pageBuffer.clear();

    spareOffset = SchemaFileConfig.PAGE_HEADER_SIZE;

    short offset, len;
    for (short i = 0; i < segOffsetLst.size(); i++) {
      if (segOffsetLst.get(i) >= 0 && i != idx) {
        // except for target segment, compact other valid segment
        offset = segOffsetLst.get(i);

        mirrorPage.clear();
        this.pageBuffer.clear();

        mirrorPage.position(offset);
        len = WrappedSegment.getSegBufLen(mirrorPage);
        mirrorPage.limit(offset + len);

        this.segOffsetLst.set(i, spareOffset);
        this.pageBuffer.position(spareOffset);
        this.pageBuffer.put(mirrorPage);
        spareOffset += len;
      }
    }

    // a negative idx meant for only compaction
    if (idx >= 0) {
      this.pageBuffer.clear();
      this.pageBuffer.position(spareOffset);

      mirrorPage.clear();
      mirrorPage.position(segOffsetLst.get(idx));
      len = WrappedSegment.getSegBufLen(mirrorPage);
      mirrorPage.limit(mirrorPage.position() + len);

      this.pageBuffer.put(mirrorPage);
      segOffsetLst.set(idx, spareOffset);
      spareOffset += len;
    }

    spareSize =
        (short)
            (this.pageBuffer.capacity() - spareOffset - memberNum * SchemaFileConfig.SEG_OFF_DIG);
  }

  /**
   * This method checks and extends the last segment to a designated size.
   *
   * @param segId segment id
   * @param oriSegSize size of the target segment
   * @param newSize extended size
   * @return extended segment based on page buffer
   */
  private ISegment<ByteBuffer, IMNode> extendSegmentInPlace(
      short segId, short oriSegSize, short newSize) throws MetadataException {
    // extend segment, modify pageSpareOffset, segCacheMap
    short offset = getSegmentOffset(segId);

    // only last segment could extend in-place
    if (offset + oriSegSize != spareOffset) {
      throw new SegmentNotFoundException(segId);
    }

    // extend to a temporary buffer
    ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
    getSegment(segId).extendsTo(newBuffer);

    // write back the buffer content
    pageBuffer.clear();
    pageBuffer.position(offset);
    newBuffer.clear();
    pageBuffer.put(newBuffer);

    // pass page buffer slice to instantiate segment
    pageBuffer.position(offset);
    pageBuffer.limit(offset + newSize);
    ISegment<ByteBuffer, IMNode> newSeg = WrappedSegment.loadAsSegment(pageBuffer.slice());

    // modify status
    segOffsetLst.set(segId, offset);
    segCacheMap.put(segId, newSeg);
    spareOffset = (short) (offset + newSeg.size());
    spareSize -= (newSize - oriSegSize);

    return newSeg;
  }

  public void updateRecordSegAddr(short segId, String key, long newSegAddr)
      throws SegmentNotFoundException {
    ISegment<ByteBuffer, IMNode> seg = getSegment(segId);
    // TODO: add to interface
    ((WrappedSegment) seg).updateRecordSegAddr(key, newSegAddr);
  }

  /**
   * Register segment instance to segCacheMap and segOffList, modify pageSpareOffset and segNum
   * respectively
   *
   * @param seg the segment to register
   * @return index of the segment
   */
  private synchronized short registerNewSegment(ISegment<ByteBuffer, IMNode> seg)
      throws MetadataException {
    short thisIndex = (short) segOffsetLst.size();
    if (segCacheMap.containsKey(thisIndex)) {
      throw new MetadataException(
          String.format("Segment cache map inconsistent with segment list in page %d.", pageIndex));
    }

    segCacheMap.put(thisIndex, seg);
    segOffsetLst.add(spareOffset);

    spareOffset += seg.size();
    spareSize -= seg.size() + 2;
    memberNum += 1;

    return thisIndex;
  }

  // endregion

  // region Test Only Methos

  @TestOnly
  @Override
  public WrappedSegment getSegmentOnTest(short idx) throws SegmentNotFoundException {
    return (WrappedSegment) getSegment(idx);
  }

  // endregion
}
