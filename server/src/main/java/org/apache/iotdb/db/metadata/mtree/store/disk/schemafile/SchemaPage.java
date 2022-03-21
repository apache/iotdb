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
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * This class is aimed to manage space inside one page. A segment inside a page has 3
 * representation: index, offset and instance. Index is meant to decouple file-wide indexing with
 * in-page compaction; Offset is meant for in-page indexing, and segment instance is meant for
 * records manipulations.
 */
public class SchemaPage implements ISchemaPage {

  // All other attributes are to describe this ByteBuffer
  final ByteBuffer pageBuffer;
  transient int pageIndex;

  boolean pageDelFlag;
  short pageSpareOffset; // start offset to allocate new segment
  short segNum; // amount of the segment, including deleted segments
  short lastDelSeg; // offset of last deleted segment, will not be wiped out immediately

  // segment address array inside a page, map segmentIndex -> segmentOffset
  // its append-only structured now, will never remove element, bringing redundant space and a bit
  // of simplicity
  // if only one full-page segment inside, it still stores the offset
  List<Short> segOffsetLst;

  // maintains segment instance inside this page, lazily instantiated, map segmentIndex ->
  // segmentInstance
  Map<Short, ISegment> segCacheMap;

  /**
   * This method will init page header for a blank page buffer.
   *
   * <p><b>Page Header Structure:</b>
   *
   * <ul>
   *   <li>1 short (2 bytes): pageSpareOffset, spare offset
   *   <li>1 short (2 bytes): segNum, amount of the segment
   *   <li>1 short (2 bytes): last deleted segment offset
   *   <li>1 boolean (1 bytes): delete flag
   * </ul>
   *
   * <b>Page Structure: </b>
   * </ul>
   *
   * <li>fixed length: Page Header
   * <li>var length: Segment <br>
   *     ... spare space ...
   * <li>var length: Segment Offset List, a sorted list of Short, length at 2*segNum
   * </ul>
   */
  public SchemaPage(ByteBuffer buffer, int index, boolean override) {
    buffer.clear();
    this.pageBuffer = buffer;
    this.pageIndex = index;
    this.segCacheMap = new HashMap<>();

    if (override) {
      pageSpareOffset = SchemaFile.PAGE_HEADER_SIZE;
      segNum = 0;
      segOffsetLst = new ArrayList<>();
      lastDelSeg = 0;
      pageDelFlag = false;
      syncPageBuffer();
    } else {
      pageSpareOffset = ReadWriteIOUtils.readShort(pageBuffer);
      segNum = ReadWriteIOUtils.readShort(pageBuffer);
      lastDelSeg = ReadWriteIOUtils.readShort(pageBuffer);
      pageDelFlag = ReadWriteIOUtils.readBool(pageBuffer);
      segOffsetLst = new ArrayList<>();
      reconstructSegmentList();
    }
  }

  public static ISchemaPage initPage(ByteBuffer buffer, int index) {
    return new SchemaPage(buffer, index, true);
  }

  public static ISchemaPage loadPage(ByteBuffer buffer, int index) {
    return new SchemaPage(buffer, index, false);
  }

  // region Interface Implementation

  /**
   * Insert a content directly into specified segment, without considering preallocate and
   * reallocate segment. Find the right segment instance, cache the segment and insert the record.
   * If not enough, reallocate inside page first, or return negative for new page then.
   *
   * @param segIdx
   * @return return 0 if write succeed, a positive for next segment address
   * @throws SchemaPageOverflowException no next segment, no spare space inside page
   */
  @Override
  public long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment tarSeg = getSegment(segIdx);
    int res = tarSeg.insertRecord(key, buffer);

    if (res < 0) {
      // If next segment exist, return next segment address first
      if (tarSeg.getNextSegAddress() > 0) {
        return tarSeg.getNextSegAddress();
      }

      // reallocate inside page, if not enough space for new size segment, throw exception
      short newSegSize = SchemaFile.reEstimateSegSize(tarSeg.size());
      res = reAllocateSeg(tarSeg, segIdx, newSegSize).insertRecord(key, buffer);
      if (res < 0) {
        // failed to insert buffer into new segment
        throw new MetadataException("failed to insert buffer into new segment");
      }
    }
    return 0L;
  }

  @Override
  public IMNode read(short segIdx, String key) throws SegmentNotFoundException {
    ISegment seg = getSegment(segIdx);
    try {
      return seg.getRecordAsIMNode(key);
    } catch (MetadataException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public boolean hasRecordKeyInSegment(String key, short segId) throws SegmentNotFoundException {
    return getSegment(segId).hasRecordKey(key) || getSegment(segId).hasRecordAlias(key);
  }

  @Override
  public Queue<IMNode> getChildren(short segId) throws SegmentNotFoundException {
    ISegment seg = getSegment(segId);
    try {
      return seg.getAllRecords();
    } catch (MetadataException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void removeRecord(short segId, String key) throws SegmentNotFoundException {
    getSegment(segId).removeRecord(key);
  }

  @Override
  public int getPageIndex() {
    return pageIndex;
  }

  /**
   * The record is definitely inside specified segment. This method compare old and new buffer to
   * decide whether update in place. If segment not enough, it will reallocate in this page first,
   * update segment offset list If no more space for reallocation, return a negative for new page.
   *
   * @param segIdx
   * @param buffer
   * @return spare space of the segment, negative if not enough
   */
  public void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment seg = getSegment(segIdx);
    try {
      seg.updateRecord(key, buffer);
    } catch (SegmentOverflowException e) {
      seg = reAllocateSeg(seg, segIdx, SchemaFile.reEstimateSegSize(seg.size()));
      int res = seg.updateRecord(key, buffer);
      if (res < 0) {
        throw new MetadataException(
            String.format("Unknown reason for key [%s] not found in page [%d].", key, pageIndex));
      }
    }
  }

  /**
   * This method extends specified segment inside original page to this page. If original segment
   * occupies a full page, new segment will be linked to the origin. If original segment smaller
   * than a full page, it will be over-write to new page.
   *
   * @param oriPage
   * @param oriIdx
   * @return index of the segment in new page
   */
  public short multiPageExtend(ISchemaPage oriPage, short oriIdx) {
    return 0;
  }

  /**
   * Bytes length from [tail of last segment] to [head of offset list].
   *
   * @return
   */
  @Override
  public short getSpareSize() {
    return (short) (SchemaFile.PAGE_LENGTH - pageSpareOffset - segNum * SchemaFile.SEG_OFF_DIG);
  }

  @Override
  public boolean isCapableForSize(short size) {
    return pageSpareOffset + size + (segNum + 1) * SchemaFile.SEG_OFF_DIG <= SchemaFile.PAGE_LENGTH;
  }

  @Override
  public boolean isSegmentCapableFor(short segId, short size) throws SegmentNotFoundException {
    return getSegment(segId).getSpareSize() > size;
  }

  @Override
  public void getPageBuffer(ByteBuffer dst) {
    this.pageBuffer.clear();
    dst.put(this.pageBuffer);
  }

  /**
   * While segments are always synchronized with buffer currentPage, header and tailer of the page
   * are not. This method will synchronize them with in mem attributes.
   */
  @Override
  public void syncPageBuffer() {
    pageBuffer.clear();
    ReadWriteIOUtils.write(pageSpareOffset, pageBuffer);
    ReadWriteIOUtils.write(segNum, pageBuffer);
    ReadWriteIOUtils.write(lastDelSeg, pageBuffer);
    ReadWriteIOUtils.write(pageDelFlag, pageBuffer);

    for (Map.Entry<Short, ISegment> entry : segCacheMap.entrySet()) {
      entry.getValue().syncBuffer();
    }

    pageBuffer.position(SchemaFile.PAGE_LENGTH - segNum * SchemaFile.SEG_OFF_DIG);
    for (short offset : segOffsetLst) {
      ReadWriteIOUtils.write(offset, pageBuffer);
    }
  }

  /**
   * Allocate space for a new segment inside this page
   *
   * @param size expected segment size
   * @return segment index in this page, negative for not enough space
   */
  @Override
  public short allocNewSegment(short size) throws IOException, SchemaPageOverflowException {
    ISegment newSeg = Segment.initAsSegment(allocSpareBufferSlice(size));

    short thisIndex = (short) segOffsetLst.size();
    if (segCacheMap.containsKey(thisIndex)) {
      throw new IOException("Segment cache map inconsistent with segment list.");
    }

    segCacheMap.put(thisIndex, newSeg);
    segOffsetLst.add(pageSpareOffset);
    pageSpareOffset += size;
    segNum += 1;

    return thisIndex;
  }

  @Override
  public short getSegmentSize(short segId) throws SegmentNotFoundException {
    return getSegment(segId).size();
  }

  @Override
  public void deleteSegment(short segId) throws SegmentNotFoundException {
    getSegment(segId).delete();
    segCacheMap.remove(segId);
    segOffsetLst.set(segId, (short) -1);
  }

  /**
   * Transplant designated segment from srcPage, to spare space of the page
   *
   * @param srcPage source page conveys source segment
   * @param segId id of the target segment
   * @param newSegSize size of new segment in this page
   * @throws MetadataException if spare not enough, segment not found or inconsistency
   */
  @Override
  public long transplantSegment(ISchemaPage srcPage, short segId, short newSegSize)
      throws MetadataException {
    if (!isCapableForSize(newSegSize)) {
      throw new SchemaPageOverflowException(pageIndex);
    }
    ByteBuffer newBuf = ByteBuffer.allocate(newSegSize);
    ((SchemaPage) srcPage).extendsSegmentTo(newBuf, segId);

    newBuf.clear();
    this.pageBuffer.clear();
    this.pageBuffer.position(pageSpareOffset);
    this.pageBuffer.put(newBuf);

    this.pageBuffer.position(pageSpareOffset);
    this.pageBuffer.limit(pageSpareOffset + newSegSize);
    ISegment newSeg = Segment.loadAsSegment(this.pageBuffer.slice());

    return SchemaFile.getGlobalIndex(pageIndex, registerNewSegment(newSeg));
  }

  /**
   * Invoke all segments, translate into string, concatenate and return.
   *
   * @return
   */
  @Override
  public String inspect() throws SegmentNotFoundException {
    syncPageBuffer();
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "SchemaPage Inspect: id:%d, totalSeg:%d, spareOffset:%d\n",
                pageIndex, segNum, pageSpareOffset));
    for (int idx = 0; idx < segOffsetLst.size(); idx++) {
      short offset = segOffsetLst.get(idx);
      if (offset < 0) {
        builder.append(String.format("Sgt id:%d deleted, offset:%d\n", idx, offset));
      } else {
        ISegment seg = getSegment((short) idx);
        builder.append(
            String.format(
                "Sgt id:%d, offset:%d, address:%d, next:%d, pref:%d, %s\n",
                idx,
                offset,
                SchemaFile.getGlobalIndex(pageIndex, (short) idx),
                seg.getNextSegAddress(),
                seg.getPrevSegAddress(),
                seg.toString()));
      }
    }
    return builder.toString();
  }

  @Override
  public void setNextSegAddress(short segId, long address) throws SegmentNotFoundException {
    getSegment(segId).setNextSegAddress(address);
  }

  @Override
  public void setPrevSegAddress(short segId, long address) throws SegmentNotFoundException {
    getSegment(segId).setPrevSegAddress(address);
  }

  @Override
  public long getNextSegAddress(short segId) throws SegmentNotFoundException {
    return getSegment(segId).getNextSegAddress();
  }

  @Override
  public long getPrevSegAddress(short segId) throws SegmentNotFoundException {
    return getSegment(segId).getPrevSegAddress();
  }

  // endregion

  private void reconstructSegmentList() {
    pageBuffer.position(SchemaFile.PAGE_LENGTH - SchemaFile.SEG_OFF_DIG * segNum);
    for (int idx = 0; idx < segNum; idx++) {
      segOffsetLst.add(ReadWriteIOUtils.readShort(pageBuffer));
    }
  }

  // region Getter Wrapper

  /**
   * Retrieve segment instance by index, instantiated and add to cache map if not yet.
   *
   * @param index index rather than offset of the segment
   * @return null if no such index, otherwise instance
   */
  private ISegment getSegment(short index) throws SegmentNotFoundException {
    if (segCacheMap.containsKey(index)) {
      return segCacheMap.get(index);
    }

    pageBuffer.clear();
    pageBuffer.position(getSegmentOffset(index));
    pageBuffer.limit(pageBuffer.position() + Segment.getSegBufLen(pageBuffer));

    ISegment res = Segment.loadAsSegment(pageBuffer.slice());
    segCacheMap.put(index, res);
    return res;
  }

  private short getSegmentOffset(short index) throws SegmentNotFoundException {
    if (index >= segOffsetLst.size() || segOffsetLst.get(index) < 0) {
      throw new SegmentNotFoundException(pageIndex, index);
    }
    return segOffsetLst.get(index);
  }

  // endregion
  // region Space Allocation

  /**
   * Allocate a new segment to extend specified segment, modify cache map and list. Mark old segment
   * instance as deleted, modify segOffsetList, pageSpareOffset and segCacheMap
   *
   * @param seg original segment instance
   * @param segIdx original segment index
   * @param newSize target segment size
   * @return reallocated segment instance
   * @throws SchemaPageOverflowException if this page has no enough space
   */
  private ISegment reAllocateSeg(ISegment seg, short segIdx, short newSize)
      throws SchemaPageOverflowException, SegmentNotFoundException {
    if (newSize >= SchemaFile.SEG_MAX_SIZ || getSpareSize() + seg.size() < newSize) {
      throw new SchemaPageOverflowException(pageIndex);
    }
    ByteBuffer newBuffer;

    try {
      newBuffer = allocSpareBufferSlice(newSize);
    } catch (SchemaPageOverflowException e) {
      rearrangeSegments(segIdx, seg);
      return extendSegmentInPlace(segIdx, seg, newSize);
    }

    // allocate buffer slice successfully
    seg.extendsTo(newBuffer);
    ISegment newSeg = Segment.loadAsSegment(newBuffer);

    // since this buffer is allocated from pageSpareOffset, new spare offset can simply add size up
    segOffsetLst.set(segIdx, pageSpareOffset);
    pageSpareOffset += newSeg.size();
    segCacheMap.put(segIdx, newSeg);

    seg.delete();

    return newSeg;
  }

  /**
   * This method will allocate DIRECTLY from spare space and return corresponding ByteBuffer. It
   * will not update segLstLen nor segCacheMap, since no segment initiated inside this method.
   *
   * @param size target size of the ByteBuffer
   * @return ByteBuffer object
   */
  private ByteBuffer allocSpareBufferSlice(short size) throws SchemaPageOverflowException {
    // check whether enough space
    if (getSpareSize() < size + SchemaFile.SEG_OFF_DIG) {
      throw new SchemaPageOverflowException(pageIndex);
    }

    pageBuffer.clear();
    pageBuffer.position(pageSpareOffset);
    pageBuffer.limit(pageSpareOffset + size);

    return pageBuffer.slice();
  }

  /**
   * To compact segments further, set deleted segments offset to -1 It modifies pageSpareOffset if
   * more space released. Over-write stash segments with existed segments.
   */
  private void compactSegments() {
    this.rearrangeSegments((short) -1, null);
  }

  /** Compact segments and move target segment (id at idx) to the tail of segments. */
  private synchronized void rearrangeSegments(short idx, ISegment tarSeg) {
    // all segment instance shall be abolished
    syncPageBuffer();
    segCacheMap.clear();

    ByteBuffer mirrorPage = ByteBuffer.allocate(SchemaFile.PAGE_LENGTH);
    this.pageBuffer.clear();
    mirrorPage.put(this.pageBuffer);
    this.pageBuffer.clear();

    pageSpareOffset = SchemaFile.PAGE_HEADER_SIZE;

    for (short i = 0; i < segOffsetLst.size(); i++) {
      if (segOffsetLst.get(i) >= 0 && i != idx) {
        // except for target segment, compact other valid segment
        short offset = segOffsetLst.get(i);

        mirrorPage.clear();
        this.pageBuffer.clear();

        mirrorPage.position(offset);
        short len = Segment.getSegBufLen(mirrorPage);
        mirrorPage.limit(offset + len);
        this.pageBuffer.position(pageSpareOffset);

        this.segOffsetLst.set(i, pageSpareOffset);
        this.pageBuffer.put(mirrorPage);
        pageSpareOffset += len;
      }
    }
    // a negative idx meant for only compaction
    if (idx >= 0) {
      this.pageBuffer.clear();
      this.pageBuffer.position(pageSpareOffset);

      mirrorPage.clear();
      mirrorPage.position(segOffsetLst.get(idx));
      short len = Segment.getSegBufLen(mirrorPage);
      mirrorPage.limit(mirrorPage.position() + len);

      this.pageBuffer.put(mirrorPage);
      segOffsetLst.set(idx, pageSpareOffset);
      pageSpareOffset += len;
    }
  }

  /**
   * This method checks and extends the last segment to a designated size.
   *
   * @param segId segment id
   * @param segment the last segment
   * @param newSize target size
   * @return extended segment
   */
  private ISegment extendSegmentInPlace(short segId, ISegment segment, short newSize)
      throws SegmentNotFoundException {
    // extend segment, modify pageSpareOffset, segCacheMap
    short offset = getSegmentOffset(segId);

    // only last segment could extend in-place
    if (offset + segment.size() != pageSpareOffset) {
      throw new SegmentNotFoundException(segId);
    }

    // extend to a temporary buffer
    ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
    segment.extendsTo(newBuffer);

    // write back the buffer content
    pageBuffer.clear();
    pageBuffer.position(offset);
    newBuffer.clear();
    pageBuffer.put(newBuffer);

    // pass page buffer slice to instantiate segment
    pageBuffer.position(offset);
    pageBuffer.limit(offset + newSize);
    ISegment newSeg = Segment.loadAsSegment(pageBuffer.slice());

    // modify status
    segOffsetLst.set(segId, offset);
    segCacheMap.put(segId, newSeg);
    pageSpareOffset = (short) (offset + newSeg.size());

    return newSeg;
  }

  protected void extendsSegmentTo(ByteBuffer dstBuffer, short segId)
      throws SegmentNotFoundException {
    ISegment sf = getSegment(segId);
    sf.extendsTo(dstBuffer);
  }

  protected void updateRecordSegAddr(short segId, String key, long newSegAddr)
      throws SegmentNotFoundException {
    ISegment seg = getSegment(segId);
    ((Segment) seg).updateRecordSegAddr(key, newSegAddr);
  }

  /**
   * Register segment instance to segCacheMap and segOffList, modify pageSpareOffset and segNum
   * respectively
   *
   * @param seg the segment to register
   * @return index of the segment
   */
  private synchronized short registerNewSegment(ISegment seg) throws MetadataException {
    short thisIndex = (short) segOffsetLst.size();
    if (segCacheMap.containsKey(thisIndex)) {
      throw new MetadataException(
          String.format("Segment cache map inconsistent with segment list in page %d.", pageIndex));
    }

    segCacheMap.put(thisIndex, seg);
    segOffsetLst.add(pageSpareOffset);
    pageSpareOffset += seg.size();
    segNum += 1;

    return thisIndex;
  }

  // endregion

  @TestOnly
  public Segment getSegmentTest(short idx) throws SegmentNotFoundException {
    return (Segment) getSegment(idx);
  }
}
