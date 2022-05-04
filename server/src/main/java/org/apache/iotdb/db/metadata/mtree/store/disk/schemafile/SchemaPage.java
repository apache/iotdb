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
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

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
 */
public class SchemaPage implements ISchemaPage {

  // All other attributes are to describe this ByteBuffer
  private final ByteBuffer pageBuffer;
  private transient int pageIndex;

  private boolean pageDelFlag;
  private short pageSpareOffset; // start offset to allocate new segment
  private short segNum; // amount of the segment, including deleted segments
  private short lastDelSeg; // offset of last deleted segment, will not be wiped out immediately

  // segment address array inside a page, map segmentIndex -> segmentOffset
  // if only one full-page segment inside, it still stores the offset
  private List<Short> segOffsetLst;

  // maintains segment instance inside this page, lazily instantiated, map segmentIndex ->
  // segmentInstance
  private Map<Short, ISegment> segCacheMap;

  /**
   * This method will init page header for a blank page buffer.
   *
   * <p><b>Page Header Structure:</b>
   *
   * <ul>
   *   <li>1 int (4 bytes): page index, a non-negative number
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
    this.segCacheMap = new ConcurrentHashMap<>();

    if (override) {
      pageIndex = index;
      pageSpareOffset = SchemaFile.PAGE_HEADER_SIZE;
      segNum = 0;
      segOffsetLst = new ArrayList<>();
      lastDelSeg = 0;
      pageDelFlag = false;
      syncPageBuffer();
    } else {
      pageIndex = ReadWriteIOUtils.readInt(pageBuffer);
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

  @Override
  public long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment tarSeg = getSegment(segIdx);
    int res = tarSeg.insertRecord(key, buffer);

    if (res < 0) {
      // If next segment exist, return next segment address first
      if (tarSeg.getNextSegAddress() > 0) {
        return tarSeg.getNextSegAddress();
      }

      // relocate inside page, if not enough space for new size segment, throw exception
      tarSeg = relocateSegment(tarSeg, segIdx, SchemaFile.reEstimateSegSize(tarSeg.size()));
      res = tarSeg.insertRecord(key, buffer);

      if (res < 0) {
        res =
            relocateSegment(tarSeg, segIdx, SchemaFile.reEstimateSegSize(tarSeg.size()))
                .insertRecord(key, buffer);
        if (res < 0) {
          // failed to insert buffer into new segment
          throw new MetadataException("failed to insert buffer into new segment");
        }
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

  @Override
  public void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException {
    ISegment seg = getSegment(segIdx);
    try {
      seg.updateRecord(key, buffer);
    } catch (SegmentOverflowException e) {
      seg = relocateSegment(seg, segIdx, SchemaFile.reEstimateSegSize(seg.size()));
    }

    // relocate and try update twice ensures safety for reasonable big node
    try {
      int res = seg.updateRecord(key, buffer);
    } catch (SegmentOverflowException e) {
      seg = relocateSegment(seg, segIdx, SchemaFile.reEstimateSegSize(seg.size()));
      int res = seg.updateRecord(key, buffer);
      if (res < 0) {
        throw new MetadataException(
            String.format("Unknown reason for key [%s] not found in page [%d].", key, pageIndex));
      }
    }
  }

  /**
   * Calculated with accurate total segment size by examine segment buffers.<br>
   * This accuracy will save much space for schema file at the cost of more frequent rearrangement.
   *
   * <p>TODO: improve with a substitute variable rather than calculating on every call
   */
  @Override
  public short getSpareSize() {
    syncPageBuffer();
    ByteBuffer bufferR = this.pageBuffer.asReadOnlyBuffer();
    bufferR.clear();
    short amountSize = 0;
    for (short ofs : segOffsetLst) {
      if (ofs >= 0) {
        bufferR.position(ofs);
        amountSize += Segment.getSegBufLen(bufferR);
      }
    }
    return (short)
        (SchemaFile.PAGE_LENGTH
            - amountSize
            - SchemaFile.PAGE_HEADER_SIZE
            - segNum * SchemaFile.SEG_OFF_DIG);
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

  @Override
  public void syncPageBuffer() {
    pageBuffer.clear();
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
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

  @Override
  public short allocNewSegment(short size) throws IOException, SchemaPageOverflowException {
    ISegment newSeg = Segment.initAsSegment(allocSpareBufferSlice(size));

    if (newSeg == null) {
      throw new SchemaPageOverflowException(pageIndex);
    }

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

  /** Invoke all segments, translate into string, concatenate and return. */
  @Override
  public String inspect() throws SegmentNotFoundException {
    syncPageBuffer();
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "page_id:%d, total_seg:%d, spare_from:%d\n", pageIndex, segNum, pageSpareOffset));
    for (int idx = 0; idx < segOffsetLst.size(); idx++) {
      short offset = segOffsetLst.get(idx);
      if (offset < 0) {
        builder.append(String.format("seg_id:%d deleted, offset:%d\n", idx, offset));
      } else {
        ISegment seg = getSegment((short) idx);
        builder.append(
            String.format(
                "seg_id:%d, offset:%d, address:%s, next_seg:%s, prev_seg:%s, %s\n",
                idx,
                offset,
                Long.toHexString(SchemaFile.getGlobalIndex(pageIndex, (short) idx)),
                seg.getNextSegAddress() == -1 ? -1 : Long.toHexString(seg.getNextSegAddress()),
                seg.getPrevSegAddress() == -1 ? -1 : Long.toHexString(seg.getPrevSegAddress()),
                seg.inspect()));
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

    synchronized (this) {
      if (segCacheMap.containsKey(index)) {
        return segCacheMap.get(index);
      }

      ByteBuffer bufferR = this.pageBuffer.duplicate();
      bufferR.clear();
      bufferR.position(getSegmentOffset(index));
      bufferR.limit(bufferR.position() + Segment.getSegBufLen(bufferR));

      ISegment res = Segment.loadAsSegment(bufferR.slice());
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
  // region Space Allocation

  /**
   * This method will allocate DIRECTLY from spare space and return corresponding ByteBuffer. It
   * will not update segLstLen nor segCacheMap, since no segment initiated inside this method.
   *
   * @param size target size of the ByteBuffer
   * @return ByteBuffer return null if {@linkplain SchemaPageOverflowException} to improve
   *     efficiency
   */
  private ByteBuffer allocSpareBufferSlice(short size) {
    // check whether enough space to be directly allocate
    if (SchemaFile.PAGE_LENGTH - pageSpareOffset - segNum * SchemaFile.SEG_OFF_DIG
        < size + SchemaFile.SEG_OFF_DIG) {
      // since this may occur frequently, throw exception here may be inefficient
      return null;
    }

    pageBuffer.clear();
    pageBuffer.position(pageSpareOffset);
    pageBuffer.limit(pageSpareOffset + size);

    return pageBuffer.slice();
  }

  /**
   * Allocate a new segment to extend specified segment, modify cache map and list.<br>
   * Mark original segment instance as deleted, modify segOffsetList, pageSpareOffset and
   * segCacheMap.
   *
   * <p><b> The new segment could be allocated from spare space or rearranged space.</b>
   *
   * @param seg original segment instance
   * @param segIdx original segment index
   * @param newSize target segment size
   * @return reallocated segment instance
   * @throws SchemaPageOverflowException if this page has no enough space
   */
  private ISegment relocateSegment(ISegment seg, short segIdx, short newSize)
      throws SchemaPageOverflowException, SegmentNotFoundException {
    if (seg.size() == SchemaFile.SEG_MAX_SIZ || getSpareSize() + seg.size() < newSize) {
      throw new SchemaPageOverflowException(pageIndex);
    }

    ByteBuffer newBuffer = allocSpareBufferSlice(newSize);
    if (newBuffer == null) {
      rearrangeSegments(segIdx);
      return extendSegmentInPlace(segIdx, seg.size(), newSize);
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
   * To compact segments further, set deleted segments offset to -1 It modifies pageSpareOffset if
   * more space released. Over-write stash segments with existed segments.
   */
  private void compactSegments() {
    this.rearrangeSegments((short) -1);
  }

  /**
   * Compact segments and move target segment (id at idx) to the tail of segments.<br>
   * Since this method may overwrite segment buffer, original buffer instance shall be abolished.
   */
  private synchronized void rearrangeSegments(short idx) {
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
   * @param oriSegSize size of the target segment
   * @param newSize extended size
   * @return extended segment based on page buffer
   */
  private ISegment extendSegmentInPlace(short segId, short oriSegSize, short newSize)
      throws SegmentNotFoundException {
    // extend segment, modify pageSpareOffset, segCacheMap
    short offset = getSegmentOffset(segId);

    // only last segment could extend in-place
    if (offset + oriSegSize != pageSpareOffset) {
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
