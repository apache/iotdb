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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * {@link SchemaFile} manages a collection of SchemaPages, which acts like a segment of index entry
 * {@link ISegment} or a collection of wrapped segments.
 */
public abstract class SchemaPage {
  // region Configuration of Page and Segment
  // TODO: may be better to move to extra Config class

  public static int PAGE_LENGTH = 16 * 1024; // 16 kib for default
  public static long PAGE_INDEX_MASK = 0xffff_ffffL; // highest bit is not included
  public static short PAGE_HEADER_SIZE = 32;

  public static final int SEG_HEADER_SIZE = 25; // in bytes
  public static final short SEG_OFF_DIG =
      2; // length of short, which is the type of segment offset and index
  public static final int SEG_INDEX_DIGIT = 16; // for type short in bits
  public static final long SEG_INDEX_MASK = 0xffffL; // help to translate address

  public static final short SEG_MAX_SIZ = (short) (16 * 1024 - PAGE_HEADER_SIZE - SEG_OFF_DIG);
  public static final short[] SEG_SIZE_LST = {1024, 2 * 1024, 4 * 1024, 8 * 1024, SEG_MAX_SIZ};
  public static final short SEG_MIN_SIZ =
      IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile() > SEG_MAX_SIZ
          ? SEG_MAX_SIZ
          : IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile();
  public static int PAGE_CACHE_SIZE =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getPageCacheSizeInSchemaFile(); // size of page cache

  public static final boolean INCLINED_SPLIT =
      true; // segment split into 2 part with different entries
  public static final boolean BULK_SPLIT = true; // split may implement a fast bulk way

  // endregion

  // value of type flag of a schema page
  public static byte INTERNAL_PAGE = 0x01;
  public static int INDEX_OFFSET = 1;
  // offset on the buffer of attributes about the page
  public static byte SEGMENTED_PAGE = 0x00;

  public static int OFFSET_DIGIT = 16;
  public static long OFFSET_MASK = 0x7fffL;

  // All other attributes are to describe this ByteBuffer
  protected final ByteBuffer pageBuffer;

  protected int pageIndex; // only change when register buffer as a new page
  protected short spareOffset; // bound of the variable length part in a slotted structure
  protected short spareSize; // traces spare space size simultaneously
  protected short memberNum; // amount of the member, definition depends on implementation

  protected SchemaPage(ByteBuffer pageBuffer) {
    this.pageBuffer = pageBuffer;

    this.pageBuffer.limit(this.pageBuffer.capacity());
    this.pageBuffer.position(INDEX_OFFSET);

    pageIndex = ReadWriteIOUtils.readInt(this.pageBuffer);
    spareOffset = ReadWriteIOUtils.readShort(this.pageBuffer);
    spareSize = ReadWriteIOUtils.readShort(this.pageBuffer);
    memberNum = ReadWriteIOUtils.readShort(this.pageBuffer);
  }

  /**
   * <b>Page Header Structure: (19 bytes used, 13 bytes reserved)</b>
   *
   * <ul>
   *   <li>1 byte: page type indicator
   *   <li>1 int (4 bytes): pageIndex, a non-negative number
   *   <li>1 short (2 bytes): spareOffset, bound of the variable length part in a slotted structure
   *   <li>1 short (2 bytes): spareSize, space left to use
   *   <li>1 short (2 bytes): memberNum, amount of the member whose type depends on implementation
   *   <li>1 long (8 bytes): firstLeaf, points to first segmented page, only exists in {@link
   *       InternalPage}
   * </ul>
   *
   * While header of a page is partly fixed, the body is dependent on implementation.
   */
  public static SchemaPage loadSchemaPage(ByteBuffer buffer) throws MetadataException {
    buffer.clear();
    byte pageType = ReadWriteIOUtils.readByte(buffer);

    if (pageType == SEGMENTED_PAGE) {
      return new SegmentedPage(buffer);
    } else if (pageType == INTERNAL_PAGE) {
      return new InternalPage(buffer);
    } else {
      throw new MetadataException(
          "ByteBuffer is corrupted or set to a wrong position to load as a SchemaPage.");
    }
  }

  /** InternalPage should be initiated with a pointer which points to the minimal child of it. */
  public static SchemaPage initInternalPage(ByteBuffer buffer, int pageIndex, int ptr) {
    buffer.clear();

    buffer.position(PAGE_HEADER_SIZE);
    ReadWriteIOUtils.write(((PAGE_INDEX_MASK & ptr) << OFFSET_DIGIT), buffer);

    buffer.position(0);
    ReadWriteIOUtils.write(INTERNAL_PAGE, buffer);
    ReadWriteIOUtils.write(pageIndex, buffer);
    ReadWriteIOUtils.write((short) buffer.capacity(), buffer);
    ReadWriteIOUtils.write(
        (short) (buffer.capacity() - PAGE_HEADER_SIZE - InternalPage.COMPOUND_POINT_LENGTH),
        buffer);
    ReadWriteIOUtils.write((short) 1, buffer);
    ReadWriteIOUtils.write(-1L, buffer);

    return new InternalPage(buffer);
  }

  public static SchemaPage initSegmentedPage(ByteBuffer buffer, int pageIndex) {
    buffer.clear();
    ReadWriteIOUtils.write(SEGMENTED_PAGE, buffer);
    ReadWriteIOUtils.write(pageIndex, buffer);
    ReadWriteIOUtils.write(PAGE_HEADER_SIZE, buffer);
    ReadWriteIOUtils.write((short) (buffer.capacity() - PAGE_HEADER_SIZE), buffer);
    ReadWriteIOUtils.write((short) 0, buffer);
    ReadWriteIOUtils.write(-1L, buffer);
    return new SegmentedPage(buffer);
  }

  public boolean isCapableForSize(short size) {
    return spareSize >= size;
  }

  public void syncPageBuffer() {
    this.pageBuffer.limit(this.pageBuffer.capacity());
    this.pageBuffer.position(INDEX_OFFSET);
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
    ReadWriteIOUtils.write(spareOffset, pageBuffer);
    ReadWriteIOUtils.write(spareSize, pageBuffer);
    ReadWriteIOUtils.write(memberNum, pageBuffer);
  };

  public void flushPageToChannel(FileChannel channel) throws IOException {
    this.syncPageBuffer();
    this.pageBuffer.clear();
    channel.write(pageBuffer, SchemaFile.getPageAddress(pageIndex));
  }

  public abstract String inspect() throws SegmentNotFoundException;

  public int getPageIndex() {
    return pageIndex;
  }

  public void setPageIndex(int pid) {
    pageIndex = pid;
    this.pageBuffer.clear();
    this.pageBuffer.position(INDEX_OFFSET);
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
    this.pageBuffer.clear();
  }

  public abstract ISegment<Integer, Integer> getAsInternalPage();

  public abstract ISegmentedPage getAsSegmentedPage();

  // for better performance when split
  protected ByteBuffer getEntireSegmentSlice() throws MetadataException {
    return null;
  }

  @TestOnly
  public WrappedSegment getSegmentOnTest(short idx) throws SegmentNotFoundException {
    return null;
  };

  @TestOnly
  public void getPageBuffer(ByteBuffer dstBuffer) {
    syncPageBuffer();
    this.pageBuffer.clear();
    dstBuffer.put(this.pageBuffer);
  }
}
