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
public abstract class SchemaPage implements ISchemaPage {
  // region Configuration of Page and Segment
  // TODO: may be better to move to extra Config class

  public static final int PAGE_LENGTH = 16 * 1024; // 16 kib for default
  public static final long PAGE_INDEX_MASK = 0xffff_ffffL; // highest bit is not included
  public static final short PAGE_HEADER_SIZE = 32;

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
  public static final int PAGE_CACHE_SIZE =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getPageCacheSizeInSchemaFile(); // size of page cache

  public static final boolean INCLINED_SPLIT =
      true; // segment split into 2 part with different entries
  public static final boolean BULK_SPLIT = true; // split may implement a fast bulk way

  // endregion

  // value of type flag of a schema page
  public static final byte SEGMENTED_PAGE = 0x00;
  public static final byte INTERNAL_PAGE = 0x01;
  public static final byte ALIAS_PAGE = 0x02;

  // offset on the buffer of attributes about the page
  public static final int INDEX_OFFSET = 1;

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

    this.pageBuffer.limit(this.pageBuffer.capacity()).position(INDEX_OFFSET);

    pageIndex = ReadWriteIOUtils.readInt(this.pageBuffer);
    spareOffset = ReadWriteIOUtils.readShort(this.pageBuffer);
    spareSize = ReadWriteIOUtils.readShort(this.pageBuffer);
    memberNum = ReadWriteIOUtils.readShort(this.pageBuffer);
  }

  @Override
  public boolean isCapableForSize(short size) {
    return spareSize >= size;
  }

  @Override
  public void syncPageBuffer() {
    this.pageBuffer.limit(this.pageBuffer.capacity());
    this.pageBuffer.position(INDEX_OFFSET);
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
    ReadWriteIOUtils.write(spareOffset, pageBuffer);
    ReadWriteIOUtils.write(spareSize, pageBuffer);
    ReadWriteIOUtils.write(memberNum, pageBuffer);
  };

  @Override
  public void flushPageToChannel(FileChannel channel) throws IOException {
    this.syncPageBuffer();
    this.pageBuffer.clear();
    channel.write(pageBuffer, SchemaFile.getPageAddress(pageIndex));
  }

  @Override
  public int getPageIndex() {
    return pageIndex;
  }

  @Override
  public void setPageIndex(int pid) {
    pageIndex = pid;
    this.pageBuffer.clear();
    this.pageBuffer.position(INDEX_OFFSET);
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
    this.pageBuffer.clear();
  }

  @Override
  public int getSubIndex() {
    return -1;
  }

  @Override
  public void setSubIndex(int pid) {}

  @Override
  public ISegment<Integer, Integer> getAsInternalPage() {
    return null;
  }

  @Override
  public ISegment<String, String> getAsAliasIndexPage() {
    return null;
  }

  @Override
  public ISegmentedPage getAsSegmentedPage() {
    return null;
  }

  // for better performance when split
  @Override
  public ByteBuffer getEntireSegmentSlice() throws MetadataException {
    return null;
  }

  @Override
  @TestOnly
  public WrappedSegment getSegmentOnTest(short idx) throws SegmentNotFoundException {
    return null;
  };

  @Override
  @TestOnly
  public void getPageBuffer(ByteBuffer dstBuffer) {
    syncPageBuffer();
    this.pageBuffer.clear();
    dstBuffer.put(this.pageBuffer);
  }
}
