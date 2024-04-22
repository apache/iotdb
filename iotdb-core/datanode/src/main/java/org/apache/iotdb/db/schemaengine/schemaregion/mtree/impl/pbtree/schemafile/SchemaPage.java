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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link SchemaFile} manages a collection of SchemaPages, which acts like a segment of index entry
 * {@link ISegment} or a collection of wrapped segments.
 */
public abstract class SchemaPage implements ISchemaPage {

  // All other attributes are to describe this ByteBuffer
  protected final ByteBuffer pageBuffer;

  protected final AtomicInteger refCnt;
  protected final ReadWriteLock lock;

  protected int pageIndex; // only change when register buffer as a new page
  protected short spareOffset; // bound of the variable length part in a slotted structure
  protected short spareSize; // traces spare space size simultaneously
  protected short memberNum; // amount of the member, definition depends on implementation

  protected boolean dirtyFlag = false; // any modification turns it true

  /** for replacement page state transfer * */
  protected SchemaPage(ByteBuffer pageBuffer, AtomicInteger ai, ReadWriteLock rwl) {
    this.pageBuffer = pageBuffer;

    this.pageBuffer
        .limit(this.pageBuffer.capacity())
        .position(SchemaFileConfig.PAGE_HEADER_INDEX_OFFSET);

    refCnt = ai;
    lock = rwl;

    pageIndex = ReadWriteIOUtils.readInt(this.pageBuffer);
    spareOffset = ReadWriteIOUtils.readShort(this.pageBuffer);
    spareSize = ReadWriteIOUtils.readShort(this.pageBuffer);
    memberNum = ReadWriteIOUtils.readShort(this.pageBuffer);
  }

  /** compatible constructor with no existing ref or lock */
  protected SchemaPage(ByteBuffer pageBuffer) {
    this(pageBuffer, new AtomicInteger(), new ReentrantReadWriteLock());
  }

  @Override
  public AtomicInteger getRefCnt() {
    return refCnt;
  }

  @Override
  public int incrementAndGetRefCnt() {
    return refCnt.incrementAndGet();
  }

  @Override
  public int decrementAndGetRefCnt() {
    return refCnt.decrementAndGet();
  }

  @Override
  public ReadWriteLock getLock() {
    return lock;
  }

  @Override
  public void syncPageBuffer() {
    this.pageBuffer.limit(this.pageBuffer.capacity());
    this.pageBuffer.position(SchemaFileConfig.PAGE_HEADER_INDEX_OFFSET);
    ReadWriteIOUtils.write(pageIndex, pageBuffer);
    ReadWriteIOUtils.write(spareOffset, pageBuffer);
    ReadWriteIOUtils.write(spareSize, pageBuffer);
    ReadWriteIOUtils.write(memberNum, pageBuffer);
  }

  @Override
  public void flushPageToChannel(FileChannel channel) throws IOException {
    this.pageBuffer.clear();
    channel.write(pageBuffer, SchemaFile.getPageAddress(pageIndex));
    dirtyFlag = false;
  }

  @Override
  public boolean isDirtyPage() {
    return dirtyFlag;
  }

  @Override
  public void setDirtyFlag() {
    this.dirtyFlag = true;
  }

  @Override
  public void flushPageToStream(OutputStream stream) throws IOException {
    if (pageIndex < 0) {
      // mark as check point
      stream.write(new byte[] {(byte) pageIndex});
      return;
    }

    this.pageBuffer.clear();
    stream.write(pageBuffer.array());
  }

  @Override
  public int getPageIndex() {
    return pageIndex;
  }

  @Override
  public void setPageIndex(int pid) {
    pageIndex = pid;
    this.pageBuffer.clear();
    this.pageBuffer.position(SchemaFileConfig.PAGE_HEADER_INDEX_OFFSET);
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
  }

  @Override
  @TestOnly
  public void getPageBuffer(ByteBuffer dstBuffer) {
    syncPageBuffer();
    this.pageBuffer.clear();
    dstBuffer.put(this.pageBuffer);
  }
}
