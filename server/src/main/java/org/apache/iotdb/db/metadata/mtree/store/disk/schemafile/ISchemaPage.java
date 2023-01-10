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
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface ISchemaPage {
  /**
   * <b>Page Header Structure: (23 bytes used, 9 bytes reserved)</b>
   *
   * <ul>
   *   <li>1 byte: page type indicator
   *   <li>1 int (4 bytes): pageIndex, a non-negative number
   *   <li>1 short (2 bytes): spareOffset, bound of the variable length part in a slotted structure
   *   <li>1 short (2 bytes): spareSize, space left to use
   *   <li>1 short (2 bytes): memberNum, amount of the member whose type depends on implementation
   *   <li>1 long (8 bytes): firstLeaf, points to first segmented page, only in {@link InternalPage}
   *   <li>1 int (4 bytes): subIndexPage, points to sub-index, only in {@link InternalPage}
   * </ul>
   *
   * <p>While header of a page is partly fixed, the body is dependent on implementation.
   */
  static SchemaPage loadSchemaPage(ByteBuffer buffer) throws MetadataException {
    buffer.clear();
    byte pageType = ReadWriteIOUtils.readByte(buffer);

    switch (pageType) {
      case SchemaFileConfig.SEGMENTED_PAGE:
        return new SegmentedPage(buffer);
      case SchemaFileConfig.INTERNAL_PAGE:
        return new InternalPage(buffer);
      case SchemaFileConfig.ALIAS_PAGE:
        return new AliasIndexPage(buffer);
      default:
        throw new MetadataException(
            "ByteBuffer is corrupted or set to a wrong position to load as a SchemaPage.");
    }
  }

  /** InternalPage should be initiated with a pointer which points to the minimal child of it. */
  static SchemaPage initInternalPage(ByteBuffer buffer, int pageIndex, int ptr) {
    buffer.clear();

    buffer.position(SchemaFileConfig.PAGE_HEADER_SIZE);
    ReadWriteIOUtils.write(
        ((SchemaFileConfig.PAGE_INDEX_MASK & ptr) << SchemaFileConfig.COMP_POINTER_OFFSET_DIGIT),
        buffer);

    buffer.position(0);
    ReadWriteIOUtils.write(SchemaFileConfig.INTERNAL_PAGE, buffer);
    ReadWriteIOUtils.write(pageIndex, buffer);
    ReadWriteIOUtils.write((short) buffer.capacity(), buffer);
    ReadWriteIOUtils.write(
        (short)
            (buffer.capacity()
                - SchemaFileConfig.PAGE_HEADER_SIZE
                - InternalPage.COMPOUND_POINT_LENGTH),
        buffer);
    ReadWriteIOUtils.write((short) 1, buffer);
    ReadWriteIOUtils.write(-1L, buffer);
    ReadWriteIOUtils.write(-1, buffer);

    return new InternalPage(buffer);
  }

  static ISegmentedPage initSegmentedPage(ByteBuffer buffer, int pageIndex) {
    buffer.clear();
    ReadWriteIOUtils.write(SchemaFileConfig.SEGMENTED_PAGE, buffer);
    ReadWriteIOUtils.write(pageIndex, buffer);
    ReadWriteIOUtils.write(SchemaFileConfig.PAGE_HEADER_SIZE, buffer);
    ReadWriteIOUtils.write((short) (buffer.capacity() - SchemaFileConfig.PAGE_HEADER_SIZE), buffer);
    ReadWriteIOUtils.write((short) 0, buffer);
    ReadWriteIOUtils.write(-1L, buffer);
    return new SegmentedPage(buffer);
  }

  static SchemaPage initAliasIndexPage(ByteBuffer buffer, int pageIndex) {
    buffer.clear();
    ReadWriteIOUtils.write(SchemaFileConfig.ALIAS_PAGE, buffer);
    ReadWriteIOUtils.write(pageIndex, buffer);
    ReadWriteIOUtils.write((short) buffer.capacity(), buffer);
    ReadWriteIOUtils.write((short) (buffer.capacity() - SchemaFileConfig.PAGE_HEADER_SIZE), buffer);
    ReadWriteIOUtils.write((short) 0, buffer);
    ReadWriteIOUtils.write(-1L, buffer);
    return new AliasIndexPage(buffer);
  }

  void syncPageBuffer();

  void flushPageToChannel(FileChannel channel) throws IOException;

  void flushPageToStream(OutputStream stream) throws IOException;

  String inspect() throws SegmentNotFoundException;

  int getPageIndex();

  void setPageIndex(int pid);

  int getSubIndex();

  void setSubIndex(int pid);

  ISegment<Integer, Integer> getAsInternalPage();

  ISegment<String, String> getAsAliasIndexPage();

  ISegmentedPage getAsSegmentedPage();

  ByteBuffer getEntireSegmentSlice() throws MetadataException;

  void markDirty();

  boolean isDirty();

  @TestOnly
  WrappedSegment getSegmentOnTest(short idx) throws SegmentNotFoundException;

  @TestOnly
  void getPageBuffer(ByteBuffer dstBuffer);
}
