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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;

public interface ISchemaPage {

  /**
   * Insert a content directly into specified segment, without considering preallocate and
   * reallocate segment. <br>
   * Find the right segment instance which MUST exists, cache the segment and insert the record.
   * <br>
   * If not enough, reallocate inside page first, or throw exception for new page then.
   *
   * <p>Notice that, since {@link SchemaFile#reEstimateSegSize(int)} may increase segment with very
   * small extent, which originates from design of {@link SchemaFile#estimateSegmentSize(IMNode)}, a
   * twice relocate will suffice any nodes smaller than 1024 KiB.<br>
   * This reason works for {@link #update(short, String, ByteBuffer)} as well.
   *
   * @return return 0 if write succeed, a positive for next segment address
   * @throws SchemaPageOverflowException no next segment, no spare space inside page
   */
  long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  IMNode read(short segIdx, String key) throws SegmentNotFoundException;

  /**
   * The record is definitely inside specified segment. This method compare old and new buffer to
   * decide whether update in place. <br>
   * If segment not enough, it will reallocate in this page first, and update segment offset list.
   * <br>
   * If no more space for reallocation, throw {@link SchemaPageOverflowException} and return a
   * negative for new page.
   *
   * <p>See {@linkplain #write(short, String, ByteBuffer)} for the detail reason of a twice try.
   *
   * @return spare space of the segment, negative if not enough
   */
  void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  /**
   * Check if record exists with name or alias.
   *
   * @param key name or alias of target child
   * @param segId target segment index
   */
  boolean hasRecordKeyInSegment(String key, short segId) throws SegmentNotFoundException;

  Queue<IMNode> getChildren(short segId) throws SegmentNotFoundException;

  void removeRecord(short segId, String key) throws SegmentNotFoundException;

  void deleteSegment(short segId) throws SegmentNotFoundException;

  int getPageIndex();

  short getSpareSize();

  short getSegmentSize(short segId) throws SegmentNotFoundException;

  void getPageBuffer(ByteBuffer dst);

  boolean isCapableForSize(short size);

  boolean isSegmentCapableFor(short segId, short size) throws SegmentNotFoundException;

  /**
   * While segments are always synchronized with buffer {@linkplain SchemaPage#pageBuffer}, header
   * and tail are not. This method will synchronize them with in mem attributes.
   */
  void syncPageBuffer();

  /**
   * Allocate space for a new segment inside this page
   *
   * @param size expected segment size
   * @return segment index in this page, negative for not enough space
   */
  short allocNewSegment(short size) throws IOException, SchemaPageOverflowException;

  /**
   * Transplant designated segment from srcPage, to spare space of the page
   *
   * @param srcPage source page conveys source segment
   * @param segId id of the target segment
   * @param newSegSize size of new segment in this page
   * @throws MetadataException if spare not enough, segment not found or inconsistency
   */
  long transplantSegment(ISchemaPage srcPage, short segId, short newSegSize)
      throws MetadataException;

  void setNextSegAddress(short segId, long address) throws SegmentNotFoundException;

  void setPrevSegAddress(short segId, long address) throws SegmentNotFoundException;

  long getNextSegAddress(short segId) throws SegmentNotFoundException;

  long getPrevSegAddress(short segId) throws SegmentNotFoundException;

  String inspect() throws SegmentNotFoundException;
}
