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
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentNotFoundException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;

/** Interface for a {@link SchemaPage} manages one or more {@link WrappedSegment}. */
public interface ISegmentedPage extends ISchemaPage {

  /**
   * Insert a content directly into specified segment. If not enough spare within the segment,
   * reallocate inside the page, or throw exception for new page then.
   *
   * @return return 0 if write succeed
   * @throws SchemaPageOverflowException no spare space inside page
   */
  long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  IMNode read(short segIdx, String key) throws MetadataException;

  IMNode readByAlias(short segIdx, String alias) throws MetadataException;

  /**
   * The record is definitely inside specified segment. {@link WrappedSegment} will compare existed
   * and updated buffer to decide whether to update in place.
   *
   * <p>If segment not enough, it will reallocate in this page first, and throw {@link
   * SchemaPageOverflowException} if no more spare space to reallocate.
   */
  void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  Queue<IMNode> getChildren(short segId) throws MetadataException;

  void removeRecord(short segId, String key) throws SegmentNotFoundException;

  void deleteSegment(short segId) throws SegmentNotFoundException;

  void purgeSegments();

  int validSegments();

  short getSpareSize();

  boolean isCapableForSegSize(short size);

  short getSegmentSize(short segId) throws SegmentNotFoundException;

  /**
   * Allocate space for a new segment inside this page
   *
   * @param size expected segment size
   * @return segment index in this page, negative for not enough space
   */
  short allocNewSegment(short size) throws IOException, MetadataException;

  /**
   * Transplant designated segment from srcPage, to spare space of the page
   *
   * @param srcPage source page conveys source segment
   * @param segId id of the target segment
   * @param newSegSize size of new segment in this page
   * @throws MetadataException if spare not enough, segment not found or inconsistency
   */
  long transplantSegment(ISegmentedPage srcPage, short segId, short newSegSize)
      throws MetadataException;

  void extendsSegmentTo(ByteBuffer dstBuffer, short segId) throws MetadataException;

  void setNextSegAddress(short segId, long address) throws SegmentNotFoundException;

  long getNextSegAddress(short segId) throws SegmentNotFoundException;

  String splitWrappedSegment(
      String key, ByteBuffer recBuf, ISchemaPage dstPage, boolean inclineSplit)
      throws MetadataException;
}
