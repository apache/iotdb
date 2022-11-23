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
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;

import java.nio.ByteBuffer;
import java.util.Queue;

/**
 * This interface interacts with bytebuffer as a segment which is the basic unit to manage index
 * entries or MNode records.<br>
 * Generic T denotes the type of its input, while R denotes the type of the return.
 */
public interface ISegment<T, R> {

  /**
   * check whether enough space, notice that pairLength including 3 parts: [var length] key string
   * itself, [int, 4 bytes] length of key string, [short, 2 bytes] key address
   *
   * @return -1 for segment space run out, otherwise for spare space
   */
  int insertRecord(String key, T rec) throws RecordDuplicatedException;

  /**
   * todo switch definition of throw and negative return for better perf
   *
   * @param key name of the record, not the alias
   * @param buffer content of the updated record
   * @return index of keyAddressList, -1 for not found, exception for space run out
   * @throws SegmentOverflowException if segment runs out of memory
   */
  int updateRecord(String key, T buffer) throws SegmentOverflowException, RecordDuplicatedException;

  int removeRecord(String key);

  /**
   * Get a record by its name
   *
   * @return index entry if {@link InternalPage}, otherwise IMNode
   */
  R getRecordByKey(String key) throws MetadataException;

  /**
   * Get a record by its alias
   *
   * @return null if {@link InternalPage}, otherwise IMNode
   */
  R getRecordByAlias(String alias) throws MetadataException;

  boolean hasRecordKey(String key);

  boolean hasRecordAlias(String alias);

  Queue<R> getAllRecords() throws MetadataException;

  /**
   * Records are always sync with buffer, but members in header are not. This method sync these
   * values to the buffer.
   */
  void syncBuffer();

  short size();

  short getSpareSize();

  void delete();

  long getNextSegAddress();

  void setNextSegAddress(long nextSegAddress);

  /**
   * This method will write info into a buffer equal or larger to existed one. There is no need to
   * call sync before this method, since it will flush header and key-offset list intrinsically.
   *
   * @param newBuffer target buffer
   */
  void extendsTo(ByteBuffer newBuffer) throws MetadataException;

  /**
   * Split the segment into dstBuffer considering the passing in key.
   *
   * <p>A better encapsulation may indicate to split the ISegment into another ISegment with same
   * generic type, rather than splitting into a raw ByteBuffer. That approach naturally migrate
   * records in a one-by-one manner, while not capable for bulk migrating records which could
   * improve performance in monotonic cases.
   *
   * @param entry content of the insert key.
   * @param dstBuffer destination of the split.
   * @param inclineSplit whether to split with incline.
   * @return always the search key of the split segment.
   */
  String splitByKey(String key, T entry, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException;

  /**
   * Reset buffer content and return corresponding slice.
   *
   * @param ptr only works for {@link InternalPage}.
   * @return slice of the buffer.
   */
  ByteBuffer resetBuffer(int ptr);

  String toString();

  String inspect();
}
