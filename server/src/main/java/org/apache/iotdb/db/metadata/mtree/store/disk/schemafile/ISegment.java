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

import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.schemafile.SegmentOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.nio.ByteBuffer;
import java.util.Queue;

/** This interface interacts with segment bytebuffer. */
public interface ISegment {
  int SEG_HEADER_SIZE = 25; // in bytes
  /**
   * write a record of node to this segment
   *
   * @param key node name for generally
   * @param buffer content of the record
   * @return spare space after insert
   */
  int insertRecord(String key, ByteBuffer buffer) throws RecordDuplicatedException;

  int insertRecords(String[] keys, ByteBuffer[] buffers);

  int updateRecord(String key, ByteBuffer buffer) throws SegmentOverflowException;

  int removeRecord(String key);

  IMNode getRecordAsIMNode(String key);

  boolean hasRecordKey(String key);

  Queue<IMNode> getAllRecords();

  /**
   * Records are always sync with buffer, but header and key-address list are not. This method sync
   * these values to the buffer.
   */
  void syncBuffer();

  short size();

  short getSpareSize();

  void delete();

  long getPrevSegAddress();

  long getNextSegAddress();

  void setPrevSegAddress(long prevSegAddress);

  void setNextSegAddress(long nextSegAddress);

  /**
   * This method write content to a ByteBuffer, which has larger capacity than itself
   *
   * @param newBuffer target buffer
   */
  void extendsTo(ByteBuffer newBuffer);

  String toString();
}
