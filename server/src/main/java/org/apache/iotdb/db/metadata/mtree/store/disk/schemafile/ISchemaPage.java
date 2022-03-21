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

  long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  IMNode read(short segIdx, String key) throws SegmentNotFoundException;

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

  void syncPageBuffer();

  short allocNewSegment(short size) throws IOException, SchemaPageOverflowException;

  long transplantSegment(ISchemaPage srcPage, short segId, short newSegSize)
      throws MetadataException;

  void setNextSegAddress(short segId, long address) throws SegmentNotFoundException;

  void setPrevSegAddress(short segId, long address) throws SegmentNotFoundException;

  long getNextSegAddress(short segId) throws SegmentNotFoundException;

  long getPrevSegAddress(short segId) throws SegmentNotFoundException;

  String inspect() throws SegmentNotFoundException;
}
