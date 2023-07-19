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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.lsm.sstable.interator.IDiskIterator;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public interface IChunkReader extends IDiskIterator<Integer> {

  /**
   * Read the {@link org.roaringbitmap.RoaringBitmap RoaringBitmap} from the given position
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a RoaringBitmap instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  RoaringBitmap readRoaringBitmap(long offset) throws IOException;

  /**
   * Read the {@link org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader
   * ChunkHeader} from the given position
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a ChunkHeader instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  ChunkHeader readChunkHeader(long offset) throws IOException;

  /**
   * Closes this reader and releases any system resources associated with the reader.
   *
   * @exception IOException if an I/O error occurs.
   */
  void close() throws IOException;
}
