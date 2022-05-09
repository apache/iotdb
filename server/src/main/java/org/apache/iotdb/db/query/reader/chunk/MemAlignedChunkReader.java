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
package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.engine.querycontext.AlignedReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** To read aligned chunk data in memory */
public class MemAlignedChunkReader implements IChunkReader {

  private List<IPageReader> pageReaderList;

  public MemAlignedChunkReader(AlignedReadOnlyMemChunk readableChunk, Filter filter) {
    // we treat one ReadOnlyMemChunk as one Page
    this.pageReaderList =
        Collections.singletonList(
            new MemAlignedPageReader(
                readableChunk.getTsBlock(),
                (AlignedChunkMetadata) readableChunk.getChunkMetaData(),
                filter));
  }

  @Override
  public boolean hasNextSatisfiedPage() throws IOException {
    throw new IOException("mem chunk reader does not support this method");
  }

  @Override
  public BatchData nextPageData() throws IOException {
    throw new IOException("mem chunk reader does not support this method");
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return this.pageReaderList;
  }
}
