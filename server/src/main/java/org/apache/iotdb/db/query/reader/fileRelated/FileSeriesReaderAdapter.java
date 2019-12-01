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
package org.apache.iotdb.db.query.reader.fileRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

/**
 * To read a sequence TsFile's on-disk data, this class implements an interface {@link
 * IAggregateReader} based on the data reader {@link FileSeriesReader}.
 * <p>
 * Note that <code>FileSeriesReader</code> is an abstract class with two concrete classes:
 * <code>FileSeriesReaderWithoutFilter</code> and <code>FileSeriesReaderWithFilter</code>.
 * <p>
 * This class is used in {@link UnSealedTsFileIterateReader} and {@link
 * org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader}.
 */
public class FileSeriesReaderAdapter implements IAggregateReader {

  private FileSeriesReader fileSeriesReader;

  public FileSeriesReaderAdapter(FileSeriesReader fileSeriesReader) {
    this.fileSeriesReader = fileSeriesReader;
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return fileSeriesReader.hasNextChunk();
  }

  @Override
  public ChunkMetaData nextChunkMeta() {
    return fileSeriesReader.currentChunkMeta();
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return fileSeriesReader.nextPageHeader();
  }

  @Override
  public void skipPageData() {
    fileSeriesReader.skipPageData();
  }

  @Override
  public boolean hasNext() throws IOException {
    return fileSeriesReader.hasNextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return fileSeriesReader.nextBatch();
  }

  @Override
  public void close() throws IOException {
    fileSeriesReader.close();
  }
}
