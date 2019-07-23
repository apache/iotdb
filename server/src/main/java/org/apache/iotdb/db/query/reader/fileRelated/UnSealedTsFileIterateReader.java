/**
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
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.IterateReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

/**
 * To read an unsealed sequence TsFile, this class extends {@link IterateReader} to implement {@link
 * IAggregateReader} for the TsFile.
 * <p>
 * Note that an unsealed sequence TsFile consists of two parts of data in chronological order: 1)
 * data that has been flushed to disk and 2) data in the flushing memtable list.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader}.
 */
public class UnSealedTsFileIterateReader extends IterateReader {

  private TsFileResource unsealedTsFile;
  private Filter filter;

  /**
   * Whether the reverse order is enabled.
   * <p>
   * True to iterate over chunk data in reverse chronological order (from newest to oldest); False
   * to iterate over chunk data in chronological order (from oldest to newest).
   */
  private boolean enableReverse;

  /**
   * Constructor function.
   * <p>
   * An unsealed sequence TsFile consists of two parts of data in chronological order: 1) data that
   * has been flushed to disk and 2) data in the flushing memtable list. <code>IterateReader</code>
   * is used to iterate over the two parts. Therefore, this method calls the parent class
   * <code>IterateReader</code>'s constructor to set <code>readerSize</code> to be 2. Readers for
   * the two parts of data are created in order later in the method <code>constructNextReader</code>.
   *
   * @param unsealedTsFile the TsFileResource corresponding to the unsealed TsFile
   * @param filter filter condition
   * @param isReverse True to iterate over chunk data in reverse chronological order (from newest to
   * oldest); False to iterate over chunk data in chronological order (from oldest to newest).
   */
  public UnSealedTsFileIterateReader(TsFileResource unsealedTsFile, Filter filter,
      boolean isReverse) {
    super(2);
    this.enableReverse = isReverse;
    this.unsealedTsFile = unsealedTsFile;
    this.filter = filter;
  }

  @Override
  protected boolean constructNextReader(int idx) throws IOException {
    if (idx == 0) {
      if (enableReverse) {
        // data in memory first if it is to iterate over chunk data in reverse chronological order
        currentSeriesReader = new MemChunkReader(unsealedTsFile.getReadOnlyMemChunk(), filter);
      } else {
        // data on disk first if it is to iterate over chunk data in chronological order
        currentSeriesReader = initUnSealedTsFileDiskReader(unsealedTsFile, filter);
      }
    } else { // idx=1
      if (enableReverse) {
        currentSeriesReader = initUnSealedTsFileDiskReader(unsealedTsFile, filter);
      } else {
        currentSeriesReader = new MemChunkReader(unsealedTsFile.getReadOnlyMemChunk(), filter);
      }
    }
    return true;
  }

  /**
   * Creates <code>IAggregateReader</code> for an unsealed sequence TsFile's on-disk data.
   */
  private IAggregateReader initUnSealedTsFileDiskReader(TsFileResource unSealedTsFile,
      Filter filter)
      throws IOException {

    // prepare metaDataList
    List<ChunkMetaData> metaDataList = unSealedTsFile.getChunkMetaDatas();
    if (enableReverse && metaDataList != null && !metaDataList.isEmpty()) {
      Collections.reverse(metaDataList);
    }

    // prepare chunkLoader
    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(unSealedTsFile, false);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

    // init fileSeriesReader
    FileSeriesReader fileSeriesReader;
    if (filter == null) {
      fileSeriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      fileSeriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }

    return new FileSeriesReaderAdapter(fileSeriesReader);
  }
}
