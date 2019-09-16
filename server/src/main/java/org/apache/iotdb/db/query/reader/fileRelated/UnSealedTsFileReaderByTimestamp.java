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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

/**
 * To read an unsealed sequence TsFile by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} for the TsFile.
 * <p>
 * Note that an unsealed sequence TsFile consists of two parts of data in chronological order: 1)
 * data that has been flushed to disk and 2) data in the flushing memtable list.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceReaderByTimestamp}.
 */
public class UnSealedTsFileReaderByTimestamp implements IReaderByTimestamp {

  /**
   * <code>FileSeriesReaderByTimestamp</code> for data which has been flushed to disk.
   */
  private FileSeriesReaderByTimestamp unSealedTsFileDiskReaderByTs;

  /**
   * <code>IReaderByTimestamp</code> for data in the flushing memtable list.
   */
  private IReaderByTimestamp unSealedTsFileMemReaderByTs;

  /**
   * Whether unSealedTsFileDiskReaderByTs has been run out of.
   * <p>
   * True means the current reader is unSealedTsFileMemReaderByTs; False means the current reader is
   * still unSealedTsFileDiskReaderByTs.
   */
  private boolean unSealedTsFileDiskReaderEnded;

  public UnSealedTsFileReaderByTimestamp(TsFileResource unsealedTsFile) throws IOException {
    // create IReaderByTimestamp for data in the flushing memtable list
    unSealedTsFileMemReaderByTs = new MemChunkReaderByTimestamp(
        unsealedTsFile.getReadOnlyMemChunk());

    // create FileSeriesReaderByTimestamp for data which has been flushed to disk
    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(unsealedTsFile, false);
    IChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);
    unSealedTsFileDiskReaderByTs = new FileSeriesReaderByTimestamp(chunkLoader,
        unsealedTsFile.getChunkMetaDataList());

    unSealedTsFileDiskReaderEnded = false;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    if (!unSealedTsFileDiskReaderEnded) {
      Object value = unSealedTsFileDiskReaderByTs.getValueInTimestamp(timestamp);
      if (value != null || unSealedTsFileDiskReaderByTs.hasNext()) {
        return value;
      } else {
        unSealedTsFileDiskReaderEnded = true;
      }
    }
    return unSealedTsFileMemReaderByTs.getValueInTimestamp(timestamp);
  }

  @Override
  public boolean hasNext() throws IOException {
    if (unSealedTsFileDiskReaderEnded) {
      return unSealedTsFileMemReaderByTs.hasNext();
    }
    return (unSealedTsFileDiskReaderByTs.hasNext() || unSealedTsFileMemReaderByTs.hasNext());
  }

}
