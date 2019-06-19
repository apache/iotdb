/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.bufferwriteV2;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.AbstractUnsealedDataFileProcessorV2;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.memtable.Callback;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class BufferWriteProcessorV2 extends AbstractUnsealedDataFileProcessorV2 {

  public BufferWriteProcessorV2(String storageGroupName, File file, FileSchema fileSchema, VersionController versionController, Callback closeBufferWriteProcessor) throws IOException {
    super(storageGroupName, file, fileSchema, versionController, closeBufferWriteProcessor);
  }

  /**
   * get the chunk(s) in the memtable ( one from work memtable and the other ones in flushing status and then
   * compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data and chunk metadata in memory
   */
  @Override
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> queryUnsealedFile(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props) {
    flushQueryLock.readLock().lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      if (flushingMemTables != null) {
        for (IMemTable flushingMemTable : flushingMemTables) {
          memSeriesLazyMerger.addMemSeries(flushingMemTable.query(deviceId, measurementId, dataType, props));
        }
      }
      if (workMemTable != null) {
        memSeriesLazyMerger.addMemSeries(workMemTable.query(deviceId, measurementId, dataType, props));
      }
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger,
          Collections.emptyMap());
      return new Pair<>(timeValuePairSorter,
          writer.getMetadatas(deviceId, measurementId, dataType));
    } finally {
      flushQueryLock.readLock().unlock();
    }
  }
}
