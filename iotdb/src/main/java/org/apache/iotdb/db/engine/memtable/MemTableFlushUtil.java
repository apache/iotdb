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
package org.apache.iotdb.db.engine.memtable;

import java.io.IOException;
import java.util.List;

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushUtil.class);
  private static final int PAGE_SIZE_THRESHOLD = TSFileConfig.pageSizeInByte;

  private MemTableFlushUtil() {

  }

  private static void writeOneSeries(List<TimeValuePair> tvPairs, IChunkWriter seriesWriterImpl,
      TSDataType dataType)
      throws IOException {
    for (TimeValuePair timeValuePair : tvPairs) {
      switch (dataType) {
        case BOOLEAN:
            seriesWriterImpl
                    .write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
          break;
        case INT32:
            seriesWriterImpl.write(timeValuePair.getTimestamp(),
                    timeValuePair.getValue().getInt());
          break;
        case INT64:
            seriesWriterImpl.write(timeValuePair.getTimestamp(),
                    timeValuePair.getValue().getLong());
          break;
        case FLOAT:
            seriesWriterImpl.write(timeValuePair.getTimestamp(),
                    timeValuePair.getValue().getFloat());
          break;
        case DOUBLE:
            seriesWriterImpl
                    .write(timeValuePair.getTimestamp(),
                            timeValuePair.getValue().getDouble());
          break;
        case TEXT:
            seriesWriterImpl
                    .write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
          break;
        default:
          LOGGER.error("don't support data type: {}", dataType);
          break;
      }
    }
  }

  /**
   * the function for flushing memtable.
   */
  public static void flushMemTable(FileSchema fileSchema, TsFileIOWriter tsFileIoWriter,
      IMemTable imemTable, long version) throws IOException {
    long tmpTime;
    long sortTime = 0;
    long memSerializeTime = 0;
    long ioTime = 0;
    for (String deviceId : imemTable.getMemTableMap().keySet()) {
      long startPos = tsFileIoWriter.getPos();
      tsFileIoWriter.startFlushChunkGroup(deviceId);
      int seriesNumber = imemTable.getMemTableMap().get(deviceId).size();
      for (String measurementId : imemTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
        IWritableMemChunk series = imemTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
        tmpTime = System.currentTimeMillis();
        sortTime += tmpTime - startTime;
        ChunkBuffer chunkBuffer = new ChunkBuffer(desc);
        IChunkWriter seriesWriter = new ChunkWriterImpl(desc, chunkBuffer, PAGE_SIZE_THRESHOLD);
        writeOneSeries(sortedTimeValuePairs, seriesWriter, desc.getType());
        startTime = System.currentTimeMillis();
        memSerializeTime += startTime - tmpTime;
        seriesWriter.writeToFileWriter(tsFileIoWriter);
        ioTime += System.currentTimeMillis() - startTime;
      }
      tmpTime = System.currentTimeMillis();
      tsFileIoWriter.endChunkGroup(version);
      ioTime += System.currentTimeMillis() - tmpTime;
    }
    LOGGER.info(
        "flushing a memtable into disk: data sort time cost {} ms, serialize data into mem cost {} ms, io cost {} ms.",
        sortTime , memSerializeTime , ioTime );
  }
}
