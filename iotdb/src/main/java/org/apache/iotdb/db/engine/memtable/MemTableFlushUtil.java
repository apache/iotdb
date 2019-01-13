/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.io.IOException;
import java.util.List;

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class MemTableFlushUtil {
    private static final Logger logger = LoggerFactory.getLogger(MemTableFlushUtil.class);
    private static final int pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().pageSizeInByte;

    private static int writeOneSeries(List<TimeValuePair> tvPairs, IChunkWriter seriesWriterImpl, TSDataType dataType)
            throws IOException {
        int count = 0;
        switch (dataType) {
        case BOOLEAN:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
            }
            break;
        case INT32:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
            }
            break;
        case INT64:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
            }
            break;
        case FLOAT:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
            }
            break;
        case DOUBLE:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
            }
            break;
        case TEXT:
            for (TimeValuePair timeValuePair : tvPairs) {
                count++;
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
            }
            break;
        default:
            logger.error("don't support data type: {}", dataType);
            break;
        }
        return count;
    }

    public static void flushMemTable(FileSchema fileSchema, TsFileIOWriter tsFileIOWriter, IMemTable iMemTable)
            throws IOException {
        for (String deviceId : iMemTable.getMemTableMap().keySet()) {
            long startPos = tsFileIOWriter.getPos();
            long recordCount = 0;
            tsFileIOWriter.startFlushChunkGroup(deviceId);
            int seriesNumber = iMemTable.getMemTableMap().get(deviceId).size();
            for (String measurementId : iMemTable.getMemTableMap().get(deviceId).keySet()) {
                // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
                IWritableMemChunk series = iMemTable.getMemTableMap().get(deviceId).get(measurementId);
                MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
                PageWriter pageWriter = new PageWriter(desc);
                ChunkBuffer chunkBuffer = new ChunkBuffer(desc);
                IChunkWriter seriesWriter = new ChunkWriterImpl(desc, chunkBuffer, pageSizeThreshold);
                recordCount += writeOneSeries(series.getSortedTimeValuePairList(), seriesWriter, desc.getType());
                seriesWriter.writeToFileWriter(tsFileIOWriter);
            }
            long memSize = tsFileIOWriter.getPos() - startPos;
            ChunkGroupFooter footer = new ChunkGroupFooter(deviceId, memSize, seriesNumber);
            tsFileIOWriter.endChunkGroup(footer);
        }
    }
}
