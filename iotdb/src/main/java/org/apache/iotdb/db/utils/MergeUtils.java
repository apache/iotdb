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

package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

public class MergeUtils {
  private MergeUtils() {
    // util class
  }
  
  public static void writeTVPair(TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i));
        break;
      case DOUBLE:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i));
        break;
      case BOOLEAN:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i));
        break;
      case INT64:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i));
        break;
      case INT32:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i));
        break;
      case FLOAT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i));
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  public static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader) throws IOException {
    TsFileMetaData metaData = sequenceReader.readFileMetadata();
    Set<String> deviceIds = metaData.getDeviceMap().keySet();
    Set<String> measurements = metaData.getMeasurementSchema().keySet();
    List<Path> paths = new ArrayList<>();
    for (String deviceId : deviceIds) {
      for (String measurement : measurements) {
        paths.add(new Path(deviceId, measurement));
      }
    }
    return paths;
  }

  /**
   * Collect all paths contained in the all SeqFiles and UnseqFiles in a merge.
   * @param resource
   * @return all paths contained in the merge.
   * @throws IOException
   */
  public static List<Path> collectPaths(MergeResource resource)
      throws IOException {
    Set<Path> pathSet = new HashSet<>();
    for (TsFileResource tsFileResource : resource.getUnseqFiles()) {
      TsFileSequenceReader sequenceReader = resource.getFileReader(tsFileResource);
      resource.getMeasurementSchemaMap().putAll(sequenceReader.readFileMetadata().getMeasurementSchema());
      pathSet.addAll(collectFileSeries(sequenceReader));
    }
    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
      TsFileSequenceReader sequenceReader = resource.getFileReader(tsFileResource);
      resource.getMeasurementSchemaMap().putAll(sequenceReader.readFileMetadata().getMeasurementSchema());
      pathSet.addAll(collectFileSeries(sequenceReader));
    }
    List<Path> ret = new ArrayList<>(pathSet);
    ret.sort(Comparator.comparing(Path::getFullPath));
    return ret;
  }

  public static long collectFileSizes(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter) throws IOException {
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
    }
    return chunk.getHeader().getNumOfPages();
  }

  public static boolean fileOverlap(TsFileResource seqFile, TsFileResource unseqFile) {
    Map<String, Long> seqStartTimes = seqFile.getStartTimeMap();
    Map<String, Long> seqEndTimes = seqFile.getEndTimeMap();
    Map<String, Long> unseqStartTimes = unseqFile.getStartTimeMap();
    Map<String, Long> unseqEndTimes = unseqFile.getEndTimeMap();

    for (Entry<String, Long> seqEntry : seqStartTimes.entrySet()) {
      Long unseqStartTime = unseqStartTimes.get(seqEntry.getKey());
      if (unseqStartTime == null) {
        continue;
      }
      Long unseqEndTime = unseqEndTimes.get(seqEntry.getKey());
      Long seqStartTime = seqEntry.getValue();
      Long seqEndTime = seqEndTimes.get(seqEntry.getKey());

      if (intervalOverlap(seqStartTime, seqEndTime, unseqStartTime, unseqEndTime)) {
        return true;
      }
    }
    return false;
  }

  private static boolean intervalOverlap(long l1, long r1, long l2, long r2) {
   return  (l1 <= l2 && l2 <= r1) ||
        (l1 <= r2 && r2 <= r1) ||
        (l2 <= l1 && l1 <= r2) ||
        (l2 <= r1 && r1 <= r2);
  }
}
