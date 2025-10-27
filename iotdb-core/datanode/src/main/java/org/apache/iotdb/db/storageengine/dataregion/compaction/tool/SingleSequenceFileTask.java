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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.Callable;

public class SingleSequenceFileTask implements Callable<SequenceFileTaskSummary> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleSequenceFileTask.class);
  private final UnseqSpaceStatistics unseqSpaceStatistics;
  private final String seqFile;

  public SingleSequenceFileTask(UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {
    this.unseqSpaceStatistics = unseqSpaceStatistics;
    this.seqFile = seqFile;
  }

  @Override
  public SequenceFileTaskSummary call() throws Exception {
    return checkSeqFile(unseqSpaceStatistics, seqFile);
  }

  private SequenceFileTaskSummary checkSeqFile(
      UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {
    SequenceFileTaskSummary summary = new SequenceFileTaskSummary();
    File f = new File(seqFile);
    if (!f.exists()) {
      return summary;
    }
    summary.fileSize += f.length();
    try (TsFileStatisticReader reader = new TsFileStatisticReader(seqFile)) {
      // statistics sequence file information and updates to overlapStatistic
      List<TsFileStatisticReader.ChunkGroupStatistics> chunkGroupStatisticsList =
          reader.getChunkGroupStatisticsList();
      for (TsFileStatisticReader.ChunkGroupStatistics chunkGroupStatistics :
          chunkGroupStatisticsList) {
        summary.totalChunks += chunkGroupStatistics.getTotalChunkNum();
        IDeviceID deviceId = chunkGroupStatistics.getDeviceID();

        long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;

        for (ChunkMetadata chunkMetadata : chunkGroupStatistics.getChunkMetadataList()) {
          // skip empty chunk
          if (chunkMetadata.getStartTime() > chunkMetadata.getEndTime()) {
            continue;
          }
          // update device start time and end time
          deviceStartTime = Math.min(deviceStartTime, chunkMetadata.getStartTime());
          deviceEndTime = Math.max(deviceEndTime, chunkMetadata.getEndTime());

          summary.setMinStartTime(deviceStartTime);
          summary.setMaxEndTime(deviceEndTime);

          // check chunk overlap
          Interval interval =
              new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime());
          String measurementId = chunkMetadata.getMeasurementUid();
          if (unseqSpaceStatistics.chunkHasOverlap(deviceId, measurementId, interval)) {
            summary.overlapChunk++;
          }
        }
        // check device overlap
        if (deviceStartTime > deviceEndTime) {
          continue;
        }
        Interval deviceInterval = new Interval(deviceStartTime, deviceEndTime);
        if (!unseqSpaceStatistics.chunkGroupHasOverlap(deviceId, deviceInterval)) {
          continue;
        }
        summary.overlapChunkGroup++;
      }
      summary.totalChunkGroups = chunkGroupStatisticsList.size();
    } catch (IOException e) {
      if (e instanceof NoSuchFileException) {
        LOGGER.warn("{} doesn't exist.", seqFile);
        return new SequenceFileTaskSummary();
      }
      LOGGER.error("check {} failed.", seqFile, e);
      return new SequenceFileTaskSummary();
    }
    return summary;
  }
}
