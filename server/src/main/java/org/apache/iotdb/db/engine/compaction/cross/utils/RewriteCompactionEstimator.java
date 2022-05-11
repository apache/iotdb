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
package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RewriteCompactionEstimator implements ICompactionEstimator {

  private final CrossSpaceCompactionResource resource;

  private long maxCostOfReadingSeqFile;

  private Pair<Integer, Integer> maxUnseqChunkNumInDevice;

  private final List<Pair<Integer, Integer>> maxSeqChunkNumInDeviceList;

  // the number of timeseries being compacted at the same time
  private final int subCompactionTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public RewriteCompactionEstimator(CrossSpaceCompactionResource resource) {
    this.resource = resource;
    this.maxCostOfReadingSeqFile = 0;
    this.maxSeqChunkNumInDeviceList = new ArrayList<>();
  }

  @Override
  public long estimateMemory(int unseqIndex, List<Integer> seqIndexes) throws IOException {
    long cost = 0;
    cost += calculateReadingUnseqFile(unseqIndex);
    cost += calculateReadingSeqFiles(seqIndexes);
    cost += calculatingWritingTargetFiles(unseqIndex, seqIndexes);
    maxSeqChunkNumInDeviceList.clear();
    return cost;
  }

  /**
   * Calculate memory cost of reading source unseq files in the cross space compaction. Double the
   * total size of the timeseries to be compacted at the same time in all unseq files.
   */
  private long calculateReadingUnseqFile(int unseqIndex) throws IOException {
    TsFileResource unseqResource = resource.getUnseqFiles().get(unseqIndex);
    TsFileSequenceReader reader = resource.getFileReader(unseqResource);
    int[] fileInfo = getSeriesAndDeviceChunkNum(reader);
    // it is max aligned series num of one device when tsfile contains aligned series,
    // else is sub compaction task num.
    int concurrentSeriesNum = fileInfo[2] == -1 ? subCompactionTaskNum : fileInfo[2];
    maxUnseqChunkNumInDevice = new Pair<>(fileInfo[3], fileInfo[0]);
    // not only reading chunk into chunk cache, but also need to deserialize data point into merge
    // reader, so we have to double the cost here.
    return 2 * concurrentSeriesNum * (unseqResource.getTsFileSize() * fileInfo[1] / fileInfo[0]);
  }

  /**
   * Calculate memory cost of reading source seq files in the cross space compaction. Double the
   * maximun size of the timeseries to be compacted at the same time in one seq file, because only
   * one seq file will be queried at the same time.
   */
  private long calculateReadingSeqFiles(List<Integer> seqIndexes) throws IOException {
    long cost = 0;
    for (int seqIndex : seqIndexes) {
      TsFileResource seqResource = resource.getSeqFiles().get(seqIndex);
      TsFileSequenceReader reader = resource.getFileReader(seqResource);
      int[] fileInfo = getSeriesAndDeviceChunkNum(reader);
      // it is max aligned series num of one device when tsfile contains aligned series,
      // else is sub compaction task num.
      int concurrentSeriesNum = fileInfo[2] == -1 ? subCompactionTaskNum : fileInfo[2];
      long seqFileCost =
          concurrentSeriesNum * (seqResource.getTsFileSize() * fileInfo[1] / fileInfo[0]);
      if (seqFileCost > maxCostOfReadingSeqFile) {
        // Only one seq file will be read at the same time.
        // not only reading chunk into chunk cache, but also need to deserialize data point into
        // merge reader, so we have to double the cost here.
        cost -= 2 * maxCostOfReadingSeqFile;
        cost += 2 * seqFileCost;
        maxCostOfReadingSeqFile = seqFileCost;
      }
      maxSeqChunkNumInDeviceList.add(new Pair<>(fileInfo[3], fileInfo[0]));
    }
    return cost;
  }

  /**
   * Calculate memory cost of writing target files in the cross space compaction. Including metadata
   * size of all seq files, max chunk group size of each seq file and max chunk group size of
   * corresponding overlapped unseq file.
   */
  private long calculatingWritingTargetFiles(int unseqIndex, List<Integer> seqIndexes)
      throws IOException {
    long cost = 0;
    for (int seqIndex : seqIndexes) {
      TsFileResource seqResource = resource.getSeqFiles().get(seqIndex);
      TsFileSequenceReader reader = resource.getFileReader(seqResource);
      // add seq file metadata size
      cost += reader.getFileMetadataSize();
      // add max chunk group size of this seq tsfile
      cost +=
          seqResource.getTsFileSize()
              * maxSeqChunkNumInDeviceList.get(0).left
              / maxSeqChunkNumInDeviceList.get(0).right;
    }
    // add max chunk group size of overlapped unseq tsfile
    cost +=
        resource.getUnseqFiles().get(unseqIndex).getTsFileSize()
            * maxUnseqChunkNumInDevice.left
            / maxUnseqChunkNumInDevice.right;
    return cost;
  }

  /**
   * Get the details of the tsfile, the returned array contains the following elements in sequence:
   *
   * <p>total chunk num in this tsfile
   *
   * <p>max chunk num of one timeseries in this tsfile
   *
   * <p>max aligned series num in one device. If there is no aligned series in this file, then it
   * turns to be -1.
   *
   * <p>max chunk num of one device in this tsfile
   */
  private int[] getSeriesAndDeviceChunkNum(TsFileSequenceReader reader) throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    Map<String, List<TimeseriesMetadata>> deviceMetadata = reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : deviceMetadata.entrySet()) {
      int deviceChunkNum = 0;
      List<TimeseriesMetadata> deviceTimeseriesMetadata = entry.getValue();
      if (deviceTimeseriesMetadata.get(0).getMeasurementId().equals("")) {
        // aligned device
        maxAlignedSeriesNumInDevice =
            Math.max(maxAlignedSeriesNumInDevice, deviceTimeseriesMetadata.size());
      }
      for (TimeseriesMetadata timeseriesMetadata : deviceTimeseriesMetadata) {
        deviceChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        totalChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        maxChunkNum = Math.max(maxChunkNum, timeseriesMetadata.getChunkMetadataList().size());
      }
      maxDeviceChunkNum = Math.max(maxDeviceChunkNum, deviceChunkNum);
    }
    return new int[] {totalChunkNum, maxChunkNum, maxAlignedSeriesNumInDevice, maxDeviceChunkNum};
  }
}
