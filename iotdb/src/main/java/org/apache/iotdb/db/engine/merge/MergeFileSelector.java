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

package org.apache.iotdb.db.engine.merge;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;

/**
 * MergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given budget.
 */
public class MergeFileSelector {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;
  private long memoryBudget;

  private Map<TsFileResource, Long> costMap = new HashMap<>();
  private Map<TsFileResource, Long> tightCostMap = new HashMap<>();

  private List<TsFileResource> selectedUnseqFiles = new ArrayList<>();
  private List<TsFileResource> selectedSeqFiles = new ArrayList<>();

  private List<Integer> tmpSelectedSeqFiles;

  private boolean[] seqSelected;
  private boolean[] unseqSelected;

  public MergeFileSelector(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, long memoryBudget) {
    this.seqFiles = seqFiles.stream().filter(TsFileResource::isClosed).collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(TsFileResource::isClosed).collect(Collectors.toList());
    this.memoryBudget = memoryBudget;
  }

  public List[] doSelect() throws MergeException {
    try {
      doSelect(false);
      if (selectedUnseqFiles.isEmpty()) {
        doSelect(true);
      }
      if (selectedUnseqFiles.isEmpty()) {
        return new List[0];
      }
    } catch (IOException | MetadataErrorException e) {
      throw new MergeException(e);
    }
    return new List[]{selectedSeqFiles, selectedUnseqFiles};
  }

  private void doSelect(boolean useTightBound) throws IOException, MetadataErrorException {

    tmpSelectedSeqFiles = new ArrayList<>();
    seqSelected = new boolean[seqFiles.size()];
    unseqSelected = new boolean[unseqFiles.size()];

    long totalCost = 0;

    int unseqIndex = 0;
    while (unseqIndex < unseqFiles.size()) {
      // select next unseq files
      if (unseqSelected[unseqIndex]) {
        unseqIndex++;
        continue;
      }
      TsFileResource unseqFile = unseqFiles.get(unseqIndex);

      selectOverlappedSeqFiles(unseqFile);

      long newCost = useTightBound ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles) :
          calculateMemoryCost(unseqFile, tmpSelectedSeqFiles);

      if (totalCost + newCost < memoryBudget) {
        selectedUnseqFiles.add(unseqFile);
        unseqSelected[unseqIndex] = true;

        for (Integer seqIdx : tmpSelectedSeqFiles) {
          seqSelected[seqIdx] = true;
          selectedSeqFiles.add(seqFiles.get(seqIdx));
        }
      }
      tmpSelectedSeqFiles.clear();
      unseqIndex++;
    }
  }

  private void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    for (int i = 0; i < seqFiles.size(); i++) {
      if (seqSelected[i]) {
        continue;
      }
      TsFileResource seqFile = seqFiles.get(i);
      if (fileOverlap(seqFile, unseqFile)) {
        tmpSelectedSeqFiles.add(i);
      }
    }
  }

  private long calculateMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles) throws IOException {
    long cost = 0;
    Long fileCost = costMap.get(tmpSelectedUnseqFile);
    if (fileCost == null) {
      fileCost = calculateUnseqMemoryCost(tmpSelectedUnseqFile);
      costMap.put(tmpSelectedUnseqFile, fileCost);
    }
    cost += fileCost;
    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      TsFileResource seqFile = seqFiles.get(seqFileIdx);
      fileCost = costMap.get(seqFile);
      if (fileCost == null) {
        fileCost = calculateSeqMemoryCost(seqFile);
        costMap.put(seqFile, fileCost);
      }
      cost += fileCost;
    }
    return cost;
  }

  private long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles) throws IOException, MetadataErrorException {
    long cost = 0;
    Long fileCost = tightCostMap.get(tmpSelectedUnseqFile);
    if (fileCost == null) {
      fileCost = calculateTightUnseqMemoryCost(tmpSelectedUnseqFile);
      tightCostMap.put(tmpSelectedUnseqFile, fileCost);
    }
    cost += fileCost;
    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      TsFileResource seqFile = seqFiles.get(seqFileIdx);
      fileCost = tightCostMap.get(seqFile);
      if (fileCost == null) {
        fileCost = calculateTightSeqMemoryCost(seqFile);
        tightCostMap.put(seqFile, fileCost);
      }
      cost += fileCost;
    }
    return cost;
  }

  // this method uses the total size of a seqFile's metadata as the maximum memory it may occupy
  // (when the file contains only one series)
  private long calculateSeqMemoryCost(TsFileResource seqFile) throws IOException {
    long minPos = Long.MAX_VALUE;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(seqFile.getFile().getPath())) {
      TsFileMetaData fileMetaData = sequenceReader.readFileMetadata();
      Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
      for (TsDeviceMetadataIndex metadataIndex : deviceMap.values()) {
        minPos = metadataIndex.getOffset() < minPos ? metadataIndex.getOffset() : minPos;
      }
    }
    return seqFile.getFileSize() - minPos;
  }

  // the worst case is when the file contains only one series and all chunks and chunkMetadata
  // will be read into memory to perform a merge, so almost the whole file will be loaded into
  // memory
  private long calculateUnseqMemoryCost(TsFileResource unseqFile) {
    return unseqFile.getFileSize();
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightSeqMemoryCost(TsFileResource seqFile)
      throws IOException, MetadataErrorException {
    long[] chunkNums = findLargestSeriesChunkNum(seqFile);
    long totalChunkNum = chunkNums[0];
    long maxChunkNum = chunkNums[1];
    return calculateSeqMemoryCost(seqFile) * maxChunkNum / totalChunkNum;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile)
      throws IOException, MetadataErrorException {
    long[] chunkNums = findLargestSeriesChunkNum(unseqFile);
    long totalChunkNum = chunkNums[0];
    long maxChunkNum = chunkNums[1];
    return calculateUnseqMemoryCost(unseqFile) * maxChunkNum / totalChunkNum;
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  private long[] findLargestSeriesChunkNum(TsFileResource tsFileResource)
      throws IOException, MetadataErrorException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(tsFileResource.getFile().getPath())) {
      List<Path> paths = new ArrayList<>();
      for (String deviceId : tsFileResource.getEndTimeMap().keySet()) {
        List<String> strPaths = MManager.getInstance().getPaths(deviceId + ".*");
        for (String strPath : strPaths) {
          paths.add(new Path(strPath));
        }
      }

      MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(sequenceReader);
      for (Path path : paths) {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
        metadataQuerier.clear();
        totalChunkNum += chunkMetaDataList.size();
        maxChunkNum = chunkMetaDataList.size() > maxChunkNum ? chunkMetaDataList.size() :
            maxChunkNum;
      }
    }
    return new long[] {totalChunkNum, maxChunkNum};
  }


  private boolean fileOverlap(TsFileResource seqFile, TsFileResource unseqFile) {
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
  
  private boolean intervalOverlap(long l1, long r1, long l2, long r2) {
   return  (l1 <= l2 && l2 <= r1) ||
        (l1 <= r2 && r2 <= r1) ||
        (l2 <= l1 && l1 <= r2) ||
        (l2 <= r1 && r1 <= r2);
  }
  
}
