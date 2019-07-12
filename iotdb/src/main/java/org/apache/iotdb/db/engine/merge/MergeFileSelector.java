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


import static org.apache.iotdb.db.engine.merge.MergeUtils.collectFileSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given budget.
 */
public class MergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  private long totalCost;
  private long memoryBudget;
  private long maxSeqFileCost;
  private long tempMaxSeqFileCost;

  private Map<TsFileResource, Long> costMap = new HashMap<>();
  private Map<TsFileResource, Long> tightCostMap = new HashMap<>();

  private List<TsFileResource> selectedUnseqFiles = new ArrayList<>();
  private List<TsFileResource> selectedSeqFiles = new ArrayList<>();

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();

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

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget.
   * This process iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles
   * as newly-added candidates and computes their estimated memory cost. If the current cost
   * pluses the new cost is still under the budget, accept the unseqFile and the seqFiles as
   * candidates, otherwise go to the next iteration.
   * The memory cost of a file is calculated in two ways:
   *    The rough estimation: for a seqFile, the size of its metadata is the estimation. Since in
   *    the worst case, the file only contains one timeseries and all its metadata will be loaded
   *    into memory with at most one actual data chunk (which is negligible) and writing the
   *    timeseries into a new file generate metadata of the similar size, so the size of all
   *    seqFiles' metadata (generated when writing new chunks) pluses the largest one (loaded
   *    when reading a timeseries from the seqFiles) is the total estimation of all seqFiles; for
   *    an unseqFile, since the merge reader may read all its chunks to perform a merge read, the
   *    whole file may be loaded into memory and for the same reason of writing series, so we use
   *    the file's length pluses its metadata size as the maximum estimation.
   *    The tight estimation: based on the rough estimation, we scan the file's meta data to
   *    count the number of chunks for each series, and the series which have the most chunks in
   *    the file and its chunk proportion to shrink the rough estimation.
   * The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is
   * selected unseqFiles or an empty array if there is no proper candidates by the budget.
   * @throws MergeException
   */
  public List[] doSelect() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqFiles.size());
      doSelect(false);
      if (selectedUnseqFiles.isEmpty()) {
        doSelect(true);
      }
      clean();
      if (selectedUnseqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return new List[0];
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(), selectedUnseqFiles.size(), totalCost,
          System.currentTimeMillis() - startTime);
    }
    return new List[]{selectedSeqFiles, selectedUnseqFiles};
  }

  private void clean() throws IOException {
    for (TsFileSequenceReader reader : fileReaderCache.values()) {
      reader.close();
    }
    fileReaderCache.clear();
  }

  private void doSelect(boolean useTightBound) throws IOException {

    tmpSelectedSeqFiles = new ArrayList<>();
    seqSelected = new boolean[seqFiles.size()];
    unseqSelected = new boolean[unseqFiles.size()];

    totalCost = 0;

    int unseqIndex = 0;
    while (unseqIndex < unseqFiles.size()) {
      // select next unseq files
      if (unseqSelected[unseqIndex]) {
        unseqIndex++;
        continue;
      }
      TsFileResource unseqFile = unseqFiles.get(unseqIndex);

      selectOverlappedSeqFiles(unseqFile);

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost = useTightBound ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles) :
          calculateMemoryCost(unseqFile, tmpSelectedSeqFiles);

      if (totalCost + newCost < memoryBudget) {
        selectedUnseqFiles.add(unseqFile);
        unseqSelected[unseqIndex] = true;
        maxSeqFileCost = tempMaxSeqFileCost;

        for (Integer seqIdx : tmpSelectedSeqFiles) {
          seqSelected[seqIdx] = true;
          selectedSeqFiles.add(seqFiles.get(seqIdx));
        }
        totalCost += newCost;
        logger.debug("Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
                + " cost {}",
            unseqFile, tmpSelectedSeqFiles, newCost, totalCost);
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
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata
      cost += fileCost;
    }
    return cost;
  }

  private long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles) throws IOException {
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
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata
      cost += fileCost;
    }
    return cost;
  }

  // this method uses the total size of a seqFile's metadata as the maximum memory it may occupy
  // (when the file contains only one series)
  private long calculateSeqMemoryCost(TsFileResource seqFile) throws IOException {
    long cost = calculateMetadataSize(seqFile);
    logger.debug(LOG_FILE_COST, seqFile, cost);
    return cost;
  }

  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    long minPos = Long.MAX_VALUE;
    TsFileSequenceReader sequenceReader = getReader(seqFile);
    TsFileMetaData fileMetaData = sequenceReader.readFileMetadata();
    Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
    for (TsDeviceMetadataIndex metadataIndex : deviceMap.values()) {
      minPos = metadataIndex.getOffset() < minPos ? metadataIndex.getOffset() : minPos;
    }
    return seqFile.getFileSize() - minPos;
  }

  // the worst case is when the file contains only one series and all chunks and chunkMetadata
  // will be read into memory to perform a merge, so almost the whole file will be loaded into
  // memory and writing those chunks to a new file creating new metadata, so the metadata is doubled
  // in the worst case
  private long calculateUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    long cost = unseqFile.getFileSize() + calculateMetadataSize(unseqFile);
    logger.debug(LOG_FILE_COST, unseqFile, cost);
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightSeqMemoryCost(TsFileResource seqFile)
      throws IOException {
    long[] chunkNums = findLargestSeriesChunkNum(seqFile);
    long totalChunkNum = chunkNums[0];
    long maxChunkNum = chunkNums[1];
    long cost = calculateMetadataSize(seqFile) * maxChunkNum / totalChunkNum;
    logger.debug(LOG_FILE_COST, seqFile, cost);
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile)
      throws IOException {
    long[] chunkNums = findLargestSeriesChunkNum(unseqFile);
    long totalChunkNum = chunkNums[0];
    long maxChunkNum = chunkNums[1];
    long cost = calculateUnseqMemoryCost(unseqFile) * maxChunkNum / totalChunkNum;
    logger.debug(LOG_FILE_COST, unseqFile, cost);
    return cost;
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  private long[] findLargestSeriesChunkNum(TsFileResource tsFileResource)
      throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    TsFileSequenceReader sequenceReader = getReader(tsFileResource);
    List<Path> paths = collectFileSeries(sequenceReader);

    MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(sequenceReader);
    for (Path path : paths) {
      List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
      metadataQuerier.clear();
      totalChunkNum += chunkMetaDataList.size();
      maxChunkNum = chunkMetaDataList.size() > maxChunkNum ? chunkMetaDataList.size() :
          maxChunkNum;
    }
    logger.debug("In file {}, total chunk num {}, series max chunk num {}", tsFileResource,
        totalChunkNum, maxChunkNum);
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

  private TsFileSequenceReader getReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getFile().getPath());
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }
}
