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

package org.apache.iotdb.db.engine.merge.selector;


import static org.apache.iotdb.db.utils.MergeUtils.collectFileSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget.
 */
public class MergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  private long totalCost;
  private long memoryBudget;
  private long maxSeqFileCost;

  /**
   * Total metadata size of each file.
   */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /**
   * Maximum memory cost of querying a timeseries in each file.
   */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  private List<TsFileResource> selectedUnseqFiles = new ArrayList<>();
  private List<TsFileResource> selectedSeqFiles = new ArrayList<>();

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();

  private List<Integer> tmpSelectedSeqFiles;
  private long tempMaxSeqFileCost;

  private boolean[] seqSelected;

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
   *    The rough estimation: for a seqFile, the size of its metadata is used for estimation.
   *    Since in the worst case, the file only contains one timeseries and all its metadata will
   *    be loaded into memory with at most one actual data page (which is negligible) and writing
   *    the timeseries into a new file generate metadata of the similar size, so the size of all
   *    seqFiles' metadata (generated when writing new chunks) pluses the largest one (loaded
   *    when reading a timeseries from the seqFiles) is the total estimation of all seqFiles; for
   *    an unseqFile, since the merge reader may read all chunks of a series to perform a merge
   *    read, the whole file may be loaded into memory, so we use the file's length as the
   *    maximum estimation.
   *    The tight estimation: based on the rough estimation, we scan the file's metadata to
   *    count the number of chunks for each series, find the series which have the most
   *    chunks in the file and use its chunk proportion to refine the rough estimation.
   * The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is
   * selected unseqFiles or an empty array if there are no proper candidates by the budget.
   * @throws MergeException
   */
  public List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqFiles.size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      clear();
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

  private void clear() throws IOException {
    for (TsFileSequenceReader reader : fileReaderCache.values()) {
      reader.close();
    }
    fileReaderCache = null;
    tmpSelectedSeqFiles = null;
    fileMetaSizeMap = null;
    maxSeriesQueryCostMap = null;
  }

  private void select(boolean useTightBound) throws IOException {
    tmpSelectedSeqFiles = new ArrayList<>();
    seqSelected = new boolean[seqFiles.size()];

    totalCost = 0;

    int unseqIndex = 0;
    while (unseqIndex < unseqFiles.size()) {
      // select next unseq files
      TsFileResource unseqFile = unseqFiles.get(unseqIndex);

      selectOverlappedSeqFiles(unseqFile);

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost = useTightBound ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles) :
          calculateLooseMemoryCost(unseqFile, tmpSelectedSeqFiles);

      if (totalCost + newCost < memoryBudget) {
        selectedUnseqFiles.add(unseqFile);
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
      if (MergeUtils.fileOverlap(seqFile, unseqFile)) {
        tmpSelectedSeqFiles.add(i);
      }
    }
  }

  private long calculateMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles, FileQueryMemMeasurement unseqMeasurement,
      FileQueryMemMeasurement seqMeasurement) throws IOException {
    long cost = 0;
    Long fileCost = unseqMeasurement.measure(tmpSelectedUnseqFile);
    cost += fileCost;

    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      TsFileResource seqFile = seqFiles.get(seqFileIdx);
      fileCost = seqMeasurement.measure(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = calculateMetadataSize(seqFile);
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += fileCost;
    }
    return cost;
  }

  private long calculateLooseMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles) throws IOException {
    return calculateMemoryCost(tmpSelectedUnseqFile, tmpSelectedSeqFiles,
        TsFileResource::getFileSize, this::calculateMetadataSize);
  }

  private long calculateTightMemoryCost(TsFileResource tmpSelectedUnseqFile,
      List<Integer> tmpSelectedSeqFiles) throws IOException {
    return calculateMemoryCost(tmpSelectedUnseqFile, tmpSelectedSeqFiles,
        this::calculateTightUnseqMemoryCost, this::calculateTightSeqMemoryCost);
  }

  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      long minPos = Long.MAX_VALUE;
      TsFileSequenceReader sequenceReader = getReader(seqFile);
      TsFileMetaData fileMetaData = sequenceReader.readFileMetadata();
      Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
      for (TsDeviceMetadataIndex metadataIndex : deviceMap.values()) {
        minPos = metadataIndex.getOffset() < minPos ? metadataIndex.getOffset() : minPos;
      }
      cost = seqFile.getFileSize() - minPos;
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  private long calculateTightFileMemoryCost(TsFileResource seqFile, FileQueryMemMeasurement measurement)
      throws IOException {
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      long[] chunkNums = findLargestSeriesChunkNum(seqFile);
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
      maxSeriesQueryCostMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion to all series to get a maximum estimation
  private long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
    return calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile)
      throws IOException {
    return calculateTightFileMemoryCost(unseqFile, TsFileResource::getFileSize);
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  private long[] findLargestSeriesChunkNum(TsFileResource tsFileResource)
      throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    TsFileSequenceReader sequenceReader = getReader(tsFileResource);
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetaData> chunkMetaDataList = sequenceReader.getChunkMetadata(path);
      totalChunkNum += chunkMetaDataList.size();
      maxChunkNum = chunkMetaDataList.size() > maxChunkNum ? chunkMetaDataList.size() : maxChunkNum;
    }
    logger.debug("In file {}, total chunk num {}, series max chunk num {}", tsFileResource,
        totalChunkNum, maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }


  private TsFileSequenceReader getReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getFile().getPath(), true, true);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }
}
