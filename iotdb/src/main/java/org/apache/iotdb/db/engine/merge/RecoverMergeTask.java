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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverMergeTask extends MergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverMergeTask.class);

  private String currLine;
  private List<TsFileResource> mergeSeqFiles = new ArrayList<>();
  private List<TsFileResource> mergeUnseqFiles = new ArrayList<>();

  private Map<File, Long> fileLastPositions = new HashMap<>();
  private Map<File, Long> tempFileLastPositions = new HashMap<>();

  private List<Path> unmergedPaths;
  private List<Path> mergedPaths = new ArrayList<>();
  private List<TsFileResource> unmergedFiles;

  public RecoverMergeTask(
      List<TsFileResource> allSeqFiles,
      List<TsFileResource> allUnseqFiles,
      String storageGroupDir, MergeCallback callback, String taskName) throws IOException {
    super(allSeqFiles, allUnseqFiles, storageGroupDir, callback, taskName);
  }

  public void recoverMerge(boolean continueMerge) throws IOException, MetadataErrorException {
    File logFile = new File(storageGroupDir, MergeLogger.MERGE_LOG_NAME);
    if (!logFile.exists()) {
      logger.info("{} no merge.log, merge recovery ends", taskName);
      return;
    }
    mergeSeqFiles = new ArrayList<>();
    mergeUnseqFiles = new ArrayList<>();
    long startTime = System.currentTimeMillis();

    Status status = determineStatus(logFile);
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery status determined: {} after {}ms", taskName, status,
          (System.currentTimeMillis() - startTime));
    }
    switch (status) {
      case NONE:
        logFile.delete();
        break;
      case FILES_LOGGED:
        if (continueMerge) {
          mergeLogger = new MergeLogger(storageGroupDir);
          truncateFiles();
          recoverChunkCounts();
          mergeSeries(unmergedPaths);

          mergeFiles(seqFiles);
        }
        cleanUp(continueMerge);
        break;
      case ALL_TS_MERGED:
        if (continueMerge) {
          mergeLogger = new MergeLogger(storageGroupDir);
          truncateFiles();
          recoverChunkCounts();
          mergeFiles(unmergedFiles);
        } else {
          // NOTICE: although some of the seqFiles may have been truncated, later TsFile recovery
          // will recover them, so they are not a concern here
          truncateFiles();
        }
        cleanUp(continueMerge);
        break;
      case MERGE_END:
        cleanUp(continueMerge);
        break;
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery ends after", taskName, (System.currentTimeMillis() - startTime));
    }
  }


  // scan metadata to compute how many chunks are merged/unmerged so at last we can decide to
  // move the merged chunks or the unmerged chunks
  private void recoverChunkCounts() throws IOException {
    logger.debug("{} recovering chunk counts", taskName);
    for (TsFileResource tsFileResource : seqFiles) {
      RestorableTsFileIOWriter mergeFileWriter = getMergeFileWriter(tsFileResource);
      mergeFileWriter.makeMetadataVisible();
      unmergedChunkStartTimes.put(tsFileResource, new HashMap<>());
      for(Path path : mergedPaths) {
        recoverChunkCounts(path, tsFileResource, mergeFileWriter);
      }
    }
  }

  private void recoverChunkCounts(Path path, TsFileResource tsFileResource,
      RestorableTsFileIOWriter mergeFileWriter) throws IOException {
    unmergedChunkStartTimes.get(tsFileResource).put(path, new ArrayList<>());

    List<ChunkMetaData> seqFileChunks = queryChunkMetadata(path, tsFileResource);
    List<ChunkMetaData> mergeFileChunks =
        mergeFileWriter.getVisibleMetadataList(path.getDevice(), path.getMeasurement(), null);
    mergedChunkCnt.compute(tsFileResource, (k, v) -> v == null ? mergeFileChunks.size() :
        v + mergeFileChunks.size());
    int seqIndex = 0;
    int mergeIndex = 0;
    int unmergedCnt = 0;
    while (seqIndex < seqFileChunks.size() && mergeIndex < mergeFileChunks.size()) {
      ChunkMetaData seqChunk = seqFileChunks.get(seqIndex);
      ChunkMetaData mergedChunk = mergeFileChunks.get(mergeIndex);
      if (seqChunk.getStartTime() < mergedChunk.getStartTime()) {
        // this seqChunk is unmerged
        unmergedCnt ++;
        seqIndex ++;
        unmergedChunkStartTimes.get(tsFileResource).get(path).add(seqChunk.getStartTime());
      } else if (mergedChunk.getStartTime() <= seqChunk.getStartTime() &&
          seqChunk.getStartTime() <= mergedChunk.getEndTime()) {
        // this seqChunk is merged
        seqIndex ++;
      } else {
        // seqChunk.startTime > mergeChunk.endTime, find next mergedChunk that may cover the
        // seqChunk
        mergeIndex ++;
      }
    }
    int finalUnmergedCnt = unmergedCnt;
    unmergedChunkCnt.compute(tsFileResource, (k, v) -> v == null ? finalUnmergedCnt :
        v + finalUnmergedCnt);
  }

  private void truncateFiles() throws IOException {
    logger.debug("{} truncating {} files", taskName, fileLastPositions.size());
    for (Entry<File, Long> entry : fileLastPositions.entrySet()) {
      File file = entry.getKey();
      Long lastPosition = entry.getValue();
      if (file.exists() && file.length() != lastPosition) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
          FileChannel channel = fileInputStream.getChannel();
          channel.truncate(lastPosition);
          channel.close();
        }
      }
    }
  }

  private Status determineStatus(File logFile) throws IOException, MetadataErrorException {
    Status status = Status.NONE;
    try (BufferedReader bufferedReader =
        new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        if (currLine.equals(MergeLogger.STR_SEQ_FILES)) {
          analyzeSeqFiles(bufferedReader);
        }
        if (currLine.equals(MergeLogger.STR_UNSEQ_FILES)) {
          analyzeUnseqFiles(bufferedReader);
        }
        if (currLine.equals(MergeLogger.STR_MERGE_START)) {
          status = Status.FILES_LOGGED;
          seqFiles = mergeSeqFiles;
          unseqFiles = mergeUnseqFiles;
          for (TsFileResource seqFile : seqFiles) {
            File mergeFile = new File(seqFile.getFile().getPath() + MergeTask.MERGE_SUFFIX);
            fileLastPositions.put(mergeFile, 0L);
          }
          unmergedPaths = collectPathsInUnseqFiles();
          analyzeMergedSeries(bufferedReader, unmergedPaths);
        }
        if (currLine.equals(MergeLogger.STR_ALL_TS_END)) {
          status = Status.ALL_TS_MERGED;
          unmergedFiles = seqFiles;
          analyzeMergedFiles(bufferedReader);
        }
        if (currLine.equals(MergeLogger.STR_MERGE_END)) {
          status = Status.MERGE_END;
        }
      }
    }
    return status;
  }

  private void analyzeSeqFiles(BufferedReader bufferedReader) throws IOException {
    long startTime = System.currentTimeMillis();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(MergeLogger.STR_UNSEQ_FILES)) {
        break;
      }
      Iterator<TsFileResource> iterator = seqFiles.iterator();
      while (iterator.hasNext()) {
        TsFileResource seqFile = iterator.next();
        if (seqFile.getFile().getAbsolutePath().equals(currLine)) {
          mergeSeqFiles.add(seqFile);
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} seq files after {}ms", taskName, mergeSeqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
  }

  private void analyzeUnseqFiles(BufferedReader bufferedReader) throws IOException {
    long startTime = System.currentTimeMillis();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(MergeLogger.STR_MERGE_START)) {
        break;
      }
      Iterator<TsFileResource> iterator = unseqFiles.iterator();
      while (iterator.hasNext()) {
        TsFileResource unseqFile = iterator.next();
        if (unseqFile.getFile().getAbsolutePath().equals(currLine)) {
          mergeUnseqFiles.add(unseqFile);
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} seq files after {}ms", taskName, mergeUnseqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
  }

  private void analyzeMergedSeries(BufferedReader bufferedReader, List<Path> unmergedPaths) throws IOException {
    Path currTS = null;
    long startTime = System.currentTimeMillis();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(MergeLogger.STR_ALL_TS_END)) {
        break;
      }
      if (currLine.contains(MergeLogger.STR_START)) {
        // a TS starts to merge
        String[] splits = currLine.split(" ");
        currTS = new Path(splits[0]);
        tempFileLastPositions.clear();
      } else if (!currLine.contains(MergeLogger.STR_END)) {
        // file position
        String[] splits = currLine.split(" ");
        File file = new File(splits[0]);
        Long position = Long.parseLong(splits[1]);
        tempFileLastPositions.put(file, position);
      } else {
        // a TS ends merging
        unmergedPaths.remove(currTS);
        for (Entry<File, Long> entry : tempFileLastPositions.entrySet()) {
          fileLastPositions.put(entry.getKey(), entry.getValue());
        }
        mergedPaths.add(currTS);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} series have already been merged after {}ms", taskName,
          mergeSeqFiles.size(), (System.currentTimeMillis() - startTime));
    }
  }

  private void analyzeMergedFiles(BufferedReader bufferedReader) throws IOException {
    File currFile = null;
    long startTime = System.currentTimeMillis();
    int mergedCnt = 0;
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(MergeLogger.STR_MERGE_END)) {
         break;
      }
      if (currLine.contains(MergeLogger.STR_START)) {
        String[] splits = currLine.split(" ");
        currFile = new File(splits[0]);
        Long lastPost = Long.parseLong(splits[2]);
        fileLastPositions.put(currFile, lastPost);
      } else if (currLine.contains(MergeLogger.STR_END)) {
        fileLastPositions.remove(currFile);
        String seqFilePath = currFile.getAbsolutePath().replace(MergeTask.MERGE_SUFFIX, "");
        Iterator<TsFileResource> unmergedFileIter = unmergedFiles.iterator();
        while (unmergedFileIter.hasNext()) {
          TsFileResource seqFile = unmergedFileIter.next();
          if (seqFile.getFile().getAbsolutePath().equals(seqFilePath)) {
            mergedCnt ++;
            unmergedFileIter.remove();
            break;
          }
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} files have already been merged after {}ms", taskName,
         mergedCnt, (System.currentTimeMillis() - startTime));
    }
  }

  enum Status {
    NONE, FILES_LOGGED, ALL_TS_MERGED, MERGE_END
  }
}
