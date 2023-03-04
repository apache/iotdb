/*
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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataPrefetchReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new. If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.CompactionTaskComparator}. To maximize compaction
 * efficiency, selector searches compaction task from 0 compaction files(that is, file that never
 * been compacted, named level 0 file) to higher level files. If a compaction task is found in some
 * level, selector will not search higher level anymore.
 */
public class SizeTieredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTieredCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        sequence,
        taskFactory);
  }

  /**
   * This method searches for a batch of files to be compacted from layer 0 to the highest layer. If
   * there are more than a batch of files to be merged on a certain layer, it does not search to
   * higher layers. It creates a compaction thread for each batch of files and put it into the
   * candidateCompactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public void selectAndSubmit() {
    PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue =
        new PriorityQueue<>(new SizeTieredCompactionTaskComparator());
    try {
      int maxLevel = searchMaxFileLevel();
      for (int currentLevel = 0;
          currentLevel <= maxLevel && currentLevel <= config.getMaxCompactionLevel();
          currentLevel++) {
        if (this.sequence) {
          if (!selectLevelTaskSeq(currentLevel, taskPriorityQueue)) {
            break;
          }
        } else {
          if (!selectLevelTaskUnseq(currentLevel, taskPriorityQueue)) {
            break;
          }
        }
      }
      while (taskPriorityQueue.size() > 0) {
        createAndSubmitTask(taskPriorityQueue.poll().left);
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    }
  }

  /**
   * This method searches for all files on the given level. If there are consecutive files on the
   * level that meet the system preset conditions (the number exceeds 10 or the total file size
   * exceeds 2G), a compaction task is created for the batch of files and placed in the
   * taskPriorityQueue queue , and continue to search for the next batch. If at least one batch of
   * files to be compacted is found on this layer, it will return false (indicating that it will no
   * longer search for higher layers), otherwise it will return true.
   *
   * @param level the level to be searched
   * @param taskPriorityQueue it stores the batches of files to be compacted and the total size of
   *     each batch
   * @return return whether to continue the search to higher levels
   * @throws IOException
   */
  private boolean selectLevelTaskSeq(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        selectedFileList.clear();
        selectedFileSize = 0L;
        continue;
      }
      LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      selectedFileList.add(currentFile);
      selectedFileSize += currentFile.getTsFileSize();
      LOGGER.debug(
          "Add tsfile {}, current select file num is {}, size is {}",
          currentFile,
          selectedFileList.size(),
          selectedFileSize);
      // if the file size or file num reach threshold
      if (selectedFileSize >= targetCompactionFileSize
          || selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        shouldContinueToSearch = false;
      }
    }
    return shouldContinueToSearch;
  }

  private boolean selectLevelTaskUnseq(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    if (tsFileResources.size() <= 1) {
      return true;
    }
    if (config.getMaxInnerCompactionCandidateFileNum() == 1) {
      return false;
    }
    String compactionSelectFileMethod = config.getCompactionSelectFileMethod();

    if (compactionSelectFileMethod.equals("oldest")) {
      return selectLevelTaskUnseqOldest(level, taskPriorityQueue);
    } else if (compactionSelectFileMethod.equals("round")) {
      return selectLevelTaskUnseqRound(level, taskPriorityQueue);
    }

    /* greedy */
    long startTime = System.currentTimeMillis();
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    ArrayList<Integer> selectedFileIdx = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    ArrayList<ArrayList<Long>> timeLists = new ArrayList<>();
    ArrayList<ArrayList<ArrayList<Boolean>>> allBitmapLists = new ArrayList<>();
    ArrayList<ArrayList<String>> schemaLists = new ArrayList<>();
    ArrayList<TsFileResource> curLevelTsFileResources = new ArrayList<>();

    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        continue;
      } else {
        if (currentFile.getTsFileSize() > 0) {
          curLevelTsFileResources.add(currentFile);
        }
      }
    }

    if (curLevelTsFileResources.size() <= config.getMaxFileNumInLevel()) {
      return shouldContinueToSearch;
    }

    for (TsFileResource currentFile : curLevelTsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      // read all files
      if (!this.sequence) {
        ArrayList<Long> timeList = new ArrayList<>();
        ArrayList<ArrayList<Boolean>> bitmapLists = new ArrayList<>();
        ArrayList<String> schemaList = new ArrayList<>();
        readMultiFileDataWithoutValues(currentFile, timeList, bitmapLists, schemaList);
        timeLists.add(timeList);
        allBitmapLists.add(bitmapLists);
        schemaLists.add(schemaList);
      }
    }
    long readFileStopTime = System.currentTimeMillis();
    double readFileTime = (readFileStopTime - startTime) / 1000.0d;

    // generate new task queue for compaction
    // greedy add files
    int i;
    int maxOverlapFinal = -1;

    int selectedNum = config.getMaxInnerCompactionCandidateFileNum();

    for (i = 0; i < curLevelTsFileResources.size() - selectedNum; i++) {
      ArrayList<Integer> curSelectedFileIdx = new ArrayList<>();
      List<TsFileResource> curSelectedFileList = new ArrayList<>();
      long curSelectedFileSize = 0L;
      curSelectedFileList.add(curLevelTsFileResources.get(i));
      curSelectedFileIdx.add(i);
      curSelectedFileSize += curLevelTsFileResources.get(i).getTsFileSize();
      int totalOverlap = 0;
      while (curSelectedFileList.size() < timeLists.size()) {
        int maxOverlapIdx = -1;
        int maxOverlap = -1;
        for (int j = curSelectedFileIdx.get(curSelectedFileList.size() - 1) + 1;
            j < timeLists.size();
            j++) {
          int curOverlap = -1;
          for (int idx : curSelectedFileIdx) {
            int tmpOverlap =
                computeOverlapWithBitmap(
                    timeLists.get(idx),
                    timeLists.get(j),
                    allBitmapLists.get(idx),
                    allBitmapLists.get(j),
                    schemaLists.get(idx),
                    schemaLists.get(j));
            if (tmpOverlap > curOverlap) {
              curOverlap = tmpOverlap;
            }
          }
          if (curOverlap > maxOverlap) {
            maxOverlap = curOverlap;
            maxOverlapIdx = j;
          }
        }
        if (maxOverlap == -1 || maxOverlapIdx == -1) {
          break;
        }
        TsFileResource currentFile = curLevelTsFileResources.get(maxOverlapIdx);
        curSelectedFileIdx.add(maxOverlapIdx);
        curSelectedFileList.add(currentFile);
        curSelectedFileSize += currentFile.getTsFileSize();
        totalOverlap += maxOverlap;

        if (curSelectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {
          // check overlap
          if (totalOverlap > maxOverlapFinal) {
            maxOverlapFinal = totalOverlap;
            selectedFileSize = curSelectedFileSize;
            selectedFileIdx = new ArrayList<>();
            for (int tmpIdx : curSelectedFileIdx) {
              selectedFileIdx.add(tmpIdx);
            }
            selectedFileList = new ArrayList<>();
            for (TsFileResource tsfile : curSelectedFileList) {
              selectedFileList.add(tsfile);
            }
          }
          break;
        }
      }
    }

    if (selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {
      // submit the task
      if (selectedFileList.size() > 1) {
        taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        shouldContinueToSearch = false;
      }
    }
    long submitCompactionStopTime = System.currentTimeMillis();
    double submitCompactionTime = (submitCompactionStopTime - readFileStopTime) / 1000.0d;
    return shouldContinueToSearch;
  }

  private boolean selectLevelTaskUnseqRound(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    long startTime = System.currentTimeMillis();
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    int levelFileNum = tsFileResources.size();
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        levelFileNum--;
      }
    }
    if (levelFileNum < config.getMaxFileNumInLevel()) {
      return shouldContinueToSearch;
    }

    ArrayList<TsFileResource> randomTsFileResources = new ArrayList<>();
    ArrayList<Integer> idxList = new ArrayList<>();
    for (int i = 0; i < tsFileResources.size(); ++i) {
      idxList.add(i);
    }
    Collections.shuffle(idxList);
    for (int i = 0; i < tsFileResources.size(); ++i) {
      randomTsFileResources.add(tsFileResources.get(idxList.get(i)));
    }

    for (TsFileResource currentFile : randomTsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        selectedFileList.clear();
        selectedFileSize = 0L;
        continue;
      }
      LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      selectedFileList.add(currentFile);
      selectedFileSize += currentFile.getTsFileSize();
      LOGGER.debug(
          "Add tsfile {}, current select file num is {}, size is {}",
          currentFile,
          selectedFileList.size(),
          selectedFileSize);
      // if the file size or file num reach threshold
      if (selectedFileSize >= targetCompactionFileSize
          || selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        shouldContinueToSearch = false;
        break;
      }
    }
    return shouldContinueToSearch;
  }

  private boolean selectLevelTaskUnseqOldest(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    long startTime = System.currentTimeMillis();
    long submitCompactionStopTime = System.currentTimeMillis();
    double submitCompactionTime = (submitCompactionStopTime - startTime) / 1000.0d;

    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    int levelFileNum = tsFileResources.size();
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        levelFileNum--;
      }
    }
    if (levelFileNum < config.getMaxFileNumInLevel()) {
      return shouldContinueToSearch;
    }

    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level
          || currentFile.isCompactionCandidate()
          || currentFile.isCompacting()
          || !currentFile.isClosed()) {
        selectedFileList.clear();
        selectedFileSize = 0L;
        continue;
      }
      LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      selectedFileList.add(currentFile);
      selectedFileSize += currentFile.getTsFileSize();
      LOGGER.debug(
          "Add tsfile {}, current select file num is {}, size is {}",
          currentFile,
          selectedFileList.size(),
          selectedFileSize);
      // if the file size or file num reach threshold
      if (selectedFileSize >= targetCompactionFileSize
          || selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        shouldContinueToSearch = false;
        break;
      }
    }

    return shouldContinueToSearch;
  }

  private int searchMaxFileLevel() throws IOException {
    int maxLevel = -1;
    Iterator<TsFileResource> iterator = tsFileResources.iterator();
    while (iterator.hasNext()) {
      TsFileResource currentFile = iterator.next();
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() > maxLevel) {
        maxLevel = currentName.getInnerCompactionCnt();
      }
    }
    return maxLevel;
  }

  private boolean createAndSubmitTask(List<TsFileResource> selectedFileList)
      throws InterruptedException {
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileManager,
            selectedFileList,
            sequence);
    return CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
  }

  private void readMultiFileDataWithoutValues(
      TsFileResource tsFileResource,
      ArrayList<Long> timeList,
      ArrayList<ArrayList<Boolean>> bitmapLists,
      ArrayList<String> schemaList) {
    try {
      List<TsFileResource> unseqFileResources = new ArrayList<>();
      unseqFileResources.add(tsFileResource);
      long queryId = QueryResourceManager.getInstance().assignCompactionPrefetchQueryId();
      QueryContext queryContext = new QueryContext(queryId);
      QueryDataSource queryDataSource = new QueryDataSource(new ArrayList<>(), unseqFileResources);
      QueryResourceManager.getInstance()
          .getQueryFileManager()
          .addUsedFilesForQuery(queryId, queryDataSource);
      MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(unseqFileResources);

      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        QueryUtils.fillOrderIndexes(queryDataSource, device, true);

        if (isAligned) {
          Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
          List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
          if (measurementSchemas.isEmpty()) {
            return;
          }
          List<String> existedMeasurements =
              measurementSchemas.stream()
                  .map(IMeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList());
          for (String schema : existedMeasurements) {
            schemaList.add(schema);
          }
          IBatchReader dataBatchReader =
              //              constructReader(
              constructPrefetchReader(
                  device,
                  existedMeasurements,
                  measurementSchemas,
                  schemaMap.keySet(),
                  queryContext,
                  queryDataSource,
                  true);

          for (int i = 0; i < existedMeasurements.size(); i++) {
            bitmapLists.add(new ArrayList<>());
          }

          if (dataBatchReader.hasNextBatch()) {
            while (dataBatchReader.hasNextBatch()) {
              BatchData batchData = dataBatchReader.nextBatch();
              while (batchData.hasCurrent()) {
                long time = batchData.currentTime();
                timeList.add(time);
                TsPrimitiveType[] value = (TsPrimitiveType[]) batchData.currentValue();
                for (int j = 0; j < value.length; j++) {
                  if (value[j] == null) {
                    bitmapLists.get(j).add(false);
                  } else {
                    bitmapLists.get(j).add(true);
                  }
                }
                batchData.next();
              }
            }
          }
        }
        tsFileResource.readUnlock();
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurs while reading multiple file data", e);
    }
  }

  private int computeOverlapWithBitmap(
      ArrayList<Long> timeList1,
      ArrayList<Long> timeList2,
      ArrayList<ArrayList<Boolean>> bitmap1,
      ArrayList<ArrayList<Boolean>> bitmap2,
      ArrayList<String> schemaList1,
      ArrayList<String> schemaList2) {
    int p1 = 0;
    int p2 = 0;
    int overlap = 0;
    ArrayList<Pair<Integer, Integer>> schemaPairs = new ArrayList<>();
    for (int i = 0; i < schemaList1.size(); i++) {
      for (int j = 0; j < schemaList2.size(); j++) {
        if (schemaList1.get(i).equals(schemaList2.get(j))) {
          schemaPairs.add(new Pair<>(i, j));
        }
      }
    }
    int overlapOfRow = schemaPairs.size();
    int updateCheck = -1;
    while (p1 < timeList1.size() && p2 < timeList2.size()) {
      if (timeList1.get(p1) < timeList2.get(p2)) {
        p1++;
      } else {
        if (timeList1.get(p1) > timeList2.get(p2)) {
          p2++;
        } else {
          if (updateCheck == 0) {
            // insert
            overlap += 1;
          } else if (updateCheck == 1) {
            // update
            overlap += overlapOfRow + 1;
          } else {
            Pair<Integer, Integer> schemaPair = schemaPairs.get(0);
            if (bitmap1.get(schemaPair.left).get(p1) && bitmap2.get(schemaPair.right).get(p2)) {
              updateCheck = 1;
            } else {
              updateCheck = 0;
            }
          }

          p1++;
          p2++;
        }
      }
    }
    return overlap;
  }

  public static IBatchReader constructPrefetchReader(
      String deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors,
      QueryContext queryContext,
      QueryDataSource queryDataSource,
      boolean isAlign)
      throws IllegalPathException {
    PartialPath seriesPath;
    TSDataType tsDataType;
    if (isAlign) {
      seriesPath = new AlignedPath(deviceId, measurementIds, measurementSchemas);
      tsDataType = TSDataType.VECTOR;
    } else {
      seriesPath = new MeasurementPath(deviceId, measurementIds.get(0), measurementSchemas.get(0));
      tsDataType = measurementSchemas.get(0).getType();
    }
    return new SeriesRawDataPrefetchReader(
        seriesPath, allSensors, tsDataType, queryContext, queryDataSource, null, null, null, true);
  }

  private class SizeTieredCompactionTaskComparator
      implements Comparator<Pair<List<TsFileResource>, Long>> {

    @Override
    public int compare(Pair<List<TsFileResource>, Long> o1, Pair<List<TsFileResource>, Long> o2) {
      TsFileResource resourceOfO1 = o1.left.get(0);
      TsFileResource resourceOfO2 = o2.left.get(0);
      try {
        TsFileNameGenerator.TsFileName fileNameOfO1 =
            TsFileNameGenerator.getTsFileName(resourceOfO1.getTsFile().getName());
        TsFileNameGenerator.TsFileName fileNameOfO2 =
            TsFileNameGenerator.getTsFileName(resourceOfO2.getTsFile().getName());
        if (fileNameOfO1.getInnerCompactionCnt() != fileNameOfO2.getInnerCompactionCnt()) {
          return fileNameOfO2.getInnerCompactionCnt() - fileNameOfO1.getInnerCompactionCnt();
        }
        return (int) (fileNameOfO2.getVersion() - fileNameOfO1.getVersion());
      } catch (IOException e) {
        return 0;
      }
    }
  }
}
