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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionOverlapType;
import org.apache.iotdb.db.engine.compaction.utils.CompactionTimeseriesType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils.putOnePageChunk;

public class InnerUnseqCompactionWithFastPerformerTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(InnerUnseqCompactionWithReadPointPerformerTest.class);
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device0.sensor1",
        COMPACTION_TEST_SG + ".device0.sensor2",
        COMPACTION_TEST_SG + ".device0.sensor3",
        COMPACTION_TEST_SG + ".device0.sensor4",
        COMPACTION_TEST_SG + ".device0.sensor5",
        COMPACTION_TEST_SG + ".device0.sensor6",
        COMPACTION_TEST_SG + ".device0.sensor7",
        COMPACTION_TEST_SG + ".device0.sensor8",
        COMPACTION_TEST_SG + ".device0.sensor9",
        COMPACTION_TEST_SG + ".device1.sensor0",
        COMPACTION_TEST_SG + ".device1.sensor1",
        COMPACTION_TEST_SG + ".device1.sensor2",
        COMPACTION_TEST_SG + ".device1.sensor3",
        COMPACTION_TEST_SG + ".device1.sensor4",
      };
  static final int[] toMergeFileNums = new int[] {2, 3};
  static final CompactionTimeseriesType[] compactionTimeseriesTypes =
      new CompactionTimeseriesType[] {
        CompactionTimeseriesType.ALL_SAME,
        CompactionTimeseriesType.PART_SAME,
        CompactionTimeseriesType.NO_SAME
      };
  static final boolean[] compactionBeforeHasMods = new boolean[] {true, false};
  static final boolean[] compactionHasMods = new boolean[] {true, false};
  static final CompactionOverlapType[] compactionOverlapTypes =
      new CompactionOverlapType[] {
        CompactionOverlapType.FILE_NO_OVERLAP,
        CompactionOverlapType.FILE_OVERLAP_CHUNK_NO_OVERLAP,
        CompactionOverlapType.CHUNK_OVERLAP_PAGE_NO_OVERLAP,
        CompactionOverlapType.PAGE_OVERLAP
      };
  static final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp() throws MetadataException {
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
    EnvironmentUtils.cleanEnv();
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    Thread.currentThread().setName(oldThreadName);
    CompactionClearUtils.deleteEmptyDir(new File("target"));
  }

  // unseq space only do deserialize page
  @Test
  public void test() throws Exception {
    for (int toMergeFileNum : toMergeFileNums) {
      for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
        for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
          for (boolean compactionHasMod : compactionHasMods) {
            for (CompactionOverlapType compactionOverlapType : compactionOverlapTypes) {
              List<TsFileResource> toMergeResources = new ArrayList<>();
              for (int i = 0; i < toMergeFileNum; i++) {
                Set<String> fullPath = new HashSet<>();
                if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                  fullPath.add(fullPaths[0]);
                  fullPath.add(fullPaths[1]);
                  fullPath.add(fullPaths[2]);
                } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                  if (i == 0) {
                    fullPath.add(fullPaths[0]);
                    fullPath.add(fullPaths[1]);
                    fullPath.add(fullPaths[2]);
                  } else if (i == 1) {
                    fullPath.add(fullPaths[1]);
                    fullPath.add(fullPaths[2]);
                    fullPath.add(fullPaths[3]);
                  } else {
                    fullPath.add(fullPaths[2]);
                    fullPath.add(fullPaths[3]);
                    fullPath.add(fullPaths[4]);
                  }
                } else {
                  if (i == 0) {
                    fullPath.add(fullPaths[0]);
                    fullPath.add(fullPaths[1]);
                    fullPath.add(fullPaths[2]);
                  } else if (i == 1) {
                    fullPath.add(fullPaths[3]);
                    fullPath.add(fullPaths[4]);
                    fullPath.add(fullPaths[5]);
                  } else {
                    fullPath.add(fullPaths[6]);
                    fullPath.add(fullPaths[7]);
                    fullPath.add(fullPaths[8]);
                  }
                }
                List<List<Long>> chunkPagePointsNum;
                List<Long> pagePointsNum;
                List<List<long[][]>> chunkPagePointsRange;
                List<long[][]> pagePointsRange;
                TsFileResource tsFileResource = null;
                switch (compactionOverlapType) {
                  case FILE_NO_OVERLAP:
                    chunkPagePointsNum = new ArrayList<>();
                    pagePointsNum = new ArrayList<>();
                    pagePointsNum.add(100L);
                    chunkPagePointsNum.add(pagePointsNum);
                    pagePointsNum = new ArrayList<>();
                    pagePointsNum.add(200L);
                    chunkPagePointsNum.add(pagePointsNum);
                    pagePointsNum = new ArrayList<>();
                    pagePointsNum.add(300L);
                    chunkPagePointsNum.add(pagePointsNum);
                    tsFileResource =
                        CompactionFileGeneratorUtils.generateTsFileResource(
                            false, i + 1, COMPACTION_TEST_SG);
                    CompactionFileGeneratorUtils.writeTsFile(
                        fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
                    break;
                  case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                    if (i == 0) {
                      chunkPagePointsRange = new ArrayList<>();
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      fullPath.add(fullPaths[10]);
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 1000L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeChunkToTsFileWithTimeRange(
                          fullPath, chunkPagePointsRange, tsFileResource);
                    } else {
                      chunkPagePointsNum = new ArrayList<>();
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(100L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(200L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(300L);
                      chunkPagePointsNum.add(pagePointsNum);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeTsFile(
                          fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
                    }
                    break;
                  case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                    if (i == 0) {
                      chunkPagePointsRange = new ArrayList<>();
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      pagePointsRange = new ArrayList<>();
                      pagePointsRange.add(new long[][] {{0L, 100L}});
                      pagePointsRange.add(new long[][] {{100L, 300L}});
                      pagePointsRange.add(new long[][] {{300L, 600L}, {1800L, 3600L}});
                      chunkPagePointsRange.add(pagePointsRange);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeChunkToTsFileWithTimeRange(
                          fullPath, chunkPagePointsRange, tsFileResource);
                    } else {
                      chunkPagePointsNum = new ArrayList<>();
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(100L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(200L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(300L);
                      chunkPagePointsNum.add(pagePointsNum);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeTsFile(
                          fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
                    }
                    break;
                  case PAGE_OVERLAP:
                    if (i == 1) {
                      chunkPagePointsNum = new ArrayList<>();
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(100L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(200L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(300L);
                      chunkPagePointsNum.add(pagePointsNum);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeTsFile(
                          fullPath, chunkPagePointsNum, 50L, tsFileResource);
                    } else {
                      chunkPagePointsNum = new ArrayList<>();
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(100L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(200L);
                      chunkPagePointsNum.add(pagePointsNum);
                      pagePointsNum = new ArrayList<>();
                      pagePointsNum.add(300L);
                      chunkPagePointsNum.add(pagePointsNum);
                      tsFileResource =
                          CompactionFileGeneratorUtils.generateTsFileResource(
                              false, i + 1, COMPACTION_TEST_SG);
                      CompactionFileGeneratorUtils.writeTsFile(
                          fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
                    }
                    break;
                }
                toMergeResources.add(tsFileResource);
                // has mods files before compaction
                if (compactionBeforeHasMod) {
                  Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
                  if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                    toDeleteTimeseriesAndTime.put(
                        fullPaths[i], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                  } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                    if (i == 0) {
                      toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(250L, 300L));
                    } else if (i == 1) {
                      toDeleteTimeseriesAndTime.put(
                          fullPaths[3], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                    } else {
                      toDeleteTimeseriesAndTime.put(
                          fullPaths[4], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                    }
                  } else {
                    if (i == 0) {
                      toDeleteTimeseriesAndTime.put(fullPaths[2], new Pair<>(250L, 300L));
                    } else if (i == 1) {
                      toDeleteTimeseriesAndTime.put(
                          fullPaths[5], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                    } else {
                      toDeleteTimeseriesAndTime.put(
                          fullPaths[8], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                    }
                  }
                  CompactionFileGeneratorUtils.generateMods(
                      toDeleteTimeseriesAndTime, tsFileResource, false);
                }
              }
              LOG.error(
                  "{} {} {} {} {}",
                  toMergeFileNum,
                  compactionTimeseriesType,
                  compactionBeforeHasMod,
                  compactionHasMod,
                  compactionOverlapType);
              TsFileResource targetTsFileResource =
                  CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(
                          toMergeResources, false)
                      .get(0);
              Map<String, List<TimeValuePair>> sourceData =
                  CompactionCheckerUtils.readFiles(toMergeResources);
              if (compactionHasMod) {
                Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
                toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(250L, 300L));
                CompactionFileGeneratorUtils.generateMods(
                    toDeleteTimeseriesAndTime, toMergeResources.get(0), true);

                // remove data in source data list
                List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
                timeValuePairs.removeIf(
                    timeValuePair ->
                        timeValuePair.getTimestamp() >= 250L
                            && timeValuePair.getTimestamp() <= 300L);
              }
              ICompactionPerformer performer =
                  new FastCompactionPerformer(
                      Collections.emptyList(),
                      toMergeResources,
                      Collections.singletonList(targetTsFileResource));
              performer.setSummary(new FastCompactionTaskSummary());
              performer.perform();
              CompactionUtils.moveTargetFile(
                  Collections.singletonList(targetTsFileResource), true, COMPACTION_TEST_SG);
              CompactionUtils.combineModsInInnerCompaction(toMergeResources, targetTsFileResource);
              List<TsFileResource> targetTsFileResources = new ArrayList<>();
              targetTsFileResources.add(targetTsFileResource);
              CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
              Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
              if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3000L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 650L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3000L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 650L);
                        break;
                    }
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1749L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3549L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1199L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3600L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1250L);
                        break;
                    }
                  }
                }
              } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3000L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3000L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        break;
                    }
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 3600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 650L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1250L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        break;
                    }
                  }
                }
              } else {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 2349L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 2400L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        break;
                    }
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 2349L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                        break;
                    }
                  } else {
                    switch (compactionOverlapType) {
                      case FILE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 600L);
                        break;
                      case FILE_OVERLAP_CHUNK_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[10], 1000L);
                        break;
                      case CHUNK_OVERLAP_PAGE_NO_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 2400L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 600L);
                        break;
                      case PAGE_OVERLAP:
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                        putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 600L);
                        break;
                    }
                  }
                }
              }
              CompactionClearUtils.clearAllCompactionFiles();
            }
          }
        }
      }
    }
  }
}
