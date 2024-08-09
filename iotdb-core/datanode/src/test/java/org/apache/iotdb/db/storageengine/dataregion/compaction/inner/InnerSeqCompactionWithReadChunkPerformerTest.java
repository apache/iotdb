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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTimeseriesType;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class InnerSeqCompactionWithReadChunkPerformerTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device1.sensor0",
        COMPACTION_TEST_SG + ".device2.sensor0",
        COMPACTION_TEST_SG + ".device3.sensor0",
        COMPACTION_TEST_SG + ".device4.sensor0",
        COMPACTION_TEST_SG + ".device5.sensor0",
        COMPACTION_TEST_SG + ".device6.sensor0",
        COMPACTION_TEST_SG + ".device7.sensor0",
        COMPACTION_TEST_SG + ".device8.sensor0",
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
  private static int prevMaxDegreeOfIndexNode;

  @Before
  public void setUp() throws MetadataException {
    prevMaxDegreeOfIndexNode = TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(2);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanAllDir();
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(prevMaxDegreeOfIndexNode);
  }

  @Test
  public void testDeserializePage() throws Exception {

    long chunkSizeLowerBoundInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkSizeLowerBoundInCompaction(10240);
    long chunkPointNumLowerBoundInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkPointNumLowerBoundInCompaction(1000);
    try {
      for (int toMergeFileNum : toMergeFileNums) {
        for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
          for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
            for (boolean compactionHasMod : compactionHasMods) {
              List<TsFileResource> sourceResources = new ArrayList<>();
              // generate source file
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
                List<List<Long>> chunkPagePointsNum = new ArrayList<>();
                List<Long> pagePointsNum = new ArrayList<>();
                pagePointsNum.add(100L);
                chunkPagePointsNum.add(pagePointsNum);
                pagePointsNum = new ArrayList<>();
                pagePointsNum.add(200L);
                chunkPagePointsNum.add(pagePointsNum);
                pagePointsNum = new ArrayList<>();
                pagePointsNum.add(300L);
                chunkPagePointsNum.add(pagePointsNum);
                TsFileResource tsFileResource =
                    CompactionFileGeneratorUtils.generateTsFileResource(
                        true, i + 1, COMPACTION_TEST_SG);
                CompactionFileGeneratorUtils.writeTsFile(
                    fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
                sourceResources.add(tsFileResource);
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

              TsFileResource targetTsFileResource =
                  CompactionFileGeneratorUtils.getTargetTsFileResourceFromSourceResource(
                      sourceResources.get(0));
              Map<String, List<TimeValuePair>> sourceData =
                  CompactionCheckerUtils.readFiles(sourceResources);
              if (compactionHasMod) {
                Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
                toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(250L, 300L));
                CompactionFileGeneratorUtils.generateMods(
                    toDeleteTimeseriesAndTime, sourceResources.get(0), true);

                // remove data in source data list
                List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
                timeValuePairs.removeIf(
                    timeValuePair ->
                        timeValuePair.getTimestamp() >= 250L
                            && timeValuePair.getTimestamp() <= 300L);
              }
              ICompactionPerformer performer =
                  new ReadChunkCompactionPerformer(sourceResources, targetTsFileResource);
              performer.setSummary(new FastCompactionTaskSummary());
              performer.perform();
              CompactionUtils.moveTargetFile(
                  Collections.singletonList(targetTsFileResource),
                  CompactionTaskType.INNER_SEQ,
                  COMPACTION_TEST_SG);
              CompactionUtils.combineModsInInnerCompaction(sourceResources, targetTsFileResource);
              List<TsFileResource> targetTsFileResources = new ArrayList<>();
              targetTsFileResources.add(targetTsFileResource);
              // check data
              CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
              Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
              if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 1149L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1149L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1200L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1200L);
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 1749L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1749L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1749L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 1800L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1800L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1800L);
                  }
                }
              } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 549L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 549L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 600L);
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 549L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1800L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 1149L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 549L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 1800L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 1200L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 600L);
                  }
                }
              } else {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 549L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[5], 549L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[5], 600L);
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 549L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[5], 549L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[6], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[7], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[8], 549L);
                  } else {
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[0], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[1], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[2], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[3], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[4], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[5], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[6], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[7], 600L);
                    CompactionCheckerUtils.putOnePageChunk(
                        chunkPagePointsNumMerged, fullPaths[8], 600L);
                  }
                }
              }
              CompactionCheckerUtils.checkChunkAndPage(
                  chunkPagePointsNumMerged, targetTsFileResource);
              CompactionClearUtils.clearAllCompactionFiles();
            }
          }
        }
      }
    } catch (InterruptedException | StorageEngineException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(chunkPointNumLowerBoundInCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(chunkSizeLowerBoundInCompaction);
    }
  }

  @Test
  public void testAppendPage() throws Exception {

    for (int toMergeFileNum : toMergeFileNums) {
      for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
        for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
          for (boolean compactionHasMod : compactionHasMods) {
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
              List<List<Long>> chunkPagePointsNum = new ArrayList<>();
              List<Long> pagePointsNum = new ArrayList<>();
              pagePointsNum.add(100L);
              chunkPagePointsNum.add(pagePointsNum);
              pagePointsNum = new ArrayList<>();
              pagePointsNum.add(200L);
              chunkPagePointsNum.add(pagePointsNum);
              pagePointsNum = new ArrayList<>();
              pagePointsNum.add(300L);
              chunkPagePointsNum.add(pagePointsNum);
              TsFileResource tsFileResource =
                  CompactionFileGeneratorUtils.generateTsFileResource(
                      true, i + 1, COMPACTION_TEST_SG);
              CompactionFileGeneratorUtils.writeTsFile(
                  fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
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
            TsFileResource targetTsFileResource =
                CompactionFileGeneratorUtils.getTargetTsFileResourceFromSourceResource(
                    toMergeResources.get(0));
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
                      timeValuePair.getTimestamp() >= 250L && timeValuePair.getTimestamp() <= 300L);
            }
            ICompactionPerformer performer =
                new ReadChunkCompactionPerformer(toMergeResources, targetTsFileResource);
            performer.setSummary(new FastCompactionTaskSummary());
            performer.perform();
            CompactionUtils.moveTargetFile(
                Collections.singletonList(targetTsFileResource),
                CompactionTaskType.INNER_SEQ,
                COMPACTION_TEST_SG);
            CompactionUtils.combineModsInInnerCompaction(toMergeResources, targetTsFileResource);
            List<TsFileResource> targetTsFileResources = new ArrayList<>();
            targetTsFileResources.add(targetTsFileResource);
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
            Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
            if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[0], 1149L);
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[1], 1149L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[0], 1749L);
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[1], 1749L);
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[2], 1749L);
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                }
              }
            } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[0], 549L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[3], 549L);
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[0], 549L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[3], 1149L);
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[4], 549L);
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[3],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                }
              }
            } else {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[2], 549L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[5], 549L);
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[2], 549L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[5], 549L);
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putOnePageChunk(
                      chunkPagePointsNumMerged, fullPaths[8], 549L);
                } else {
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  CompactionCheckerUtils.putChunk(
                      chunkPagePointsNumMerged, fullPaths[8], new long[] {100L, 200L, 300L});
                }
              }
            }
            CompactionCheckerUtils.checkChunkAndPage(
                chunkPagePointsNumMerged, targetTsFileResource);
            CompactionClearUtils.clearAllCompactionFiles();
          }
        }
      }
    }
  }

  @Test
  public void testAppendChunk() throws Exception {
    long prevChunkPointNumLowerBoundInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkPointNumLowerBoundInCompaction(1);
    long prevChunkSizeLowerBoundInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkSizeLowerBoundInCompaction(1);
    long prevTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    long prevTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(1);

    try {
      for (int toMergeFileNum : toMergeFileNums) {
        for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
          for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
            for (boolean compactionHasMod : compactionHasMods) {
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
                List<List<Long>> chunkPagePointsNum = new ArrayList<>();
                List<Long> pagePointsNum = new ArrayList<>();
                pagePointsNum.add(100L);
                chunkPagePointsNum.add(pagePointsNum);
                pagePointsNum = new ArrayList<>();
                pagePointsNum.add(200L);
                chunkPagePointsNum.add(pagePointsNum);
                pagePointsNum = new ArrayList<>();
                pagePointsNum.add(300L);
                chunkPagePointsNum.add(pagePointsNum);
                TsFileResource tsFileResource =
                    CompactionFileGeneratorUtils.generateTsFileResource(
                        true, i + 1, COMPACTION_TEST_SG);
                CompactionFileGeneratorUtils.writeTsFile(
                    fullPath, chunkPagePointsNum, i * 600L, tsFileResource);
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
              TsFileResource targetTsFileResource =
                  CompactionFileGeneratorUtils.getTargetTsFileResourceFromSourceResource(
                      toMergeResources.get(0));
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
                  new ReadChunkCompactionPerformer(toMergeResources, targetTsFileResource);
              performer.setSummary(new FastCompactionTaskSummary());
              performer.perform();
              CompactionUtils.moveTargetFile(
                  Collections.singletonList(targetTsFileResource),
                  CompactionTaskType.INNER_SEQ,
                  COMPACTION_TEST_SG);
              CompactionUtils.combineModsInInnerCompaction(toMergeResources, targetTsFileResource);
              List<TsFileResource> targetTsFileResources = new ArrayList<>();
              targetTsFileResources.add(targetTsFileResource);
              CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
              Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
              if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[0],
                        new long[] {100, 150, 299, 100, 200, 300});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100, 200, 300, 100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[0],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[0],
                        new long[] {100, 150, 299, 100, 200, 300, 100, 200, 300});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100, 200, 300, 100, 150, 299, 100, 200, 300});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100, 200, 300, 100, 200, 300, 100, 150, 299});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[0],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  }
                }
              } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100, 150, 299});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[3],
                        new long[] {100, 150, 299, 100, 200, 300});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100, 150, 299});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[1],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[2],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged,
                        fullPaths[3],
                        new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  }
                }
              } else {
                if (toMergeFileNum == 2) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[2], new long[] {100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[5], new long[] {100, 150, 299});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                  }
                } else if (toMergeFileNum == 3) {
                  if (compactionBeforeHasMod) {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[2], new long[] {100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[5], new long[] {100, 150, 299});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[8], new long[] {100, 150, 299});
                  } else {
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                    CompactionCheckerUtils.putOnePageChunks(
                        chunkPagePointsNumMerged, fullPaths[8], new long[] {100L, 200L, 300L});
                  }
                }
              }
              CompactionCheckerUtils.checkChunkAndPage(
                  chunkPagePointsNumMerged, targetTsFileResource);
              CompactionClearUtils.clearAllCompactionFiles();
            }
          }
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(prevChunkSizeLowerBoundInCompaction);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(prevChunkPointNumLowerBoundInCompaction);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(prevTargetChunkPointNum);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(prevTargetChunkSize);
    }
  }

  @Test
  public void testCompactionWithDeletionsDuringCompactions()
      throws MetadataException, IOException, DataRegionException, InterruptedException {
    // create source seq files
    List<TsFileResource> sourceResources = new ArrayList<>();
    List<List<Long>> chunkPagePointsNum = new ArrayList<>();
    List<Long> pagePointsNum = new ArrayList<>();
    pagePointsNum.add(100L);
    chunkPagePointsNum.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(200L);
    chunkPagePointsNum.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(300L);
    chunkPagePointsNum.add(pagePointsNum);
    Set<String> paths = new HashSet<>();
    for (int i = 0; i < fullPaths.length; i++) {
      paths.add(fullPaths[i]);
    }

    for (int i = 0; i < 5; i++) {
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeTsFile(paths, chunkPagePointsNum, i * 600L, tsFileResource);
      sourceResources.add(tsFileResource);
    }
    DataRegion vsgp =
        new DataRegion(
            TestConstant.BASE_OUTPUT_PATH,
            "0",
            new TsFileFlushPolicy.DirectFlushPolicy(),
            COMPACTION_TEST_SG);
    vsgp.getTsFileResourceManager().addAll(sourceResources, true);
    // delete data before compaction
    vsgp.deleteByDevice(new MeasurementPath(fullPaths[0]), 0, 1000, 0);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            sourceResources,
            true,
            new ReadChunkCompactionPerformer(),
            0);
    task.setSourceFilesToCompactionCandidate();
    sourceResources.forEach(f -> f.setStatus(TsFileResourceStatus.COMPACTING));
    // delete data during compaction
    vsgp.deleteByDevice(new MeasurementPath(fullPaths[0]), 0, 1200, 0);
    vsgp.deleteByDevice(new MeasurementPath(fullPaths[0]), 0, 1800, 0);
    for (int i = 0; i < sourceResources.size() - 1; i++) {
      TsFileResource resource = sourceResources.get(i);
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertTrue(resource.getModFile().exists());
      if (i < 2) {
        Assert.assertEquals(3, resource.getModFile().getModifications().size());
        Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      } else if (i < 3) {
        Assert.assertEquals(2, resource.getModFile().getModifications().size());
        Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      } else {
        Assert.assertEquals(1, resource.getModFile().getModifications().size());
        Assert.assertEquals(1, resource.getCompactionModFile().getModifications().size());
      }
    }
    task.start();
    for (TsFileResource resource : sourceResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    TsFileResource resource =
        TsFileNameGenerator.increaseInnerCompactionCnt(sourceResources.get(0));
    resource.resetModFile();
    Assert.assertTrue(resource.getModFile().exists());
    Assert.assertEquals(2, resource.getModFile().getModifications().size());
    Assert.assertFalse(resource.getCompactionModFile().exists());
  }
}
