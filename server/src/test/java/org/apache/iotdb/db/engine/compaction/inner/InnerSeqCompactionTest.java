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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionTimeseriesType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
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

import static org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils.*;

public class InnerSeqCompactionTest {
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
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String fullPath : fullPaths) {
      PartialPath path = new PartialPath(fullPath);
      IoTDB.metaManager.createTimeseries(
          path,
          TSDataType.INT64,
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(prevMaxDegreeOfIndexNode);
  }

  @Test
  public void testDeserializePage() throws IllegalPathException, IOException {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(100000);

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
                  CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
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
                      timeValuePair.getTimestamp() >= 250L && timeValuePair.getTimestamp() <= 300L);
            }
            SizeTieredCompactionLogger sizeTieredCompactionLogger =
                new SizeTieredCompactionLogger("target", COMPACTION_TEST_SG);
            InnerSpaceCompactionUtils.compact(
                targetTsFileResource,
                sourceResources,
                COMPACTION_TEST_SG,
                sizeTieredCompactionLogger,
                new HashSet<>(),
                true);
            SizeTieredCompactionTask.combineModsInCompaction(sourceResources, targetTsFileResource);
            List<TsFileResource> targetTsFileResources = new ArrayList<>();
            targetTsFileResources.add(targetTsFileResource);
            // check data
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
            Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
            if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1749L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1800L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1800L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                }
              }
            } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1800L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1200L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                }
              }
            } else {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                } else {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[6], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[7], 600L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 600L);
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

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
  }

  @Test
  public void testAppendPage() throws IOException, IllegalPathException {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(100000);

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
                  CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
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
            SizeTieredCompactionLogger sizeTieredCompactionLogger =
                new SizeTieredCompactionLogger("target", COMPACTION_TEST_SG);
            InnerSpaceCompactionUtils.compact(
                targetTsFileResource,
                toMergeResources,
                COMPACTION_TEST_SG,
                sizeTieredCompactionLogger,
                new HashSet<>(),
                true);
            SizeTieredCompactionTask.combineModsInCompaction(
                toMergeResources, targetTsFileResource);
            List<TsFileResource> targetTsFileResources = new ArrayList<>();
            targetTsFileResources.add(targetTsFileResource);
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
            Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
            if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                } else {
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1749L);
                } else {
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                }
              }
            } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                } else {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                } else {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(
                      chunkPagePointsNumMerged,
                      fullPaths[3],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                }
              }
            } else {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putChunk(chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                } else {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putChunk(chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                  putChunk(chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                } else {
                  putChunk(chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  putChunk(chunkPagePointsNumMerged, fullPaths[8], new long[] {100L, 200L, 300L});
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

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }

  @Test
  public void testAppendChunk() throws IOException, IllegalPathException {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(1);

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
                  CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
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
            SizeTieredCompactionLogger sizeTieredCompactionLogger =
                new SizeTieredCompactionLogger("target", COMPACTION_TEST_SG);
            InnerSpaceCompactionUtils.compact(
                targetTsFileResource,
                toMergeResources,
                COMPACTION_TEST_SG,
                sizeTieredCompactionLogger,
                new HashSet<>(),
                true);
            SizeTieredCompactionTask.combineModsInCompaction(
                toMergeResources, targetTsFileResource);
            List<TsFileResource> targetTsFileResources = new ArrayList<>();
            targetTsFileResources.add(targetTsFileResource);
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResources);
            Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
            if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1749L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1749L);
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[0],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                }
              }
            } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 549L);
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 549L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[3], 1149L);
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[4], 549L);
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[1],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[2],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged,
                      fullPaths[3],
                      new long[] {100L, 200L, 300L, 100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                }
              }
            } else {
              if (toMergeFileNum == 2) {
                if (compactionBeforeHasMod) {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                }
              } else if (toMergeFileNum == 3) {
                if (compactionBeforeHasMod) {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 549L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[5], 549L);
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  putOnePageChunk(chunkPagePointsNumMerged, fullPaths[8], 549L);
                } else {
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[0], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[1], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[2], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[3], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[4], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[5], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[6], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
                      chunkPagePointsNumMerged, fullPaths[7], new long[] {100L, 200L, 300L});
                  putOnePageChunks(
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

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }
}
