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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossSpaceCompactionCandidate;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionTimeseriesType;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public class CrossSpaceCompactionWithFastPerformerTest {
  private final String oldThreadName = Thread.currentThread().getName();
  int index = 0;

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
  static final CompactionTimeseriesType[] compactionTimeseriesTypes =
      new CompactionTimeseriesType[] {
        CompactionTimeseriesType.ALL_SAME,
        CompactionTimeseriesType.PART_SAME,
        CompactionTimeseriesType.NO_SAME
      };
  static final boolean[] compactionBeforeHasMods = new boolean[] {true, false};
  static final boolean[] compactionHasMods = new boolean[] {true, false};

  @Before
  public void setUp() throws MetadataException {
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    CompactionTaskManager.getInstance().start();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    CompactionClearUtils.deleteEmptyDir(new File("target"));
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    CompactionTaskManager.getInstance().stop();
    EnvironmentUtils.cleanAllDir();
    Thread.currentThread().setName(oldThreadName);
    new CompactionConfigRestorer().restoreCompactionConfig();
  }

  // test one seq file overlaps with six unseq files with six type of relation(Contains, In,
  // Prefix-Overlap, Suffix-Overlap, Prefix-Non-Overlap, Suffix-Non-Overlap). While some timestamp
  // of timeseries in unseq files may later than seq files.
  @Test
  public void testOneSeqFileAndSixUnseqFile() throws Exception {
    for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
      for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
        for (boolean compactionHasMod : compactionHasMods) {
          // generate seq file
          List<TsFileResource> seqResources = new ArrayList<>();
          Set<String> fullPath;
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[14]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[14]);
          }
          List<List<Long>> chunkPagePointsNum = new ArrayList<>();
          List<Long> pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource tsFileResource =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(
              fullPath, chunkPagePointsNum, 2000L, tsFileResource);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(2000L, 2500L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, tsFileResource, false);
          }
          seqResources.add(tsFileResource);

          List<TsFileResource> unseqResources = new ArrayList<>();
          // unseq file with Contains relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile1 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3000L, unseqFile1);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(3000L, 3100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile1, false);
          }
          unseqResources.add(unseqFile1);
          // unseq file with In relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[2]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(2000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(2000L);
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile2 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 2, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 1000L, unseqFile2);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[2], new Pair<>(1000L, 1100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile2, false);
          }
          unseqResources.add(unseqFile2);
          // unseq file with Prefix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile3 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 3, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 1000L, unseqFile3);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[3], new Pair<>(1100L, 1200L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile3, false);
          }
          unseqResources.add(unseqFile3);
          // unseq file with Suffix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile4 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 4, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3000L, unseqFile4);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[4], new Pair<>(3100L, 3200L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile4, false);
          }
          unseqResources.add(unseqFile4);
          // unseq file with Prefix-Non-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
            fullPath.add(fullPaths[12]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile5 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 5, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 0L, unseqFile5);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[5], new Pair<>(0L, 100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile5, false);
          }
          unseqResources.add(unseqFile5);
          // unseq file with Suffix-Non-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
            fullPath.add(fullPaths[12]);
            fullPath.add(fullPaths[13]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[6]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource unseqFile6 =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 6, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 5000L, unseqFile6);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[5], new Pair<>(5000L, 5100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, unseqFile5, false);
          }
          unseqResources.add(unseqFile6);

          List<TsFileResource> sourceResources = new ArrayList<>(seqResources);
          sourceResources.addAll(unseqResources);
          Map<String, List<TimeValuePair>> sourceData =
              CompactionCheckerUtils.readFiles(sourceResources);
          if (compactionHasMod) {
            // seq mods
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(250L, 300L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, seqResources.get(0), true);
            // unseq mods
            toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(0L, 100L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, unseqResources.get(5), true);

            // remove data in source data list
            List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 250L && timeValuePair.getTimestamp() <= 300L);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 0L && timeValuePair.getTimestamp() <= 100L);
          }
          TsFileResourceList seqTsFileResourceList = new TsFileResourceList();
          seqTsFileResourceList.addAll(seqResources);
          TsFileResourceList unseqTsFileResourceList = new TsFileResourceList();
          unseqTsFileResourceList.addAll(unseqResources);
          long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
          CrossSpaceCompactionCandidate mergeResource =
              new CrossSpaceCompactionCandidate(
                  seqTsFileResourceList, unseqTsFileResourceList, timeLowerBound);
          RewriteCrossSpaceCompactionSelector selector =
              new RewriteCrossSpaceCompactionSelector("", "", 0, null);
          List<CrossCompactionTaskResource> selected =
              selector.selectCrossSpaceTask(seqTsFileResourceList, unseqTsFileResourceList);
          index++;
          if (selected.size() > 0) {
            AbstractCompactionTask compactionTask =
                new CrossSpaceCompactionTask(
                    0,
                    new TsFileManager(
                        "root.compactionTest",
                        "0",
                        "target\\data\\sequence\\test\\root.compactionTest\\0\\0\\"),
                    mergeResource.getSeqFiles(),
                    mergeResource.getUnseqFiles(),
                    new FastCompactionPerformer(true),
                    new AtomicInteger(0),
                    0,
                    0);
            compactionTask.start();
            List<TsFileResource> targetTsfileResourceList = new ArrayList<>();
            for (TsFileResource seqResource : seqResources) {
              TsFileResource targetResource =
                  new TsFileResource(
                      TsFileNameGenerator.increaseCrossCompactionCnt(seqResource).getTsFile());
              targetResource.deserialize();
              targetResource.setStatus(TsFileResourceStatus.CLOSED);
              targetTsfileResourceList.add(targetResource);
            }
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsfileResourceList);
            CompactionClearUtils.clearAllCompactionFiles();
          } else {
            fail();
          }
        }
      }
    }
  }

  // test five seq files overlaps with one unseq file with five type of relation(Contains,
  // Prefix-Overlap, Suffix-Overlap, Prefix-Non-Overlap, Suffix-Non-Overlap). Seq files do not have
  // device d1.
  @Test
  public void testFiveSeqFileAndOneUnseqFileWithSomeDeviceNotInSeqFiles() throws Exception {
    for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
      for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
        for (boolean compactionHasMod : compactionHasMods) {
          // unseq file
          List<TsFileResource> unseqResources = new ArrayList<>();
          Set<String> fullPath;
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
          }
          List<List<Long>> chunkPagePointsNum = new ArrayList<>();
          List<Long> pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource tsFileResource =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(
              fullPath, chunkPagePointsNum, 2000L, tsFileResource);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(2000L, 2500L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, tsFileResource, false);
          }
          unseqResources.add(tsFileResource);

          // seq file with Prefix-Non-Overlap relation
          List<TsFileResource> seqResources = new ArrayList<>();
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[2]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile1 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 0L, seqFile1);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(0L, 25L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile1, false);
          }
          seqResources.add(seqFile1);

          // seq file with Prefix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[14]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[14]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile2 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 2, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 1000L, seqFile2);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(3000L, 3100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile2, false);
          }
          seqResources.add(seqFile2);

          // seq file with Contains relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile3 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 3, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3500L, seqFile3);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[3], new Pair<>(3500L, 3525L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile3, false);
          }
          seqResources.add(seqFile3);

          // seq file with Suffix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile4 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 4, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3750L, seqFile4);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[4], new Pair<>(3800L, 3900L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile4, false);
          }
          seqResources.add(seqFile4);

          // seq file with Suffix-Non-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
            fullPath.add(fullPaths[12]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile5 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 5, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 6250L, seqFile5);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[5], new Pair<>(6300L, 6400L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile5, false);
          }
          seqResources.add(seqFile5);

          List<TsFileResource> sourceResources = new ArrayList<>(unseqResources);
          for (int i = 1; i < 4; i++) {
            sourceResources.add(seqResources.get(i));
          }
          Map<String, List<TimeValuePair>> sourceData =
              CompactionCheckerUtils.readFiles(sourceResources);
          if (compactionHasMod) {
            // unseq mods
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(2500L, 2600L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, unseqResources.get(0), true);
            // seq mods
            toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(0L, 100L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, seqResources.get(0), true);

            // remove data in source data list
            List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 2500L && timeValuePair.getTimestamp() <= 2600L);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 0L && timeValuePair.getTimestamp() <= 100L);
          }
          TsFileResourceList seqTsFileResourceList = new TsFileResourceList();
          seqTsFileResourceList.addAll(seqResources);
          TsFileResourceList unseqTsFileResourceList = new TsFileResourceList();
          unseqTsFileResourceList.addAll(unseqResources);
          long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
          CrossSpaceCompactionCandidate mergeResource =
              new CrossSpaceCompactionCandidate(
                  seqTsFileResourceList, unseqTsFileResourceList, timeLowerBound);
          RewriteCrossSpaceCompactionSelector selector =
              new RewriteCrossSpaceCompactionSelector("", "", 0, null);
          List<CrossCompactionTaskResource> selected =
              selector.selectCrossSpaceTask(seqTsFileResourceList, unseqTsFileResourceList);
          if (selected.size() > 0) {
            AbstractCompactionTask compactionTask =
                new CrossSpaceCompactionTask(
                    0,
                    new TsFileManager(
                        "root.compactionTest",
                        "0",
                        "target\\data\\sequence\\test\\root.compactionTest\\0\\0\\"),
                    mergeResource.getSeqFiles(),
                    mergeResource.getUnseqFiles(),
                    new FastCompactionPerformer(true),
                    new AtomicInteger(0),
                    0,
                    0);
            compactionTask.start();
            List<TsFileResource> targetTsfileResourceList = new ArrayList<>();
            for (TsFileResource seqResource : seqResources.subList(1, 4)) {
              TsFileResource targetResource =
                  new TsFileResource(
                      TsFileNameGenerator.increaseCrossCompactionCnt(seqResource).getTsFile());
              targetResource.deserialize();
              targetResource.setStatus(TsFileResourceStatus.CLOSED);
              targetTsfileResourceList.add(targetResource);
            }
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsfileResourceList);
            CompactionClearUtils.clearAllCompactionFiles();
          } else {
            fail();
          }
        }
      }
    }
  }

  // test five seq files overlaps with one unseq file with five type of relation(Contains,
  // Prefix-Overlap, Suffix-Overlap, Prefix-Non-Overlap, Suffix-Non-Overlap)
  @Test
  public void testFiveSeqFileAndOneUnseqFile() throws Exception {
    for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
      for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
        for (boolean compactionHasMod : compactionHasMods) {
          // unseq file
          List<TsFileResource> unseqResources = new ArrayList<>();
          Set<String> fullPath;
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
          }
          List<List<Long>> chunkPagePointsNum = new ArrayList<>();
          List<Long> pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource tsFileResource =
              CompactionFileGeneratorUtils.generateTsFileResource(false, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(
              fullPath, chunkPagePointsNum, 2000L, tsFileResource);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(2000L, 2500L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, tsFileResource, false);
          }
          unseqResources.add(tsFileResource);

          // seq file with Prefix-Non-Overlap relation
          List<TsFileResource> seqResources = new ArrayList<>();
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[14]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[0]);
            fullPath.add(fullPaths[14]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile1 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 1, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 0L, seqFile1);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(0L, 25L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile1, false);
          }
          seqResources.add(seqFile1);

          // seq file with Prefix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[2]);
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[1]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile2 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 2, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 1000L, seqFile2);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(3000L, 3100L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile2, false);
          }
          seqResources.add(seqFile2);

          // seq file with Contains relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[3]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(100L);
          pagePointsNum.add(50L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile3 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 3, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3500L, seqFile3);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[3], new Pair<>(3500L, 3525L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile3, false);
          }
          seqResources.add(seqFile3);

          // seq file with Suffix-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[4]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile4 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 4, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 3750L, seqFile4);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[4], new Pair<>(3800L, 3900L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile4, false);
          }
          seqResources.add(seqFile4);

          // seq file with Suffix-Non-Overlap relation
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            fullPath = new HashSet<>(Arrays.asList(fullPaths));
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
            fullPath.add(fullPaths[6]);
            fullPath.add(fullPaths[7]);
            fullPath.add(fullPaths[8]);
            fullPath.add(fullPaths[9]);
            fullPath.add(fullPaths[10]);
            fullPath.add(fullPaths[11]);
            fullPath.add(fullPaths[12]);
          } else {
            fullPath = new HashSet<>();
            fullPath.add(fullPaths[5]);
          }
          chunkPagePointsNum = new ArrayList<>();
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          chunkPagePointsNum.add(pagePointsNum);
          pagePointsNum = new ArrayList<>();
          pagePointsNum.add(1000L);
          pagePointsNum.add(500L);
          chunkPagePointsNum.add(pagePointsNum);
          TsFileResource seqFile5 =
              CompactionFileGeneratorUtils.generateTsFileResource(true, 5, COMPACTION_TEST_SG);
          CompactionFileGeneratorUtils.writeTsFile(fullPath, chunkPagePointsNum, 6250L, seqFile5);
          // has mods files before compaction
          if (compactionBeforeHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[5], new Pair<>(6300L, 6400L));
            CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, seqFile5, false);
          }
          seqResources.add(seqFile5);

          List<TsFileResource> sourceResources = new ArrayList<>(unseqResources);
          for (int i = 1; i < 4; i++) {
            sourceResources.add(seqResources.get(i));
          }
          Map<String, List<TimeValuePair>> sourceData =
              CompactionCheckerUtils.readFiles(sourceResources);
          if (compactionHasMod) {
            // unseq mods
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(2500L, 2600L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, unseqResources.get(0), true);
            // seq mods
            toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(0L, 100L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, seqResources.get(0), true);

            // remove data in source data list
            List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 2500L && timeValuePair.getTimestamp() <= 2600L);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 0L && timeValuePair.getTimestamp() <= 100L);
          }
          TsFileResourceList seqTsFileResourceList = new TsFileResourceList();
          seqTsFileResourceList.addAll(seqResources);
          TsFileResourceList unseqTsFileResourceList = new TsFileResourceList();
          unseqTsFileResourceList.addAll(unseqResources);
          long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
          CrossSpaceCompactionCandidate mergeResource =
              new CrossSpaceCompactionCandidate(
                  seqTsFileResourceList, unseqTsFileResourceList, timeLowerBound);
          RewriteCrossSpaceCompactionSelector selector =
              new RewriteCrossSpaceCompactionSelector("", "", 0, null);
          List<CrossCompactionTaskResource> selected =
              selector.selectCrossSpaceTask(seqTsFileResourceList, unseqTsFileResourceList);
          if (selected.size() > 0) {
            AbstractCompactionTask compactionTask =
                new CrossSpaceCompactionTask(
                    0,
                    new TsFileManager(
                        "root.compactionTest",
                        "0",
                        "target\\data\\sequence\\test\\root.compactionTest\\0\\0\\"),
                    mergeResource.getSeqFiles(),
                    mergeResource.getUnseqFiles(),
                    new FastCompactionPerformer(true),
                    new AtomicInteger(0),
                    0,
                    0);
            compactionTask.start();
            List<TsFileResource> targetTsfileResourceList = new ArrayList<>();
            for (TsFileResource seqResource : seqResources.subList(1, 4)) {
              TsFileResource targetResource =
                  new TsFileResource(
                      TsFileNameGenerator.increaseCrossCompactionCnt(seqResource).getTsFile());
              targetResource.deserialize();
              targetResource.setStatus(TsFileResourceStatus.CLOSED);
              targetTsfileResourceList.add(targetResource);
            }
            CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsfileResourceList);
            CompactionClearUtils.clearAllCompactionFiles();
          } else {
            fail();
          }
        }
      }
    }
  }
}
