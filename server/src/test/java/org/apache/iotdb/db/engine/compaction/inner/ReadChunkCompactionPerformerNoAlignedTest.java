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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to test InnerSpaceCompactionUtils.compact. Notice, it just tests not aligned
 * timeseries.
 */
public class ReadChunkCompactionPerformerNoAlignedTest {
  private final String storageGroup = "root.compactionTest";
  private final String[] devices = new String[] {"device0", "device1", "device2", "device3"};
  private PartialPath[] devicePath = new PartialPath[devices.length];
  private final String[] measurements = new String[] {"s0", "s1", "s2", "s3", "s4"};
  private Set<String> fullPathSet = new HashSet<>();
  private MeasurementSchema[] schemas = new MeasurementSchema[measurements.length];
  private List<PartialPath> paths = new ArrayList<>();
  private List<IMeasurementSchema> schemaList = new ArrayList<>();

  private static File tempSGDir;
  private static String SEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "sequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";
  private static String UNSEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "unsequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";

  private final ICompactionPerformer performer = new ReadChunkCompactionPerformer();

  @Before
  public void setUp() throws Exception {
    tempSGDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                + "data"
                + File.separator
                + "sequence"
                + File.separator
                + "root.compactionTest");
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    if (!new File(SEQ_DIRS).exists()) {
      Assert.assertTrue(new File(SEQ_DIRS).mkdirs());
    }
    if (!new File(UNSEQ_DIRS).exists()) {
      Assert.assertTrue(new File(UNSEQ_DIRS).mkdirs());
    }
    createTimeseries();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  @After
  public void tearDown() throws Exception {
    new CompactionConfigRestorer().restoreCompactionConfig();
    if (new File(SEQ_DIRS).exists()) {
      FileUtils.forceDelete(new File(SEQ_DIRS));
    }
    if (new File(UNSEQ_DIRS).exists()) {
      FileUtils.forceDelete(new File(UNSEQ_DIRS));
    }
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  private void createTimeseries() throws MetadataException {
    for (int i = 0; i < measurements.length; ++i) {
      schemas[i] =
          new MeasurementSchema(
              measurements[i], TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
    }
    for (int i = 0; i < devices.length; ++i) {
      devicePath[i] = new PartialPath(storageGroup + "." + devices[i]);
    }
    for (PartialPath device : devicePath) {
      for (MeasurementSchema schema : schemas) {
        fullPathSet.add(device.getFullPath() + "." + schema.getMeasurementId());
        paths.add(
            new MeasurementPath(
                new PartialPath(device.getFullPath() + "." + schema.getMeasurementId()), schema));
        schemaList.add(schema);
      }
    }
  }

  /**
   * Generate files that each chunk is greater than target size, so it can be read into memory and
   * directly flush to target files.
   *
   * @throws Exception
   */
  @Test
  public void testDirectlyFlushChunk() throws Exception {
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(100);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 200L;
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1L) * pointStep);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      long[] points = new long[fileNum];
      for (int i = 1; i <= fileNum; i++) {
        points[i - 1] = i * pointStep;
      }
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunks(chunkPagePointsNumMerged, path, points);
      }
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
    }
  }

  /**
   * Generate some middle chunk that should be merged and cached in memory, and a large chunk that
   * is larger than target size. The latter should be merged with previously cached chunk and
   * flushed.
   *
   * @throws Exception
   */
  @Test
  public void testLargeChunkMergeWithCacheChunkAndFlush() throws Exception {
    long testTargetChunkPointNum = 2000L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 100L;
      long[] points = new long[fileNum];
      for (int i = 0; i < fileNum - 1; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1) * pointStep);
        points[i] = (i + 1) * pointStep;
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(testTargetChunkPointNum + 100L);
      points[fileNum - 1] = testTargetChunkPointNum + 100L;
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource resource =
          new TsFileResource(
              new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", fileNum, fileNum)));
      sourceFiles.add(resource);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPathSet, chunkPagePointsNum, fileNum * testTargetChunkPointNum, resource);

      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putChunk(chunkPagePointsNumMerged, path, points);
      }
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  /**
   * Generate some small data that are less than lower bound, and a chunk larger than target size.
   * The large chunk will be deserialized into points and written to the chunk writer.
   *
   * @throws Exception
   */
  @Test
  public void testLargeChunkDeserializeIntoPoint() throws Exception {
    long testTargetChunkPointNum = 2000L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 10L;
      long[] points = new long[fileNum];
      for (int i = 0; i < fileNum - 1; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1) * pointStep);
        points[i] = (i + 1) * pointStep;
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(testTargetChunkPointNum + 100L);
      points[fileNum - 1] = testTargetChunkPointNum + 100L;
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource resource =
          new TsFileResource(
              new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", fileNum, fileNum)));
      sourceFiles.add(resource);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPathSet, chunkPagePointsNum, fileNum * testTargetChunkPointNum, resource);

      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(
            chunkPagePointsNumMerged,
            path,
            (fileNum - 1) * fileNum * pointStep / 2 + testTargetChunkPointNum + 100L);
      }
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  /**
   * Generate files that chunk are smaller than target chunk point num but greater than lower bound,
   * and the chunk will be merged.
   *
   * @throws Exception
   */
  @Test
  public void testMergeChunk() throws Exception {
    long testTargetChunkPointNum = 1000L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(10240);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkSizeLowerBoundInCompaction(1);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance().getConfig().setChunkPointNumLowerBoundInCompaction(1);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      Set<String> fullPathSetWithDeleted = new HashSet<>(fullPathSet);
      // we add a deleted timeseries to simulate timeseries is deleted before compaction.
      String deletedPath = "root.compactionTest.device999.s999";
      fullPathSetWithDeleted.add(deletedPath);
      int fileNum = 6;
      long pointStep = 300L;
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1L) * pointStep);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSetWithDeleted, chunkPagePointsNum, i * 1500L, resource);
      }
      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      List<List<Long>> chunkPointsArray = new ArrayList<>();
      List<Long> pointsArray = new ArrayList<>();
      long curPointNum = 0L;
      for (int i = 0; i < fileNum; ++i) {
        curPointNum += (i + 1L) * pointStep;
        pointsArray.add((i + 1L) * pointStep);
        if (curPointNum > testTargetChunkPointNum) {
          chunkPointsArray.add(pointsArray);
          pointsArray = new ArrayList<>();
          curPointNum = 0;
        }
      }
      if (curPointNum > 0) {
        chunkPointsArray.add(pointsArray);
      }
      for (String path : fullPathSetWithDeleted) {
        chunkPagePointsNumMerged.put(path, chunkPointsArray);
      }
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  /**
   * Generate chunk that size are less than lower bound, and they will be deserialized and written
   * into chunk writer. Then generate a middle size chunk, which will be deserialized and written
   * into chunk writer because there are remaining points in current chunk writer.
   *
   * @throws Exception
   */
  @Test
  public void testMiddleChunkDeserialize() throws Exception {
    long testTargetChunkPointNum = 2000L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 10L;
      long[] points = new long[fileNum];
      for (int i = 0; i < fileNum - 1; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1) * pointStep);
        points[i] = (i + 1) * pointStep;
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(testTargetChunkPointNum - 100L);
      points[fileNum - 1] = testTargetChunkPointNum - 100L;
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource resource =
          new TsFileResource(
              new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", fileNum, fileNum)));
      sourceFiles.add(resource);
      CompactionFileGeneratorUtils.writeTsFile(
          fullPathSet, chunkPagePointsNum, fileNum * testTargetChunkPointNum, resource);

      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(
            chunkPagePointsNumMerged,
            path,
            (fileNum - 1) * fileNum * pointStep / 2 + testTargetChunkPointNum - 100L);
      }
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  /**
   * Generate files that chunk are less than chunk point num lower bound, the chunk will be
   * deserialized into points and written into in ChunkWriter.
   */
  @Test
  public void testDeserializePage() throws Exception {
    long testTargetChunkPointNum = 1500L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 1000L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(10240);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 50L;
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((i + 1L) * pointStep);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      List<List<Long>> chunkPointsArray = new ArrayList<>();
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(
            chunkPagePointsNumMerged, path, fileNum * (fileNum + 1) * pointStep / 2);
      }
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  /**
   * Generate some chunks are less than the target size and greater than the lower bound, and some
   * chunks that are less than the lower bound. So the chunk will be merged first, and the cached
   * chunk will be deserialized into points later.
   *
   * @throws Exception
   */
  @Test
  public void testDeserializeCachedChunk() throws Exception {
    long testTargetChunkPointNum = 1500L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(10240);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 6;
      long pointStep = 50L;
      // 300 250 200 150 100 50
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add((fileNum - i) * pointStep);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 1500L, resource);
      }
      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      List<List<Long>> chunkPointsArray = new ArrayList<>();
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(
            chunkPagePointsNumMerged, path, fileNum * (fileNum + 1) * pointStep / 2);
      }
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  @Test
  public void testMixCompact1() throws Exception {
    long testTargetChunkPointNum = 2000L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 12;
      long[] points = new long[] {100, 200, 300, 50, 2100, 50, 600, 2300, 2500, 1000, 500, 500};
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(points[i]);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 2500L, resource);
      }

      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2750);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2950);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2500);
        CompactionCheckerUtils.putChunk(
            chunkPagePointsNumMerged, path, new long[] {1000, 500, 500});
      }
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }

  @Test
  public void testMixCompact2() throws Exception {
    long testTargetChunkPointNum = 2000L;
    long testChunkSizeLowerBound = 1024L;
    long testChunkPointNumLowerBound = 100L;
    long originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    long originTargetChunkPointNum =
        IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(testTargetChunkPointNum);
    long originChunkSizeLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkSizeLowerBoundInCompaction(testChunkSizeLowerBound);
    long originChunkPointNumLowerBound =
        IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setChunkPointNumLowerBoundInCompaction(testChunkPointNumLowerBound);
    try {
      List<TsFileResource> sourceFiles = new ArrayList();
      int fileNum = 12;
      long pointStep = 10L;
      long[] points = new long[] {1960, 50, 1960, 50, 2100, 50, 1960, 2300, 2500, 1000, 500, 500};
      for (int i = 0; i < fileNum; ++i) {
        List<List<Long>> chunkPagePointsNum = new ArrayList<>();
        List<Long> pagePointsNum = new ArrayList<>();
        pagePointsNum.add(points[i]);
        chunkPagePointsNum.add(pagePointsNum);
        TsFileResource resource =
            new TsFileResource(new File(SEQ_DIRS, String.format("%d-%d-0-0.tsfile", i + 1, i + 1)));
        sourceFiles.add(resource);
        CompactionFileGeneratorUtils.writeTsFile(
            fullPathSet, chunkPagePointsNum, i * 2500L, resource);
      }

      Map<PartialPath, List<TimeValuePair>> originData =
          CompactionCheckerUtils.getDataByQuery(paths, schemaList, sourceFiles, new ArrayList<>());
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(sourceFiles.get(0).getTsFile().getName());
      TsFileResource targetResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(sourceFiles, true);
      performer.setSourceFiles(sourceFiles);
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new CompactionTaskSummary());
      performer.perform();
      CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
      Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
      // outer list is a chunk, inner list is point num in each page
      for (String path : fullPathSet) {
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2010);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2010);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2100);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2010);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2300);
        CompactionCheckerUtils.putOnePageChunk(chunkPagePointsNumMerged, path, 2500);
        CompactionCheckerUtils.putChunk(
            chunkPagePointsNumMerged, path, new long[] {1000, 500, 500});
      }
      Map<PartialPath, List<TimeValuePair>> compactedData =
          CompactionCheckerUtils.getDataByQuery(
              paths, schemaList, Collections.singletonList(targetResource), new ArrayList<>());
      CompactionCheckerUtils.validDataByValueList(originData, compactedData);
      CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetResource);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
      IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkSizeLowerBoundInCompaction(originChunkSizeLowerBound);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setChunkPointNumLowerBoundInCompaction(originChunkPointNumLowerBound);
    }
  }
}
