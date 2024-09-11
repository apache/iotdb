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

package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.testStorageGroup;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.apache.tsfile.utils.TsFileGeneratorUtils.getDataType;

@RunWith(Parameterized.class)
public class SettleCompactionTaskTest extends AbstractCompactionTest {
  boolean originUseMultiType = TsFileGeneratorUtils.useMultiType;

  private String testModeName;

  private String performerType;

  private boolean isAligned;

  InnerSeqCompactionPerformer originSeqPerformer;
  InnerUnseqCompactionPerformer originUnseqPerformer;
  CrossCompactionPerformer originCrossPerformer;

  public SettleCompactionTaskTest(String name, String performerType, boolean isAligned) {
    this.testModeName = name;
    this.performerType = performerType;
    this.isAligned = isAligned;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"fast_nonAligned", "fast", false},
          {"fast_aligned", "fast", true},
          {"readChunk_nonAligned", "readchunk", false},
          {"readChunk_aligned", "readchunk", true},
          {"readPoint_nonAligned", "readpoint", false},
          {"readPoint_aligned", "readpoint", true}
        });
  }

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
    originSeqPerformer = IoTDBDescriptor.getInstance().getConfig().getInnerSeqCompactionPerformer();
    originUnseqPerformer =
        IoTDBDescriptor.getInstance().getConfig().getInnerUnseqCompactionPerformer();
    originCrossPerformer = IoTDBDescriptor.getInstance().getConfig().getCrossCompactionPerformer();
    if (performerType.equalsIgnoreCase("ReadChunk")) {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerSeqCompactionPerformer(InnerSeqCompactionPerformer.READ_CHUNK);
    } else if (performerType.equalsIgnoreCase("Fast")) {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerSeqCompactionPerformer(InnerSeqCompactionPerformer.FAST);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setInnerUnseqCompactionPerformer(InnerUnseqCompactionPerformer.FAST);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setCrossCompactionPerformer(CrossCompactionPerformer.FAST);
    }

    TsFileGeneratorUtils.useMultiType = true;
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
    TsFileGeneratorUtils.useMultiType = originUseMultiType;
    IoTDBDescriptor.getInstance().getConfig().setInnerSeqCompactionPerformer(originSeqPerformer);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerUnseqCompactionPerformer(originUnseqPerformer);
    IoTDBDescriptor.getInstance().getConfig().setCrossCompactionPerformer(originCrossPerformer);
  }

  @Test
  public void settleWithOnlyAllDirtyFilesByMods()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, isAligned, false);

    // set ttl
    DataNodeTTLCache.getInstance().setTTL("root.**", 1);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(6, seqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(0, seqTasks.get(0).getPartiallyDirtyFiles().size());
    Assert.assertEquals(5, unseqTasks.get(0).getFullyDirtyFiles().size());
    Assert.assertEquals(0, unseqTasks.get(0).getPartiallyDirtyFiles().size());

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources);
    allDeletedFiles.addAll(unseqResources);
    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyPartialDirtyFilesByMods()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250, isAligned);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850, isAligned);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 40, isAligned);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    SettleCompactionTask task =
        new SettleCompactionTask(
            0, tsFileManager, Collections.emptyList(), seqResources, true, getPerformer(), 0);
    SettleCompactionTask task2 =
        new SettleCompactionTask(
            0, tsFileManager, Collections.emptyList(), unseqResources, false, getPerformer(), 1);
    Assert.assertTrue(task.start());
    Assert.assertTrue(task2.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithMixedDirtyFilesByMods()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250, isAligned);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850, isAligned);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200, isAligned);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0, tsFileManager, allDeletedFiles, partialDeletedFiles, false, getPerformer(), 0);
    Assert.assertTrue(task.start());

    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());

    partialDeletedFiles.clear();
    partialDeletedFiles.addAll(seqResources);
    task =
        new SettleCompactionTask(
            0, tsFileManager, allDeletedFiles, partialDeletedFiles, true, getPerformer(), 0);
    Assert.assertTrue(task.start());

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyAllDirtyFilesByTTL()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateTTL(5, 10);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources);
    allDeletedFiles.addAll(unseqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0, tsFileManager, allDeletedFiles, Collections.emptyList(), true, getPerformer(), 0);
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyAllDirtyFilesByTTL2()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateTTL(5, 10);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> selectedFiles = new ArrayList<>(seqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0, tsFileManager, Collections.emptyList(), selectedFiles, true, getPerformer(), 0);
    Assert.assertTrue(task.start());

    selectedFiles.clear();
    selectedFiles.addAll(unseqResources);
    task =
        new SettleCompactionTask(
            0, tsFileManager, Collections.emptyList(), selectedFiles, false, getPerformer(), 0);
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyPartialDirtyFilesByTTL()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(5, 6, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateTTL(3, 50);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(seqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            Collections.emptyList(),
            partialDeletedFiles,
            true,
            getPerformer(),
            0);
    task.getEstimatedMemoryCost();
    Assert.assertTrue(task.start());

    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(0, tsFileManager, unseqResources, false, getPerformer(), 0);
    Assert.assertTrue(task2.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithMixedDirtyFilesByTTL()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 3, 10, 100, 0, 0, 0, 0, isAligned, true);
    createFiles(6, 6, 10, 100, 0, 0, 10000, 0, isAligned, true);
    createFiles(5, 6, 3, 50, 0, 10000, 50, 50, isAligned, false);

    generateTTL(3, 50);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, isAligned), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(seqResources.subList(3, 9));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources.subList(0, 3));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0, tsFileManager, allDeletedFiles, partialDeletedFiles, true, getPerformer(), 0);
    task.getEstimatedMemoryCost();
    Assert.assertTrue(task.start());

    InnerSpaceCompactionTask task2 =
        new InnerSpaceCompactionTask(0, tsFileManager, unseqResources, false, getPerformer(), 0);
    Assert.assertTrue(task2.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    DataNodeTTLCache.getInstance().clearAllTTL();
    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void testTaskEstimateMemory()
      throws IOException, MetadataException, WriteProcessException {
    ICompactionPerformer performer = getPerformer();
    if (performer instanceof ReadPointCompactionPerformer) {
      // not implemented
      return;
    }
    createFiles(3, 3, 10, 100, 0, 0, 0, 0, isAligned, true);
    SettleCompactionTask task1 =
        new SettleCompactionTask(
            0,
            tsFileManager,
            Collections.singletonList(seqResources.get(0)),
            Arrays.asList(seqResources.get(1), seqResources.get(2)),
            true,
            performer,
            0);

    SettleCompactionTask task2 =
        new SettleCompactionTask(
            0, tsFileManager, seqResources, Collections.emptyList(), true, performer, 0);

    SettleCompactionTask task3 =
        new SettleCompactionTask(
            0, tsFileManager, Collections.emptyList(), seqResources, true, performer, 0);
    Assert.assertTrue(task1.getEstimatedMemoryCost() > 0);
    Assert.assertEquals(0, task2.getEstimatedMemoryCost());
    Assert.assertTrue(task3.getEstimatedMemoryCost() > 0);
  }

  public static List<IFullPath> createTimeseries(
      int deviceNum, int measurementNum, boolean isAligned) throws IllegalPathException {
    List<IFullPath> timeseriesPath = new ArrayList<>();
    for (int d = 0; d < deviceNum; d++) {
      for (int i = 0; i < measurementNum; i++) {
        TSDataType dataType = getDataType(i);
        if (!isAligned) {
          timeseriesPath.add(
              new NonAlignedFullPath(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      testStorageGroup + PATH_SEPARATOR + "d" + d),
                  new MeasurementSchema("s" + i, dataType)));
        } else {
          timeseriesPath.add(
              new AlignedFullPath(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      testStorageGroup + PATH_SEPARATOR + "d" + (10000 + d)),
                  Collections.singletonList("s" + i),
                  Collections.singletonList(new MeasurementSchema("s" + i, dataType))));
        }
      }
    }

    return timeseriesPath;
  }

  protected void generateTTL(int deviceNum, long ttl) throws IllegalPathException {
    for (int dIndex = 0; dIndex < deviceNum; dIndex++) {
      DataNodeTTLCache.getInstance()
          .setTTL(
              COMPACTION_TEST_SG
                  + IoTDBConstant.PATH_SEPARATOR
                  + "d"
                  + (isAligned ? 10000 + dIndex : dIndex),
              ttl);
    }
  }

  private ICompactionPerformer getPerformer() {
    if (performerType.equalsIgnoreCase("ReadChunk")) {
      return new ReadChunkCompactionPerformer();
    } else if (performerType.equalsIgnoreCase("Fast")) {
      return new FastCompactionPerformer(false);
    } else {
      return new ReadPointCompactionPerformer();
    }
  }

  protected void generateModsFile(
      int deviceNum,
      int measurementNum,
      List<TsFileResource> resources,
      long startTime,
      long endTime,
      boolean isAligned)
      throws IllegalPathException, IOException {
    List<String> seriesPaths = new ArrayList<>();
    for (int dIndex = 0; dIndex < deviceNum; dIndex++) {
      for (int mIndex = 0; mIndex < measurementNum; mIndex++) {
        seriesPaths.add(
            COMPACTION_TEST_SG
                + IoTDBConstant.PATH_SEPARATOR
                + "d"
                + (isAligned ? 10000 + dIndex : dIndex)
                + IoTDBConstant.PATH_SEPARATOR
                + "s"
                + mIndex);
      }
    }
    generateModsFile(seriesPaths, resources, startTime, endTime);
  }
}
