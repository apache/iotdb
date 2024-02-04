package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;

public class SettleCompactionTaskTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
  }

  @Test
  public void settleWithOnlyAllDirtyFilesByMods()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources);
    allDeletedFiles.addAll(unseqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            Collections.emptyList(),
            true,
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyPartialDirtyFilesByMods()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 40);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(seqResources);
    partialDeletedFiles.addAll(unseqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            Collections.emptyList(),
            partialDeletedFiles,
            true,
            new FastCompactionPerformer(false),
            0);
    task.getEstimatedMemoryCost();
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithMixedDirtyFilesByMods()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(unseqResources.subList(2, 5));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(unseqResources.subList(0, 2));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            false,
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());

    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    validateTargetDatas(sourceDatas, Collections.emptyList());

    partialDeletedFiles.clear();
    partialDeletedFiles.addAll(seqResources);
    task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            true,
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyAllDirtyFilesByTTL()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateTTL(5, 10);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources);
    allDeletedFiles.addAll(unseqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            Collections.emptyList(),
            true,
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(0, tsFileManager.getTsFileList(false).size());

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyPartialDirtyFilesByTTL()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 6, 3, 50, 0, 10000, 50, 50, false, false);

    generateTTL(3, 50);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(seqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            Collections.emptyList(),
            partialDeletedFiles,
            true,
            new FastCompactionPerformer(false),
            0);
    task.getEstimatedMemoryCost();
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(5, tsFileManager.getTsFileList(false).size());

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithMixedDirtyFilesByTTL()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 3, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(6, 6, 10, 100, 0, 0, 10000, 0, false, true);
    createFiles(5, 6, 3, 50, 0, 10000, 50, 50, false, false);

    generateTTL(3, 50);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> partialDeletedFiles = new ArrayList<>();
    partialDeletedFiles.addAll(seqResources.subList(3, 9));

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources.subList(0, 3));

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            partialDeletedFiles,
            true,
            new FastCompactionPerformer(false),
            0);
    task.getEstimatedMemoryCost();
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.NORMAL, tsFileResource.getStatus());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(5, tsFileManager.getTsFileList(false).size());

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }
}
