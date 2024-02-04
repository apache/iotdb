package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class SettleCompactionRecoverTest extends AbstractCompactionTest {
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

  // region Handle exception

  @Test
  public void handExceptionWhenSettlingAllDeletedFiles()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(3, 3, seqResources.subList(0, 3), 0, 250);
    generateModsFile(3, 3, seqResources.subList(3, 6), 500, 850);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, 200);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

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

    // delete the first all_deleted file
    task.setRecoverMemoryStatus(true);
    allDeletedFiles.get(0).getTsFile().delete();

    // add compaction mods
    generateDeviceCompactionMods(3);

    // handle exception, delete all_deleted files
    task.recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.modFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    Assert.assertEquals(1, tsFileManager.getTsFileList(false).size());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void handExceptionWhenSettlingPartialDeletedFilesWithAllSourceFileExisted()
      throws Exception {
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
    // add compaction mods
    generateDeviceCompactionMods(3);

    // finish to settle all_deleted files and settle the first partial_deleted group
    task.setRecoverMemoryStatus(true);
    task.settleWithAllDeletedFiles();
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    TsFileResource targetResource =
        TsFileNameGenerator.getSettleCompactionTargetFileResources(partialDeletedFiles, false);
    File logFile =
        new File(
            targetResource.getTsFilePath() + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      compactionLogger.logSourceFiles(partialDeletedFiles);
      compactionLogger.logTargetFile(targetResource);
      compactionLogger.force();

      // carry out the compaction
      performer.setSourceFiles(partialDeletedFiles);
      // As elements in targetFiles may be removed in performer, we should use a mutable list
      // instead of Collections.singletonList()
      performer.setTargetFiles(Collections.singletonList(targetResource));
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
    }

    // handle exception, delete all_deleted files
    task.recover();

    for (TsFileResource resource : allDeletedFiles) {
      Assert.assertFalse(resource.tsFileExists());
      Assert.assertFalse(resource.modFileExists());
      Assert.assertFalse(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());
    Assert.assertEquals(6, tsFileManager.getTsFileList(true).size());
    // resource file exist
    for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.modFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
      Assert.assertTrue(resource.tsFileExists());
      Assert.assertTrue(resource.modFileExists());
      Assert.assertTrue(resource.resourceFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }

    // target resource not exist
    Assert.assertFalse(targetResource.resourceFileExists());
    Assert.assertFalse(targetResource.tsFileExists());
    Assert.assertFalse(targetResource.modFileExists());
  }

  @Test
  public void handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLosted() {}

  @Test
  public void
      handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndEmptytargetFile() {}

  @Test
  public void
      handExceptionWhenSettlingPartialDeletedFilesWithSomeSourceFileLostedAndTargetFileLosted() {}

  // endregion

  // region recover

  // endregion

  private void generateDeviceCompactionMods(int deviceNum)
      throws IllegalPathException, IOException {
    Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
    for (int d = 0; d < deviceNum; d++) {
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "**",
          new Pair(Long.MIN_VALUE, Long.MAX_VALUE));
    }
    for (TsFileResource resource : seqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, true);
    }
    for (TsFileResource resource : unseqResources) {
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, true);
    }
  }
}
