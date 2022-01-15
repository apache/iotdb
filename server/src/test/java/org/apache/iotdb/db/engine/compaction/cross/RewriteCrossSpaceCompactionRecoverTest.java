package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;

public class RewriteCrossSpaceCompactionRecoverTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  @Test
  public void testRecoverWithAllSourceFilesExisted() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.MERGE_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            null,
            null,
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
    // all source file should still exist
    for (TsFileResource resource : seqResources) {
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
    // tmp target file, target file and target resource file should be deleted
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                          TsFileConstant.TSFILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
  }

  @Test
  public void testRecoverWithSomeSourceFilesExisted() throws Exception {
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);
    TsFileManager tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", SEQ_DIRS.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    File compactionLogFile =
        new File(
            SEQ_DIRS,
            seqResources.get(0).getTsFile().getName()
                + "."
                + RewriteCrossSpaceCompactionLogger.MERGE_LOG_NAME);
    RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(compactionLogFile);
    compactionLogger.logFiles(targetResources, STR_TARGET_FILES);
    compactionLogger.logFiles(seqResources, STR_SEQ_FILES);
    compactionLogger.logFiles(unseqResources, STR_UNSEQ_FILES);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);
    seqResources.get(0).getTsFile().delete();
    compactionLogger.logStringInfo(MAGIC_STRING);
    compactionLogger.close();
    new RewriteCrossCompactionRecoverTask(
            COMPACTION_LOG_NAME,
            "0",
            0,
            SEQ_DIRS.getPath(),
            null,
            null,
            compactionLogFile,
            CompactionTaskManager.currentTaskNum,
            tsFileManager)
        .call();
    // all source file should not exist
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
    }
    // tmp target file and tmp target resource file should not exist, target file and target
    // resource file should exist
    for (TsFileResource resource : targetResources) {
      Assert.assertFalse(
          new File(
                  resource
                      .getTsFilePath()
                      .replace(
                          TsFileConstant.TSFILE_SUFFIX,
                          IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX))
              .exists());
      Assert.assertFalse(
          new File(
                  resource
                          .getTsFilePath()
                          .replace(
                              TsFileConstant.TSFILE_SUFFIX,
                              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)
                      + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      Assert.assertTrue(resource.getTsFile().exists());
      Assert.assertTrue(
          new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
  }
}
