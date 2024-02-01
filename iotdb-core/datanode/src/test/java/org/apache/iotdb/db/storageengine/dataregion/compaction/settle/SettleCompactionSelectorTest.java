package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SettleCompactionSelectorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
  }

  @Test
  public void testSelectContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(3, (seqTasks.get(0).getSelectedTsFileResourceList().size()));
    Assert.assertEquals(3, (unseqTasks.get(0).getSelectedTsFileResourceList().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(3, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(3, tsFileManager.getTsFileList(false).size());

    Assert.assertFalse(tsFileManager.getTsFileList(true).get(0).getModFile().exists());
    Assert.assertFalse(tsFileManager.getTsFileList(false).get(0).getModFile().exists());

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(2, (seqTasks.get(0).getSelectedTsFileResourceList().size()));
    Assert.assertEquals(2, (unseqTasks.get(0).getSelectedTsFileResourceList().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectUnContinuousFileWithLightSelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, false);

    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        generateModsFile(5, 10, Collections.singletonList(seqResources.get(i)), 0, 200);
        generateModsFile(5, 10, Collections.singletonList(unseqResources.get(i)), 0, 200);
      }
    }
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // settle task, not continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(false, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(
        3, (((SettleCompactionTask) seqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(0, (((SettleCompactionTask) seqTasks.get(0)).getAllDeletedFiles().size()));
    Assert.assertEquals(
        3, (((SettleCompactionTask) unseqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(
        0, (((SettleCompactionTask) unseqTasks.get(0)).getAllDeletedFiles().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(9, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(9, tsFileManager.getTsFileList(false).size());

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(1, unseqTasks.size());
    Assert.assertEquals(
        2, (((SettleCompactionTask) seqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(0, (((SettleCompactionTask) seqTasks.get(0)).getAllDeletedFiles().size()));
    Assert.assertEquals(
        2, (((SettleCompactionTask) unseqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(
        0, (((SettleCompactionTask) unseqTasks.get(0)).getAllDeletedFiles().size()));

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertEquals(9, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(9, tsFileManager.getTsFileList(false).size());
    for (int i = 0; i < 9; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  @Test
  public void testSelectContinuousFilesBaseOnModsSizeWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionTaskSelectionModsFileThreshold(1);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(2, unseqTasks.size());
    for (int i = 0; i < 2; i++) {
      if (i == 0) {
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      } else {
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      }
    }

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(unseqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select second time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add device mods
    for (TsFileResource resource : seqResources) {
      resource
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    for (TsFileResource resource : unseqResources) {
      resource
          .getModFile()
          .write(
              new Deletion(
                  new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0.**"),
                  Long.MAX_VALUE,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE));
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(2, unseqTasks.size());

    for (int i = 0; i < 2; i++) {
      if (i == 0) {
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      } else {
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      }
    }

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(unseqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on outdated too long
  @Test
  public void testSelectContinuousFileBaseOnDirtyDataOutdatedTooLongWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, true);
    createFiles(5, 5, 10, 200, 0, 0, 100, 100, false, false);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 100);
    generateModsFile(1, 10, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(1, 10, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    // inner task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    List<SettleCompactionTask> unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());

    // select second time
    // add the longest expired time
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(1000);
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(2, seqTasks.size());
    Assert.assertEquals(2, unseqTasks.size());

    for (int i = 0; i < 2; i++) {
      if (i == 0) {
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            3,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      } else {
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) seqTasks.get(i)).getSelectedTsFileResourceList().size()));
        Assert.assertEquals(
            2,
            (((InnerSpaceCompactionTask) unseqTasks.get(i))
                .getSelectedTsFileResourceList()
                .size()));
      }
    }

    Assert.assertTrue(seqTasks.get(0).start());
    Assert.assertTrue(unseqTasks.get(0).start());
    Assert.assertTrue(seqTasks.get(1).start());
    Assert.assertTrue(unseqTasks.get(1).start());
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());
    Assert.assertEquals(2, tsFileManager.getTsFileList(false).size());

    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(tsFileManager.getTsFileList(true).get(i).getModFile().exists());
      Assert.assertFalse(tsFileManager.getTsFileList(false).get(i).getModFile().exists());
    }

    // select third time
    seqTasks = settleSelector.selectSettleTask(seqResources);
    unseqTasks = settleSelector.selectSettleTask(unseqResources);
    Assert.assertEquals(0, seqTasks.size());
    Assert.assertEquals(0, unseqTasks.size());
  }

  // base on dirty data rate
  @Test
  public void testSelectUncontinuousFileBaseOnDirtyDataRateWithHeavySelect()
      throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setFileLimitPerInnerTask(3);
    IoTDBDescriptor.getInstance().getConfig().setLongestExpiredTime(Long.MAX_VALUE);
    createFiles(10, 5, 10, 200, 0, 0, 100, 100, false, true);

    generateModsFile(4, 10, seqResources, 0, 200);
    generateModsFile(4, 10, unseqResources, 0, 200);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0", 100);
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        for (int d = 1; d < 5; d++) {
          seqResources
              .get(i)
              .getModFile()
              .write(
                  new Deletion(
                      new PartialPath(
                          COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d" + d + ".**"),
                      Long.MAX_VALUE,
                      Long.MIN_VALUE,
                      Long.MAX_VALUE));
        }
      }
    }

    // settle task, continuous
    // select first time
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(true, COMPACTION_TEST_SG, "0", 0, tsFileManager);
    List<SettleCompactionTask> seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(
        0, (((SettleCompactionTask) seqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(5, (((SettleCompactionTask) seqTasks.get(0)).getAllDeletedFiles().size()));

    // select second time
    // add device mods
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 1) {

        seqResources
            .get(i)
            .getModFile()
            .write(
                new Deletion(
                    new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1.**"),
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    seqTasks = settleSelector.selectSettleTask(seqResources);
    Assert.assertEquals(1, seqTasks.size());
    Assert.assertEquals(
        5, (((SettleCompactionTask) seqTasks.get(0)).getPartialDeletedFileGroups().size()));
    Assert.assertEquals(5, (((SettleCompactionTask) seqTasks.get(0)).getAllDeletedFiles().size()));
  }
}
