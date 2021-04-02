package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LevelCompactionVectorMergeTest extends LevelCompactionVectorTest {
  File tempSGDir;
  boolean compactionMergeWorking = false;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  /** just compaction once */
  @Test
  public void testCompactionMergeOnce() {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.run();
    while (compactionMergeWorking) {
      // wait
    }
  }

  /** just compaction stable list */
  @Test
  public void testCompactionMergeStableList() {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(2);
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.run();
    while (compactionMergeWorking) {
      // wait
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack() {
    this.compactionMergeWorking = false;
  }
}
