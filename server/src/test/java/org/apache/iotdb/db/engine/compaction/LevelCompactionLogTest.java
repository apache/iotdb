package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LevelCompactionLogTest extends LevelCompactionTest {

  File tempSGDir;
  boolean compactionMergeWorking = false;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testCompactionLog() {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement = new LevelCompactionTsFileManagement(
        COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask = levelCompactionTsFileManagement.new CompactionMergeTask(
        this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.run();
    while (compactionMergeWorking) {
      //wait
    }
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME);
    assertFalse(logFile.exists());
  }

  /**
   * close compaction merge callback, to release some locks
   */
  private void closeCompactionMergeCallBack() {
    this.compactionMergeWorking = false;
  }
}
