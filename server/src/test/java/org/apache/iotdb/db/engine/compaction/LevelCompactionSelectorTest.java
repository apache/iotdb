package org.apache.iotdb.db.engine.compaction;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LevelCompactionSelectorTest extends LevelCompactionTest {

  File tempSGDir;

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

  /**
   * just compaction once
   */
  @Test
  public void testCompactionSelector() throws NoSuchFieldException, IllegalAccessException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement = new LevelCompactionTsFileManagement(
        COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    Field fieldForkedSequenceTsFileResources = LevelCompactionTsFileManagement.class
        .getDeclaredField("forkedSequenceTsFileResources");
    fieldForkedSequenceTsFileResources.setAccessible(true);
    List<TsFileResource> forkedSequenceTsFileResources = (List<TsFileResource>) fieldForkedSequenceTsFileResources
        .get(levelCompactionTsFileManagement);
    assertEquals(2, forkedSequenceTsFileResources.size());
  }
}
