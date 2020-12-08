package org.apache.iotdb.db.engine.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.no.NoCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NoCompactionTsFileManagementTest extends LevelCompactionTest {

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
  public void testAddRemoveAndIterator() {
    NoCompactionTsFileManagement noCompactionTsFileManagement = new NoCompactionTsFileManagement(
        COMPACTION_TEST_SG, tempSGDir.getPath());
    for (TsFileResource tsFileResource : seqResources) {
      noCompactionTsFileManagement.add(tsFileResource, true);
    }
    noCompactionTsFileManagement.addAll(seqResources, false);
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(true).size());
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(false).size());
    assertEquals(6, noCompactionTsFileManagement.size(true));
    assertEquals(6, noCompactionTsFileManagement.size(false));
    assertTrue(noCompactionTsFileManagement.contains(seqResources.get(0), true));
    assertFalse(noCompactionTsFileManagement.contains(new TsFileResource(new File(
        TestConstant.BASE_OUTPUT_PATH.concat(
            10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10 + IoTDBConstant.FILE_NAME_SEPARATOR + 0
                + ".tsfile"))), false));
    assertTrue(noCompactionTsFileManagement.contains(seqResources.get(0), false));
    assertFalse(noCompactionTsFileManagement.contains(new TsFileResource(new File(
        TestConstant.BASE_OUTPUT_PATH.concat(
            10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10 + IoTDBConstant.FILE_NAME_SEPARATOR + 0
                + ".tsfile"))), false));
    assertFalse(noCompactionTsFileManagement.isEmpty(true));
    assertFalse(noCompactionTsFileManagement.isEmpty(false));
    noCompactionTsFileManagement
        .remove(noCompactionTsFileManagement.getTsFileList(true).get(0), true);
    noCompactionTsFileManagement
        .remove(noCompactionTsFileManagement.getTsFileList(false).get(0), false);
    assertEquals(5, noCompactionTsFileManagement.getTsFileList(true).size());
    assertEquals(5, noCompactionTsFileManagement.getStableTsFileList(false).size());
    noCompactionTsFileManagement
        .removeAll(noCompactionTsFileManagement.getTsFileList(false), false);
    assertEquals(0, noCompactionTsFileManagement.getTsFileList(false).size());
    long count = 0;
    Iterator<TsFileResource> iterator = noCompactionTsFileManagement.getIterator(true);
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(5, count);
    noCompactionTsFileManagement
        .removeAll(noCompactionTsFileManagement.getTsFileList(true), true);
    assertEquals(0, noCompactionTsFileManagement.getTsFileList(true).size());
    assertTrue(noCompactionTsFileManagement.isEmpty(true));
    assertTrue(noCompactionTsFileManagement.isEmpty(false));
    noCompactionTsFileManagement.add(new TsFileResource(new File(
        TestConstant.BASE_OUTPUT_PATH.concat(
            10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10
                + ".tsfile"))), true);
    noCompactionTsFileManagement.add(new TsFileResource(new File(
        TestConstant.BASE_OUTPUT_PATH.concat(
            10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10 + IoTDBConstant.FILE_NAME_SEPARATOR + 10
                + ".tsfile"))), false);
    noCompactionTsFileManagement.forkCurrentFileList(0);
    noCompactionTsFileManagement.recover();
    CompactionMergeTask compactionMergeTask = noCompactionTsFileManagement.new CompactionMergeTask(
        () -> {
        }, 0);
    compactionMergeTask.run();
    assertEquals(1, noCompactionTsFileManagement.size(true));
    assertEquals(1, noCompactionTsFileManagement.size(false));
    noCompactionTsFileManagement.clear();
    assertEquals(0, noCompactionTsFileManagement.size(true));
    assertEquals(0, noCompactionTsFileManagement.size(false));
  }
}