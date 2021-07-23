package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CompactionSchedulerTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device0.sensor1",
        COMPACTION_TEST_SG + ".device0.sensor2",
        COMPACTION_TEST_SG + ".device0.sensor3",
        COMPACTION_TEST_SG + ".device0.sensor4",
        COMPACTION_TEST_SG + ".device0.sensor5",
        COMPACTION_TEST_SG + ".device0.sensor6",
        COMPACTION_TEST_SG + ".device0.sensor7",
        COMPACTION_TEST_SG + ".device0.sensor8",
        COMPACTION_TEST_SG + ".device0.sensor9",
        COMPACTION_TEST_SG + ".device1.sensor0",
        COMPACTION_TEST_SG + ".device1.sensor1",
        COMPACTION_TEST_SG + ".device1.sensor2",
        COMPACTION_TEST_SG + ".device1.sensor3",
        COMPACTION_TEST_SG + ".device1.sensor4",
      };

  @Before
  public void setUp() throws MetadataException {
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String fullPath : fullPaths) {
      PartialPath path = new PartialPath(fullPath);
      IoTDB.metaManager.createTimeseries(
          path,
          TSDataType.INT64,
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
    MergeManager.getINSTANCE().start();
    CompactionTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    CompactionTaskManager.getInstance().stop();
    MergeManager.getINSTANCE().stop();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test1() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    while (tsFileResourceManager.getTsFileList(false).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test2() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test3() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(1, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=100
   */
  @Test
  public void test4() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test5() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test6() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test7() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 1) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=100
   */
  @Test
  public void test8() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(100);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test9() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 50) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 75) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(25, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test10() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 50) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test11() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 50) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }

  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=50 max_compaction_candidate_file_num=2
   */
  @Test
  public void test12() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(50);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 98) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 96) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test13() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 99) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 98) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=true
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test14() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 99) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 98) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=true enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test15() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 99) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(true).size() != 98) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(false).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
  /**
   * enable_seq_space_compaction=false enable_unseq_space_compaction=false
   * compaction_concurrent_thread=1 max_compaction_candidate_file_num=2
   */
  @Test
  public void test16() throws IOException, IllegalPathException {
    boolean prevEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    boolean prevEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    int prevCompactionConcurrentThread =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(1);
    int prevMaxCompactionCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(2);

    TsFileResourceManager tsFileResourceManager =
        new TsFileResourceManager(COMPACTION_TEST_SG, "0", "target");
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(true, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 100, tsFileResource);
      tsFileResourceManager.add(tsFileResource, true);
    }
    for (int i = 0; i < 100; i++) {
      Set<String> fullPath = new HashSet<>(Arrays.asList(fullPaths));
      List<List<Long>> chunkPagePointsNum = new ArrayList<>();
      List<Long> pagePointsNum = new ArrayList<>();
      pagePointsNum.add(100L);
      chunkPagePointsNum.add(pagePointsNum);
      TsFileResource tsFileResource =
          CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
      CompactionFileGeneratorUtils.writeChunkToTsFile(
          fullPath, chunkPagePointsNum, 100 * i + 50, tsFileResource);
      tsFileResourceManager.add(tsFileResource, false);
    }

    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 98) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());
    CompactionScheduler.scheduleCompaction(tsFileResourceManager, 0);
    while (tsFileResourceManager.getTsFileList(false).size() != 96) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(100, tsFileResourceManager.getTsFileList(true).size());

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(prevEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(prevEnableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(prevCompactionConcurrentThread);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxCompactionCandidateFileNum(prevMaxCompactionCandidateFileNum);
  }
}
