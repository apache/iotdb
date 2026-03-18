package com.timecho.iotdb.dataregion.compaction.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.dataregion.compaction.execute.recover.SharedStorageCompactionRecoverTask;
import com.timecho.iotdb.dataregion.compaction.execute.task.SharedStorageCompactionTask;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.conf.provider.TestConfig;
import com.timecho.iotdb.os.utils.ObjectStorageType;
import com.timecho.iotdb.os.utils.RemoteStorageBlock;
import com.timecho.iotdb.utils.EnvironmentUtils;
import com.timecho.iotdb.utils.TsFileGeneratorUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.tsfile.utils.FSUtils.OS_FILE_SEPARATOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SharedStorageCompactionRecoverTaskTest {
  private static IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static TSFileConfig TSFILE_CONFIG = TSFileDescriptor.getInstance().getConfig();
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static String COMPACTION_TEST_SG = TsFileGeneratorUtils.testStorageGroup;
  private static String COMPACTION_TEST_DEVICE = "testd";
  private static String COMPACTION_TEST_SERIES = "tests";
  private String[][] testDataDirs =
      new String[][] {{TestConstant.BASE_OUTPUT_PATH + "data"}, {IoTDBConstant.OBJECT_STORAGE_DIR}};
  private static File SEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH,
          "data"
              + File.separator
              + "sequence"
              + File.separator
              + COMPACTION_TEST_SG
              + File.separator
              + "0"
              + File.separator
              + "0");
  private static String osPrefix =
      FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId())
          + OS_FILE_SEPARATOR
          + String.join(OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});

  private File logFile =
      new File(TestConstant.BASE_OUTPUT_PATH, SharedStorageCompactionTask.LOG_FILE_NAME);
  private String[][] prevDataDirs;
  private String prevObjectStorageType;
  private FSType[] prevTSFileStorageFs;
  private DataRegion dataRegion;

  @Before
  public void setUp() throws StartupException {
    // set configs
    TSFILE_CONFIG.setObjectStorageFile("com.timecho.iotdb.os.fileSystem.OSFile");
    TSFILE_CONFIG.setObjectStorageTsFileInput("com.timecho.iotdb.os.fileSystem.OSTsFileInput");
    TSFILE_CONFIG.setObjectStorageTsFileOutput("com.timecho.iotdb.os.fileSystem.OSTsFileOutput");
    prevTSFileStorageFs = TSFILE_CONFIG.getTSFileStorageFs();
    TSFILE_CONFIG.setTSFileStorageFs(new FSType[] {FSType.LOCAL, FSType.OBJECT_STORAGE});
    FSUtils.reload();
    prevObjectStorageType = CONFIG.getObjectStorageType();
    CONFIG.setObjectStorageType(ObjectStorageType.TEST.name());
    TestConfig osTestConfig =
        (TestConfig) ObjectStorageDescriptor.getInstance().getConfig().getProviderConfig();
    osTestConfig.setTestDir(TestConstant.BASE_OUTPUT_PATH + "object_storage_test");
    new File(TestConstant.BASE_OUTPUT_PATH + "object_storage_test").mkdirs();
    prevDataDirs = CONFIG.getTierDataDirs();
    CONFIG.setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    // new dataregion
    dataRegion = new DataRegion(COMPACTION_TEST_SG, "0");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanAllDir();
    EnvironmentUtils.cleanDir(TestConstant.BASE_OUTPUT_PATH + "object_storage_test");
    logFile.delete();
    TSFILE_CONFIG.setTSFileStorageFs(prevTSFileStorageFs);
    FSUtils.reload();
    CONFIG.setObjectStorageType(prevObjectStorageType);
    CONFIG.setTierDataDirs(prevDataDirs);
    TierManager.getInstance().resetFolders();
  }

  private TsFileResource createFile(String localPath, String remotePath) throws IOException {
    TsFileResource resource =
        TsFileGeneratorUtils.generateSingleNonAlignedSeriesFile(
            COMPACTION_TEST_DEVICE,
            COMPACTION_TEST_SERIES,
            new TimeRange[] {new TimeRange(100, 101)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            localPath);
    resource.setRemoteStorageBlock(new RemoteStorageBlock(FSType.OBJECT_STORAGE, remotePath));
    resource.serialize();
    fsFactory.copyFile(fsFactory.getFile(localPath), fsFactory.getFile(remotePath));
    fsFactory.deleteIfExists(fsFactory.getFile(localPath));
    return resource;
  }

  @Test
  public void testLoadTargetFilesBroken() throws Exception {
    // prepare
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      sourceFiles.add(tsFileResource);
    }
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      tsFileResource.setLastValues(Collections.emptyMap());
      targetFiles.add(tsFileResource);
    }
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFiles(targetFiles);
      dataRegion.loadNewTsFile(targetFiles.get(0), false, true, true, Optional.empty());
      compactionLogger.logTargetFile(targetFiles.get(0));
      compactionLogger.logFiles(Collections.emptyList(), STR_DELETED_TARGET_FILES);
    }
    // recover
    SharedStorageCompactionRecoverTask recoverTask =
        new SharedStorageCompactionRecoverTask(
            dataRegion.getDataRegionIdString(), dataRegion.getTsFileManager(), logFile);
    recoverTask.recover();
    // verify
    for (TsFileResource sourceFile : sourceFiles) {
      assertTrue(
          fsFactory
              .getFile(
                  sourceFile.getTsFile().getParent(),
                  sourceFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(sourceFile.getRemoteStorageBlock().getPath()).exists());
    }
    for (TsFileResource targetFile : targetFiles) {
      assertFalse(
          fsFactory
              .getFile(
                  targetFile.getTsFile().getParent(),
                  targetFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(targetFile.getRemoteStorageBlock().getPath()).exists());
    }
  }

  @Test
  public void testLoadTargetFilesBrokenWithOverlap() throws Exception {
    // prepare
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      sourceFiles.add(tsFileResource);
    }
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(
              SEQ_DIRS + File.separator + fileName,
              sourceFiles.get(i).getRemoteStorageBlock().getPath());
      tsFileResource.setLastValues(Collections.emptyMap());
      targetFiles.add(tsFileResource);
    }
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFiles(targetFiles);
      dataRegion.loadNewTsFile(targetFiles.get(0), false, true, true, Optional.empty());
      compactionLogger.logTargetFile(targetFiles.get(0));
      compactionLogger.logFiles(
          Collections.singletonList(targetFiles.get(0)), STR_DELETED_TARGET_FILES);
    }
    // recover
    SharedStorageCompactionRecoverTask recoverTask =
        new SharedStorageCompactionRecoverTask(
            dataRegion.getDataRegionIdString(), dataRegion.getTsFileManager(), logFile);
    recoverTask.recover();
    // verify
    for (TsFileResource sourceFile : sourceFiles) {
      assertTrue(
          fsFactory
              .getFile(
                  sourceFile.getTsFile().getParent(),
                  sourceFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(sourceFile.getRemoteStorageBlock().getPath()).exists());
    }
    for (TsFileResource targetFile : targetFiles) {
      assertFalse(
          fsFactory
              .getFile(
                  targetFile.getTsFile().getParent(),
                  targetFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(targetFile.getRemoteStorageBlock().getPath()).exists());
    }
  }

  @Test
  public void testRemoveLocalFilesBroken() throws Exception {
    // prepare
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      sourceFiles.add(tsFileResource);
    }
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      tsFileResource.setLastValues(Collections.emptyMap());
      targetFiles.add(tsFileResource);
    }
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFiles(targetFiles);
      for (TsFileResource targetFile : targetFiles) {
        dataRegion.loadNewTsFile(targetFile, false, true, true, Optional.empty());
        compactionLogger.logTargetFile(targetFile);
      }
      compactionLogger.logFiles(Collections.emptyList(), STR_DELETED_TARGET_FILES);
    }
    sourceFiles.get(0).remove();
    // recover
    SharedStorageCompactionRecoverTask recoverTask =
        new SharedStorageCompactionRecoverTask(
            dataRegion.getDataRegionIdString(), dataRegion.getTsFileManager(), logFile);
    recoverTask.recover();
    // verify
    for (TsFileResource sourceFile : sourceFiles) {
      assertFalse(
          fsFactory
              .getFile(
                  sourceFile.getTsFile().getParent(),
                  sourceFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertFalse(fsFactory.getFile(sourceFile.getRemoteStorageBlock().getPath()).exists());
    }
    for (TsFileResource targetFile : targetFiles) {
      assertTrue(
          fsFactory
              .getFile(
                  targetFile.getTsFile().getParent(),
                  targetFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(targetFile.getRemoteStorageBlock().getPath()).exists());
    }
  }

  @Test
  public void testRemoveLocalFilesBrokenWithOverlap() throws Exception {
    // prepare
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(SEQ_DIRS + File.separator + fileName, osPrefix + OS_FILE_SEPARATOR + fileName);
      sourceFiles.add(tsFileResource);
    }
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 4; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource tsFileResource =
          createFile(
              SEQ_DIRS + File.separator + fileName,
              i < 1
                  ? sourceFiles.get(i).getRemoteStorageBlock().getPath()
                  : osPrefix + OS_FILE_SEPARATOR + fileName);
      tsFileResource.setLastValues(Collections.emptyMap());
      targetFiles.add(tsFileResource);
    }
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFiles(targetFiles);
      for (TsFileResource targetFile : targetFiles) {
        dataRegion.loadNewTsFile(targetFile, false, true, true, Optional.empty());
        compactionLogger.logTargetFile(targetFile);
      }
      compactionLogger.logFiles(sourceFiles.subList(0, 1), STR_DELETED_TARGET_FILES);
    }
    sourceFiles.get(2).remove();
    // recover
    SharedStorageCompactionRecoverTask recoverTask =
        new SharedStorageCompactionRecoverTask(
            dataRegion.getDataRegionIdString(), dataRegion.getTsFileManager(), logFile);
    recoverTask.recover();
    // verify
    for (TsFileResource sourceFile : sourceFiles.subList(0, 1)) {
      assertTrue(
          fsFactory
              .getFile(
                  sourceFile.getTsFile().getParent(),
                  sourceFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(sourceFile.getRemoteStorageBlock().getPath()).exists());
    }
    for (TsFileResource sourceFile : sourceFiles.subList(1, 3)) {
      assertFalse(
          fsFactory
              .getFile(
                  sourceFile.getTsFile().getParent(),
                  sourceFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertFalse(fsFactory.getFile(sourceFile.getRemoteStorageBlock().getPath()).exists());
    }
    for (TsFileResource targetFile : targetFiles.subList(1, 4)) {
      assertTrue(
          fsFactory
              .getFile(
                  targetFile.getTsFile().getParent(),
                  targetFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists());
      assertTrue(fsFactory.getFile(targetFile.getRemoteStorageBlock().getPath()).exists());
    }
  }
}
