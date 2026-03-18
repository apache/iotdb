package com.timecho.iotdb.dataregion.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.dataregion.compaction.execute.task.SharedStorageCompactionTask;
import com.timecho.iotdb.dataregion.compaction.selector.utils.SharedStorageCompactionTaskResource;
import com.timecho.iotdb.dataregion.compaction.tool.SharedStorageCompactionUtils;
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
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.timecho.iotdb.dataregion.compaction.execute.task.SharedStorageCompactionTask.REMOTE_TMP_FILE_SUFFIX;
import static org.apache.tsfile.utils.FSUtils.OS_FILE_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
  "com.sun.org.apache.xerces.*",
  "javax.xml.*",
  "org.xml.*",
  "org.w3c.*",
  "javax.management.*"
})
@PrepareForTest(SharedStorageCompactionUtils.class)
public class SharedStorageCompactionTaskTest {
  private static IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static TSFileConfig TSFILE_CONFIG = TSFileDescriptor.getInstance().getConfig();
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static String COMPACTION_TEST_SG = TsFileGeneratorUtils.testStorageGroup;
  private static String COMPACTION_TEST_DEVICE = "testd";
  private static String COMPACTION_TEST_SERIES = "tests";
  private static String COMPACTION_TEST_DEVICE_PATH =
      TsFileGeneratorUtils.testStorageGroup
          + "."
          + COMPACTION_TEST_DEVICE
          + "."
          + COMPACTION_TEST_SERIES;
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
  private static File UNSEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH,
          "data"
              + File.separator
              + "unsequence"
              + File.separator
              + COMPACTION_TEST_SG
              + File.separator
              + "0"
              + File.separator
              + "0");

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
    TSFILE_CONFIG.setTSFileStorageFs(prevTSFileStorageFs);
    FSUtils.reload();
    CONFIG.setObjectStorageType(prevObjectStorageType);
    CONFIG.setTierDataDirs(prevDataDirs);
    TierManager.getInstance().resetFolders();
  }

  private TsFileResource createRemoteFile(
      String device,
      String measurement,
      TimeRange[] chunkTimeRanges,
      String localPath,
      String remotePath)
      throws IOException {
    TsFileResource resource =
        TsFileGeneratorUtils.generateSingleNonAlignedSeriesFile(
            device,
            measurement,
            chunkTimeRanges,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            localPath);
    resource.setRemoteStorageBlock(new RemoteStorageBlock(FSType.OBJECT_STORAGE, remotePath));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL_ON_REMOTE);
    resource.serialize();
    fsFactory.copyFile(fsFactory.getFile(localPath), fsFactory.getFile(remotePath));
    fsFactory.deleteIfExists(fsFactory.getFile(localPath));
    return resource;
  }

  @Test
  public void testPullEmptyFiles() throws Exception {
    // create follower's source files
    String followerOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId())
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              SEQ_DIRS + File.separator + fileName,
              followerOSPrefix + OS_FILE_SEPARATOR + fileName);
      resource.setStatusForShareStorageCompaction(TsFileResourceStatus.COMPACTION_CANDIDATE);
      resource.setStatus(TsFileResourceStatus.COMPACTING);
      sourceFiles.add(resource);
    }
    // mock
    PowerMockito.mockStatic(SharedStorageCompactionUtils.class);
    PowerMockito.when(
            SharedStorageCompactionUtils.pullRemoteReplica(
                Mockito.any(), Mockito.anyLong(), Mockito.any()))
        .thenReturn(null);
    // do compaction
    for (TsFileResource resource : sourceFiles) {
      resource.setLastValues(Collections.emptyMap());
      dataRegion.loadNewTsFile(resource, false, true, true, Optional.empty());
    }
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(2, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    SharedStorageCompactionTaskResource taskResource =
        new SharedStorageCompactionTaskResource(sourceFiles, Collections.emptyList(), 2);
    SharedStorageCompactionTask task =
        new SharedStorageCompactionTask(
            0, dataRegion, dataRegion.getTsFileManager(), taskResource, 0);
    assertFalse(task.start());
    // verify
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(2, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    assertEquals(sourceFiles, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false));
  }

  @Test
  public void testReplaceAll() throws Exception {
    // create follower's source files
    String followerOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId())
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              SEQ_DIRS + File.separator + fileName,
              followerOSPrefix + OS_FILE_SEPARATOR + fileName);
      resource.setStatusForShareStorageCompaction(TsFileResourceStatus.COMPACTION_CANDIDATE);
      resource.setStatus(TsFileResourceStatus.COMPACTING);
      sourceFiles.add(resource);
    }
    // create leader's target files
    String leaderOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId() + 1)
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              testDataDirs[0][0] + File.separator + fileName,
              leaderOSPrefix + OS_FILE_SEPARATOR + fileName);
      targetFiles.add(resource);
    }
    // mock
    List<TsFileResource> pulledResources = new ArrayList<>();
    for (TsFileResource resource : targetFiles) {
      File tsFile = fsFactory.getFile(SEQ_DIRS, resource.getTsFile().getName());
      File toSesourceFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + TsFileResource.RESOURCE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromResourceFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX);
      fsFactory.copyFile(fromResourceFile, toSesourceFile);
      File toModsFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + ModificationFile.FILE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromModsFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + ModificationFile.FILE_SUFFIX);
      if (fromModsFile.exists()) {
        fsFactory.copyFile(fromModsFile, toModsFile);
      }
      pulledResources.add(new TsFileResource(tsFile, TsFileResourceStatus.NORMAL_ON_REMOTE));
    }
    PowerMockito.mockStatic(SharedStorageCompactionUtils.class);
    PowerMockito.when(
            SharedStorageCompactionUtils.pullRemoteReplica(
                Mockito.any(), Mockito.anyLong(), Mockito.any()))
        .thenReturn(pulledResources);
    // do compaction
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    SharedStorageCompactionTaskResource taskResource =
        new SharedStorageCompactionTaskResource(sourceFiles, Collections.emptyList(), 2);
    SharedStorageCompactionTask task =
        new SharedStorageCompactionTask(
            0, dataRegion, dataRegion.getTsFileManager(), taskResource, 0);
    assertTrue(task.start());
    // verify
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(2, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    Set<String> followerRemotePaths = new HashSet<>();
    for (TsFileResource resource : dataRegion.getTsFileManager().getTsFileListSnapshot(0, false)) {
      followerRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    Set<String> leaderRemotePaths = new HashSet<>();
    for (TsFileResource resource : targetFiles) {
      leaderRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    assertEquals(leaderRemotePaths, followerRemotePaths);
    for (TsFileResource sourceFile : sourceFiles) {
      assertTrue(sourceFile.isDeleted());
    }
    for (TsFileResource targetFile : pulledResources) {
      assertEquals(TsFileResourceStatus.NORMAL_ON_REMOTE, targetFile.getStatus());
    }
  }

  @Test
  public void testReplaceWithOverlappedFiles() throws Exception {
    // create follower's source files
    String followerOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId())
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              UNSEQ_DIRS + File.separator + fileName,
              followerOSPrefix + OS_FILE_SEPARATOR + fileName);
      resource.setStatusForShareStorageCompaction(TsFileResourceStatus.COMPACTION_CANDIDATE);
      resource.setStatus(TsFileResourceStatus.COMPACTING);
      sourceFiles.add(resource);
    }
    // create leader's target files
    String leaderOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId() + 1)
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              testDataDirs[0][0] + File.separator + fileName,
              leaderOSPrefix + OS_FILE_SEPARATOR + fileName);
      targetFiles.add(resource);
    }
    targetFiles.add(sourceFiles.get(1));
    // mock
    List<TsFileResource> pulledResources = new ArrayList<>();
    for (TsFileResource resource : targetFiles) {
      File tsFile = fsFactory.getFile(SEQ_DIRS, resource.getTsFile().getName());
      File toSesourceFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + TsFileResource.RESOURCE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromResourceFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX);
      fsFactory.copyFile(fromResourceFile, toSesourceFile);
      File toModsFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + ModificationFile.FILE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromModsFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + ModificationFile.FILE_SUFFIX);
      if (fromModsFile.exists()) {
        fsFactory.copyFile(fromModsFile, toModsFile);
      }
      pulledResources.add(new TsFileResource(tsFile, TsFileResourceStatus.NORMAL_ON_REMOTE));
    }
    PowerMockito.mockStatic(SharedStorageCompactionUtils.class);
    PowerMockito.when(
            SharedStorageCompactionUtils.pullRemoteReplica(
                Mockito.any(), Mockito.anyLong(), Mockito.any()))
        .thenReturn(pulledResources);
    // do compaction
    sourceFiles.get(1).setLastValues(Collections.emptyMap());
    dataRegion.loadNewTsFile(sourceFiles.get(1), false, true, true, Optional.empty());
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(1, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    SharedStorageCompactionTaskResource taskResource =
        new SharedStorageCompactionTaskResource(sourceFiles, Collections.emptyList(), 2);
    SharedStorageCompactionTask task =
        new SharedStorageCompactionTask(
            0, dataRegion, dataRegion.getTsFileManager(), taskResource, 0);
    assertTrue(task.start());
    // verify
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(3, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    Set<String> followerRemotePaths = new HashSet<>();
    for (TsFileResource resource : dataRegion.getTsFileManager().getTsFileListSnapshot(0, false)) {
      followerRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    Set<String> leaderRemotePaths = new HashSet<>();
    for (TsFileResource resource : targetFiles) {
      leaderRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    assertEquals(leaderRemotePaths, followerRemotePaths);
    assertTrue(sourceFiles.get(0).isDeleted());
    assertFalse(sourceFiles.get(1).isDeleted());
    for (TsFileResource targetFile : pulledResources) {
      assertEquals(TsFileResourceStatus.NORMAL_ON_REMOTE, targetFile.getStatus());
    }
  }

  @Test
  public void testReplaceAllOverlappedFiles() throws Exception {
    // create follower's source files
    String followerOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId())
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              UNSEQ_DIRS + File.separator + fileName,
              followerOSPrefix + OS_FILE_SEPARATOR + fileName);
      resource.setStatusForShareStorageCompaction(TsFileResourceStatus.COMPACTION_CANDIDATE);
      resource.setStatus(TsFileResourceStatus.COMPACTING);
      sourceFiles.add(resource);
    }
    // create leader's target files
    List<TsFileResource> targetFiles = new ArrayList<>(sourceFiles);
    // mock
    List<TsFileResource> pulledResources = new ArrayList<>();
    for (TsFileResource resource : targetFiles) {
      File tsFile = fsFactory.getFile(SEQ_DIRS, resource.getTsFile().getName());
      File toSesourceFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + TsFileResource.RESOURCE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromResourceFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX);
      fsFactory.copyFile(fromResourceFile, toSesourceFile);
      File toModsFile =
          fsFactory.getFile(
              SEQ_DIRS,
              resource.getTsFile().getName()
                  + ModificationFile.FILE_SUFFIX
                  + REMOTE_TMP_FILE_SUFFIX);
      File fromModsFile =
          fsFactory.getFile(
              resource.getTsFile().getParent(),
              resource.getTsFile().getName() + ModificationFile.FILE_SUFFIX);
      if (fromModsFile.exists()) {
        fsFactory.copyFile(fromModsFile, toModsFile);
      }
      pulledResources.add(new TsFileResource(tsFile, TsFileResourceStatus.NORMAL_ON_REMOTE));
    }
    PowerMockito.mockStatic(SharedStorageCompactionUtils.class);
    PowerMockito.when(
            SharedStorageCompactionUtils.pullRemoteReplica(
                Mockito.any(), Mockito.anyLong(), Mockito.any()))
        .thenReturn(pulledResources);
    // do compaction
    for (TsFileResource resource : sourceFiles) {
      resource.setLastValues(Collections.emptyMap());
      dataRegion.loadNewTsFile(resource, false, true, true, Optional.empty());
    }
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(2, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    SharedStorageCompactionTaskResource taskResource =
        new SharedStorageCompactionTaskResource(sourceFiles, Collections.emptyList(), 2);
    SharedStorageCompactionTask task =
        new SharedStorageCompactionTask(
            0, dataRegion, dataRegion.getTsFileManager(), taskResource, 0);
    assertTrue(task.start());
    // verify
    assertEquals(0, dataRegion.getTsFileManager().getTsFileListSnapshot(0, true).size());
    assertEquals(2, dataRegion.getTsFileManager().getTsFileListSnapshot(0, false).size());
    Set<String> followerRemotePaths = new HashSet<>();
    for (TsFileResource resource : dataRegion.getTsFileManager().getTsFileListSnapshot(0, false)) {
      followerRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    Set<String> leaderRemotePaths = new HashSet<>();
    for (TsFileResource resource : targetFiles) {
      leaderRemotePaths.add(resource.getRemoteStorageBlock().getPath());
    }
    assertEquals(leaderRemotePaths, followerRemotePaths);
    for (TsFileResource sourceFile : sourceFiles) {
      assertFalse(sourceFile.isDeleted());
      assertEquals(TsFileResourceStatus.NORMAL_ON_REMOTE, sourceFile.getStatus());
    }
    for (TsFileResource targetFile : pulledResources) {
      assertEquals(TsFileResourceStatus.NORMAL_ON_REMOTE, targetFile.getStatus());
    }
  }

  @Test
  public void testDelete() throws Exception {
    // create leader's target files
    String leaderOSPrefix =
        FSUtils.getOSDefaultPath(CONFIG.getObjectStorageBucket(), CONFIG.getDataNodeId() + 1)
            + OS_FILE_SEPARATOR
            + String.join(
                OS_FILE_SEPARATOR, new String[] {"sequence", COMPACTION_TEST_SG, "0", "0"});
    List<TsFileResource> targetFiles = new ArrayList<>();
    for (int i = 0; i < 4; ++i) {
      String fileName =
          TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), i, 0, 0);
      TsFileResource resource =
          createRemoteFile(
              COMPACTION_TEST_DEVICE,
              COMPACTION_TEST_SERIES,
              new TimeRange[] {new TimeRange(100 + i, 100 + i)},
              testDataDirs[0][0] + File.separator + fileName,
              leaderOSPrefix + OS_FILE_SEPARATOR + fileName);
      resource.setStatusForShareStorageCompaction(TsFileResourceStatus.COMPACTION_CANDIDATE);
      targetFiles.add(resource);
    }
    targetFiles.get(0).setStatus(TsFileResourceStatus.COMPACTING);
    // delete
    SharedStorageCompactionTaskResource taskResource =
        new SharedStorageCompactionTaskResource(
            Collections.emptyList(), Collections.emptyList(), 2);
    SharedStorageCompactionTask task =
        new SharedStorageCompactionTask(
            0, dataRegion, dataRegion.getTsFileManager(), taskResource, 0);
    task.setTargetFiles(targetFiles);
    ModEntry deletion =
        new TreeDeletionEntry(new MeasurementPath(COMPACTION_TEST_DEVICE_PATH), 100, 102);
    task.deleteData(deletion);
    // verify
    assertFalse(
        fsFactory
            .getFile(
                targetFiles.get(0).getTsFilePath()
                    + ModificationFile.FILE_SUFFIX
                    + REMOTE_TMP_FILE_SUFFIX)
            .exists());
    assertTrue(
        fsFactory
            .getFile(
                targetFiles.get(1).getTsFilePath()
                    + ModificationFile.FILE_SUFFIX
                    + REMOTE_TMP_FILE_SUFFIX)
            .exists());
    assertTrue(
        fsFactory
            .getFile(
                targetFiles.get(2).getTsFilePath()
                    + ModificationFile.FILE_SUFFIX
                    + REMOTE_TMP_FILE_SUFFIX)
            .exists());
    assertFalse(
        fsFactory
            .getFile(
                targetFiles.get(3).getTsFilePath()
                    + ModificationFile.FILE_SUFFIX
                    + REMOTE_TMP_FILE_SUFFIX)
            .exists());
  }
}
