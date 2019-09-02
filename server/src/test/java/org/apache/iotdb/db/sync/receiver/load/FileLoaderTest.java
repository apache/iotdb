package org.apache.iotdb.db.sync.receiver.load;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoaderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderTest.class);
  private static final String SG_NAME = "root.sg";
  private static IoTDB daemon;
  private String dataDir;
  private FileLoader fileLoader;

  @Before
  public void setUp()
      throws IOException, InterruptedException, StartupException, DiskSpaceInsufficientException, MetadataErrorException {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    dataDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile())
        .getParentFile().getAbsolutePath();
    initMetadata();
  }

  private void initMetadata() throws MetadataErrorException {
    MManager mmanager = MManager.getInstance();
    mmanager.init();
    mmanager.clear();
    mmanager.setStorageLevelToMTree("root.sg0");
    mmanager.setStorageLevelToMTree("root.sg1");
    mmanager.setStorageLevelToMTree("root.sg2");
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void loadNewTsfiles() throws IOException, StorageEngineException {
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, Set<File>> allFileList = new HashMap<>();
    Map<String, Set<File>> correctSequenceLoadedFileMap = new HashMap<>();

    // add some new tsfiles
    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new HashSet<>());
        correctSequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            getSnapshotFolder() + File.separator + SG_NAME + i + File.separator + rand + ".tsfile";
        File syncFile = new File(fileName);
        File dataFile = new File(
            syncFile.getParentFile().getParentFile().getParentFile().getParentFile()
                .getParentFile(), IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separatorChar + syncFile.getParentFile().getName() + File.separatorChar
            + syncFile.getName());
        correctSequenceLoadedFileMap.get(SG_NAME + i).add(dataFile);
        allFileList.get(SG_NAME + i).add(syncFile);
        if (!syncFile.getParentFile().exists()) {
          syncFile.getParentFile().mkdirs();
        }
        if (!syncFile.exists() && !syncFile.createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        if (!new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)
            .createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        TsFileResource tsFileResource = new TsFileResource(syncFile);
        tsFileResource.getStartTimeMap().put(String.valueOf(i), (long) j * 10);
        tsFileResource.getEndTimeMap().put(String.valueOf(i), (long) j * 10 + 5);
        tsFileResource.serialize();
      }
    }

    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      assert processor.getSequenceFileList().isEmpty();
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert getReceiverFolderFile().exists();
    for (Set<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }
    fileLoader.endSync();

    try {
      long waitTime = 0;
      while (FileLoaderManager.getInstance()
          .containsFileLoader(getReceiverFolderFile().getName())) {
        Thread.sleep(100);
        waitTime += 100;
        LOGGER.info("Has waited for loading new tsfiles {}s", waitTime);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Fail to wait for loading new tsfiles", e);
    }

    assert !new File(getReceiverFolderFile(), Constans.RECEIVER_DATA_FOLDER_NAME).exists();
    Map<String, Set<File>> sequenceLoadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      sequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assert processor.getSequenceFileList().size() == 10;
      for (TsFileResource tsFileResource : processor.getSequenceFileList()) {
        sequenceLoadedFileMap.get(SG_NAME + i).add(tsFileResource.getFile());
      }
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert sequenceLoadedFileMap.size() == correctSequenceLoadedFileMap.size();
    for (Entry<String, Set<File>> entry : correctSequenceLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assert entry.getValue().size() == sequenceLoadedFileMap.get(sg).size();
      assert entry.getValue().containsAll(sequenceLoadedFileMap.get(sg));
    }


    // add some overlap new tsfiles
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, Set<File>> correctUnSequenceLoadedFileMap = new HashMap<>();
    allFileList = new HashMap<>();
    r = new Random(1);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new HashSet<>());
        correctUnSequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            getSnapshotFolder() + File.separator + SG_NAME + i + File.separator + rand + ".tsfile";
        File syncFile = new File(fileName);
        File dataFile = new File(
            syncFile.getParentFile().getParentFile().getParentFile().getParentFile()
                .getParentFile(), IoTDBConstant.UNSEQUENCE_FLODER_NAME
            + File.separatorChar + syncFile.getParentFile().getName() + File.separatorChar
            + syncFile.getName());
        correctUnSequenceLoadedFileMap.get(SG_NAME + i).add(dataFile);
        allFileList.get(SG_NAME + i).add(syncFile);
        if (!syncFile.getParentFile().exists()) {
          syncFile.getParentFile().mkdirs();
        }
        if (!syncFile.exists() && !syncFile.createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        if (!new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)
            .createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        TsFileResource tsFileResource = new TsFileResource(syncFile);
        tsFileResource.getStartTimeMap().put(String.valueOf(i), (long) j * 10);
        tsFileResource.getEndTimeMap().put(String.valueOf(i), (long) j * 10 + 3);
        tsFileResource.serialize();
      }
    }

    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      assert !processor.getSequenceFileList().isEmpty();
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert getReceiverFolderFile().exists();
    for (Set<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }
    fileLoader.endSync();

    try {
      long waitTime = 0;
      while (FileLoaderManager.getInstance()
          .containsFileLoader(getReceiverFolderFile().getName())) {
        Thread.sleep(100);
        waitTime += 100;
        LOGGER.info("Has waited for loading new tsfiles {}s", waitTime);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Fail to wait for loading new tsfiles", e);
    }

    assert !new File(getReceiverFolderFile(), Constans.RECEIVER_DATA_FOLDER_NAME).exists();
    sequenceLoadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      sequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assert processor.getSequenceFileList().size() == 10;
      for (TsFileResource tsFileResource : processor.getSequenceFileList()) {
        sequenceLoadedFileMap.get(SG_NAME + i).add(tsFileResource.getFile());
      }
      assert !processor.getUnSequenceFileList().isEmpty();
    }

    assert sequenceLoadedFileMap.size() == correctSequenceLoadedFileMap.size();
    for (Entry<String, Set<File>> entry : correctSequenceLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assert entry.getValue().size() == sequenceLoadedFileMap.get(sg).size();
      assert entry.getValue().containsAll(sequenceLoadedFileMap.get(sg));
    }

    Map<String, Set<File>> unsequenceLoadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      unsequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assert processor.getUnSequenceFileList().size() == 10;
      for (TsFileResource tsFileResource : processor.getUnSequenceFileList()) {
        unsequenceLoadedFileMap.get(SG_NAME + i).add(tsFileResource.getFile());
      }
    }

    assert unsequenceLoadedFileMap.size() == correctUnSequenceLoadedFileMap.size();
    for (Entry<String, Set<File>> entry : correctUnSequenceLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assert entry.getValue().size() == unsequenceLoadedFileMap.get(sg).size();
      assert entry.getValue().containsAll(unsequenceLoadedFileMap.get(sg));
    }
  }

  @Test
  public void loadDeletedFileName() throws IOException, StorageEngineException {
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, Set<File>> allFileList = new HashMap<>();
    Map<String, Set<File>> correctLoadedFileMap = new HashMap<>();

    // add some tsfiles
    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 25; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new HashSet<>());
        correctLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            getSnapshotFolder() + File.separator + SG_NAME + i + File.separator + rand + ".tsfile";
        File syncFile = new File(fileName);
        File dataFile = new File(
            syncFile.getParentFile().getParentFile().getParentFile().getParentFile()
                .getParentFile(), IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separatorChar + syncFile.getParentFile().getName() + File.separatorChar
            + syncFile.getName());
        correctLoadedFileMap.get(SG_NAME + i).add(dataFile);
        allFileList.get(SG_NAME + i).add(syncFile);
        if (!syncFile.getParentFile().exists()) {
          syncFile.getParentFile().mkdirs();
        }
        if (!syncFile.exists() && !syncFile.createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        if (!new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)
            .createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        TsFileResource tsFileResource = new TsFileResource(syncFile);
        tsFileResource.serialize();
      }
    }

    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      assert processor.getSequenceFileList().isEmpty();
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert getReceiverFolderFile().exists();
    for (Set<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }
    fileLoader.endSync();

    try {
      long waitTime = 0;
      while (FileLoaderManager.getInstance()
          .containsFileLoader(getReceiverFolderFile().getName())) {
        Thread.sleep(100);
        waitTime += 100;
        LOGGER.info("Has waited for loading new tsfiles {}s", waitTime);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Fail to wait for loading new tsfiles", e);
    }

    assert !new File(getReceiverFolderFile(), Constans.RECEIVER_DATA_FOLDER_NAME).exists();
    Map<String, Set<File>> loadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      loadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assert processor.getSequenceFileList().size() == 25;
      for (TsFileResource tsFileResource : processor.getSequenceFileList()) {
        loadedFileMap.get(SG_NAME + i).add(tsFileResource.getFile());
      }
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert loadedFileMap.size() == correctLoadedFileMap.size();
    for (Entry<String, Set<File>> entry : correctLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assert entry.getValue().size() == loadedFileMap.get(sg).size();
      assert entry.getValue().containsAll(loadedFileMap.get(sg));
    }

    // delete some tsfiles
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    for(Entry<String, Set<File>> entry:allFileList.entrySet()){
      String sg = entry.getKey();
      Set<File> files = entry.getValue();
      int cnt = 0;
      for(File snapFile:files){
        if(!snapFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)){
          File dataFile = new File(
              snapFile.getParentFile().getParentFile().getParentFile().getParentFile()
                  .getParentFile(), IoTDBConstant.SEQUENCE_FLODER_NAME
              + File.separatorChar + snapFile.getParentFile().getName() + File.separatorChar
              + snapFile.getName());
          correctLoadedFileMap.get(sg).remove(dataFile);
          snapFile.delete();
          fileLoader.addDeletedFileName(snapFile);
          new File(snapFile + TsFileResource.RESOURCE_SUFFIX).delete();
          if(++cnt == 15){
            break;
          }
        }
      }
    }
    fileLoader.endSync();

    try {
      long waitTime = 0;
      while (FileLoaderManager.getInstance()
          .containsFileLoader(getReceiverFolderFile().getName())) {
        Thread.sleep(100);
        waitTime += 100;
        LOGGER.info("Has waited for loading new tsfiles {}s", waitTime);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Fail to wait for loading new tsfiles", e);
    }

    loadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance().getProcessor(SG_NAME + i);
      loadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      for (TsFileResource tsFileResource : processor.getSequenceFileList()) {
        loadedFileMap.get(SG_NAME + i).add(tsFileResource.getFile());
      }
      assert processor.getUnSequenceFileList().isEmpty();
    }

    assert loadedFileMap.size() == correctLoadedFileMap.size();
    for (Entry<String, Set<File>> entry : correctLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assert entry.getValue().size() == loadedFileMap.get(sg).size();
      assert entry.getValue().containsAll(loadedFileMap.get(sg));
    }
  }

  private File getReceiverFolderFile() {
    return new File(dataDir + File.separatorChar + Constans.SYNC_RECEIVER + File.separatorChar
        + "127.0.0.1_5555");
  }

  private File getSnapshotFolder() {
    return new File(getReceiverFolderFile(), Constans.RECEIVER_DATA_FOLDER_NAME);
  }
}