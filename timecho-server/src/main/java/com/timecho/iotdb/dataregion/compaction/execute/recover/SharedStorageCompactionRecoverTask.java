package com.timecho.iotdb.dataregion.compaction.execute.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.timecho.iotdb.dataregion.compaction.tool.SharedStorageCompactionUtils;
import com.timecho.iotdb.os.HybridFileInputFactoryDecorator;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSPath;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SharedStorageCompactionRecoverTask extends SettleCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SharedStorageCompactionRecoverTask.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private final String dataRegionId;
  private final File logFile;
  private final List<TsFileResource> sourceFiles = new ArrayList<>();
  private final List<TsFileResource> targetFiles = new ArrayList<>();

  public SharedStorageCompactionRecoverTask(
      String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(null, dataRegionId, tsFileManager, logFile);
    this.dataRegionId = dataRegionId;
    this.logFile = logFile;
  }

  @Override
  public void recover() {
    try {
      recoverTaskInfoFromLogFile();
      if (allSourceFileExists()) {
        deleteTargetFiles();
      } else {
        deleteSourceFiles();
      }
    } catch (Exception e) {
      LOGGER.error(
          "{} [Compaction][Recover] Failed to recover compaction. TaskInfo: {}, Exception ",
          dataRegionId,
          this,
          e);
      LOGGER.error(TimechoServerMessages.STOP_COMPACTION_BECAUSE_OF_EXCEPTION_DURING_RECOVERING);
    } finally {
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        LOGGER.error(TimechoServerMessages.FAIL_TO_DELETE_OLD_LOG_FILE, logFile, e);
      }
    }
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    Set<TsFileIdentifier> overlappedSourceFileIdentifiers =
        new HashSet<>(logAnalyzer.getDeletedTargetFileInfos());
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    for (TsFileIdentifier identifier : sourceFileIdentifiers) {
      if (overlappedSourceFileIdentifiers.contains(identifier)) {
        continue;
      }
      sourceFiles.add(new TsFileResource(getFileFromDataDirs(identifier)));
    }
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    for (TsFileIdentifier identifier : targetFileIdentifiers) {
      targetFiles.add(new TsFileResource(getFileFromDataDirs(identifier)));
    }
  }

  private File getFileFromDataDirs(TsFileIdentifier identifier) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    String partialFileString =
        (identifier.isSequence()
                ? IoTDBConstant.SEQUENCE_FOLDER_NAME
                : IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
            + File.separator
            + identifier.getLogicalStorageGroupName()
            + File.separator
            + identifier.getDataRegionId()
            + File.separator
            + identifier.getTimePartitionId()
            + File.separator
            + identifier.getFilename();
    String partialResourceFileString = partialFileString + TsFileResource.RESOURCE_SUFFIX;
    for (String dataDir : dataDirs) {
      File file = FSFactoryProducer.getFSFactory().getFile(dataDir, partialFileString);
      File resourceFile =
          FSFactoryProducer.getFSFactory().getFile(dataDir, partialResourceFileString);
      if (file.exists() || resourceFile.exists()) {
        return file;
      }
    }
    // file has been deleted
    return FSFactoryProducer.getFSFactory()
        .getFile(
            IoTDBDescriptor.getInstance().getConfig().getTierDataDirs()[0][0], partialFileString);
  }

  private boolean allSourceFileExists() throws IOException {
    for (TsFileResource tsFileResource : sourceFiles) {
      FSPath remoteFilePath =
          HybridFileInputFactoryDecorator.getTsFileRemotePath(
              tsFileResource.getTsFile(), config.getDataNodeId());
      if (!fsFactory.getFile(remoteFilePath.getPath()).exists()
          || !tsFileResource.resourceFileExists()) {
        return false;
      }
    }
    return true;
  }

  private void deleteSourceFiles() {
    if (sourceFiles.isEmpty()) {
      return;
    }
    for (TsFileResource resource : sourceFiles) {
      resource.remove();
    }
  }

  private void deleteTargetFiles() {
    if (targetFiles.isEmpty()) {
      return;
    }
    for (TsFileResource resource : targetFiles) {
      try {
        SharedStorageCompactionUtils.removeLocalReplica(resource);
      } catch (IOException e) {
        LOGGER.error(TimechoServerMessages.TSFILE_RESOURCE_CANNOT_BE_DELETED, resource, e);
      }
    }
  }
}
