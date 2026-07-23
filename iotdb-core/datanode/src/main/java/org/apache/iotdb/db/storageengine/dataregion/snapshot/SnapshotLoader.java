/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SnapshotLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLoader.class);
  private static final String CANNOT_CREATE_DIR_MSG = "Cannot create directory %s";
  private static final String HARD_LINK_CREATED_LOG = "Created hard link from {} to {}";
  private static final String LINK_FALLBACK_COPY_LOG =
      "Cannot create link from {} to {}, fallback to copy";
  private static final String FAILED_PROCESS_FILE_LOG = "Failed to process file {} in dir {}: {}";
  private String storageGroupName;
  private String snapshotPath;
  private List<String> snapshotPaths;
  private String dataRegionId;
  private SnapshotLogAnalyzer logAnalyzer;

  public SnapshotLoader(String snapshotPath, String storageGroupName, String dataRegionId) {
    this.snapshotPath = snapshotPath;
    this.snapshotPaths = Collections.singletonList(snapshotPath);
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
  }

  /**
   * A snapshot received by IoTConsensus is spread across several receive folders (one per local
   * data dir), so loading it means relinking the fragments from all of them. The data dirs must be
   * cleared exactly once, before relinking from any folder; see {@link
   * #loadSnapshotFromMultipleDirs()}.
   */
  public SnapshotLoader(List<String> snapshotPaths, String storageGroupName, String dataRegionId) {
    this.snapshotPaths = snapshotPaths;
    this.snapshotPath = snapshotPaths.isEmpty() ? null : snapshotPaths.get(0);
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
  }

  private DataRegion loadSnapshot() {
    try {
      return new DataRegion(
          IoTDBDescriptor.getInstance().getConfig().getSystemDir()
              + File.separator
              + "databases"
              + File.separator
              + storageGroupName,
          dataRegionId,
          StorageEngine.getInstance().getFileFlushPolicy(),
          storageGroupName);
    } catch (Exception e) {
      LOGGER.error(StorageEngineMessages.EXCEPTION_LOAD_SNAPSHOT, snapshotPath, e);
      return null;
    }
  }

  private File getSnapshotLogFile() {
    File sourceDataDir = new File(snapshotPath);

    if (sourceDataDir.exists()) {
      File[] files =
          sourceDataDir.listFiles((dir, name) -> name.equals(SnapshotLogger.SNAPSHOT_LOG_NAME));
      if (files != null && files.length == 1) {
        LOGGER.info(StorageEngineMessages.READING_SNAPSHOT_LOG_FILE, files[0]);
        return files[0];
      }
    }
    return null;
  }

  /**
   * 1. Clear origin data 2. Move snapshot data to data dir 3. Load data region
   *
   * @return
   */
  public DataRegion loadSnapshotForStateMachine() {
    if (snapshotPaths.size() > 1) {
      return loadSnapshotFromMultipleDirs();
    }

    LOGGER.info(
        StorageEngineMessages.LOADING_SNAPSHOT_FOR, storageGroupName, dataRegionId, snapshotPath);

    File snapshotLogFile = getSnapshotLogFile();

    if (snapshotLogFile == null) {
      return loadSnapshotWithoutLog();
    } else {
      return loadSnapshotWithLog(snapshotLogFile);
    }
  }

  /**
   * Load a snapshot whose fragments are spread across several dirs (the IoTConsensus receive
   * folders). The snapshot log is not transferred during an IoTConsensus snapshot, so every
   * received fragment dir takes the without-log path. Crucially, the data dirs are cleared exactly
   * once before relinking from all dirs: clearing per-dir (as one load call per dir would) erases
   * the fragments linked by the previous dirs and leaves only the last dir's data. Because each dir
   * contributes a disjoint set of files, the relink order does not affect the result.
   */
  private DataRegion loadSnapshotFromMultipleDirs() {
    LOGGER.info(
        StorageEngineMessages.LOADING_SNAPSHOT_FOR, storageGroupName, dataRegionId, snapshotPaths);
    try {
      deleteAllFilesInDataDirs();
      LOGGER.info(StorageEngineMessages.REMOVE_ALL_DATA_FILES_IN_ORIGINAL_DIR);
      // IoTConsensus may spread the fragments of one snapshot across several receive folders.
      // The fileTarget map must be shared across all of them so that a tsfile and its companion
      // files (resource, exclusive mods, etc.) are relinked to the same data dir even when their
      // fragments were received on different disks.
      Map<String, String> fileTarget = new HashMap<>();
      for (String path : snapshotPaths) {
        File snapshotDir = new File(path);
        // IoTConsensus fragments arrive under different recv folders; do not map each
        // fragment back to the same disk as its recv path, rely on fileTarget instead.
        createLinksFromSnapshotDirToDataDirWithoutLog(snapshotDir, fileTarget, false);
        loadCompressionRatio(snapshotDir);
      }
      return loadSnapshot();
    } catch (IOException | DiskSpaceInsufficientException e) {
      LOGGER.error(
          StorageEngineMessages.EXCEPTION_LOADING_SNAPSHOT_FOR, storageGroupName, dataRegionId, e);
      return null;
    }
  }

  private DataRegion loadSnapshotWithoutLog() {
    try {
      try {
        deleteAllFilesInDataDirs();
        LOGGER.info(StorageEngineMessages.REMOVE_ALL_DATA_FILES_IN_ORIGINAL_DIR);
      } catch (IOException e) {
        LOGGER.error(StorageEngineMessages.FAILED_TO_REMOVE_ORIGIN_DATA_FILES, e);
        return null;
      }
      LOGGER.info(StorageEngineMessages.MOVING_SNAPSHOT_FILE_TO_DATA_DIRS);
      File snapshotDir = new File(snapshotPath);
      createLinksFromSnapshotDirToDataDirWithoutLog(snapshotDir, new HashMap<>(), true);
      loadCompressionRatio(snapshotDir);
      return loadSnapshot();
    } catch (IOException | DiskSpaceInsufficientException e) {
      LOGGER.error(
          StorageEngineMessages.EXCEPTION_LOADING_SNAPSHOT_FOR, storageGroupName, dataRegionId, e);
      return null;
    }
  }

  private void loadCompressionRatio(File snapshotDir) {
    File[] compressionFiles =
        snapshotDir.listFiles(f -> f.getName().startsWith(CompressionRatio.FILE_PREFIX));
    if (compressionFiles == null || compressionFiles.length == 0) {
      LOGGER.info(StorageEngineMessages.NO_COMPRESSION_RATIO_FILE_IN_DIR, snapshotPath);
      return;
    }
    File ratioFile = compressionFiles[0];
    String fileName = ratioFile.getName();
    String ratioPart = fileName.substring(0, fileName.lastIndexOf("."));
    String dataRegionId = fileName.substring(fileName.lastIndexOf(".") + 1);

    String[] fileNameArray = ratioPart.split("-");
    // fileNameArray.length != 3 means the compression ratio may be negative, ignore it
    if (fileNameArray.length == 3) {
      try {
        long rawSize = Long.parseLong(fileNameArray[1]);
        long diskSize = Long.parseLong(fileNameArray[2]);
        CompressionRatio.getInstance().updateRatio(rawSize, diskSize, dataRegionId);
      } catch (NumberFormatException ignore) {
        // ignore illegal compression file name
      } catch (IOException e) {
        LOGGER.warn(StorageEngineMessages.CANNOT_LOAD_COMPRESSION_RATIO, ratioFile, e);
      }
    }
    LOGGER.info(StorageEngineMessages.LOADED_COMPRESSION_RATIO, ratioFile);
  }

  private DataRegion loadSnapshotWithLog(File logFile) {
    boolean snapshotComplete = false;
    try {
      logAnalyzer = new SnapshotLogAnalyzer(logFile);
      snapshotComplete = logAnalyzer.isSnapshotComplete();
    } catch (Exception e) {
      LOGGER.error(StorageEngineMessages.EXCEPTION_READING_SNAPSHOT_FILE, e);
      return null;
    }

    if (!snapshotComplete) {
      // Do not load this snapshot because it's not complete.
      LOGGER.error(StorageEngineMessages.SNAPSHOT_NOT_COMPLETE_CANNOT_LOAD);
      return null;
    }

    try {
      try {
        deleteAllFilesInDataDirs();
        LOGGER.info(StorageEngineMessages.REMOVE_ALL_DATA_FILES_IN_ORIGINAL_DIR);
        createLinksFromSnapshotDirToDataDirWithLog();
        loadCompressionRatio(new File(snapshotPath));
        return loadSnapshot();
      } catch (IOException e) {
        LOGGER.error(StorageEngineMessages.FAILED_TO_REMOVE_ORIGIN_DATA_FILES, e);
        return null;
      }
    } finally {
      logAnalyzer.close();
    }
  }

  private void deleteAllFilesInDataDirs() throws IOException {
    String[] dataDirPaths = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();

    // delete
    List<File> timePartitions = new ArrayList<>();
    for (String dataDirPath : dataDirPaths) {
      File seqDataDirForThisRegion =
          new File(
              dataDirPath
                  + File.separator
                  + IoTDBConstant.SEQUENCE_FOLDER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId);
      if (seqDataDirForThisRegion.exists()) {
        File[] files = seqDataDirForThisRegion.listFiles();
        if (files != null) {
          timePartitions.addAll(Arrays.asList(files));
        }
      }

      File unseqDataDirForThisRegion =
          new File(
              dataDirPath
                  + File.separator
                  + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId);

      if (unseqDataDirForThisRegion.exists()) {
        File[] files = unseqDataDirForThisRegion.listFiles();
        if (files != null) {
          timePartitions.addAll(Arrays.asList(files));
        }
      }

      File objectRegionDir =
          Paths.get(dataDirPath)
              .resolve(IoTDBConstant.OBJECT_FOLDER_NAME)
              .resolve(dataRegionId)
              .toFile();
      if (objectRegionDir.exists()) {
        timePartitions.add(objectRegionDir);
      }
    }

    try {
      for (File timePartition : timePartitions) {
        FileUtils.forceDelete(timePartition);
      }
    } catch (IOException e) {
      LOGGER.error(
          StorageEngineMessages.EXCEPTION_DELETING_TIME_PARTITION_DIR,
          storageGroupName,
          dataRegionId,
          e);
      throw e;
    }
  }

  private void createLinksFromSnapshotDirToDataDirWithoutLog(
      File sourceDir, Map<String, String> fileTarget, boolean preferKeepSameDiskWhenLoading)
      throws IOException, DiskSpaceInsufficientException {
    if (!sourceDir.exists()) {
      throw new IOException(
          String.format(
              StorageEngineMessages.CANNOT_FIND_SNAPSHOT_DIRECTORY, sourceDir.getAbsolutePath()));
    }
    File seqFileDir =
        new File(
            sourceDir,
            IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    File unseqFileDir =
        new File(
            sourceDir,
            IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    // Multi-dir IoTConsensus receive may put object/ in a folder without sequence/unsequence.
    // Only skip when the folder has neither TsFiles nor object data.
    File snapshotObjectDir = new File(sourceDir, IoTDBConstant.OBJECT_FOLDER_NAME);
    boolean hasSeqOrUnseq = seqFileDir.exists() || unseqFileDir.exists();
    boolean hasObject = snapshotObjectDir.exists();
    if (!hasSeqOrUnseq && !hasObject) {
      LOGGER.warn(StorageEngineMessages.NO_SEQ_OR_UNSEQ_FILES_IN_SNAPSHOT, sourceDir);
      return;
    }

    if (hasSeqOrUnseq) {
      FolderManager folderManager =
          new FolderManager(
              Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs()),
              DirectoryStrategyType.SEQUENCE_STRATEGY);
      File[] timePartitionFolders = seqFileDir.listFiles();
      if (timePartitionFolders != null) {
        for (File timePartitionFolder : timePartitionFolders) {
          File[] files = timePartitionFolder.listFiles();
          if (files == null || files.length == 0) {
            continue;
          }
          String targetSuffix =
              IoTDBConstant.SEQUENCE_FOLDER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId
                  + File.separator
                  + timePartitionFolder.getName();
          createLinksFromSnapshotToSourceDir(
              targetSuffix, files, folderManager, fileTarget, preferKeepSameDiskWhenLoading);
        }
      }

      timePartitionFolders = unseqFileDir.listFiles();
      if (timePartitionFolders != null) {
        for (File timePartitionFolder : timePartitionFolders) {
          File[] files = timePartitionFolder.listFiles();
          if (files == null || files.length == 0) {
            continue;
          }
          String targetSuffix =
              IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                  + File.separator
                  + storageGroupName
                  + File.separator
                  + dataRegionId
                  + File.separator
                  + timePartitionFolder.getName();
          createLinksFromSnapshotToSourceDir(
              targetSuffix, files, folderManager, fileTarget, preferKeepSameDiskWhenLoading);
        }
      }
    }

    if (hasObject) {
      FolderManager objectFolderManager =
          new FolderManager(
              TierManager.getInstance().getAllObjectFileFolders(),
              DirectoryStrategyType.SEQUENCE_STRATEGY);
      linkObjectTreeFromSnapshotToObjectDirs(snapshotObjectDir, objectFolderManager);
    }
  }

  private void linkObjectTreeFromSnapshotToObjectDirs(
      File sourceObjectRoot, FolderManager folderManager) throws IOException {
    Path sourceRootPath = sourceObjectRoot.toPath();
    // Process one directory at a time to avoid loading all object file paths into memory.
    Files.walkFileTree(
        sourceRootPath,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            processObjectFilesInDirectoryWithFolderRetry(
                folderManager,
                dir,
                sourceRootPath,
                StorageEngineMessages.FAILED_TO_PROCESS_OBJECT_FILE_AFTER_RETRIES);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private File createLinksFromSnapshotToSourceDir(
      String targetSuffix,
      File file,
      Map<String, String> fileTarget,
      String fileKey,
      String finalDir)
      throws IOException {
    File targetFile =
        new File(finalDir + File.separator + targetSuffix + File.separator + file.getName());

    try {
      if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
        throw new IOException(
            String.format(
                StorageEngineMessages.FAILED_TO_CREATE_DIR,
                targetFile.getParentFile().getAbsolutePath()));
      }

      try {
        Files.createLink(targetFile.toPath(), file.toPath());
        LOGGER.debug(StorageEngineMessages.CREATED_HARD_LINK, file, targetFile);
        fileTarget.put(fileKey, finalDir);
        return targetFile;
      } catch (IOException e) {
        LOGGER.info(StorageEngineMessages.CANNOT_CREATE_LINK_FALLBACK_COPY, file, targetFile);
      }

      Files.copy(file.toPath(), targetFile.toPath());
      fileTarget.put(fileKey, finalDir);
      return targetFile;
    } catch (Exception e) {
      LOGGER.warn(
          StorageEngineMessages.FAILED_TO_PROCESS_SNAPSHOT_FILE,
          file.getName(),
          finalDir,
          e.getMessage(),
          e);
      throw e;
    }
  }

  private void createLinksFromSnapshotToSourceDir(
      String targetSuffix,
      File[] files,
      FolderManager folderManager,
      Map<String, String> fileTarget,
      boolean preferKeepSameDiskWhenLoading)
      throws IOException {
    for (File file : files) {
      checkTsFileResourceExists(file);

      String fileKey = file.getName().split("\\.")[0];
      String dataDir = fileTarget.get(fileKey);

      if (dataDir != null) {
        createLinksFromSnapshotToSourceDir(targetSuffix, file, fileTarget, fileKey, dataDir);
        continue;
      }

      try {
        String firstFolderOfSameDisk =
            preferKeepSameDiskWhenLoading
                    && IoTDBDescriptor.getInstance().getConfig().isKeepSameDiskWhenLoadingSnapshot()
                ? folderManager.getFirstFolderOfSameDisk(file.getAbsolutePath())
                : null;

        if (firstFolderOfSameDisk != null) {
          createLinksFromSnapshotToSourceDir(
              targetSuffix, file, fileTarget, fileKey, firstFolderOfSameDisk);
        } else {
          folderManager.getNextWithRetry(
              currentDataDir ->
                  createLinksFromSnapshotToSourceDir(
                      targetSuffix, file, fileTarget, fileKey, currentDataDir));
        }
      } catch (Exception e) {
        throw new IOException(
            String.format(
                StorageEngineMessages.FAILED_TO_PROCESS_SNAPSHOT_FILE_AFTER_RETRIES,
                file.getAbsolutePath(),
                targetSuffix),
            e);
      }
    }
  }

  private void checkTsFileResourceExists(File file) {
    if (!file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
      return;
    }

    String resourceFileName = file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;
    if (!new File(resourceFileName).exists()) {
      LOGGER.warn(
          StorageEngineMessages
              .MESSAGE_THE_ASSOCIATED_RESOURCE_FILE_OF_ARG_IS_NOT_FOUND_IN_THE_SNAPSHOT_CB9152B5,
          file);
    }
  }

  private void createLinksFromSnapshotDirToDataDirWithLog() throws IOException {
    String snapshotId = logAnalyzer.getSnapshotId();
    int loggedFileNum = logAnalyzer.getTotalFileCountInSnapshot();
    Set<String> fileInfoSet = logAnalyzer.getFileInfoSet();
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
    int fileCnt = 0;
    for (String dataDir : dataDirs) {
      String snapshotDir =
          dataDir
              + File.separator
              + IoTDBConstant.SNAPSHOT_FOLDER_NAME
              + File.separator
              + storageGroupName
              + "-"
              + dataRegionId
              + File.separator
              + snapshotId;
      fileCnt += takeHardLinksFromSnapshotToDataDir(dataDir, new File(snapshotDir), fileInfoSet);
    }
    if (fileCnt != loggedFileNum) {
      throw new IOException(
          String.format(StorageEngineMessages.SNAPSHOT_FILE_NUM_MISMATCH, loggedFileNum, fileCnt));
    }
  }

  private int takeHardLinksFromSnapshotToDataDir(
      String dataDir, File snapshotFolder, Set<String> fileInfoSet) throws IOException {
    int cnt = 0;
    File sequenceTimePartitionFolders =
        new File(
            snapshotFolder.getAbsolutePath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    File[] timePartitionFolders = sequenceTimePartitionFolders.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        String timePartition = timePartitionFolder.getName();
        File[] sourceFiles = timePartitionFolder.listFiles();
        if (sourceFiles == null) {
          continue;
        }
        File targetDir =
            new File(
                dataDir
                    + File.separator
                    + IoTDBConstant.SEQUENCE_FOLDER_NAME
                    + File.separator
                    + storageGroupName
                    + File.separator
                    + dataRegionId
                    + File.separator
                    + timePartition);
        createLinksFromSourceToTarget(targetDir, sourceFiles, fileInfoSet);
        cnt += sourceFiles.length;
      }
    }

    File unsequenceTimePartitionFolders =
        new File(
            snapshotFolder.getAbsolutePath()
                + File.separator
                + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    timePartitionFolders = unsequenceTimePartitionFolders.listFiles();
    if (timePartitionFolders != null) {
      for (File timePartitionFolder : timePartitionFolders) {
        String timePartition = timePartitionFolder.getName();
        File[] sourceFiles = timePartitionFolder.listFiles();
        if (sourceFiles == null) {
          continue;
        }
        File targetDir =
            new File(
                dataDir
                    + File.separator
                    + IoTDBConstant.UNSEQUENCE_FOLDER_NAME
                    + File.separator
                    + storageGroupName
                    + File.separator
                    + dataRegionId
                    + File.separator
                    + timePartition);
        createLinksFromSourceToTarget(targetDir, sourceFiles, fileInfoSet);
        cnt += sourceFiles.length;
      }
    }

    File objectSnapshotRoot =
        new File(
            snapshotFolder.getAbsolutePath() + File.separator + IoTDBConstant.OBJECT_FOLDER_NAME);
    if (objectSnapshotRoot.exists()) {
      cnt += linkObjectSnapshotTreeToDataDir(objectSnapshotRoot, fileInfoSet);
    }

    return cnt;
  }

  private int linkObjectSnapshotTreeToDataDir(File objectSnapshotRoot, Set<String> fileInfoSet)
      throws IOException {
    final FolderManager folderManager;
    try {
      folderManager =
          new FolderManager(
              TierManager.getInstance().getAllObjectFileFolders(),
              DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (DiskSpaceInsufficientException e) {
      throw new IOException("Failed to initialize object folder manager", e);
    }
    Path rootPath = objectSnapshotRoot.toPath();
    AtomicInteger cnt = new AtomicInteger(0);
    // Process one directory at a time to avoid loading all object file paths into memory.
    Files.walkFileTree(
        rootPath, new ObjectSnapshotLinkFileVisitor(rootPath, fileInfoSet, folderManager, cnt));

    return cnt.get();
  }

  private final class ObjectSnapshotLinkFileVisitor extends SimpleFileVisitor<Path> {
    private final Path rootPath;
    private final Set<String> fileInfoSet;
    private final FolderManager folderManager;
    private final AtomicInteger cnt;

    private ObjectSnapshotLinkFileVisitor(
        Path rootPath, Set<String> fileInfoSet, FolderManager folderManager, AtomicInteger cnt) {
      this.rootPath = rootPath;
      this.fileInfoSet = fileInfoSet;
      this.folderManager = folderManager;
      this.cnt = cnt;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      int objectFileCount =
          processObjectFilesInDirectoryWithFolderRetry(
              folderManager,
              dir,
              rootPath,
              fileInfoSet,
              StorageEngineMessages.FAILED_TO_PROCESS_OBJECT_SNAPSHOT_FILE_AFTER_RETRIES);
      cnt.addAndGet(objectFileCount);
      return FileVisitResult.CONTINUE;
    }
  }

  private void createLinksFromSourceToTarget(File targetDir, File[] files, Set<String> fileInfoSet)
      throws IOException {
    for (File file : files) {
      String infoStr = getFileInfoString(file);
      if (!fileInfoSet.contains(infoStr)) {
        throw new IOException(
            String.format(StorageEngineMessages.SNAPSHOT_FILE_NOT_IN_LOG, file.getAbsolutePath()));
      }
      File targetFile = new File(targetDir, file.getName());
      if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
        throw new IOException(
            String.format(
                StorageEngineMessages.FAILED_TO_CREATE_DIR,
                targetFile.getParentFile().getAbsolutePath()));
      }
      Files.createLink(targetFile.toPath(), file.toPath());
    }
  }

  private int processObjectFilesInDirectoryWithFolderRetry(
      FolderManager folderManager, Path sourceDir, Path sourceRootPath, String finalErrorTemplate)
      throws IOException {
    return processObjectFilesInDirectoryWithFolderRetry(
        folderManager, sourceDir, sourceRootPath, (Set<String>) null, finalErrorTemplate);
  }

  private int processObjectFilesInDirectoryWithFolderRetry(
      FolderManager folderManager,
      Path sourceDir,
      Path sourceRootPath,
      Set<String> fileInfoSet,
      String finalErrorTemplate)
      throws IOException {
    List<Path> currentBatch = new ArrayList<>();
    long currentBatchBytes = 0;
    int objectFileCount = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
      for (Path objectFilePath : stream) {
        File objectFile = objectFilePath.toFile();
        if (!objectFile.isFile() || !ObjectTypeUtils.isObjectCandidate(objectFile.getName())) {
          continue;
        }
        if (fileInfoSet != null) {
          String infoStr = getFileInfoString(objectFile);
          if (!fileInfoSet.contains(infoStr)) {
            throw new IOException(
                String.format(
                    StorageEngineMessages.SNAPSHOT_FILE_NOT_IN_LOG, objectFile.getAbsolutePath()));
          }
        }
        long objectFileSize = objectFile.length();
        if (shouldFlushCurrentObjectBatch(currentBatch, currentBatchBytes, objectFileSize)) {
          processObjectFileBatchWithFolderRetry(
              folderManager, sourceDir, sourceRootPath, currentBatch, finalErrorTemplate);
          currentBatch.clear();
          currentBatchBytes = 0;
        }
        currentBatch.add(objectFilePath);
        currentBatchBytes += objectFileSize;
        objectFileCount++;
      }
    }
    processObjectFileBatchWithFolderRetry(
        folderManager, sourceDir, sourceRootPath, currentBatch, finalErrorTemplate);
    return objectFileCount;
  }

  private void processObjectFileBatchWithFolderRetry(
      FolderManager folderManager,
      Path sourceDir,
      Path sourceRootPath,
      List<Path> objectFiles,
      String finalErrorTemplate)
      throws IOException {
    if (objectFiles.isEmpty()) {
      return;
    }
    try {
      Path targetRelDir = sourceRootPath.relativize(sourceDir);
      processObjectFilesInDir(folderManager, sourceDir, objectFiles, targetRelDir);
    } catch (Exception e) {
      throw new IOException(String.format(finalErrorTemplate, sourceDir.toAbsolutePath()), e);
    }
  }

  private boolean shouldFlushCurrentObjectBatch(
      List<Path> currentBatch, long currentBatchBytes, long nextObjectFileSize) {
    if (currentBatch.isEmpty()) {
      return false;
    }
    return currentBatchBytes + nextObjectFileSize
            > IoTDBDescriptor.getInstance().getConfig().getLoadSnapshotObjectBatchBytes()
        || currentBatch.size()
            >= IoTDBDescriptor.getInstance().getConfig().getLoadSnapshotObjectBatchFileCount();
  }

  private void processObjectFilesInDir(
      FolderManager folderManager, Path sourceDir, List<Path> sourceFiles, Path targetRelDir)
      throws Exception {
    String firstFolderOfSameDisk =
        IoTDBDescriptor.getInstance().getConfig().isKeepSameDiskWhenLoadingSnapshot()
            ? folderManager.getFirstFolderOfSameDisk(sourceDir.toAbsolutePath().toString())
            : null;

    if (firstFolderOfSameDisk != null) {
      processObjectFilesInDir(firstFolderOfSameDisk, sourceFiles, targetRelDir);
    } else {
      folderManager.getNextWithRetry(
          currentObjectDir -> {
            processObjectFilesInDir(currentObjectDir, sourceFiles, targetRelDir);
            return null;
          });
    }
  }

  private void processObjectFilesInDir(String objectDir, List<Path> sourceFiles, Path targetRelDir)
      throws IOException {
    Path targetDir =
        targetRelDir == null
            ? new File(objectDir).toPath()
            : new File(objectDir).toPath().resolve(targetRelDir);
    createDirectoryIfNeeded(targetDir.toFile());
    List<File> createdFiles = new ArrayList<>(sourceFiles.size());
    for (Path sourceFile : sourceFiles) {
      File targetFile = targetDir.resolve(sourceFile.getFileName()).toFile();
      try {
        createLinkOrCopy(sourceFile, targetFile);
        createdFiles.add(targetFile);
      } catch (IOException e) {
        LOGGER.warn(
            FAILED_PROCESS_FILE_LOG, sourceFile.getFileName(), objectDir, e.getMessage(), e);
        rollbackCreatedObjectFiles(createdFiles);
        throw new IOException(
            String.format(
                "Failed to process object file. Source: %s, Target dir: %s",
                sourceFile.toAbsolutePath(), objectDir),
            e);
      }
    }
  }

  private void rollbackCreatedObjectFiles(List<File> createdFiles) {
    for (File createdFile : createdFiles) {
      try {
        Files.deleteIfExists(createdFile.toPath());
      } catch (IOException deleteException) {
        LOGGER.warn("Failed to rollback object snapshot file {}", createdFile, deleteException);
      }
    }
  }

  private File createLinkOrCopy(Path sourceFile, File targetFile) throws IOException {
    createParentDirectoryIfNeeded(targetFile);
    try {
      Files.createLink(targetFile.toPath(), sourceFile);
      LOGGER.debug(HARD_LINK_CREATED_LOG, sourceFile, targetFile);
      return targetFile;
    } catch (IOException e) {
      LOGGER.info(LINK_FALLBACK_COPY_LOG, sourceFile, targetFile);
    }
    Files.copy(sourceFile, targetFile.toPath());
    return targetFile;
  }

  private void createParentDirectoryIfNeeded(File targetFile) throws IOException {
    File parentDir = targetFile.getParentFile();
    createDirectoryIfNeeded(parentDir);
  }

  private void createDirectoryIfNeeded(File dir) throws IOException {
    if (dir != null && !dir.exists() && !dir.mkdirs()) {
      throw new IOException(String.format(CANNOT_CREATE_DIR_MSG, dir.getAbsolutePath()));
    }
  }

  private String getFileInfoString(File file) {
    Path filePath = file.toPath();
    int objectDirIndex = -1;
    int nameCount = filePath.getNameCount();
    for (int i = 0; i < nameCount; i++) {
      if (IoTDBConstant.OBJECT_FOLDER_NAME.equals(filePath.getName(i).toString())) {
        objectDirIndex = i;
        break;
      }
    }
    if (objectDirIndex >= 0 && objectDirIndex < nameCount - 1) {
      Path relativeToObject = filePath.subpath(objectDirIndex + 1, nameCount);
      String fileName = relativeToObject.getFileName().toString();
      Path parentPath = relativeToObject.getParent();
      String middlePath = "";
      if (parentPath != null) {
        List<String> pathElements = new ArrayList<>();
        for (Path element : parentPath) {
          pathElements.add(element.toString());
        }
        middlePath = String.join("/", pathElements);
      }
      return fileName
          + SnapshotLogger.SPLIT_CHAR
          + middlePath
          + SnapshotLogger.SPLIT_CHAR
          + "object";
    }
    String[] splittedStr = file.getAbsolutePath().split(File.separator.equals("\\") ? "\\\\" : "/");
    int length = splittedStr.length;
    return splittedStr[length - SnapshotLogger.FILE_NAME_OFFSET]
        + SnapshotLogger.SPLIT_CHAR
        + splittedStr[length - SnapshotLogger.TIME_PARTITION_OFFSET]
        + SnapshotLogger.SPLIT_CHAR
        + splittedStr[length - SnapshotLogger.SEQUENCE_OFFSET];
  }

  public List<File> getSnapshotFileInfo() throws IOException {
    File snapshotLogFile = getSnapshotLogFile();

    if (snapshotLogFile == null) {
      return searchDataFilesRecursively(snapshotPath);
    } else {
      return getSnapshotFileWithLog(snapshotLogFile);
    }
  }

  private List<File> getSnapshotFileWithLog(File logFile) throws IOException {
    SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(logFile);
    try {
      String snapshotId = analyzer.getSnapshotId();
      String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
      List<File> fileList = new LinkedList<>();
      for (String dataDir : dataDirs) {
        String snapshotDir =
            dataDir
                + File.separator
                + IoTDBConstant.SNAPSHOT_FOLDER_NAME
                + File.separator
                + storageGroupName
                + "-"
                + dataRegionId
                + File.separator
                + snapshotId;
        fileList.addAll(searchDataFilesRecursively(snapshotDir));
      }

      File[] compressionRatioFiles =
          logFile
              .getParentFile()
              .listFiles(f -> f.getName().startsWith(CompressionRatio.FILE_PREFIX));
      if (compressionRatioFiles != null) {
        fileList.addAll(Arrays.asList(compressionRatioFiles));
      }
      return fileList;
    } finally {
      analyzer.close();
    }
  }

  /**
   * Search all data files in one directory recursively.
   *
   * @return
   */
  private List<File> searchDataFilesRecursively(String dir) throws IOException {
    LinkedList<File> fileList = new LinkedList<>();
    Files.walkFileTree(
        new File(dir).toPath(),
        new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (SnapshotFileSet.isDataFile(file.toFile())) {
              fileList.add(file.toFile());
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
    return fileList;
  }
}
