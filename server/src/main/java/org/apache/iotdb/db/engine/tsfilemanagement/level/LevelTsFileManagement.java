package org.apache.iotdb.db.engine.tsfilemanagement.level;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionLogger.HOT_COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionLogger.TARGET_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tsfilemanagement.TsFileManagement;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionLogger;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.HotCompactionUtils;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelTsFileManagement extends TsFileManagement {

  private static final Logger logger = LoggerFactory.getLogger(LevelTsFileManagement.class);
  private int maxLevelNum = IoTDBDescriptor.getInstance().getConfig().getMaxLevelNum();
  private final List<List<TsFileResource>> sequenceTsFileResources = new CopyOnWriteArrayList<>();
  private final List<List<TsFileResource>> unSequenceTsFileResources = new CopyOnWriteArrayList<>();
  private final List<List<TsFileResource>> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();

  public LevelTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
    clear();
  }

  private void deleteLevelFiles(List<TsFileResource> vmMergeTsFiles) {
    logger.debug("{} [hot compaction] merge starts to delete file", storageGroupName);
    for (TsFileResource vmMergeTsFile : vmMergeTsFiles) {
      deleteLevelFile(vmMergeTsFile);
    }
    for (int i = 0; i < maxLevelNum; i++) {
      sequenceTsFileResources.get(i).removeAll(vmMergeTsFiles);
      unSequenceTsFileResources.get(i).removeAll(vmMergeTsFiles);
    }
  }

  private static void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      if (seqFile.getTsFile().exists()) {
        Files.delete(seqFile.getTsFile().toPath());
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  private void flushAllFilesToLastLevel(List<List<TsFileResource>> currMergeFiles,
      HotCompactionLogger hotCompactionLogger, boolean sequence,
      ReadWriteLock hotCompactionMergeLock) throws IOException {
    TsFileResource sourceFile = currMergeFiles.get(0).get(0);
    File newTargetFile = createNewTsFileName(sourceFile.getTsFile(), maxLevelNum - 1);
    TsFileResource targetResource = new TsFileResource(newTargetFile);
    HotCompactionUtils.merge(targetResource, getTsFileList(sequence),
        storageGroupName, hotCompactionLogger, new HashSet<>(), sequence);
    hotCompactionLogger.logFullMerge();
    hotCompactionLogger.logSequence(sequence);
    hotCompactionLogger.logFile(TARGET_NAME, newTargetFile);
    hotCompactionMergeLock.writeLock().lock();
    for (int i = 0; i < maxLevelNum - 1; i++) {
      deleteLevelFiles(currMergeFiles.get(i));
    }
    hotCompactionMergeLock.writeLock().unlock();
    hotCompactionLogger.logMergeFinish();
  }

  @Override
  public List<TsFileResource> getMergeTsFileList(boolean sequence) {
    if (sequence) {
      return sequenceTsFileResources.get(maxLevelNum - 1);
    } else {
      return unSequenceTsFileResources.get(maxLevelNum - 1);
    }
  }

  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      for (int i = sequenceTsFileResources.size() - 1; i >= 0; i--) {
        result.addAll(sequenceTsFileResources.get(i));
      }
    } else {
      for (int i = unSequenceTsFileResources.size() - 1; i >= 0; i--) {
        result.addAll(unSequenceTsFileResources.get(i));
      }
    }
    return result;
  }

  @Override
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    return getTsFileList(sequence).iterator();
  }

  @Override
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      for (List<TsFileResource> sequenceTsFileResource : sequenceTsFileResources) {
        sequenceTsFileResource.remove(tsFileResource);
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources) {
        unSequenceTsFileResource.remove(tsFileResource);
      }
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (sequence) {
      for (List<TsFileResource> sequenceTsFileResource : sequenceTsFileResources) {
        sequenceTsFileResource.removeAll(tsFileResourceList);
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources) {
        unSequenceTsFileResource.removeAll(tsFileResourceList);
      }
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) {
    int level = getMergeLevel(tsFileResource.getTsFile());
    if (level <= maxLevelNum - 1) {
      if (sequence) {
        sequenceTsFileResources.get(level).add(tsFileResource);
      } else {
        unSequenceTsFileResources.get(level).add(tsFileResource);
      }
    } else {
      if (sequence) {
        sequenceTsFileResources.get(maxLevelNum).add(tsFileResource);
      } else {
        unSequenceTsFileResources.get(maxLevelNum).add(tsFileResource);
      }
    }
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      add(tsFileResource, sequence);
    }
  }

  @Override
  public void addMerged(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceTsFileResources.get(maxLevelNum - 1).add(tsFileResource);
    } else {
      unSequenceTsFileResources.get(maxLevelNum - 1).add(tsFileResource);
    }
  }

  @Override
  public void addMergedAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (sequence) {
      sequenceTsFileResources.get(maxLevelNum - 1).addAll(tsFileResourceList);
    } else {
      unSequenceTsFileResources.get(maxLevelNum - 1).addAll(tsFileResourceList);
    }
  }

  @Override
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      for (List<TsFileResource> sequenceTsFileResource : sequenceTsFileResources) {
        if (sequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources) {
        if (unSequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void clear() {
    sequenceTsFileResources.clear();
    for (int i = 0; i < maxLevelNum + 1; i++) {
      sequenceTsFileResources.add(new CopyOnWriteArrayList<>());
    }
    unSequenceTsFileResources.clear();
    for (int i = 0; i < maxLevelNum + 1; i++) {
      unSequenceTsFileResources.add(new CopyOnWriteArrayList<>());
    }
  }

  @Override
  public boolean isEmpty(boolean sequence) {
    if (sequence) {
      for (List<TsFileResource> sequenceTsFileResource : sequenceTsFileResources) {
        if (!sequenceTsFileResource.isEmpty()) {
          return false;
        }
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources) {
        if (!unSequenceTsFileResource.isEmpty()) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public int size(boolean sequence) {
    int result = 0;
    if (sequence) {
      for (int i = sequenceTsFileResources.size() - 1; i >= 0; i--) {
        result += sequenceTsFileResources.size();
      }
    } else {
      for (int i = unSequenceTsFileResources.size() - 1; i >= 0; i--) {
        result += unSequenceTsFileResources.size();
      }
    }
    return result;
  }

  /**
   * recover files
   */
  @Override
  public void recover() {
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(storageGroupDir, storageGroupName + HOT_COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        HotCompactionLogAnalyzer logAnalyzer = new HotCompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<File> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        File targetFile = logAnalyzer.getTargetFile();
        boolean isMergeFinished = logAnalyzer.isMergeFinished();
        boolean fullMerge = logAnalyzer.isFullMerge();
        boolean isSeq = logAnalyzer.isSeq();
        if (targetFile == null) {
          return;
        }
        if (fullMerge) {
          if (!isMergeFinished) {
            RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetFile);
            writer.getIOWriterOut().truncate(offset - 1);
            writer.close();
            HotCompactionUtils
                .merge(new TsFileResource(targetFile), getTsFileList(isSeq), storageGroupName,
                    new HotCompactionLogger(storageGroupDir, storageGroupName), deviceSet, isSeq);
            if (isSeq) {
              for (List<TsFileResource> currMergeFile : sequenceTsFileResources) {
                deleteLevelFiles(currMergeFile);
              }
            } else {
              for (List<TsFileResource> currMergeFile : unSequenceTsFileResources) {
                deleteLevelFiles(currMergeFile);
              }
            }
          }
        } else {
          TsFileResource targetResource = new TsFileResource(targetFile);
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetFile);
          if (sourceFileList.isEmpty()) {
            return;
          }
          int level = getMergeLevel(sourceFileList.get(0));
          if (!isMergeFinished) {
            if (deviceSet.isEmpty()) {
              Files.delete(targetFile.toPath());
            } else {
              writer.getIOWriterOut().truncate(offset - 1);
              writer.close();
              if (isSeq) {
                HotCompactionUtils
                    .merge(targetResource, sequenceTsFileResources.get(level),
                        storageGroupName,
                        new HotCompactionLogger(storageGroupDir, storageGroupName), deviceSet,
                        true);
                deleteLevelFiles(sequenceTsFileResources.get(level));
                sequenceTsFileResources.get(level + 1).add(targetResource);
              } else {
                HotCompactionUtils
                    .merge(targetResource, unSequenceTsFileResources.get(level),
                        storageGroupName,
                        new HotCompactionLogger(storageGroupDir, storageGroupName), deviceSet,
                        false);
                deleteLevelFiles(unSequenceTsFileResources.get(level));
                unSequenceTsFileResources.get(level + 1).add(targetResource);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("recover vm error ", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete vm log file error ", e);
        }
      }
    }
  }

  @Override
  public void forkCurrentFileList() {
    forkTsFileList(forkedSequenceTsFileResources, sequenceTsFileResources);
    forkTsFileList(forkedUnSequenceTsFileResources, unSequenceTsFileResources);
  }

  private void forkTsFileList(List<List<TsFileResource>> forkedSequenceTsFileResources,
      List<List<TsFileResource>> sequenceTsFileResources) {
    forkedSequenceTsFileResources.clear();
    for (int i = 0; i < maxLevelNum - 1; i++) {
      List<TsFileResource> tsFileResources = sequenceTsFileResources.get(i);
      // first level may have unclosed files, filter these files
      if (i == 0) {
        List<TsFileResource> forkedFirstLevelTsFileResources = new ArrayList<>();
        for (TsFileResource tsFileResource : tsFileResources) {
          if (tsFileResource.isClosed()) {
            forkedFirstLevelTsFileResources.add(tsFileResource);
          } else {
            break;
          }
        }
        forkedSequenceTsFileResources.add(forkedFirstLevelTsFileResources);
      } else {
        forkedSequenceTsFileResources.add(new ArrayList<>(tsFileResources));
      }
    }
  }

  @Override
  protected void merge(ReadWriteLock hotCompactionMergeLock) {
    merge(forkedSequenceTsFileResources, true, hotCompactionMergeLock);
    merge(forkedUnSequenceTsFileResources, false, hotCompactionMergeLock);
  }

  private void merge(List<List<TsFileResource>> mergeResources, boolean sequence,
      ReadWriteLock hotCompactionMergeLock) {
    long startTimeMillis = System.currentTimeMillis();
    try {
      logger.info("{} start to filter hot compaction condition", storageGroupName);
      long pointNum = 0;
      // all flush to target file
      Map<Path, MeasurementSchema> pathMeasurementSchemaMap = new HashMap<>();
      for (int i = 0; i < maxLevelNum - 1; i++) {
        List<TsFileResource> tsFileResources = mergeResources.get(i);
        for (TsFileResource tsFileResource : tsFileResources) {
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(
              tsFileResource.getTsFile());
          Map<String, Map<String, List<ChunkMetadata>>> schemaMap = writer
              .getMetadatasForQuery();
          for (Entry<String, Map<String, List<ChunkMetadata>>> schemaMapEntry : schemaMap
              .entrySet()) {
            String device = schemaMapEntry.getKey();
            for (Entry<String, List<ChunkMetadata>> entry : schemaMapEntry.getValue()
                .entrySet()) {
              String measurement = entry.getKey();
              List<ChunkMetadata> chunkMetadataList = entry.getValue();
              for (ChunkMetadata chunkMetadata : chunkMetadataList) {
                pointNum += chunkMetadata.getNumOfPoints();
              }
              pathMeasurementSchemaMap.computeIfAbsent(new Path(device, measurement), k ->
                  new MeasurementSchema(measurement, chunkMetadataList.get(0).getDataType()));
            }
          }
          writer.close();
        }
      }
      logger.info("{} current sg subLevel point num: {}, measurement num: {}", storageGroupName,
          pointNum, pathMeasurementSchemaMap.size());
      HotCompactionLogger hotCompactionLogger = new HotCompactionLogger(storageGroupDir,
          storageGroupName);
      if (pathMeasurementSchemaMap.size() > 0
          && pointNum / pathMeasurementSchemaMap.size() > IoTDBDescriptor.getInstance().getConfig()
          .getMergeChunkPointNumberThreshold()) {
        // merge all tsfile to last level
        logger.info("{} merge {} level tsfiles to next level", storageGroupName,
            mergeResources.size());
        flushAllFilesToLastLevel(mergeResources, hotCompactionLogger, sequence,
            hotCompactionMergeLock);
      } else {
        for (int i = 0; i < maxLevelNum - 1; i++) {
          if (IoTDBDescriptor.getInstance().getConfig().getMaxFileNumInEachLevel() <= mergeResources
              .get(i).size()) {
            for (TsFileResource mergeResource : mergeResources.get(i)) {
              hotCompactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile = createNewTsFileName(mergeResources.get(i).get(0).getTsFile(),
                i + 1);
            hotCompactionLogger.logSequence(sequence);
            hotCompactionLogger.logFile(TARGET_NAME, newLevelFile);
            logger.info("{} [Hot Compaction] merge level-{}'s {} tsfiles to next level vm",
                storageGroupName, i, mergeResources.get(i).size());

            TsFileResource newResource = new TsFileResource(newLevelFile);
            HotCompactionUtils
                .merge(newResource, mergeResources.get(i), storageGroupName, hotCompactionLogger,
                    new HashSet<>(), sequence);
            hotCompactionMergeLock.writeLock().lock();
            try {
              deleteLevelFiles(mergeResources.get(i));
              hotCompactionLogger.logMergeFinish();
              if (sequence) {
                sequenceTsFileResources.get(i + 1).add(newResource);
              } else {
                unSequenceTsFileResources.get(i + 1).add(newResource);
              }
              mergeResources.get(i + 1).add(newResource);
            } finally {
              hotCompactionMergeLock.writeLock().unlock();
            }
          }
        }
      }
      hotCompactionLogger.close();
      File logFile = FSFactoryProducer.getFSFactory()
          .getFile(storageGroupDir, storageGroupName + HOT_COMPACTION_LOG_NAME);
      if (logFile.exists()) {
        Files.delete(logFile.toPath());
      }
    } catch (Exception e) {
      logger.error("Error occurred in Hot Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info("{} [Hot Compaction] merge end time isSeq = {}, consumption: {} ms",
          storageGroupName, sequence,
          System.currentTimeMillis() - startTimeMillis);
    }
  }

  /**
   * if level < maxLevel-1, the file need hot compaction else, the file can be merged later
   */
  private File createNewTsFileName(File sourceFile, int level) {
    String path = sourceFile.getAbsolutePath();
    String prefixPath = path.substring(0, path.lastIndexOf(FILE_NAME_SEPARATOR) + 1);
    return new File(prefixPath + level + TSFILE_SUFFIX);
  }

  private static int getMergeLevel(File file) {
    String mergeLevelStr = file.getPath()
        .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
  }

}
