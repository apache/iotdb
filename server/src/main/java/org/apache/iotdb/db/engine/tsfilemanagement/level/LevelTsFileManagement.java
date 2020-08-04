package org.apache.iotdb.db.engine.tsfilemanagement.level;

import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.getVmLevel;
import static org.apache.iotdb.db.engine.tsfilemanagement.utils.VmLogger.VM_LOG_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tsfilemanagement.TsFileManagement;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.VmLogAnalyzer;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.VmLogger;
import org.apache.iotdb.db.engine.tsfilemanagement.utils.VmMergeUtils;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

public class LevelTsFileManagement extends TsFileManagement {

  private final List<List<TsFileResource>> sequenceTsFileResources = new CopyOnWriteArrayList<>();
  private final List<List<RestorableTsFileIOWriter>> sequenceWriters = new CopyOnWriteArrayList<>();
  private final List<List<TsFileResource>> unSequenceTsFileResources = new CopyOnWriteArrayList<>();
  private final List<List<RestorableTsFileIOWriter>> unSequenceWriters = new CopyOnWriteArrayList<>();

  private void deleteVmFiles(List<TsFileResource> vmMergeTsFiles,
      List<RestorableTsFileIOWriter> vmMergeWriters) throws IOException {
    logger.debug("{}: {} vm merge starts to delete file", storageGroupName,
        tsFileResource.getTsFile().getName());
    for (int i = 0; i < vmMergeTsFiles.size(); i++) {
      vmMergeWriters.get(i).close();
      logger.debug("{} vm file close a writer", vmMergeWriters.get(i).getFile().getName());
      deleteVmFile(vmMergeTsFiles.get(i));
    }
    for (int i = 0; i < vmWriters.size(); i++) {
      vmWriters.get(i).removeAll(vmMergeWriters);
      vmTsFileResources.get(i).removeAll(vmMergeTsFiles);
    }
  }

  public static void deleteVmFile(TsFileResource seqFile) {
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

  /**
   * recover vm processor and files
   */
  public void recover() {
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(tsFileResource.getTsFile().getParent(),
            tsFileResource.getTsFile().getName() + VM_LOG_NAME);
    try {
      if (logFile.exists()) {
        VmLogAnalyzer logAnalyzer = new VmLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<File> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        File targetFile = logAnalyzer.getTargetFile();
        boolean isMergeFinished = logAnalyzer.isMergeFinished();
        if (targetFile == null) {
          return;
        }
        if (targetFile.getName().endsWith(TSFILE_SUFFIX)) {
          if (!isMergeFinished) {
            writer.getIOWriterOut().truncate(offset - 1);
            VmMergeUtils.merge(writer, packVmWritersToSequenceList(vmWriters),
                storageGroupName,
                new VmLogger(tsFileResource.getTsFile().getParent(),
                    tsFileResource.getTsFile().getName()),
                deviceSet, sequence);
            for (int i = 0; i < vmWriters.size(); i++) {
              deleteVmFiles(vmTsFileResources.get(i), vmWriters.get(i));
            }
          }
        } else {
          RestorableTsFileIOWriter newVmWriter = new RestorableTsFileIOWriter(targetFile);
          if (sourceFileList.isEmpty()) {
            return;
          }
          int level = getVmLevel(sourceFileList.get(0));
          if (isMergeFinished) {
            File newVmFile = createNewVMFileWithLock(tsFileResource, level + 1);
            if (!targetFile.renameTo(newVmFile)) {
              logger.error("Failed to rename {} to {}", targetFile, newVmFile);
            } else {
              newVmWriter.setFile(newVmFile);
            }
          } else {
            if (deviceSet.isEmpty()) {
              Files.delete(targetFile.toPath());
            } else {
              newVmWriter.getIOWriterOut().truncate(offset - 1);
              // vm files must be sequence, so we just have to find the first file
              int startIndex = 0;
              for (startIndex = 0; startIndex < vmWriters.get(level).size(); startIndex++) {
                RestorableTsFileIOWriter levelVmWriter = vmWriters.get(level).get(startIndex);
                if (levelVmWriter.getFile().getAbsolutePath()
                    .equals(sourceFileList.get(0).getAbsolutePath())) {
                  break;
                }
              }
              List<RestorableTsFileIOWriter> levelVmWriters = new ArrayList<>(
                  vmWriters.get(level).subList(startIndex, startIndex + sourceFileList.size()));
              List<TsFileResource> levelVmFiles = new ArrayList<>(
                  vmTsFileResources.get(level)
                      .subList(startIndex, startIndex + sourceFileList.size()));
              VmMergeUtils.merge(newVmWriter, levelVmWriters,
                  storageGroupName,
                  new VmLogger(tsFileResource.getTsFile().getParent(),
                      tsFileResource.getTsFile().getName()),
                  deviceSet, sequence);
              for (int i = 0; i < vmWriters.size(); i++) {
                deleteVmFiles(levelVmFiles, levelVmWriters);
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

}
