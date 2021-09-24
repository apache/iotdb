package org.apache.iotdb.db.tools.settle;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline Settle tool, which is used to settle TsFile and its corresponding mods file to a new
 * TsFile.
 */
public class TsFileAndModSettleTool extends TsFileRewriteTool {
  private static final Logger logger = LoggerFactory.getLogger(TsFileAndModSettleTool.class);
  public static Map<String, Integer> recoverSettleFileMap =
      new HashMap<>(); // FilePath -> SettleCheckStatus

  public TsFileAndModSettleTool(TsFileResource resourceToBeRewritten) throws IOException {
    super(resourceToBeRewritten);
  }

  public static void main(String[] args) {
    Map<String, TsFileResource> oldTsFileResources = new HashMap<>();
    // List<TsFileResource> oldTsFileResources = new ArrayList<>();
    findFilesToBeRecovered();
    for (Map.Entry<String, Integer> entry : recoverSettleFileMap.entrySet()) {
      String path = entry.getKey();
      TsFileResource resource = new TsFileResource(new File(path));
      resource.setClosed(true);
      oldTsFileResources.put(resource.getTsFile().getName(), resource);
    }
    List<File> tsFiles = checkArgs(args);
    if (tsFiles != null) {
      for (File file : tsFiles) {
        if (!oldTsFileResources.containsKey(file.getName())) {
          if (new File(file + TsFileResource.RESOURCE_SUFFIX).exists()) {
            TsFileResource resource = new TsFileResource(file);
            resource.setClosed(true);
            oldTsFileResources.put(file.getName(), resource);
          }
        }
      }
    }
    System.out.println(
        "Totally find "
            + oldTsFileResources.size()
            + " tsFiles to be settled, including "
            + recoverSettleFileMap.size()
            + " tsFiles to be recovered.");
    settleTsFilesAndMods(oldTsFileResources);
  }

  public static List<File> checkArgs(String[] args) {
    String filePath = "test.tsfile";
    List<File> files = new ArrayList<>();
    if (args.length == 0) {
      return null;
    } else {
      for (String arg : args) {
        if (arg.endsWith(TSFILE_SUFFIX)) { // it's a file
          File f = new File(arg);
          if (!f.exists()) {
            logger.warn("Cannot find TsFile : " + arg);
            continue;
          }
          files.add(f);
        } else { // it's a dir
          List<File> tmpFiles = getAllFilesInOneDirBySuffix(arg, TSFILE_SUFFIX);
          if (tmpFiles == null) {
            continue;
          }
          files.addAll(tmpFiles);
        }
      }
    }
    return files;
  }

  private static List<File> getAllFilesInOneDirBySuffix(String dirPath, String suffix) {
    File dir = new File(dirPath);
    if (!dir.isDirectory()) {
      logger.warn("It's not a directory path : " + dirPath);
      return null;
    }
    if (!dir.exists()) {
      logger.warn("Cannot find Directory : " + dirPath);
      return null;
    }
    List<File> tsFiles = new ArrayList<>();
    tsFiles.addAll(
        Arrays.asList(FSFactoryProducer.getFSFactory().listFilesBySuffix(dirPath, TSFILE_SUFFIX)));
    List<File> tmpFiles = Arrays.asList(dir.listFiles());
    for (File f : tmpFiles) {
      if (f.isDirectory()) {
        tsFiles.addAll(getAllFilesInOneDirBySuffix(f.getAbsolutePath(), TSFILE_SUFFIX));
      }
    }
    return tsFiles;
  }

  /**
   * This method is used to settle tsFiles and mods files, so that each old TsFile corresponds to a
   * new TsFile. This method is only applicable to v0.12 TsFile, which data in one TsFile only
   * belongs to one time partition.
   *
   * @return Each old TsFile corresponds to a new TsFileResource of the new TsFile
   */
  public static List<TsFileResource> settleTsFilesAndMods(
      Map<String, TsFileResource> resourcesToBeSettled) {
    int successCount = 0;
    List<TsFileResource> newTsFileResources = new ArrayList<>();
    SettleLog.createSettleLog();
    for (Map.Entry<String, TsFileResource> entry : resourcesToBeSettled.entrySet()) {
      TsFileResource resourceToBeSettled = entry.getValue();
      TsFileResource newResource = null;
      try (TsFileAndModSettleTool tsFileAndModSettleTool =
          new TsFileAndModSettleTool(resourceToBeSettled)) {
        System.out.println("Start settling for tsFile : " + resourceToBeSettled.getTsFilePath());
        if (isSettledFileGenerated(resourceToBeSettled)) {
          newResource = findSettledFile(resourceToBeSettled);
        } else {
          // Write Settle Log, Status 1
          SettleLog.writeSettleLog(
              resourceToBeSettled.getTsFilePath()
                  + SettleLog.COMMA_SEPERATOR
                  + SettleCheckStatus.BEGIN_SETTLE_FILE);
          newResource = tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled);
          // Write Settle Log, Status 2
          SettleLog.writeSettleLog(
              resourceToBeSettled.getTsFilePath()
                  + SettleLog.COMMA_SEPERATOR
                  + SettleCheckStatus.AFTER_SETTLE_FILE);
          newTsFileResources.add(newResource);
        }

        TsFileRewriteTool.moveNewTsFile(resourceToBeSettled, newResource);
        // Write Settle Log, Status 3
        SettleLog.writeSettleLog(
            resourceToBeSettled.getTsFilePath()
                + SettleLog.COMMA_SEPERATOR
                + SettleCheckStatus.SETTLE_SUCCESS);
        System.out.println(
            "Finish settling successfully for tsFile : " + resourceToBeSettled.getTsFilePath());
        successCount++;
      } catch (IOException | WriteProcessException | IllegalPathException e) {
        System.out.println(
            "Meet error while settling the tsFile : " + resourceToBeSettled.getTsFilePath());
        e.printStackTrace();
      }
    }
    if (resourcesToBeSettled.size() == successCount) {
      SettleLog.closeLogWriter();
      FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).delete();
      System.out.println("Finish settling all tsfiles Successfully!");
    } else {
      System.out.println(
          "Finish Settling, "
              + (resourcesToBeSettled.size() - successCount)
              + " tsfiles meet errors.");
    }
    return newTsFileResources;
  }

  public TsFileResource settleOneTsFileAndMod(TsFileResource resourceToBeSettled)
      throws WriteProcessException, IOException, IllegalPathException {
    if (!resourceToBeSettled.isClosed()) {
      logger.warn(
          "The tsFile {} should be sealed when rewritting.", resourceToBeSettled.getTsFilePath());
      return null;
    }
    if (!resourceToBeSettled
        .getModFile()
        .exists()) { // if no deletions to this tsfile, then return null.
      return null;
    }
    List<TsFileResource> newResources = new ArrayList<>();
    try (TsFileAndModSettleTool tsFileAndModSettleTool =
        new TsFileAndModSettleTool(resourceToBeSettled)) {
      tsFileAndModSettleTool.parseAndRewriteFile(newResources);
    }
    if (newResources.size() == 0) { // if all the data in this tsfile has been deleted
      resourceToBeSettled.readUnlock();
      resourceToBeSettled.writeLock();
      resourceToBeSettled.delete();
      resourceToBeSettled.writeUnlock();
      resourceToBeSettled.readLock();
      return null;
    }
    return newResources.get(0);
  }

  public static void findFilesToBeRecovered() {
    if (FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).exists()) {
      try (BufferedReader settleLogReader =
          new BufferedReader(
              new FileReader(
                  FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath())))) {
        String line = null;
        while ((line = settleLogReader.readLine()) != null && !line.equals("")) {
          String oldFilePath = line.split(SettleLog.COMMA_SEPERATOR)[0];
          Integer settleCheckStatus = Integer.parseInt(line.split(SettleLog.COMMA_SEPERATOR)[1]);
          if (settleCheckStatus == SettleCheckStatus.SETTLE_SUCCESS.getCheckStatus()) {
            recoverSettleFileMap.remove(oldFilePath);
            continue;
          }
          recoverSettleFileMap.put(oldFilePath, settleCheckStatus);
        }
      } catch (IOException e) {
        logger.error(
            "meet error when recover settle process, file path:{}",
            SettleLog.getSettleLogPath(),
            e);
      }
    }
  }

  /** this method is used to check whether the new file is settled when recovering old tsFile. */
  public static boolean isSettledFileGenerated(TsFileResource oldTsFileResource) {
    String oldFilePath = oldTsFileResource.getTsFilePath();
    return TsFileAndModSettleTool.recoverSettleFileMap.containsKey(oldFilePath)
        && TsFileAndModSettleTool.recoverSettleFileMap.get(oldFilePath)
            == SettleCheckStatus.AFTER_SETTLE_FILE.getCheckStatus();
  }

  /** when the new file is settled , we need to find and deserialize it. */
  public static TsFileResource findSettledFile(TsFileResource resourceToBeSettled)
      throws IOException {
    TsFileResource settledTsFileResource = null;
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.BEGIN_SETTLE_FILE);

    for (File tempPartitionDir : resourceToBeSettled.getTsFile().getParentFile().listFiles()) {
      if (tempPartitionDir.isDirectory()
          && FSFactoryProducer.getFSFactory()
              .getFile(
                  tempPartitionDir,
                  resourceToBeSettled.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists()) {
        settledTsFileResource =
            new TsFileResource(
                FSFactoryProducer.getFSFactory()
                    .getFile(tempPartitionDir, resourceToBeSettled.getTsFile().getName()));
        settledTsFileResource.deserialize();
      }
    }
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.AFTER_SETTLE_FILE);
    return settledTsFileResource;
  }

  public static void clearRecoverSettleFileMap() {
    recoverSettleFileMap.clear();
  }
}
