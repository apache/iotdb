package org.apache.iotdb.db.tools.settle;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline Settle tool, which is used to settle TsFile and its corresponding mods file to a new TsFile.
 */
public class TsFileAndModSettleTool extends TsFileRewriteTool {
  private static final Logger logger = LoggerFactory.getLogger(TsFileAndModSettleTool.class);
  private final static ReadWriteLock fileLock = new ReentrantReadWriteLock();
  public static Map<String,Integer> recoverSettleFileMap = new HashMap<>();  //FilePath -> SettleCheckStatus


  public TsFileAndModSettleTool(
      TsFileResource resourceToBeRewritten) throws IOException {
    super(resourceToBeRewritten);
  }

  public static void main(String[] args) {
    File[] tsFiles = checkArgs(args);
    List<TsFileResource> oldTsFileResources = new ArrayList<>();
    for (File file : tsFiles) {
      oldTsFileResources.add(new TsFileResource(file));
    }
    try {
      settleTsFilesAndMods(oldTsFileResources);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  public static File[] checkArgs(String[] args) { // 根据路径名获得其下存在的所有TsFile文件
    String filePath = "test.tsfile";
    if (args.length == 1) {
      filePath = args[0];
    } else {
      try {
        throw new IOException("Uncorrect args");
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
    return FSFactoryProducer.getFSFactory().listFilesBySuffix(filePath, TSFILE_SUFFIX);
  }

  /**
   * This method is used to settle tsFiles and mods files, so that each old TsFile corresponds to a
   * new TsFile. This method is only applicable to v0.12 TsFile, which data in one TsFile only
   * belongs to one time partition.
   *
   * @return Each old TsFile corresponds to a new TsFileResource of the new TsFile
   */
  public static List<TsFileResource> settleTsFilesAndMods(
      List<TsFileResource> resourcesToBeSettled) throws IOException, WriteProcessException {
    List<TsFileResource> newTsFileResources = new ArrayList<>();
    for (TsFileResource resourceToBeSettled : resourcesToBeSettled) {
      TsFileResource newResource=null;
      try(TsFileAndModSettleTool tsFileAndModSettleTool=new TsFileAndModSettleTool(resourceToBeSettled)) {
        newResource = tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled);
      }
      newTsFileResources.add(newResource);
      TsFileRewriteTool.writeNewModification(resourceToBeSettled,newResource);
      TsFileRewriteTool.moveNewTsFile(resourceToBeSettled,newResource);
    }
    return newTsFileResources;
  }

  public TsFileResource settleOneTsFileAndMod(TsFileResource resourceToBeSettled)
      throws WriteProcessException, IOException {
    if(!resourceToBeSettled.isClosed()){
      logger.warn("The tsFile {} should be sealed when rewritting.", resourceToBeSettled.getTsFilePath());
      return null;
    }
    addTmpModsOfCurrentTsFile(resourceToBeSettled.getTimePartition());
    if (!resourceToBeSettled.getModFile().exists()) { //if no deletions to this tsfile, then return.
      return null;
    }
    List<TsFileResource> newResources=new ArrayList<>();
    try (TsFileAndModSettleTool tsFileAndModSettleTool = new TsFileAndModSettleTool(
        resourceToBeSettled)) {
      tsFileAndModSettleTool.parseAndRewriteFile(newResources);
    }
    if(newResources.size()==0){ //if all the data in this tsfile has been deleted, then it will be null!
      dealWithEmptyFile();
      return null;
    }
    return newResources.get(0);
  }

  public static void findFilesToBeRecovered(){
    if (FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).exists()) {// if file "data/system/settle/settle.txt" exists
      try (BufferedReader settleLogReader =
          new BufferedReader(
              new FileReader(
                  FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath())))) {
        String line = null;
        while ((line = settleLogReader.readLine()) != null && !line.equals("")) {
          String oldFilePath = line.split(SettleLog.COMMA_SEPERATOR)[0];
          Integer settleCheckStatus = Integer.parseInt(line.split(SettleLog.COMMA_SEPERATOR)[1]);
          //String oldFileName = new File(oldFilePath).getName();
          if(settleCheckStatus== SettleCheckStatus.SETTLE_SUCCESS.getCheckStatus()){ //settle success
            recoverSettleFileMap.remove(oldFilePath);
            continue;
          }
          recoverSettleFileMap.put(oldFilePath,settleCheckStatus);  //in recoverSettleFileMap, each crashed file's settleCheckStatus maybe 1 or 2

//          if(!recoverSettleFileMap.containsKey(oldFileName)){
//            recoverSettleFileMap.put(oldFileName,settleCheckStatus);
//          }
//          else if(recoverSettleFileMap.get(oldFileName)<settleCheckStatus){
//            recoverSettleFileMap.put(oldFileName,settleCheckStatus);
//          }
        }
      }
      catch (IOException e) {
        logger.error(
            "meet error when recover settle process, file path:{}",
            SettleLog.getSettleLogPath(),
            e);
      } finally {
        //FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).delete();
      }
    }
  }

  public static void clearRecoverSettleFileMap(){
    recoverSettleFileMap.clear();
  }

  public static void writeLock(){
    fileLock.writeLock().lock();
  }

  public static void writeUnLock(){
    fileLock.writeLock().unlock();
  }

  public static void readLock(){
    fileLock.readLock().lock();
  }

  public static void readUnLock(){
    fileLock.readLock().unlock();
  }

}
