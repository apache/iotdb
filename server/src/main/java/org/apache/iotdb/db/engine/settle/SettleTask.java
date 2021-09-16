package org.apache.iotdb.db.engine.settle;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettleTask extends WrappedRunnable {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);
  private TsFileResource resourceToBeSettled; //待升级旧的TsFile文件的TsFileResource
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public SettleTask(TsFileResource resourceToBeSettled) {
    this.resourceToBeSettled = resourceToBeSettled;
  }


  @Override
  public void runMayThrow() {
    try {
      settleTsFile();
    } catch (Exception e) {
      logger.error("meet error when settling file:{}", resourceToBeSettled.getTsFilePath(), e);
    }
  }

  private void settleTsFile() throws IOException, WriteProcessException {
    TsFileResource settledResource = null;
    if (isSettledFileGenerated()) {
      logger.info("find settled file for {}", resourceToBeSettled.getTsFile());
      settledResource = findSettledFile();
    } else {
      logger.info("generate settled file for {}", resourceToBeSettled.getTsFile());
      //Write Settle Log, State 1
      SettleLog.writeSettleLog(resourceToBeSettled.getTsFilePath() + SettleLog.COMMA_SEPERATOR
          + SettleCheckStatus.BEGIN_SETTLE_FILE);
      try(TsFileAndModSettleTool tsFileAndModSettleTool=new TsFileAndModSettleTool(resourceToBeSettled)) {
        settledResource = tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled);
      }

      //Write Settle Log, State 2
      SettleLog.writeSettleLog(resourceToBeSettled.getTsFilePath() + SettleLog.COMMA_SEPERATOR
          + SettleCheckStatus.AFTER_SETTLE_FILE);
    }
    resourceToBeSettled.getSettleTsFileCallBack().call(resourceToBeSettled, settledResource);
    if(TsFileAndModSettleTool.recoverSettleFileMap.containsKey(resourceToBeSettled.getTsFilePath())) {
      TsFileAndModSettleTool.recoverSettleFileMap.remove(resourceToBeSettled.getTsFilePath());
    }
    //Write Settle Log, State 3
    SettleLog.writeSettleLog(resourceToBeSettled.getTsFilePath() + SettleLog.COMMA_SEPERATOR
        + SettleCheckStatus.SETTLE_SUCCESS);
    logger.info(
        "Settle completes, file path:{} , the remaining file to be settled num: {}",
        resourceToBeSettled.getTsFilePath(),
        SettleService.getFilesToBeSettledCount().get());

    if (SettleService.getFilesToBeSettledCount().get()
        == 0) {  //delete settle log when finishing settling all files
      SettleLog.closeLogWriter();
      fsFactory.getFile(SettleLog.getSettleLogPath()).delete();
      SettleService.getINSTANCE().stop();
      logger.info("All files settled successfully! ");
    }
  }

  /**
   * this method is used to check whether the new file is settled when recovering old tsFile.
   */
  private boolean isSettledFileGenerated() {
    String oldFilePath = resourceToBeSettled.getTsFilePath();
    return TsFileAndModSettleTool.recoverSettleFileMap.containsKey(oldFilePath)
        && TsFileAndModSettleTool.recoverSettleFileMap.get(oldFilePath)
        == SettleCheckStatus.AFTER_SETTLE_FILE.getCheckStatus();
  }

  /**
   * when the new file is settled , we need to find and deserialize it.
   */
  private TsFileResource findSettledFile() throws IOException {
    resourceToBeSettled.readLock();
    TsFileResource settledTsFileResource = null;
    String oldTsFilePath = resourceToBeSettled.getTsFilePath();
    SettleLog.writeSettleLog(resourceToBeSettled.getTsFilePath() + SettleLog.COMMA_SEPERATOR
        + SettleCheckStatus.BEGIN_SETTLE_FILE);
    try {
      for (File tempPartitionDir : resourceToBeSettled.getTsFile().getParentFile().listFiles()) {
        if (tempPartitionDir.isDirectory() && fsFactory.getFile(tempPartitionDir,
            resourceToBeSettled.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX).exists()) {
          settledTsFileResource = new TsFileResource(
              fsFactory.getFile(tempPartitionDir, resourceToBeSettled.getTsFile().getName()));
          settledTsFileResource.deserialize();
        }
      }
      SettleLog.writeSettleLog(resourceToBeSettled.getTsFilePath() + SettleLog.COMMA_SEPERATOR
          + SettleCheckStatus.AFTER_SETTLE_FILE);
    } finally {
      resourceToBeSettled.readUnlock();
    }
    return settledTsFileResource;
  }

}
