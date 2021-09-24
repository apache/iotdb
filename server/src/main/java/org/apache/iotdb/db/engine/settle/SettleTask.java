package org.apache.iotdb.db.engine.settle;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SettleTask extends WrappedRunnable {

  private static final Logger logger = LoggerFactory.getLogger(SettleTask.class);
  private TsFileResource resourceToBeSettled;
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

  public void settleTsFile() throws IOException, WriteProcessException, IllegalPathException {
    TsFileResource settledResource = null;
    if (!resourceToBeSettled.isClosed()) {
      logger.warn(
          "The tsFile {} should be sealed when settling.", resourceToBeSettled.getTsFilePath());
      return;
    }
    if (TsFileAndModSettleTool.isSettledFileGenerated(resourceToBeSettled)) {
      logger.info("find settled file for {}", resourceToBeSettled.getTsFile());
      settledResource = TsFileAndModSettleTool.findSettledFile(resourceToBeSettled);
    } else {
      logger.info("generate settled file for {}", resourceToBeSettled.getTsFile());
      // Write Settle Log, Status 1
      SettleLog.writeSettleLog(
          resourceToBeSettled.getTsFilePath()
              + SettleLog.COMMA_SEPERATOR
              + SettleCheckStatus.BEGIN_SETTLE_FILE);
      try (TsFileAndModSettleTool tsFileAndModSettleTool =
          new TsFileAndModSettleTool(resourceToBeSettled)) {
        settledResource = tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled);
      }

      // Write Settle Log, Status 2
      SettleLog.writeSettleLog(
          resourceToBeSettled.getTsFilePath()
              + SettleLog.COMMA_SEPERATOR
              + SettleCheckStatus.AFTER_SETTLE_FILE);
    }
    resourceToBeSettled.getSettleTsFileCallBack().call(resourceToBeSettled, settledResource);

    // Write Settle Log, Status 3
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.SETTLE_SUCCESS);
    logger.info(
        "Settle completes, file path:{} , the remaining file to be settled num: {}",
        resourceToBeSettled.getTsFilePath(),
        SettleService.getFilesToBeSettledCount().get());

    if (SettleService.getFilesToBeSettledCount().get() == 0) {
      SettleLog.closeLogWriter();
      fsFactory.getFile(SettleLog.getSettleLogPath()).delete();
      SettleService.getINSTANCE().stop();
      logger.info("All files settled successfully! ");
    }
  }
}
