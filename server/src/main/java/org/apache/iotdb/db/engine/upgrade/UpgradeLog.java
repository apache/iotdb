package org.apache.iotdb.db.engine.upgrade;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeLog {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeLog.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String UPGRADE_DIR = "upgrade";
  private static final String UPGRADE_LOG_NAME = "upgrade.txt";
  private static File upgradeLogPath = SystemFileFactory.INSTANCE
      .getFile(SystemFileFactory.INSTANCE.getFile(config.getSystemDir(), UPGRADE_DIR),
          UPGRADE_LOG_NAME);

  public static boolean createUpgradeLog() {
    try {
      if (!upgradeLogPath.getParentFile().exists()) {
        upgradeLogPath.getParentFile().mkdirs();
      }
      upgradeLogPath.createNewFile();
      return true;
    } catch (IOException e) {
      logger.error("meet error when create upgrade log, file path:{}",
          upgradeLogPath, e);
      return false;
    }
  }

  public static String getUpgradeLogPath() {
    return upgradeLogPath.getAbsolutePath();
  }

  public static boolean writeUpgradeLogFile(String content) {
    UpgradeUtils.getUpgradeLogLock().writeLock().lock();
    try (BufferedWriter upgradeLogWriter = new BufferedWriter(
        FSFactoryProducer.getFSFactory().getBufferedWriter(getUpgradeLogPath(), true))) {
      upgradeLogWriter.write(content);
      upgradeLogWriter.newLine();
      return true;
    } catch(IOException e) {
      logger.error("write upgrade log file failed, the log file:{}", getUpgradeLogPath(), e);
      return false;
    } finally {
      UpgradeUtils.getUpgradeLogLock().writeLock().unlock();
    }
  }
}
