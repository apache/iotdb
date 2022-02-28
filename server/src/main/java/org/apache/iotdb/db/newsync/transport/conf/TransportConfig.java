package org.apache.iotdb.db.newsync.transport.conf;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.sync.conf.SyncConstant;

import java.io.File;

public class TransportConfig {
  private TransportConfig() {}

  /** default base dir, stores all IoTDB runtime files */
  private static final String DEFAULT_BASE_DIR = addHomeDir("data");

  private static String addHomeDir(String dir) {
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  public static String getSyncedDir(String ipAddress, String uuid) {
    return DEFAULT_BASE_DIR
        + File.separator
        + SyncConstant.SYNC_RECEIVER
        + File.separator
        + ipAddress
        + SyncConstant.SYNC_DIR_NAME_SEPARATOR
        + uuid
        + File.separator
        + SyncConstant.RECEIVER_DATA_FOLDER_NAME;
  }

  public static boolean isCheckFileDegistAgain = false;
}
