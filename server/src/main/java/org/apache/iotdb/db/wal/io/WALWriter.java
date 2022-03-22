package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.wal.buffer.WALEdit;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** WALWriter writes the binary {@link WALEdit} into .wal file. */
public class WALWriter extends LogWriter {
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_FILE_SUFFIX;
  public static final Pattern WAL_FILE_NAME_PATTERN = Pattern.compile("_(?<versionId>\\d+)\\.wal");

  /** Return true when this file is .wal file */
  public static boolean walFilenameFilter(File dir, String name) {
    return WAL_FILE_NAME_PATTERN.matcher(name).find();
  }

  /**
   * Parse version id from filename
   *
   * @return Return {@link Integer#MIN_VALUE} when this file is not .wal file
   */
  public static int parseVersionId(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group("versionId"));
    }
    return Integer.MIN_VALUE;
  }

  /** Get .wal filename */
  public static String getLogFileName(long version) {
    return FILE_PREFIX + version + FILE_SUFFIX;
  }

  public WALWriter(File logFile) throws FileNotFoundException {
    super(logFile);
  }
}
