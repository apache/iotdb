package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.wal.checkpoint.Checkpoint;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** CheckpointWriter writes the binary {@link Checkpoint} into .checkpoint file. */
public class CheckpointWriter extends LogWriter {
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX;
  public static final Pattern CHECKPOINT_FILE_NAME_PATTERN =
      Pattern.compile("_(?<versionId>\\d+)\\.checkpoint");

  /** Return true when this file is .checkpoint file */
  public static boolean checkpointFilenameFilter(File dir, String name) {
    return CHECKPOINT_FILE_NAME_PATTERN.matcher(name).find();
  }

  /**
   * Parse version id from filename
   *
   * @return Return {@link Integer#MIN_VALUE} when this file is not .checkpoint file
   */
  public static int parseVersionId(String filename) {
    Matcher matcher = CHECKPOINT_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group("versionId"));
    }
    return Integer.MIN_VALUE;
  }

  /** Get .checkpoint filename */
  public static String getLogFileName(long version) {
    return FILE_PREFIX + version + FILE_SUFFIX;
  }

  public CheckpointWriter(File logFile) throws FileNotFoundException {
    super(logFile);
  }
}
