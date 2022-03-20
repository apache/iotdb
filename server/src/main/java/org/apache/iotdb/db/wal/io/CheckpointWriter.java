package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.wal.checkpoint.Checkpoint;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Pattern;

/** CheckpointWriter writes the binary {@link Checkpoint} into .checkpoint file. */
public class CheckpointWriter extends LogWriter {
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX;
  public static final Pattern CHECKPOINT_FILE_NAME_PATTERN =
      Pattern.compile("_(?<versionId>\\d+)\\.checkpoint");

  public CheckpointWriter(File logFile) throws FileNotFoundException {
    super(logFile);
  }
}
