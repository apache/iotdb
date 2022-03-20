package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.wal.buffer.WALEdit;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Pattern;

/** WALWriter writes the binary {@link WALEdit} into .wal file. */
public class WALWriter extends LogWriter {
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_FILE_SUFFIX;
  public static final Pattern WAL_FILE_NAME_PATTERN = Pattern.compile("_(?<versionId>\\d+)\\.wal");

  public WALWriter(File logFile) throws FileNotFoundException {
    super(logFile);
  }
}
