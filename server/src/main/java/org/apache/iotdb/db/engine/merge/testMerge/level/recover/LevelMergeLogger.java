package org.apache.iotdb.db.engine.merge.testMerge.level.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class LevelMergeLogger implements MergeLogger {

  public static final String MERGE_LOG_NAME = "merge.log.level";

  static final String STR_ALL_TS_END = "all ts end";
  static final String STR_MERGE_START = "merge start";

  private BufferedWriter logStream;

  public LevelMergeLogger(String storageGroupDir) throws IOException {
    logStream = new BufferedWriter(new FileWriter(new File(storageGroupDir, MERGE_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  @Override
  public void logAllTsEnd() throws IOException {
    logStream.write(STR_ALL_TS_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logMergeStart() throws IOException {
    logStream.write(STR_MERGE_START);
    logStream.newLine();
    logStream.flush();
  }

  @Override
  public void logNewFile(TsFileResource resource) throws IOException {
    logStream.write(resource.getFile().getAbsolutePath());
    logStream.newLine();
    logStream.flush();
  }
}
