package org.apache.iotdb.db.sync.receiver.load;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LoadLogger implements ILoadLogger {

  public static final String LOAD_DELETED_FILE_NAME_START = "load deleted files start";
  public static final String LOAD_TSFILE_START = "load tsfile start";
  private BufferedWriter bw;

  public LoadLogger(File logFile) throws IOException {
    bw = new BufferedWriter(new FileWriter(logFile));
  }

  @Override
  public void startLoadDeletedFiles() throws IOException {
    bw.write(LOAD_DELETED_FILE_NAME_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishLoadDeletedFile(File file) throws IOException {
    bw.write(file.getAbsolutePath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void startLoadTsFiles() throws IOException {
    bw.write(LOAD_TSFILE_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishLoadTsfile(File file) throws IOException {
    bw.write(file.getAbsolutePath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void close() throws IOException {
    bw.close();
  }
}
