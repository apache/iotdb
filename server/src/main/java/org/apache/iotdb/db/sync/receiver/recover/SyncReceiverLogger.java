package org.apache.iotdb.db.sync.receiver.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SyncReceiverLogger implements ISyncReceiverLogger {

  public static final String SYNC_DELETED_FILE_NAME_START = "sync deleted file names start";
  public static final String SYNC_TSFILE_START = "sync tsfile start";
  private BufferedWriter bw;

  public SyncReceiverLogger(File logFile) throws IOException {
    bw = new BufferedWriter(new FileWriter(logFile));
  }

  @Override
  public void startSyncDeletedFilesName() throws IOException {
    bw.write(SYNC_DELETED_FILE_NAME_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishSyncDeletedFileName(File file) throws IOException {
    bw.write(file.getAbsolutePath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void startSyncTsFiles() throws IOException {
    bw.write(SYNC_TSFILE_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishSyncTsfile(File file) throws IOException {
    bw.write(file.getAbsolutePath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void close() throws IOException {
    bw.close();
  }
}
