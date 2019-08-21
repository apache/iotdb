package org.apache.iotdb.db.sync.sender.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SyncSenderLogger implements ISyncSenderLogger {

  public static final String SYNC_START = "sync start";
  public static final String SYNC_END = "sync end";
  public static final String SYNC_DELETED_FILE_NAME_START = "sync deleted file names start";
  public static final String SYNC_DELETED_FILE_NAME_END = "sync deleted file names end";
  public static final String SYNC_TSFILE_START = "sync tsfile start";
  public static final String SYNC_TSFILE_END = "sync tsfile end";
  private BufferedWriter bw;

  public SyncSenderLogger(String filePath) throws IOException {
    this.bw = new BufferedWriter(new FileWriter(filePath));
  }

  public SyncSenderLogger(File file) throws IOException {
    this(file.getAbsolutePath());
  }

  @Override
  public void startSync() throws IOException {
    bw.write(SYNC_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void endSync() throws IOException {
    bw.write(SYNC_END);
    bw.newLine();
    bw.flush();
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
  public void endSyncDeletedFilsName() throws IOException {
    bw.write(SYNC_DELETED_FILE_NAME_END);
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
  public void endSyncTsFiles() throws IOException {
    bw.write(SYNC_TSFILE_END);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void close() throws IOException {
    bw.close();
  }
}
