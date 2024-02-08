package org.apache.iotdb.db.query.executor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogWriter {

  private BufferedWriter bufferedWriter;
  private String fileName;
  private int lines;
  private int linesToCommit;
  private boolean isOpen;
  // 每隔LINES_TO_COMMIT 行把新数据写入文件中，便于在录数据时查看
  private static final int LINES_TO_COMMIT = 1;

  /**
   * 输入要写的文件名
   *
   * @param fileName
   */
  public LogWriter(String fileName) {
    this.fileName = fileName;
    this.linesToCommit = LINES_TO_COMMIT;
  }

  /**
   * bufferedWriter有多少行就来写入数据
   *
   * @param fileName
   * @param linesToCommit
   */
  public LogWriter(String fileName, int linesToCommit) {
    this.fileName = fileName;
    this.linesToCommit = linesToCommit;
  }

  public void open() {
    File file = new File(fileName);
    File father = file.getParentFile();
    if (!father.exists()) father.mkdirs();
    try {
      bufferedWriter = new BufferedWriter(new FileWriter(file, true));
      isOpen = true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    lines = 0;
  }

  /**
   * 将一条数据写入文件
   *
   * @param line 写入文件的一行
   */
  public void log(CharSequence line) {
    try {
      if (bufferedWriter != null) {
        bufferedWriter.append(line);
        lines++;
        // 每隔LINES_TO_COMMIT 行把新数据写入文件中，便于在录数据时查看
        if (lines == linesToCommit) {
          bufferedWriter.flush();
          lines = 0;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** 关闭文件输出流 */
  public void close() {
    if (bufferedWriter != null)
      try {
        bufferedWriter.close();
        isOpen = false;
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  public boolean isOpen() {
    return isOpen;
  }
}
