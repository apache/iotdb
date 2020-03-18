package org.apache.iotdb.cluster.log.manage.serializable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class SyncLogDequeSerializer implements LogDequeSerializer {

  File logFile;
  File metaFile;
  FileOutputStream logOutputStream;
  FileOutputStream metaOutputStream;
  LogParser parser = LogParser.getINSTANCE();
  private Deque<Integer> logSizeDeque = new ArrayDeque<>();
  private LogManagerMeta meta;
  // mark first log position
  private long firstLogPosition = 0;
  // removed log size
  private long removedLogSize = 0;
  // when the removedLogSize larger than this, we actually delete logs
  private long maxRemovedLogSize = ClusterDescriptor.getINSTANCE().getConfig()
      .getMaxRemovedLogSize();

  /**
   * for log tools
   * @param logPath log dir path
   */
  public SyncLogDequeSerializer(String logPath) {
    logFile = SystemFileFactory.INSTANCE
        .getFile(logPath + File.separator + "logData");
    metaFile = SystemFileFactory.INSTANCE
        .getFile(logPath + File.separator + "logMeta");
    String name = logFile.getAbsolutePath();
    init();
  }

  /**
   * log in disk is
   * size of log | log buffer
   * meta in disk is
   * firstLogPosition | size of log meta | log meta buffer
   */
  public SyncLogDequeSerializer() {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    logFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logData");
    metaFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logMeta");
    init();
  }

  // only for test
  public void setMaxRemovedLogSize(long maxRemovedLogSize) {
    this.maxRemovedLogSize = maxRemovedLogSize;
  }

  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  // init output stream
  private void init() {
    try {
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdir();
        logFile.createNewFile();
        logFile.createNewFile();
      }

      logOutputStream = new FileOutputStream(logFile, true);
      metaOutputStream = new FileOutputStream(metaFile, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addLast(Log log, LogManagerMeta meta) {
    ByteBuffer data = log.serialize();
    int totalSize = 0;
    // write into disk
    try {
      totalSize += ReadWriteIOUtils.write(data, logOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    logSizeDeque.addLast(totalSize);
    serializeMeta(meta);
  }

  @Override
  public void removeLast(LogManagerMeta meta) {
    truncateLogIntern(1);
    serializeMeta(meta);
  }

  @Override
  public void truncateLog(int count, LogManagerMeta meta){
    truncateLogIntern(count);
    serializeMeta(meta);
  }

  private void truncateLogIntern(int count){
    if(logSizeDeque.size() > count){
      throw new IllegalArgumentException("truncate log count is bigger than total log count");
    }

    int size = 0;
    for (int i = 0; i < count; i++) {
      size += logSizeDeque.removeLast();
    }
    // write into disk
    try {
      logOutputStream.getChannel().truncate(logFile.length() - size);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void removeFirst(int num) {
    firstLogPosition += num;
    for (int i = 0; i < num; i++) {
      removedLogSize += logSizeDeque.removeFirst();
    }

    // do actual deletion
    if (removedLogSize > maxRemovedLogSize) {
      deleteRemovedLog();
    }

    // firstLogPosition changed
    serializeMeta(meta);
  }

  @Override
  public Deque<Log> recoverLog() {
    if (meta == null) {
      recoverMeta();
    }

    if (!logFile.exists()) {
      return new ArrayDeque<>();
    }

    Deque<Log> result = new ArrayDeque<>();
    long count = 0;
    try {
      FileInputStream logReader = new FileInputStream(logFile);
      FileChannel logChannel = logReader.getChannel();
      while (logChannel.position() < logFile.length()) {
        // actual log
        if (count >= firstLogPosition) {
          Log log = readLog(logReader, false);
          result.addLast(log);
        }
        // removed log, skip
        else {
          readLog(logReader, true);
        }
        count++;
      }
      logReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  // read single log
  private Log readLog(FileInputStream logReader, boolean canSkip) throws IOException {
    int logSize = ReadWriteIOUtils.readInt(logReader);
    int totalSize = Integer.BYTES + logSize;

    if (canSkip) {
      logReader.skip(logSize);
      removedLogSize += totalSize;
      return null;
    }

    Log log = null;

    try {
      log = parser.parse(ByteBuffer.wrap(ReadWriteIOUtils.readBytes(logReader, logSize)));
    } catch (UnknownLogTypeException e) {
      e.printStackTrace();
    }

    logSizeDeque.addLast(totalSize);

    return log;
  }

  @Override
  public LogManagerMeta recoverMeta() {
    if (meta == null && metaFile.exists() && metaFile.length() > 0) {
      try {
        FileInputStream metaReader = new FileInputStream(metaFile);
        firstLogPosition = ReadWriteIOUtils.readLong(metaReader);
        meta = LogManagerMeta.deserialize(
            ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(metaReader)));
        metaReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return meta;
  }

  @Override
  public void serializeMeta(LogManagerMeta meta) {
    try {
      metaOutputStream.getChannel().truncate(0);
      ReadWriteIOUtils.write(firstLogPosition, metaOutputStream);
      ReadWriteIOUtils.write(meta.serialize(), metaOutputStream);

      this.meta = meta;
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void close() {
    try {
      logOutputStream.close();
      metaOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // actually delete removed logs, this may take lots of times
  // TODO : use an async method to delete file
  private void deleteRemovedLog() {
    try {
      FileInputStream reader = new FileInputStream(logFile);
      // skip removed file
      for (int i = 0; i < firstLogPosition; i++) {
        readLog(reader, true);
      }

      // begin to write
      File tempLogFile = SystemFileFactory.INSTANCE
          .getFile(
              IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "raftLog"
                  + File.separator + "logData.temp");

      logOutputStream.close();
      logOutputStream = new FileOutputStream(tempLogFile);
      int blockSize = 4096;
      long curPosition = reader.getChannel().position();
      while (curPosition < logFile.length()) {
        long size = Math.min(blockSize, logFile.length() - curPosition);
        logOutputStream.write(ReadWriteIOUtils.readBytes(reader, (int) size));
        curPosition = reader.getChannel().position();
      }

      // rename file
      logFile.delete();
      tempLogFile.renameTo(logFile);
      logOutputStream.close();
      reader.close();
      logOutputStream = new FileOutputStream(logFile, true);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // re init and serialize
    removedLogSize = 0;
    firstLogPosition = 0;
    // meta will be serialized by caller
  }
}
