package org.apache.iotdb.cluster.log.manage.serializable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.LogFactory;
import org.apache.iotdb.cluster.log.logtypes.LogType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class SyncLogDequeSerializer implements LogDequeSerializer {

  File logFile;
  File metaFile;
  FileOutputStream logOutputStream;
  FileOutputStream metaOutputStream;
  private Deque<Integer> logSizeDeque = new ArrayDeque<>();
  private LogManagerMeta meta;
  // mark first log position
  private long firstLogPosition = 0;
  // removed log size
  private long removedLogSize = 0;

  // only for test
  public void setMaxRemovedLogSize(long maxRemovedLogSize) {
    this.maxRemovedLogSize = maxRemovedLogSize;
  }

  // when the removedLogSize larger than this, we actually delete logs
  private long maxRemovedLogSize = ClusterDescriptor.getINSTANCE().getConfig().getMaxRemovedLogSize();

  // log in disk is
  // size of type | type string | size of log | log buffer
  // meta in disk is
  // firstLogPosition | size of log meta | log meta buffer
  public SyncLogDequeSerializer() {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    logFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logData");
    metaFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logMeta");
    init();
  }

  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  // init output stream
  private void init() {
    try {
      logOutputStream = new FileOutputStream(logFile);
      metaOutputStream = new FileOutputStream(metaFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addLast(Log log) {
    ByteBuffer data = log.serialize();
    String logType = log.getLogType().toString();
    int totalSize = 0;
    // write into disk
    try {
      totalSize += ReadWriteIOUtils.write(logType, logOutputStream);
      totalSize += ReadWriteIOUtils.write(data, logOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    logSizeDeque.addLast(totalSize);
  }

  @Override
  public void removeLast() {
    int size = logSizeDeque.removeLast();
    // write into disk
    try {
      logOutputStream.getChannel().truncate(logFile.length() - size);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void removeFirst(int removeNum) {
    firstLogPosition++;
    removedLogSize += logSizeDeque.removeFirst();
    // do actual deletion
    if(removedLogSize > maxRemovedLogSize){
      deleteRemovedLog();
    }
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
      }
      logReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  // read single log
  private Log readLog(FileInputStream logReader, boolean canSkip) throws IOException {
    String logType = ReadWriteIOUtils.readString(logReader);
    int logSize = ReadWriteIOUtils.readInt(logReader);
    int totalSize = Integer.BYTES + Integer.BYTES + logType.length() + logSize;

    if (canSkip) {
      logReader.skip(logSize);
      removedLogSize += totalSize;
    }

    Log log = LogFactory.getLogByType(LogType.valueOf(logType));
    log.deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytes(logReader, logSize)));
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
      metaOutputStream.getChannel().position(0);
      int size = 0;
      size += ReadWriteIOUtils.write(firstLogPosition, metaOutputStream);
      size += ReadWriteIOUtils.write(meta.serialize(), metaOutputStream);
      metaOutputStream.getChannel().truncate(size);
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
  private void deleteRemovedLog(){
    try {
      FileInputStream reader = new FileInputStream(logFile);
      // skip removed file
      for (int i = 0; i < removedLogSize; i++) {
        readLog(reader, true);
      }

      // begin to write
      logOutputStream.getChannel().position(0);
      int blockSize = 4096;

      long curPosition = reader.getChannel().position();
      while(curPosition < logFile.length()){
        long size = Math.min(blockSize, logFile.length() - curPosition);
        logOutputStream.write(ReadWriteIOUtils.readBytes(reader, (int) size));
        curPosition = reader.getChannel().position();
      }

      // truncate unused file part
      logOutputStream.getChannel().truncate(logOutputStream.getChannel().position());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // re init and serialize
    removedLogSize = 0;
    firstLogPosition = 0;
    serializeMeta(meta);
  }
}
