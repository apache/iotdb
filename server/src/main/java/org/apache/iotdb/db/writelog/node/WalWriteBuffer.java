package org.apache.iotdb.db.writelog.node;

import java.nio.ByteBuffer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.writelog.io.ILogWriter;

public class WalWriteBuffer {

  private boolean isPrevious;

  private ILogWriter fileWriter;

  private ByteBuffer logBufferWorking = ByteBuffer
      .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
  private ByteBuffer logBufferIdle = ByteBuffer
      .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);

  private ByteBuffer logBufferFlushing;

  private int bufferedLogNum = 0;

  public WalWriteBuffer(boolean isPrevious) {
    this.isPrevious = isPrevious;
  }

  public void setPrevious(boolean previous) {
    isPrevious = previous;
  }

  public boolean isPrevois() {
    return isPrevious;
  }

  public ByteBuffer getLogBufferWorking() {
    return logBufferWorking;
  }

  public void setLogBufferWorking(ByteBuffer logBufferWorking) {
    this.logBufferWorking = logBufferWorking;
  }

  public ByteBuffer getLogBufferIdle() {
    return logBufferIdle;
  }

  public void setLogBufferIdle(ByteBuffer logBufferIdle) {
    this.logBufferIdle = logBufferIdle;
  }

  public ByteBuffer getLogBufferFlushing() {
    return logBufferFlushing;
  }

  public void setLogBufferFlushing(ByteBuffer logBufferFlushing) {
    this.logBufferFlushing = logBufferFlushing;
  }

  public void incrementLogCnt(){
    bufferedLogNum++;
  }

  public int getBufferedLogNum(){
    return bufferedLogNum;
  }

  public void setBufferedLogNum(int bufferedLogNum) {
    this.bufferedLogNum = bufferedLogNum;
  }

  public ILogWriter getFileWriter() {
    return fileWriter;
  }

  public void setFileWriter(ILogWriter fileWriter) {
    this.fileWriter = fileWriter;
  }
}
