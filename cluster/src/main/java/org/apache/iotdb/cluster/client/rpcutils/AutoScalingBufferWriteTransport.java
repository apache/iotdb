package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.AutoExpandingBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-11-11 20:08
 */
public class AutoScalingBufferWriteTransport extends TTransport {

  private static final Logger logger = LoggerFactory
      .getLogger(AutoScalingBufferWriteTransport.class);
  private final AutoScalingBuffer buf;
  private int pos;

  public AutoScalingBufferWriteTransport(int initialCapacity, double growthCoefficient) {
    this.buf = new AutoScalingBuffer(initialCapacity, growthCoefficient);
    this.pos = 0;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void open() throws TTransportException {
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] toWrite, int off, int len) throws TTransportException {
    buf.resizeIfNecessary(pos + len);
    System.arraycopy(toWrite, off, buf.array(), pos, len);
    pos += len;
  }

  public AutoExpandingBuffer getBuf() {
    return buf;
  }

  public int getPos() {
    return pos;
  }

  public void reset() {
    pos = 0;
  }

  public void shrinkSizeIfNecessary(int size) {
    logger.debug("try to shrink the write buffer, size={}", size);
    buf.shrinkSizeIfNecessary(size);
  }
}
