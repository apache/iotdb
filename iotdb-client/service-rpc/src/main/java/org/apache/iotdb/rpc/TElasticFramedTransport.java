/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import java.io.EOFException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md
public class TElasticFramedTransport extends TTransport {

  public static class Factory extends TTransportFactory {

    /**
     * It is used to prevent the size of the parsing package from being too large and allocating the
     * buffer will cause oom. Therefore, the maximum length of the requested memory is limited when
     * reading.
     */
    protected final int thriftMaxFrameSize;

    /**
     * The capacity of the underlying buffer is allowed to exceed thriftDefaultBufferSize, but if
     * adjacent requests all have sizes smaller than thriftDefaultBufferSize, the underlying buffer
     * will be shrunk beneath thriftDefaultBufferSize. The shrinking is limited at most once per
     * minute to reduce overhead when thriftDefaultBufferSize is set unreasonably or the workload
     * naturally contains both ver large and very small requests.
     */
    protected final int thriftDefaultBufferSize;

    /**
     * When copyBinary flag is true, the transport will copy the binary data from the underlying.
     * This is a protection for the underlying buffer to be reused by the caller protocol.
     */
    protected final boolean copyBinary;

    public Factory() {
      this(RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY, RpcUtils.THRIFT_FRAME_MAX_SIZE, true);
    }

    public Factory(int thriftDefaultBufferSize, int thriftMaxFrameSize, boolean copyBinary) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      this.copyBinary = copyBinary;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return new TElasticFramedTransport(
          trans, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary);
    }
  }

  public TElasticFramedTransport(
      TTransport underlying,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean copyBinary) {
    this.underlying = underlying;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.copyBinary = copyBinary;
    readBuffer = new AutoScalingBufferReadTransport(thriftDefaultBufferSize);
    writeBuffer = new AutoScalingBufferWriteTransport(thriftDefaultBufferSize);
  }

  protected final int thriftDefaultBufferSize;
  protected final int thriftMaxFrameSize;

  protected final TTransport underlying;
  protected AutoScalingBufferReadTransport readBuffer;
  protected AutoScalingBufferWriteTransport writeBuffer;
  protected final byte[] i32buf = new byte[4];
  private final boolean copyBinary;

  @Override
  public boolean isOpen() {
    return underlying.isOpen();
  }

  @Override
  public void open() throws TTransportException {
    underlying.open();
  }

  @Override
  public void close() {
    underlying.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int got = readBuffer.read(buf, off, len);
    if (got > 0) {
      return got;
    }

    try {
      // Read another frame of data
      readFrame();
    } catch (TTransportException e) {
      // Adding this workaround to avoid the Connection reset error log printed.
      if (e.getCause() instanceof SocketException && e.getMessage().contains("Connection reset")) {
        throw new TTransportException(TTransportException.END_OF_FILE, e.getCause());
      }
      // There is a bug fixed in Thrift 0.15. Some unnecessary error logs may be printed.
      // See https://issues.apache.org/jira/browse/THRIFT-5411 and
      // https://github.com/apache/thrift/commit/be20ad7e08fab200391e3eab41acde9da2a4fd07
      // Adding this workaround to avoid the problem.
      if (e.getCause() instanceof SocketTimeoutException) {
        throw new TTransportException(TTransportException.TIMED_OUT, e.getCause());
      }
      if (e.getCause() instanceof SSLHandshakeException) {
        // There is an unsolved JDK bug https://bugs.openjdk.org/browse/JDK-8221218.
        // Adding this workaround to avoid the error log printed.
        if (e.getMessage()
            .contains("Insufficient buffer remaining for AEAD cipher fragment (2).")) {
          throw new TTransportException(TTransportException.END_OF_FILE, e.getCause());
        }
        // When client with SSL shutdown due to time out. Some unnecessary error logs may be
        // printed.
        // Adding this workaround to avoid the problem.
        if (e.getCause().getCause() != null && e.getCause().getCause() instanceof EOFException) {
          throw new TTransportException(TTransportException.END_OF_FILE, e.getCause());
        }
      }

      if (e.getCause() instanceof SSLException
          && e.getMessage().contains("Unsupported or unrecognized SSL message")) {
        SocketAddress remoteAddress = null;
        if (underlying instanceof TSocket) {
          remoteAddress = ((TSocket) underlying).getSocket().getRemoteSocketAddress();
        }
        throw new TTransportException(
            TTransportException.CORRUPTED_DATA,
            String.format(
                "You may be sending non-SSL requests"
                    + "%s to the SSL-enabled Thrift-RPC port, please confirm that you are "
                    + "using the right configuration",
                remoteAddress == null ? "" : " from " + remoteAddress));
      }
      throw e;
    }
    return readBuffer.read(buf, off, len);
  }

  protected void readFrame() throws TTransportException {
    underlying.readAll(i32buf, 0, 4);
    int size = TFramedTransport.decodeFrameSize(i32buf);
    checkFrameSize(size);
    readBuffer.fill(underlying, size);
  }

  protected void checkFrameSize(int size) throws TTransportException {
    final int HTTP_GET_SIGNATURE = 0x47455420; // "GET "
    final int HTTP_POST_SIGNATURE = 0x504F5354; // "POST"
    final int TLS_MIN_VERSION = 0x160300;
    final int TLS_MAX_VERSION = 0x160303;
    final int TLS_LENGTH_HIGH_MAX = 0x02;

    FrameError error = null;
    if (size == HTTP_GET_SIGNATURE || size == HTTP_POST_SIGNATURE) {
      error = FrameError.HTTP_REQUEST;
    } else {
      int high24 = size >>> 8;
      if (high24 >= TLS_MIN_VERSION
          && high24 <= TLS_MAX_VERSION
          && (i32buf[3] & 0xFF) <= TLS_LENGTH_HIGH_MAX) {
        error = FrameError.TLS_REQUEST;
      } else if (size < 0) {
        error = FrameError.NEGATIVE_FRAME_SIZE;
      } else if (size > thriftMaxFrameSize) {
        error = FrameError.FRAME_SIZE_EXCEEDED;
      }
    }

    if (error == null) {
      return;
    }

    SocketAddress remoteAddress = null;
    if (underlying instanceof TSocket) {
      remoteAddress = ((TSocket) underlying).getSocket().getRemoteSocketAddress();
    }
    String remoteInfo = (remoteAddress == null) ? "" : " from " + remoteAddress;
    close();

    error.throwException(size, remoteInfo, thriftMaxFrameSize);
  }

  private enum FrameError {
    HTTP_REQUEST(
        "Singular frame size (%d) detected, you may be sending HTTP GET/POST%s "
            + "requests to the Thrift-RPC port, please confirm that you are using the right port"),
    TLS_REQUEST(
        "Singular frame size (%d) detected, you may be sending TLS ClientHello "
            + "requests%s to the Non-SSL Thrift-RPC port, please confirm that you are using "
            + "the right configuration"),
    NEGATIVE_FRAME_SIZE("Read a negative frame size (%d)%s!"),
    FRAME_SIZE_EXCEEDED("Frame size (%d) larger than protect max size (%d)%s!");

    private final String messageFormat;

    FrameError(String messageFormat) {
      this.messageFormat = messageFormat;
    }

    void throwException(int size, String remoteInfo, int maxSize) throws TTransportException {
      String message =
          (this == FRAME_SIZE_EXCEEDED)
              ? String.format(messageFormat, size, maxSize, remoteInfo)
              : String.format(messageFormat, size, remoteInfo);
      throw new TTransportException(TTransportException.CORRUPTED_DATA, message);
    }
  }

  @Override
  public void flush() throws TTransportException {
    int length = writeBuffer.getPos();
    TFramedTransport.encodeFrameSize(length, i32buf);
    underlying.write(i32buf, 0, 4);
    underlying.write(writeBuffer.getBuffer(), 0, length);
    writeBuffer.reset();
    if (length > thriftDefaultBufferSize) {
      writeBuffer.resizeIfNecessary(thriftDefaultBufferSize);
    }
    underlying.flush();
  }

  @Override
  public TConfiguration getConfiguration() {
    return underlying.getConfiguration();
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {
    // do nothing now.
  }

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {
    // do nothing now.
    // here we can do some checkm, e.g., see whether the memory is enough.
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    writeBuffer.write(buf, off, len);
  }

  public TTransport getSocket() {
    return underlying;
  }

  @Override
  public int getBytesRemainingInBuffer() {
    // return -1 can make the caller protocol to copy binary data from the underlying transport.
    return copyBinary ? -1 : readBuffer.getBytesRemainingInBuffer();
  }

  @Override
  public byte[] getBuffer() {
    return readBuffer.getBuffer();
  }

  @Override
  public int getBufferPosition() {
    return readBuffer.getBufferPosition();
  }

  @Override
  public void consumeBuffer(int len) {
    readBuffer.consumeBuffer(len);
  }
}
