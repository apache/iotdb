/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.KeyStore;

/** Transport for use with async client. */
public class TNonblockingSSLSocket extends TNonblockingSocket {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TNonblockingSSLSocket.class.getName());

  private final SSLEngine sslEngine_;

  private final ByteBuffer appUnwrap;
  private final ByteBuffer netUnwrap;

  private final ByteBuffer appWrap;
  private final ByteBuffer netWrap;

  private ByteBuffer decodedBytes;

  private boolean isHandshakeCompleted;

  private SelectionKey selectionKey;

  public TNonblockingSSLSocket(
      String host,
      int port,
      int timeout,
      String keystore,
      String keystorePassword,
      String truststore,
      String truststorePassword)
      throws TTransportException, IOException {
    this(
        host,
        port,
        timeout,
        createSSLContext(keystore, keystorePassword, truststore, truststorePassword));
  }

  private static SSLContext createSSLContext(
      String keystore, String keystorePassword, String truststore, String truststorePassword)
      throws TTransportException {
    SSLContext ctx;
    InputStream in = null;
    InputStream is = null;

    try {
      ctx = SSLContext.getInstance("TLS");
      TrustManagerFactory tmf = null;
      KeyManagerFactory kmf = null;

      if (truststore != null) {
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ts = KeyStore.getInstance("JKS");
        in = getStoreAsStream(truststore);
        ts.load(in, (truststorePassword != null ? truststorePassword.toCharArray() : null));
        tmf.init(ts);
      }

      if (keystore != null) {
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("JKS");
        is = getStoreAsStream(keystore);
        ks.load(is, keystorePassword.toCharArray());
        kmf.init(ks, keystorePassword.toCharArray());
      }

      if (keystore != null && truststore != null) {
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
      } else if (keystore != null) {
        ctx.init(kmf.getKeyManagers(), null, null);
      } else {
        ctx.init(null, tmf.getTrustManagers(), null);
      }

    } catch (Exception e) {
      throw new TTransportException(
          TTransportException.NOT_OPEN, "Error creating the transport", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOGGER.warn("Unable to close stream", e);
        }
      }
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOGGER.warn("Unable to close stream", e);
        }
      }
    }

    return ctx;
  }

  private static InputStream getStoreAsStream(String store) throws IOException {
    try {
      return new FileInputStream(store);
    } catch (FileNotFoundException e) {
    }

    InputStream storeStream = null;
    try {
      storeStream = new URL(store).openStream();
      if (storeStream != null) {
        return storeStream;
      }
    } catch (MalformedURLException e) {
    }

    storeStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(store);

    if (storeStream != null) {
      return storeStream;
    } else {
      throw new IOException("Could not load file: " + store);
    }
  }

  protected TNonblockingSSLSocket(String host, int port, int timeout, SSLContext sslContext)
      throws IOException, TTransportException {
    super(host, port, timeout);
    sslEngine_ = sslContext.createSSLEngine(host, port);
    sslEngine_.setUseClientMode(true);

    int appBufferSize = sslEngine_.getSession().getApplicationBufferSize();
    int netBufferSize = sslEngine_.getSession().getPacketBufferSize();
    appUnwrap = ByteBuffer.allocate(appBufferSize);
    netUnwrap = ByteBuffer.allocate(netBufferSize);
    appWrap = ByteBuffer.allocate(appBufferSize);
    netWrap = ByteBuffer.allocate(netBufferSize);
    decodedBytes = ByteBuffer.allocate(appBufferSize);
    decodedBytes.flip();
    isHandshakeCompleted = false;
  }

  /** {@inheritDoc} */
  @Override
  public SelectionKey registerSelector(Selector selector, int interests) throws IOException {
    selectionKey = super.registerSelector(selector, interests);
    return selectionKey;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOpen() {
    // isConnected() does not return false after close(), but isOpen() does
    return super.isOpen() && isHandshakeCompleted;
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws TTransportException {
    throw new RuntimeException("open() is not implemented for TNonblockingSSLSocket");
  }

  /** {@inheritDoc} */
  @Override
  public synchronized int read(ByteBuffer buffer) throws TTransportException {
    int numBytes = buffer.limit();
    while (decodedBytes.remaining() < numBytes) {
      try {
        if (doUnwrap() == -1) {
          throw new IOException("Unable to read " + numBytes + " bytes");
        }
      } catch (IOException exc) {
        throw new TTransportException(TTransportException.UNKNOWN, exc.getMessage());
      }
      if (appUnwrap.position() > 0) {
        int t;
        appUnwrap.flip();
        if (decodedBytes.position() > 0) decodedBytes.flip();
        t = appUnwrap.limit() + decodedBytes.limit();
        byte[] tmpBuffer = new byte[t];
        decodedBytes.get(tmpBuffer, 0, decodedBytes.remaining());
        appUnwrap.get(tmpBuffer, 0, appUnwrap.remaining());
        if (appUnwrap.position() > 0) {
          appUnwrap.clear();
          appUnwrap.flip();
          appUnwrap.compact();
        }
        decodedBytes = ByteBuffer.wrap(tmpBuffer);
      }
    }
    byte[] b = new byte[numBytes];
    decodedBytes.get(b, 0, numBytes);
    if (decodedBytes.position() > 0) {
      decodedBytes.compact();
      decodedBytes.flip();
    }
    buffer.put(b);
    selectionKey.interestOps(SelectionKey.OP_WRITE);
    return numBytes;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized int write(ByteBuffer buffer) throws TTransportException {
    int numBytes = 0;

    if (buffer.position() > 0) buffer.flip();

    int nTransfer;
    int num;
    while (buffer.remaining() != 0) {
      nTransfer = Math.min(appWrap.remaining(), buffer.remaining());
      if (nTransfer > 0) {
        appWrap.put(buffer.array(), buffer.arrayOffset() + buffer.position(), nTransfer);
        buffer.position(buffer.position() + nTransfer);
      }

      try {
        num = doWrap();
      } catch (IOException iox) {
        throw new TTransportException(TTransportException.UNKNOWN, iox);
      }
      if (num < 0) {
        LOGGER.error("Failed while writing. Probably server is down");
        return -1;
      }
      numBytes += num;
    }
    return numBytes;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    sslEngine_.closeOutbound();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public boolean startConnect() throws IOException {
    if (this.isOpen()) {
      return true;
    }
    sslEngine_.beginHandshake();
    return super.startConnect() && doHandShake();
  }

  /** {@inheritDoc} */
  @Override
  public boolean finishConnect() throws IOException {
    return super.finishConnect() && doHandShake();
  }

  private synchronized boolean doHandShake() throws IOException {
    LOGGER.debug("Handshake is started");
    while (true) {
      HandshakeStatus hs = sslEngine_.getHandshakeStatus();
      switch (hs) {
        case NEED_UNWRAP:
          if (doUnwrap() == -1) {
            LOGGER.error("Unexpected. Handshake failed abruptly during unwrap");
            return false;
          }
          break;
        case NEED_WRAP:
          if (doWrap() == -1) {
            LOGGER.error("Unexpected. Handshake failed abruptly during wrap");
            return false;
          }
          break;
        case NEED_TASK:
          if (!doTask()) {
            LOGGER.error("Unexpected. Handshake failed abruptly during task");
            return false;
          }
          break;
        case FINISHED:
        case NOT_HANDSHAKING:
          isHandshakeCompleted = true;
          return true;
        default:
          LOGGER.error("Unknown handshake status. Handshake failed");
          return false;
      }
    }
  }

  private synchronized boolean doTask() {
    Runnable runnable;
    while ((runnable = sslEngine_.getDelegatedTask()) != null) {
      runnable.run();
    }
    HandshakeStatus hs = sslEngine_.getHandshakeStatus();
    return hs != HandshakeStatus.NEED_TASK;
  }

  private synchronized int doUnwrap() throws IOException {
    int num = getSocketChannel().read(netUnwrap);
    if (num < 0) {
      LOGGER.error("Failed during read operation. Probably server is down");
      return -1;
    }
    SSLEngineResult unwrapResult;

    try {
      netUnwrap.flip();
      unwrapResult = sslEngine_.unwrap(netUnwrap, appUnwrap);
      netUnwrap.compact();
    } catch (SSLException ex) {
      LOGGER.error(ex.getMessage());
      throw ex;
    }

    switch (unwrapResult.getStatus()) {
      case OK:
        if (appUnwrap.position() > 0) {
          appUnwrap.flip();
          appUnwrap.compact();
        }
        break;
      case CLOSED:
        return -1;
      case BUFFER_OVERFLOW:
        throw new IllegalStateException("Failed to unwrap");
      case BUFFER_UNDERFLOW:
        break;
    }
    return num;
  }

  private synchronized int doWrap() throws IOException {
    int num = 0;
    SSLEngineResult wrapResult;
    try {
      appWrap.flip();
      wrapResult = sslEngine_.wrap(appWrap, netWrap);
      appWrap.compact();
    } catch (SSLException exc) {
      LOGGER.error(exc.getMessage());
      throw exc;
    }

    switch (wrapResult.getStatus()) {
      case OK:
        if (netWrap.position() > 0) {
          netWrap.flip();
          num = getSocketChannel().write(netWrap);
          netWrap.compact();
        }
        break;
      case BUFFER_UNDERFLOW:
        // try again later
        break;
      case BUFFER_OVERFLOW:
        throw new IllegalStateException("Failed to wrap");
      case CLOSED:
        LOGGER.error("SSL session is closed");
        return -1;
    }
    return num;
  }
}
