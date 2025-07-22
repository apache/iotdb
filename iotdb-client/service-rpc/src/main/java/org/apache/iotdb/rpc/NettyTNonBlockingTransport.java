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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.security.KeyStore;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A non-blocking Thrift transport implementation using Netty for asynchronous I/O. Integrates with
 * Thrift's TAsyncClientManager using a dummy local SocketChannel for selector events. Supports
 * SSL/TLS for secure communication.
 */
public class NettyTNonBlockingTransport extends TNonblockingTransport {

  private static final Logger logger = LoggerFactory.getLogger(NettyTNonBlockingTransport.class);

  private final String host;
  private final int port;
  private final int connectTimeoutMs;
  private final long sslHandshakeTimeoutMs;
  private final EventLoopGroup eventLoopGroup;
  private final Bootstrap bootstrap;
  private volatile Channel channel;
  private final AtomicBoolean connected = new AtomicBoolean(false);
  private final AtomicBoolean connecting = new AtomicBoolean(false);
  private final BlockingQueue<ByteBuf> readQueue = new LinkedBlockingQueue<>();
  private final Object lock = new Object();

  // SSL configuration
  private final String keystorePath;
  private final String keystorePassword;
  private final String truststorePath;
  private final String truststorePassword;

  // Dummy local socket for selector integration
  private ServerSocketChannel dummyServer;
  private java.nio.channels.SocketChannel dummyClient;
  private java.nio.channels.SocketChannel dummyServerAccepted;
  private int dummyPort;
  private Selector selector; // Stored for wakeup if needed

  public NettyTNonBlockingTransport(
      String host,
      int port,
      int connectTimeoutMs,
      String keystorePath,
      String keystorePassword,
      String truststorePath,
      String truststorePassword)
      throws TTransportException {
    super(new TConfiguration());
    this.host = host;
    this.port = port;
    this.connectTimeoutMs = connectTimeoutMs;
    this.sslHandshakeTimeoutMs = connectTimeoutMs;
    this.eventLoopGroup = new NioEventLoopGroup();
    this.bootstrap = new Bootstrap();
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.truststorePath = truststorePath;
    this.truststorePassword = truststorePassword;
    initDummyChannels();
    initBootstrap();
  }

  /** Initializes dummy local channels for selector event simulation. */
  private void initDummyChannels() throws TTransportException {
    try {
      dummyServer = ServerSocketChannel.open();
      dummyServer.configureBlocking(false);
      dummyServer.bind(new InetSocketAddress("localhost", 0));
      dummyPort = dummyServer.socket().getLocalPort();
      logger.debug("Dummy server bound to localhost:{}", dummyPort);

      dummyClient = java.nio.channels.SocketChannel.open();
      dummyClient.configureBlocking(false);
    } catch (IOException e) {
      throw new TTransportException("Failed to initialize dummy channels", e);
    }
  }

  private void initBootstrap() {
    bootstrap
        .group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                logger.debug("Initializing channel for {}:{}", host, port);

                ChannelPipeline pipeline = ch.pipeline();
                SslContext sslContext = createSslContext();
                SslHandler sslHandler = sslContext.newHandler(ch.alloc(), host, port);
                sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMs);

                pipeline.addLast("ssl", sslHandler);
                sslHandler
                    .handshakeFuture()
                    .addListener(
                        future -> {
                          if (future.isSuccess()) {
                            logger.debug(
                                "SSL handshake completed successfully for {}:{}", host, port);
                          } else {
                            logger.debug(
                                "SSL handshake failed for {}:{}: {}",
                                host,
                                port,
                                future.cause().getMessage());
                          }
                        });

                pipeline.addLast("handler", new NettyTransportHandler());
              }
            });
  }

  private SslContext createSslContext() throws Exception {
    SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

    if (keystorePath != null && keystorePassword != null) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(keystorePath)) {
        keyStore.load(fis, keystorePassword.toCharArray());
      }
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keystorePassword.toCharArray());
      sslContextBuilder.keyManager(kmf);
    }

    if (truststorePath != null && truststorePassword != null) {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(truststorePath)) {
        trustStore.load(fis, truststorePassword.toCharArray());
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      sslContextBuilder.trustManager(tmf);
    }

    return sslContextBuilder.build();
  }

  @Override
  public boolean isOpen() {
    synchronized (lock) {
      return connected.get() || (channel != null && channel.isActive());
    }
  }

  @Override
  public void open() throws TTransportException {
    throw new TTransportException(
        TTransportException.NOT_OPEN, "open() is not implemented; use startConnect() instead");
  }

  @Override
  public int read(ByteBuffer buffer) throws TTransportException {
    boolean readingResponseSize = buffer.remaining() == 4;

    if (!isOpen()) {
      logger.debug("Transport not open for ByteBuffer read");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    ByteBuf byteBuf = null;
    try {
      byteBuf = readQueue.poll();
      if (byteBuf == null) {
        logger.debug("No data available for ByteBuffer read");
        return 0;
      }

      int available = Math.min(buffer.remaining(), byteBuf.readableBytes());
      if (available > 0) {
        byte[] tempArray = new byte[available];
        byteBuf.readBytes(tempArray);
        buffer.put(tempArray);
        logger.debug(
            "Read {} bytes into ByteBuffer, remaining space: {}", available, buffer.remaining());
      }

      if (byteBuf.readableBytes() > 0) {
        ByteBuf remaining = byteBuf.slice();
        remaining.retain();
        readQueue.offer(remaining);
        logger.debug("Put back {} remaining bytes", remaining.readableBytes());
      }

      // Drain dummy channel to clear OP_READ
      ByteBuffer discard = ByteBuffer.allocate(16);
      try {
        while (dummyClient.read(discard) > 0) {
          discard.clear();
        }
      } catch (IOException e) {
        logger.debug("Failed to drain dummy channel", e);
      }
      if (readingResponseSize) {
        // Trigger OP_READ on dummy by writing dummy byte
        if (dummyServerAccepted != null) {
          ByteBuffer dummyByte = ByteBuffer.wrap(new byte[1]);
          dummyServerAccepted.write(dummyByte);
        }
        // Wakeup selector if needed
        if (selector != null) {
          selector.wakeup();
        }
      }

      return available;
    } catch (Exception e) {
      logger.debug("ByteBuffer read failed: {}", e.getMessage());
      throw new TTransportException(TTransportException.UNKNOWN, "Read failed", e);
    } finally {
      if (byteBuf != null) {
        byteBuf.release();
      }
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      logger.debug("Transport not open for read");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    try {
      ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
      return read(buffer);
    } catch (Exception e) {
      logger.debug("Read failed: {}", e.getMessage());
      throw new TTransportException(TTransportException.UNKNOWN, "Read failed", e);
    }
  }

  @Override
  public int write(ByteBuffer buffer) throws TTransportException {
    if (!isOpen()) {
      logger.debug("Transport not open for ByteBuffer write");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    int remaining = buffer.remaining();
    if (remaining == 0) {
      return 0;
    }

    logger.debug("Writing {} bytes from ByteBuffer", remaining);

    synchronized (lock) {
      ByteBuf byteBuf = Unpooled.buffer(remaining);
      try {
        byteBuf.writeBytes(buffer);
        ChannelFuture future = channel.writeAndFlush(byteBuf);
        future.addListener(
            (GenericFutureListener<ChannelFuture>)
                future1 -> {
                  if (future1.isSuccess()) {
                    logger.debug("ByteBuffer write completed successfully: {} bytes", remaining);
                  } else {
                    logger.debug("ByteBuffer write failed: {}", future1.cause().getMessage());
                  }
                });
        return remaining;
      } catch (Exception e) {
        byteBuf.release();
        logger.debug("ByteBuffer write failed: {}", e.getMessage());
        throw new TTransportException(TTransportException.UNKNOWN, "Write failed", e);
      }
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      logger.debug("Transport not open for write");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
    write(buffer);
  }

  @Override
  public void flush() throws TTransportException {
    if (!isOpen()) {
      logger.debug("Transport not open for flush");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }
    synchronized (lock) {
      channel.flush();
      logger.debug("Flushed channel");
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      connected.set(false);
      if (channel != null) {
        channel.close();
        channel = null;
        logger.debug("Channel closed for {}:{}", host, port);
      }
      try {
        if (dummyClient != null) {
          dummyClient.close();
        }
        if (dummyServerAccepted != null) {
          dummyServerAccepted.close();
        }
        if (dummyServer != null) {
          dummyServer.close();
        }
      } catch (IOException e) {
        logger.debug("Failed to close dummy channels", e);
      }
      eventLoopGroup.shutdownGracefully();
      logger.debug("EventLoopGroup shutdown initiated");
    }
  }

  @Override
  public boolean startConnect() throws IOException {
    if (connected.get() || connecting.get()) {
      logger.debug("Connection already started or established for {}:{}", host, port);
      return connected.get();
    }

    if (!connecting.compareAndSet(false, true)) {
      logger.debug("Concurrent connection attempt detected for {}:{}", host, port);
      return false;
    }

    logger.debug("Starting connection to {}:{}", host, port);

    try {
      // Initiate dummy connect, it will pend until accept
      dummyClient.connect(new InetSocketAddress("localhost", dummyPort));

      // Initiate Netty connect
      ChannelFuture future = bootstrap.connect(host, port);
      future.addListener(
          (GenericFutureListener<ChannelFuture>)
              future1 -> {
                synchronized (lock) {
                  if (future1.isSuccess()) {
                    logger.debug("Connection established successfully to {}:{}", host, port);
                    channel = future1.channel();
                    connected.set(true);
                    // Now accept the dummy connection to complete it
                    try {
                      dummyServerAccepted = dummyServer.accept();
                      if (dummyServerAccepted != null) {
                        dummyServerAccepted.configureBlocking(false);
                        logger.debug("Dummy server accepted connection");
                        // Wakeup selector to detect OP_CONNECT
                        if (selector != null) {
                          selector.wakeup();
                        }
                      }
                    } catch (IOException e) {
                      logger.debug("Failed to accept dummy connection", e);
                    }
                  } else {
                    logger.debug(
                        "Connection failed to {}:{}: {}", host, port, future1.cause().getMessage());
                  }
                  connecting.set(false);
                }
              });
      future.get();
      return false; // Return false to indicate pending connect for dummy
    } catch (Exception e) {
      connecting.set(false);
      return false;
    }
  }

  @Override
  public boolean finishConnect() throws IOException {
    synchronized (lock) {
      boolean dummyFinished = dummyClient.finishConnect();
      boolean isConnected = connected.get() && dummyFinished;
      logger.debug(
          "finishConnect called, netty connected: {}, dummy finished: {}",
          connected.get(),
          dummyFinished);
      return isConnected;
    }
  }

  @Override
  public SelectionKey registerSelector(Selector selector, int interests) throws IOException {
    synchronized (lock) {
      this.selector = selector;
      return dummyClient.register(selector, interests);
    }
  }

  @Override
  public String toString() {
    synchronized (lock) {
      return "[remote: " + getRemoteAddress() + ", local: " + getLocalAddress() + "]";
    }
  }

  public SocketAddress getRemoteAddress() {
    synchronized (lock) {
      return channel != null ? channel.remoteAddress() : null;
    }
  }

  public SocketAddress getLocalAddress() {
    synchronized (lock) {
      return channel != null ? channel.localAddress() : null;
    }
  }

  private class NettyTransportHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      logger.debug("Channel active: {}", ctx.channel().remoteAddress());
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf byteBuf = (ByteBuf) msg;
        logger.debug("Received {} bytes", byteBuf.readableBytes());
        try {
          synchronized (lock) {
            readQueue.offer(byteBuf.retain());
            // Trigger OP_READ on dummy by writing dummy byte
            if (dummyServerAccepted != null) {
              ByteBuffer dummyByte = ByteBuffer.wrap(new byte[1]);
              dummyServerAccepted.write(dummyByte);
            }
            // Wakeup selector if needed
            if (selector != null) {
              selector.wakeup();
            }
          }
        } finally {
          byteBuf.release();
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      logger.debug("Channel inactive: {}", ctx.channel().remoteAddress());
      synchronized (lock) {
        connected.set(false);
        connecting.set(false);
      }
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      logger.debug("Channel exception: {}", cause.getMessage());
      synchronized (lock) {
        ctx.close();
      }
    }
  }
}
