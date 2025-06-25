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
import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.KeyStore;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyTNonBlockingTransport extends TNonblockingTransport {

  private static final Logger logger = LoggerFactory.getLogger(NettyTNonBlockingTransport.class);

  private final String host;
  private final int port;
  private final int connectTimeoutMs = 60000;
  private final EventLoopGroup eventLoopGroup;
  private final Bootstrap bootstrap;
  private Channel channel;
  private final AtomicBoolean connected = new AtomicBoolean(false);
  private final AtomicBoolean connecting = new AtomicBoolean(false);
  private final BlockingQueue<ByteBuf> readQueue = new LinkedBlockingQueue<>();
  private final Object writeLock = new Object();
  private NettySelectionKeyAdapter selectionKeyAdapter;

  // SSL 配置
  private final String keystorePath;
  private final String keystorePassword;
  private final String truststorePath;
  private final String truststorePassword;
  private boolean sslEnabled = false;

  public NettyTNonBlockingTransport(
      String host,
      int port,
      String keystorePath,
      String keystorePassword,
      String truststorePath,
      String truststorePassword)
      throws TTransportException {
    super(new TConfiguration());
    this.host = host;
    this.port = port;
    this.eventLoopGroup = new NioEventLoopGroup();
    this.bootstrap = new Bootstrap();
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.truststorePath = truststorePath;
    this.truststorePassword = truststorePassword;
    this.sslEnabled = true;
    initBootstrap();
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
                logger.info("Initializing channel for {}:{}", host, port);

                ChannelPipeline pipeline = ch.pipeline();

                // 添加 SSL 处理器（如果启用）
                if (sslEnabled) {
                  SslContext sslContext = createSslContext();
                  SslHandler sslHandler = sslContext.newHandler(ch.alloc(), host, port);
                  // 增加握手超时时间
                  sslHandler.setHandshakeTimeoutMillis(30000);

                  pipeline.addLast("ssl", sslHandler);
                  // 添加SSL握手完成监听器
                  sslHandler
                      .handshakeFuture()
                      .addListener(
                          future -> {
                            if (future.isSuccess()) {
                              logger.info("SSL handshake completed successfully");
                            } else {
                              logger.info("SSL handshake failed: ", future.cause());
                            }
                          });
                }

                // 添加业务处理器
                pipeline.addLast("handler", new NettyTransportHandler());
              }
            });
  }

  private SslContext createSslContext() throws Exception {
    SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

    // 配置 KeyStore（客户端证书）
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

    // 配置 TrustStore（信任的服务器证书）
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
    return channel != null && channel.isActive() && connected.get();
  }

  @Override
  public void open() throws TTransportException {
    throw new RuntimeException("open() is not implemented for NettyTNonblockingTransport");
  }

  /** Perform a nonblocking read into buffer. */
  public int read(ByteBuffer buffer) throws TTransportException {
    if (!isOpen()) {
      logger.info("Transport not open for ByteBuffer read");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    try {
      ByteBuf byteBuf = readQueue.take();
      //      if (byteBuf == null) {
      //        logger.info("No data available for ByteBuffer read (non-blocking)");
      //        return 0; // 非阻塞读取，没有数据时返回 0
      //      }

      int available = Math.min(buffer.remaining(), byteBuf.readableBytes());
      if (available > 0) {
        // 从 ByteBuf 读取数据到 ByteBuffer
        byte[] tempArray = new byte[available];
        byteBuf.readBytes(tempArray);
        buffer.put(tempArray);

        logger.info(
            "Read {} bytes into ByteBuffer, remaining space: {}", available, buffer.remaining());
      }

      // 如果还有剩余数据，创建一个新的 ByteBuf 包含剩余数据
      if (byteBuf.readableBytes() > 0) {
        ByteBuf remaining = byteBuf.slice();
        remaining.retain();
        readQueue.offer(remaining);
        logger.info("Put back {} remaining bytes", remaining.readableBytes());
      }

      byteBuf.release();
      return available;

    } catch (Exception e) {
      logger.warn("ByteBuffer read failed: ", e);
      throw new TTransportException(TTransportException.UNKNOWN, e);
    }
  }

  /** Reads from the underlying input stream if not null. */
  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      logger.info(
          "Transport not open for read - channel: "
              + (channel != null ? channel.isActive() : "null")
              + ", connected: "
              + connected.get());
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    try {
      // 使用 ByteBuffer 包装数组进行读取
      ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
      return read(buffer);
    } catch (Exception e) {
      logger.warn("Read failed: ", e);
      throw new TTransportException(TTransportException.UNKNOWN, e);
    }
  }

  /** Perform a nonblocking write of the data in buffer; */
  public int write(ByteBuffer buffer) throws TTransportException {
    if (!isOpen()) {
      logger.info("Transport not open for ByteBuffer write");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    int remaining = buffer.remaining();
    if (remaining == 0) {
      return 0;
    }

    logger.info("Writing " + remaining + " bytes from ByteBuffer");

    synchronized (writeLock) {
      // 创建 ByteBuf 从 ByteBuffer
      ByteBuf byteBuf = Unpooled.buffer();
      byteBuf.writeBytes(buffer);
      ChannelFuture future = channel.writeAndFlush(byteBuf);

      final int bytesToWrite = remaining;
      future.addListener(
          (GenericFutureListener<ChannelFuture>)
              future1 -> {
                if (future1.isSuccess()) {
                  logger.info(
                      "ByteBuffer write completed successfully: " + bytesToWrite + " bytes");
                } else {
                  logger.warn("ByteBuffer write failed: " + future1.cause().getMessage());
                  future1.cause().printStackTrace();
                }
              });
    }

    // 对于非阻塞写入，我们假设所有数据都能写入
    // 实际的写入状态通过 Future 监听器处理
    return remaining;
  }

  /** Writes to the underlying output stream if not null. */
  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      logger.info("Transport not open for write");
      throw new TTransportException(TTransportException.NOT_OPEN, "Transport not open");
    }

    // 使用 ByteBuffer 包装数组进行写入
    ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
    write(buffer);
  }

  @Override
  public void flush() throws TTransportException {
    // Not supported by SocketChannel.
  }

  @Override
  public void close() {
    connected.set(false);
    if (channel != null) {
      channel.close();
    }
    eventLoopGroup.shutdownGracefully();
  }

  @Override
  public boolean startConnect() throws IOException {
    if (connected.get() || connecting.get()) {
      logger.info("Starting connection return " + (connected.get() || connecting.get()));
      return connected.get();
    }

    if (!connecting.compareAndSet(false, true)) {
      return false;
    }
    logger.info("Starting connection to " + host + ":" + port);

    try {
      ChannelFuture future = bootstrap.connect(host, port);
      future.addListener(
          (GenericFutureListener<ChannelFuture>)
              future1 -> {
                if (future1.isSuccess()) {
                  logger.info("Connection established successfully");
                  channel = future1.channel();
                  connected.set(true);
                  if (selectionKeyAdapter != null) {
                    selectionKeyAdapter.setConnected(true);
                  }
                }
                connecting.set(false);
              });
      future.get();
      return false; // 异步连接，立即返回 false
    } catch (Exception e) {
      connecting.set(false);
      //      throw new IOException("Failed to start connection", e);
      return false;
    }
  }

  @Override
  public boolean finishConnect() throws IOException {
    return connected.get();
  }

  @Override
  public SelectionKey registerSelector(Selector selector, int interests) throws IOException {
    if (selectionKeyAdapter == null) {
      selectionKeyAdapter = new NettySelectionKeyAdapter(this, selector, interests);

      // 尝试通过反射获取 selectedKeys 的可修改引用
      try {
        // 尝试不同的字段名，因为不同的 Selector 实现可能使用不同的字段名
        String[] possibleFieldNames = {"selectedKeys", "publicSelectedKeys", "keys"};
        Field selectedKeysField = null;

        for (String fieldName : possibleFieldNames) {
          try {
            selectedKeysField = selector.getClass().getSuperclass().getDeclaredField(fieldName);
            break;
          } catch (NoSuchFieldException e) {
            // 继续尝试下一个字段名
          }
        }

        if (selectedKeysField != null) {
          selectedKeysField.setAccessible(true);
          Object selectedKeysObj = selectedKeysField.get(selector);

          if (selectedKeysObj instanceof Set) {
            @SuppressWarnings("unchecked")
            Set<SelectionKey> selectedKeys = (Set<SelectionKey>) selectedKeysObj;
            selectionKeyAdapter.setSelectedKeysReference(selectedKeys);
            logger.info("Successfully obtained selectedKeys reference via reflection");
            try {
              selectionKeyAdapter.selectedKeysReference.add(selectionKeyAdapter);
              selector.wakeup();
            } catch (Exception e) {
              logger.warn("Failed to add to selectedKeys: " + e.getMessage());
            }
          }
        }
      } catch (Exception e) {
        logger.warn("Failed to access selectedKeys via reflection: " + e.getMessage());
        // 继续执行，使用备用方案
      }

    } else {
      selectionKeyAdapter.interestOps(interests);
    }

    return selectionKeyAdapter;
  }

  @Override
  public String toString() {
    return "[remote: " + getRemoteAddress() + ", local: " + getLocalAddress() + "]";
  }

  // Netty 处理器
  private class NettyTransportHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      logger.info("Channel active: " + ctx.channel().remoteAddress());

      connected.set(true);

      // 更新 SelectionKey 状态
      if (selectionKeyAdapter != null) {
        selectionKeyAdapter.setConnected(true);
        selectionKeyAdapter.setReadReady(true);
      }

      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf byteBuf = (ByteBuf) msg;
        logger.info("Received " + byteBuf.readableBytes() + " bytes");

        // 保留引用计数，将数据放入读取队列
        readQueue.offer(byteBuf.retain());
        byteBuf.release(); // 释放原始引用

        // 通知选择器适配器有数据可读
        if (selectionKeyAdapter != null) {
          selectionKeyAdapter.setReadReady(true);
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      logger.info("Channel inactive");

      connected.set(false);
      connecting.set(false);

      // 更新 SelectionKey 状态
      if (selectionKeyAdapter != null) {
        selectionKeyAdapter.setConnected(false);
        selectionKeyAdapter.setReadReady(false);
      }

      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      logger.warn("Channel exception: {}", cause.getMessage());

      // 更新 SelectionKey 状态
      if (selectionKeyAdapter != null) {
        selectionKeyAdapter.setConnected(false);
        selectionKeyAdapter.setReadReady(false);
        selectionKeyAdapter.cancel(); // 取消 SelectionKey
      }

      ctx.close();
    }
  }

  // SelectionKey 适配器类
  private static class NettySelectionKeyAdapter extends SelectionKey {
    private final NettyTNonBlockingTransport transport;
    private Selector selector;
    private int interestOps;
    private int readyOps = 0;
    private volatile boolean connected = false;
    private volatile boolean readReady = false;
    private volatile boolean valid = true;
    private Set<SelectionKey> selectedKeysReference;

    public NettySelectionKeyAdapter(
        NettyTNonBlockingTransport transport, Selector selector, int ops) {
      this.transport = transport;
      this.selector = selector;
      this.interestOps = ops;
    }

    public void setSelectedKeysReference(Set<SelectionKey> selectedKeys) {
      this.selectedKeysReference = selectedKeys;
    }

    @Override
    public SelectableChannel channel() {
      return null; // Netty 管理通道
    }

    @Override
    public Selector selector() {
      return selector;
    }

    @Override
    public void cancel() {
      this.valid = false;
      if (selectedKeysReference != null) {
        selectedKeysReference.remove(this);
      }
      logger.info("SelectionKey cancelled");
    }

    @Override
    public boolean isValid() {
      return valid;
    }

    @Override
    public SelectionKey interestOps(int ops) {
      this.interestOps = ops;
      logger.info("SelectionKey interestOps set to: " + ops);
      updateSelectorIfReady();
      return this;
    }

    @Override
    public int interestOps() {
      return interestOps;
    }

    @Override
    public int readyOps() {
      int ops = 0;

      // 检查连接状态
      if (connected && (interestOps & OP_CONNECT) != 0) {
        ops |= OP_CONNECT;
      }

      // 检查读取状态
      if (readReady && (interestOps & OP_READ) != 0) {
        ops |= OP_READ;
      }

      // 写入通常总是就绪的（如果连接是活跃的）
      if ((interestOps & OP_WRITE) != 0 && transport.isOpen()) {
        ops |= OP_WRITE;
      }

      if (ops != readyOps) {
        logger.info("SelectionKey readyOps changed: " + readyOps + " -> " + ops);
      }

      readyOps = ops;
      return readyOps;
    }

    public void setConnected(boolean connected) {
      this.connected = connected;
      updateSelectorIfReady();
    }

    public void setReadReady(boolean readReady) {
      if (this.readReady != readReady) {
        logger.info("ReadReady changed: {} -> {}", this.readReady, readReady);
      }
      this.readReady = readReady;
      updateSelectorIfReady();
    }

    private void updateSelectorIfReady() {
      if (selector != null && isValid() && selectedKeysReference != null) {
        int ready = readyOps();
        if (ready != 0) {
          try {
            selectedKeysReference.add(this);
            selector.wakeup();
            logger.info("Added self to selectedKeys via reference, readyOps: " + ready);
          } catch (Exception e) {
            logger.warn("Failed to add to selectedKeys: " + e.getMessage());
          }
        }
      }
    }
  }

  // 辅助方法：获取远程地址
  public SocketAddress getRemoteAddress() {
    return channel != null ? channel.remoteAddress() : null;
  }

  // 辅助方法：获取本地地址
  public SocketAddress getLocalAddress() {
    return channel != null ? channel.localAddress() : null;
  }
}
