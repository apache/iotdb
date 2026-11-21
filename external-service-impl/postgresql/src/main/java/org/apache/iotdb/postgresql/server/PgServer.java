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

package org.apache.iotdb.postgresql.server;

import org.apache.iotdb.postgresql.protocol.PgFrameDecoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** The listening endpoint: Netty event loops, TLS on demand, timeouts, back pressure. */
public final class PgServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PgServer.class);

  /** Largest client message; Bind messages with large values stay well below. */
  static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

  private final PgServerContext context;
  private EventLoopGroup boss;
  private EventLoopGroup workers;
  private Channel channel;

  public PgServer(PgServerContext context) {
    this.context = context;
  }

  /** Binds the endpoint; fails (with the address named) when the port cannot be opened. */
  public void start() throws InterruptedException {
    boss = new NioEventLoopGroup(1, new DefaultThreadFactory("pg-service-accept", true));
    workers =
        new NioEventLoopGroup(
            Math.min(4, Runtime.getRuntime().availableProcessors()),
            new DefaultThreadFactory("pg-service-io", true));
    long idleMillis = context.config.idleTimeout().toMillis();
    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(boss, workers)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                  @Override
                  protected void initChannel(SocketChannel ch) {
                    PgFrameDecoder decoder = new PgFrameDecoder(MAX_MESSAGE_SIZE);
                    ch.pipeline()
                        // the connection handler decides whether an idle connection is closed
                        .addLast(
                            "idle", new IdleStateHandler(idleMillis, 0, 0, TimeUnit.MILLISECONDS))
                        .addLast("frames", decoder)
                        .addLast("connection", new PgConnectionHandler(context, decoder));
                  }
                });
    InetSocketAddress address =
        context.config.isWildcard()
            ? new InetSocketAddress(context.config.port())
            : new InetSocketAddress(context.config.address(), context.config.port());
    try {
      channel = bootstrap.bind(address).sync().channel();
    } catch (RuntimeException | InterruptedException e) {
      stop();
      throw e;
    }
    LOGGER.info("PostgreSQL endpoint listening on {}", channel.localAddress());
  }

  /** The bound address (with the actual port when port 0 was configured). */
  public InetSocketAddress localAddress() {
    return channel == null ? null : (InetSocketAddress) channel.localAddress();
  }

  public void stop() {
    if (channel != null) {
      channel.close().syncUninterruptibly();
      channel = null;
    }
    if (boss != null) {
      boss.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      boss = null;
    }
    if (workers != null) {
      workers.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      workers = null;
    }
  }
}
