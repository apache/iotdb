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
package org.apache.iotdb.db.http.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServer {

  private static final boolean SSL = System.getProperty("ssl") != null;
  private final int port;
  private ServerBootstrap b;
  private static final Logger logger = LoggerFactory.getLogger(HttpServer.class);
  private Channel ch;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;


  public HttpServer(int port){
    this.port = port;
    b = new ServerBootstrap();
  }

  public void start() throws CertificateException, SSLException, InterruptedException {
    // Configure SSL.
    final SslContext sslCtx;
    if (SSL) {
      SelfSignedCertificate ssc = new SelfSignedCertificate();
      sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
    } else {
      sslCtx = null;
    }
    try {
      // Configure the server.
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new HttpSnoopServerInitializer(sslCtx));

      ch = b.bind(port).sync().channel();

      logger.info("Open your web browser and navigate to {}{}{}/",
          (SSL ? "https" : "http"), IoTDBDescriptor.getInstance().getConfig().getHttpAddress() ,port );
    } catch (Exception e) {
      stop();
    }
  }

  public void stop() throws InterruptedException {
    bossGroup.shutdownGracefully().sync();
    workerGroup.shutdownGracefully().sync();
    ch.closeFuture().sync();
  }

}
