package org.apache.iotdb.db.http.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class HttpServer {

  private static final boolean SSL = System.getProperty("ssl") != null;
  private final int port;
  private ServerBootstrap b;


  public HttpServer(int port){
    this.port = port;
    b = new ServerBootstrap();
  }

  public void start() throws Exception{
    // Configure SSL.
    final SslContext sslCtx;
    if (SSL) {
      SelfSignedCertificate ssc = new SelfSignedCertificate();
      sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
    } else {
      sslCtx = null;
    }

    // Configure the server.
    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new HttpSnoopServerInitializer(sslCtx));

      Channel ch = b.bind(port).sync().channel();

      System.err.println("Open your web browser and navigate to " +
          (SSL? "https" : "http") + "://127.0.0.1:" + port + '/');

      ch.closeFuture().sync();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  public void stop() throws InterruptedException {
    ChannelFuture f = b.bind().sync();
// Call this once you want to stop accepting new connections.
    f.channel().close().sync();
  }

}
