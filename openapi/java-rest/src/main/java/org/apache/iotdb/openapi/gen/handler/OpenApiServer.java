/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.openapi.gen.handler;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;

public class OpenApiServer {
  public void start(int port) {
    Server server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

    ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
    holder.setInitOrder(1);
    holder.setInitParameter(
        "jersey.config.server.provider.packages",
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.openapi.gen.handler");
    holder.setInitParameter(
        "jersey.config.server.provider.classnames",
        "org.glassfish.jersey.media.multipart.MultiPartFeature");
    holder.setInitParameter("jersey.config.server.wadl.disableWadl", "true");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      server.join();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.destroy();
    }
  }

  public void startSSL(
      int port,
      String keyStorePath,
      String trustStorePath,
      String keyStorePwd,
      String keyManagerPwd) {
    Server server = new Server();
    HttpConfiguration https_config = new HttpConfiguration();
    https_config.setSecureScheme("https");
    https_config.setSecurePort(port);
    https_config.setOutputBufferSize(32768);
    https_config.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory();
    // sslContextFactory.setKeyStorePath("/Users/miaohongshan/crethttps/keystore");
    // sslContextFactory.setKeyStorePassword("yoursslpassword");
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setTrustStorePath(trustStorePath);
    sslContextFactory.setKeyStorePassword(keyStorePwd);
    sslContextFactory.setKeyManagerPassword(keyManagerPwd);

    ServerConnector httpsConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, "http/1.1"),
            new HttpConnectionFactory(https_config));
    httpsConnector.setPort(port);
    httpsConnector.setIdleTimeout(500000);
    server.addConnector(httpsConnector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
    holder.setInitOrder(1);
    holder.setInitParameter(
        "jersey.config.server.provider.packages",
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.openapi.gen.handler");
    holder.setInitParameter(
        "jersey.config.server.provider.classnames",
        "org.glassfish.jersey.media.multipart.MultiPartFeature");
    holder.setInitParameter("jersey.config.server.wadl.disableWadl", "true");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      server.join();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.destroy();
    }
  }
}
