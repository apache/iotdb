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
package org.apache.iotdb.db.rest.handler;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.rest.handler.filter.ApiOriginFilter;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import org.eclipse.jetty.http.HttpVersion;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;

import java.util.EnumSet;

public class OpenApiServer implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenApiServer.class);

  public static OpenApiServer getInstance() {
    return OpenApiServerHolder.INSTANCE;
  }

  private void start(int port) {
    Server server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addFilter(
        ApiOriginFilter.class, "/*", EnumSet.of(DispatcherType.INCLUDE, DispatcherType.REQUEST));
    ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
    holder.setInitOrder(1);
    holder.setInitParameter(
        "jersey.config.server.provider.packages",
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.db.rest.handler");
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
      LOGGER.error("OpenApiServer start failed: {}", e.getMessage());
    } finally {
      server.destroy();
    }
  }

  private void startSSL(
      int port,
      String keyStorePath,
      String trustStorePath,
      String keyStorePwd,
      String trustStorePwd,
      int idleTime) {
    Server server = new Server();
    HttpConfiguration https_config = new HttpConfiguration();
    https_config.setSecurePort(port);
    https_config.addCustomizer(new SecureRequestCustomizer());
    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword(keyStorePwd);
    sslContextFactory.setTrustStorePath(trustStorePath);
    sslContextFactory.setTrustStorePassword(trustStorePwd);
    ServerConnector httpsConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(https_config));
    httpsConnector.setPort(port);
    httpsConnector.setIdleTimeout(idleTime);
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

  private int getOpenApiPort() {
    return IoTDBDescriptor.getInstance().getConfig().getOpenApiPort();
  }

  private String getKeyStorePath() {
    return IoTDBDescriptor.getInstance().getConfig().getKeyStorePath();
  }

  private String getTrustStorePath() {
    return IoTDBDescriptor.getInstance().getConfig().getTrustStorePath();
  }

  private String getKeyStorePwd() {
    return IoTDBDescriptor.getInstance().getConfig().getKeyStorePwd();
  }

  private String getTrustStorePwd() {
    return IoTDBDescriptor.getInstance().getConfig().getTrustStorePwd();
  }

  public int getIdleTimeout() {
    return IoTDBDescriptor.getInstance().getConfig().getIdleTimeout();
  }

  @Override
  public void start() throws StartupException {
    if (IoTDBDescriptor.getInstance().getConfig().isEnable_https()) {
      startSSL(
          getOpenApiPort(),
          getKeyStorePath(),
          getTrustStorePath(),
          getKeyStorePwd(),
          getTrustStorePwd(),
          getIdleTimeout());
    } else {
      start(getOpenApiPort());
    }
  }

  @Override
  public void stop() {
    // do nothing.
  }

  @Override
  public ServiceType getID() {
    return ServiceType.OPEN_API_SERVICE;
  }

  private static class OpenApiServerHolder {

    private static final OpenApiServer INSTANCE = new OpenApiServer();

    private OpenApiServerHolder() {}
  }
}
