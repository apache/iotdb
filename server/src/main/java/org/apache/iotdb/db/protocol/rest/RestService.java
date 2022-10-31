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
package org.apache.iotdb.db.protocol.rest;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.filter.ApiOriginFilter;

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

public class RestService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestService.class);

  private static Server server;

  private void startSSL(
      int port,
      String keyStorePath,
      String trustStorePath,
      String keyStorePwd,
      String trustStorePwd,
      int idleTime,
      boolean clientAuth) {
    server = new Server();

    HttpConfiguration httpsConfig = new HttpConfiguration();
    httpsConfig.setSecurePort(port);
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword(keyStorePwd);
    if (clientAuth) {
      sslContextFactory.setTrustStorePath(trustStorePath);
      sslContextFactory.setTrustStorePassword(trustStorePwd);
      sslContextFactory.setNeedClientAuth(clientAuth);
    }

    ServerConnector httpsConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(httpsConfig));
    httpsConnector.setPort(port);
    httpsConnector.setIdleTimeout(idleTime);
    server.addConnector(httpsConnector);

    server.setHandler(constructServletContextHandler());
    serverStart();
  }

  private void startNonSSL(int port) {
    server = new Server(port);
    server.setHandler(constructServletContextHandler());
    serverStart();
  }

  private ServletContextHandler constructServletContextHandler() {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addFilter(
        ApiOriginFilter.class, "/*", EnumSet.of(DispatcherType.INCLUDE, DispatcherType.REQUEST));
    ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
    holder.setInitOrder(1);
    holder.setInitParameter(
        "jersey.config.server.provider.packages",
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.db.protocol.rest");
    holder.setInitParameter(
        "jersey.config.server.provider.classnames",
        "org.glassfish.jersey.media.multipart.MultiPartFeature");
    holder.setInitParameter("jersey.config.server.wadl.disableWadl", "true");
    context.setContextPath("/");
    return context;
  }

  private void serverStart() {
    try {
      server.start();
    } catch (Exception e) {
      LOGGER.warn("RestService failed to start: {}", e.getMessage());
      server.destroy();
    }
    LOGGER.info("start RestService successfully");
  }

  @Override
  public void start() throws StartupException {
    IoTDBRestServiceConfig config = IoTDBRestServiceDescriptor.getInstance().getConfig();
    if (IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableHttps()) {
      startSSL(
          config.getRestServicePort(),
          config.getKeyStorePath(),
          config.getTrustStorePath(),
          config.getKeyStorePwd(),
          config.getTrustStorePwd(),
          config.getIdleTimeoutInSeconds(),
          config.isClientAuth());
    } else {
      startNonSSL(config.getRestServicePort());
    }
  }

  @Override
  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      LOGGER.warn("RestService failed to stop: {}", e.getMessage());
    } finally {
      server.destroy();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.REST_SERVICE;
  }

  public static RestService getInstance() {
    return RestServiceHolder.INSTANCE;
  }

  private static class RestServiceHolder {

    private static final RestService INSTANCE = new RestService();

    private RestServiceHolder() {}
  }
}
