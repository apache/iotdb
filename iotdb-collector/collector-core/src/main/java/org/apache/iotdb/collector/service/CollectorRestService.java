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

package org.apache.iotdb.collector.service;

import org.apache.iotdb.collector.config.CollectorConfig;
import org.apache.iotdb.collector.config.CollectorDescriptor;
import org.apache.iotdb.collector.protocol.rest.filter.ApiOriginFilter;
import org.apache.iotdb.commons.service.ServiceType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;

import java.util.EnumSet;

public class CollectorRestService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorRestService.class);

  private static final CollectorConfig CONFIG = CollectorDescriptor.getInstance().getConfig();

  private static Server server;

  private CollectorRestService() {}

  @Override
  public void start() {
    startNonSSL(CONFIG.getRestServicePort());
  }

  private void startNonSSL(final int restServicePort) {
    server = new Server(restServicePort);
    server.setHandler(constructServletContextHandler());
    serverStart();
  }

  private ServletContextHandler constructServletContextHandler() {
    final ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addFilter(
        ApiOriginFilter.class, "/*", EnumSet.of(DispatcherType.INCLUDE, DispatcherType.REQUEST));
    final ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
    holder.setInitOrder(1);
    holder.setInitParameter(
        "jersey.config.server.provider.packages",
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.collector.protocol.rest");
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
    } catch (final Exception e) {
      LOGGER.warn("CollectorRestService failed to start: {}", e.getMessage());
      server.destroy();
    }
    LOGGER.info("start CollectorRestService successfully");
  }

  @Override
  public void stop() {
    try {
      server.stop();
    } catch (final Exception e) {
      LOGGER.warn("CollectorRestService failed to stop: {}", e.getMessage());
    } finally {
      server.destroy();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.REST_SERVICE;
  }

  public static CollectorRestService getInstance() {
    return CollectorRestServiceHolder.INSTANCE;
  }

  private static class CollectorRestServiceHolder {

    private static final CollectorRestService INSTANCE = new CollectorRestService();

    private CollectorRestServiceHolder() {}
  }
}
