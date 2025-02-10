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

import org.apache.iotdb.collector.api.filter.ApiOriginFilter;
import org.apache.iotdb.collector.config.ApiServiceOptions;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;

import java.util.EnumSet;

public class ApiService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiService.class);

  private Server server;

  @Override
  public void start() {
    server = new Server(ApiServiceOptions.PORT.value());
    server.setHandler(constructServletContextHandler());
    try {
      server.start();
      LOGGER.info(
          "[ApiService] Started successfully. Listening on port {}",
          ApiServiceOptions.PORT.value());
    } catch (final Exception e) {
      LOGGER.warn("[ApiService] Failed to start: {}", e.getMessage(), e);
      server.destroy();
    }
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
        "io.swagger.jaxrs.listing, io.swagger.sample.resource, org.apache.iotdb.collector.api");
    holder.setInitParameter(
        "jersey.config.server.provider.classnames",
        "org.glassfish.jersey.media.multipart.MultiPartFeature");
    holder.setInitParameter("jersey.config.server.wadl.disableWadl", "true");
    context.setContextPath("/");
    return context;
  }

  @Override
  public void stop() {
    if (server == null) {
      LOGGER.info("[ApiService] Not started yet. Nothing to stop.");
      return;
    }

    try {
      server.stop();
      LOGGER.info("[ApiService] Stopped successfully.");
    } catch (final Exception e) {
      LOGGER.warn("[ApiService] Failed to stop: {}", e.getMessage(), e);
    } finally {
      server.destroy();
    }
  }

  @Override
  public String name() {
    return "ApiService";
  }
}
