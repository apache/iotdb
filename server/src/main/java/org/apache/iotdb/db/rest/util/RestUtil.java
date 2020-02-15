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
package org.apache.iotdb.db.rest.util;

import org.apache.iotdb.db.rest.filter.CORSFilter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

public class RestUtil {
  public static ServletContextHandler getRestContextHandler() {
    ServletContextHandler ctx =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(CORSFilter.class);
    ctx.setContextPath("/");
    resourceConfig.packages("org.apache.iotdb.db.rest.controller");
    ServletHolder servletHolder = new ServletHolder(new ServletContainer(resourceConfig));
    ctx.addServlet(servletHolder, "/*");
    return ctx;
  }

  public static Server getJettyServer(ServletContextHandler handler, int port) {
    Server server = new Server(port);
    ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setServer(server);
    server.addBean(errorHandler);
    server.setHandler(handler);
    return server;
  }

}
