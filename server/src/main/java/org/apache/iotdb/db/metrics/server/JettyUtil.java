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
package org.apache.iotdb.db.metrics.server;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;

public class JettyUtil {

  public static ServletContextHandler createMetricsServletHandler(
      ObjectMapper mapper, MetricRegistry metricRegistry) {
    HttpServlet httpServlet =
        new HttpServlet() {
          private static final long serialVersionUID = 1L;

          private final ObjectMapper om = mapper;
          private final MetricRegistry mr = metricRegistry;

          @Override
          protected void doGet(HttpServletRequest req, HttpServletResponse resp)
              throws IOException {
            resp.setContentType("text/html;charset=utf-8");
            resp.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = resp.getWriter();
            out.write(om.writeValueAsString(mr));
            out.flush();
            out.close();
          }

          @Override
          public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            doGet(req, resp);
          }
        };

    return createServletHandler("/json", httpServlet);
  }

  public static ServletContextHandler createServletHandler(String path, HttpServlet servlet) {
    ServletContextHandler contextHandler =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    ServletHolder holder = new ServletHolder(servlet);
    contextHandler.setContextPath(path);
    contextHandler.addServlet(holder, "/");
    return contextHandler;
  }

  public static ServletContextHandler createStaticHandler() {
    ServletContextHandler contextHandler =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    URL res = JettyUtil.class.getClassLoader().getResource("iotdb/ui/static");
    HttpServlet servlet = new DefaultServlet();
    ServletHolder holder = new ServletHolder(servlet);
    holder.setInitParameter("resourceBase", res.toString());
    contextHandler.setContextPath("/static");
    contextHandler.addServlet(holder, "/");
    return contextHandler;
  }

  public static Server getJettyServer(List<ServletContextHandler> handlers, int port) {
    Server server = new Server(port);
    ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setServer(server);
    server.addBean(errorHandler);

    ContextHandlerCollection collection = new ContextHandlerCollection();
    ServletContextHandler[] sch = new ServletContextHandler[handlers.size()];
    for (int i = 0; i < handlers.size(); i++) {
      sch[i] = handlers.get(i);
    }
    collection.setHandlers(sch);
    server.setHandler(collection);
    return server;
  }
}
