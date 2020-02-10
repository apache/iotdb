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
package org.apache.iotdb.db.metrics.ui;

import org.apache.iotdb.db.metrics.server.JettyUtil;
import org.apache.iotdb.db.metrics.server.QueryServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import com.codahale.metrics.MetricRegistry;
import org.eclipse.jetty.servlet.ServletHolder;

public class MetricsWebUI {

  private ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
  private MetricRegistry metricRegistry;

  public MetricsWebUI(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void initialize() {
    MetricsPage masterPage = new MetricsPage(metricRegistry);
    QueryServlet queryServlet = new QueryServlet(masterPage);
    handler.setContextPath("/metrics");
    handler.setResourceBase(
        String.valueOf(MetricsWebUI.class.getClassLoader().getResource("iotdb/ui/static")));
    ServletHolder queryServletHolder = new ServletHolder(queryServlet);
    handler.addServlet(queryServletHolder, "/query");
    ServletHolder staticServletHolder = JettyUtil.createStaticServletHolder();
    handler.addServlet(staticServletHolder, "/");
  }

  public ServletContextHandler getHandler() {
    return handler;
  }
}
