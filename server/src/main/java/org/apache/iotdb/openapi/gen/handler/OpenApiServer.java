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

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.webapp.WebAppContext;

public class OpenApiServer {
  public void start(int port) {
    Server server = new Server(port);
    server.setAttribute("CharacterEncoding", "UTF-8");
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

    context.setContextPath("/");
    server.setHandler(context);
    String rootPath = server.getClass().getClassLoader().getResource(".").toString();
    WebAppContext webapp = new WebAppContext(rootPath + "../../src/main/webapp", "");
    server.setHandler(webapp);

    try {
      server.start();
      server.join();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.destroy();
    }
  };
}
