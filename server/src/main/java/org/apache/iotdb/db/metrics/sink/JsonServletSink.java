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
package org.apache.iotdb.db.metrics.sink;

import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.metrics.server.JettyUtil;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.servlet.ServletHolder;

public class JsonServletSink implements Sink {

  private MetricRegistry registry;

  public JsonServletSink(MetricRegistry registry) {
    this.registry = registry;
  }

  public ServletHolder getHolder() {
    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false));
    return JettyUtil.createJsonServletHolder(mapper, registry);
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void report() {}
}
