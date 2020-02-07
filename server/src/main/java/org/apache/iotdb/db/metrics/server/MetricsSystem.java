/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.iotdb.db.metrics.server;

import java.util.ArrayList;
import org.apache.iotdb.db.metrics.sink.JsonServletSink;
import org.apache.iotdb.db.metrics.sink.Sink;
import org.apache.iotdb.db.metrics.source.MetricsSource;
import org.apache.iotdb.db.metrics.source.Source;
import com.codahale.metrics.MetricRegistry;
import org.eclipse.jetty.servlet.ServletHolder;

public class MetricsSystem {

  private ArrayList<Sink> sinks;
  private ArrayList<Source> sources;
  private MetricRegistry metricRegistry;
  private ServerArgument serverArgument;

  public MetricsSystem(ServerArgument serverArgument) {
    this.sinks = new ArrayList<>();
    this.sources = new ArrayList<>();
    this.metricRegistry = new MetricRegistry();
    this.serverArgument = serverArgument;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * to get a json Servlet Holder
   */
  public ServletHolder getServletHolder() {
    return new JsonServletSink(metricRegistry).getHolder();
  }

  public void start() {
    registerSource();
    sinks.forEach(Sink::start);
  }

  public void stop() {
    sinks.forEach(Sink::stop);
  }

  private void registerSource() {
    MetricsSource source = new MetricsSource(serverArgument, metricRegistry);
    source.registerInfo();
    sources.add(source);
  }

}
