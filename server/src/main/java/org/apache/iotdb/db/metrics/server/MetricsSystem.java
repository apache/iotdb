/**
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

import java.util.ArrayList;

import org.apache.iotdb.db.metrics.sink.MetricsServletSink;
import org.apache.iotdb.db.metrics.sink.Sink;
import org.apache.iotdb.db.metrics.source.MetricsSource;
import org.apache.iotdb.db.metrics.source.Source;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.codahale.metrics.MetricRegistry;

public class MetricsSystem {

	private ArrayList<Sink> sinks = new ArrayList<>();
	private ArrayList<Source> sources = new ArrayList<>();
	public static MetricRegistry metricRegistry = new MetricRegistry();
	private ServerArgument serverArgument;

	public MetricsSystem(ServerArgument serverArgument) {
		this.serverArgument = serverArgument;
	}

	public MetricsSystem() {
	}

	public ServerArgument getServerArgument() {
		return serverArgument;
	}

	public void setServerArgument(ServerArgument serverArgument) {
		this.serverArgument = serverArgument;
	}

	public ServletContextHandler getServletHandlers() {
		return new MetricsServletSink(metricRegistry).getHandler();
	}

	public void start() {
		registerSource();
		registerSinks();
		sinks.forEach(sink -> sink.start());
	}

	public void stop() {
		sinks.forEach(sink -> sink.stop());
	}

	public void report() {
		sinks.forEach(sink -> sink.report());
	}

	public void registerSource() {
		Source source = new MetricsSource(serverArgument);
		sources.add(source);
	}

	public void registerSinks() {
	}
}
