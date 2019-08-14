package org.apache.iotdb.db.metrics.server;

import java.util.ArrayList;

import org.apache.iotdb.db.metrics.sink.ConsoleSink;
import org.apache.iotdb.db.metrics.sink.MetricsServletSink;
import org.apache.iotdb.db.metrics.sink.Sink;
import org.apache.iotdb.db.metrics.source.MetricsSource;
import org.apache.iotdb.db.metrics.source.Source;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.codahale.metrics.MetricRegistry;

public class MetricsSystem {
	
	private ArrayList<Sink> sinks = new ArrayList<Sink>();
	private ArrayList<Source> sources = new ArrayList<Source>();
	private MetricRegistry metricRegistry = new MetricRegistry();
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

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public void setMetricRegistry(MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
	}

	public ServletContextHandler getServletHandlers() {
		return new MetricsServletSink(metricRegistry).getHandler();
	}

	public void start() {
		Source source = new MetricsSource(serverArgument);
		registerSource(source);
		registerSinks();
		sinks.forEach(sink -> sink.start());
	}

	public void stop() {
		sinks.forEach(sink -> sink.stop());
	}

	public void report() {
		sinks.forEach(sink -> sink.report());
	}

	public void registerSource(Source source) {
		sources.add(source);
		String regName = MetricRegistry.name(source.sourceName());
		metricRegistry.register(regName, source.metricRegistry());
	}

	public void registerSinks() {
//		ConsoleSink consoleSink = new ConsoleSink(metricRegistry);
//		sinks.add(consoleSink);
	}
}
