package org.apache.iotdb.db.metrics.sink;

import java.util.concurrent.TimeUnit;

import org.apache.iotdb.db.metrics.server.JettyUtil;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetricsServletSink implements Sink{

	public MetricRegistry registry;
	
	public MetricsServletSink(MetricRegistry registry) {
		this.registry = registry;
	}

	public ObjectMapper mapper = new ObjectMapper().registerModule(
	    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false));

	public ServletContextHandler getHandler(){
		return JettyUtil.createMetricsServletHandler(mapper,registry);
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
	}

	@Override
	public void report() {
	}
}
