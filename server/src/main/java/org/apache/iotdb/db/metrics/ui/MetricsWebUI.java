package org.apache.iotdb.db.metrics.ui;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.metrics.server.JettyUtil;
import org.apache.iotdb.db.metrics.server.QueryServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.codahale.metrics.MetricRegistry;

public class MetricsWebUI {

	private static final String STATIC_RESOURCE_BASE = "iotdb/ui/static";
	private List<ServletContextHandler> handlers = new ArrayList<ServletContextHandler>();
	private MetricRegistry metricRegistry;

	public MetricsWebUI(MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
	}

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public void setMetricRegistry(MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
	}

	public List<ServletContextHandler> getHandlers() {
		return handlers;
	}

	public void setHandlers(List<ServletContextHandler> handlers) {
		this.handlers = handlers;
	}

	public void initialize() {
		ServletContextHandler staticHandler = JettyUtil.createStaticHandler(STATIC_RESOURCE_BASE, "/static");
		MetricsPage masterPage = new MetricsPage(metricRegistry);
		QueryServlet queryServlet = new QueryServlet(masterPage);
		ServletContextHandler queryHandler = JettyUtil.createServletHandler("/", queryServlet);
		handlers.add(staticHandler);
		handlers.add(queryHandler);
	}

	public Server getServer() throws Exception {
		return JettyUtil.getJettyServer(handlers);
	}
}
