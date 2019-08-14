package org.apache.iotdb.db.metrics.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JettyUtil {

	public static ServletContextHandler createMetricsServletHandler(ObjectMapper mapper,
			MetricRegistry metricRegistry) {
		HttpServlet httpServlet = new HttpServlet() {
			private static final long serialVersionUID = 1L;

			ObjectMapper om = mapper;
			MetricRegistry mr = metricRegistry;

			@Override
			protected void doGet(HttpServletRequest req, HttpServletResponse resp)
					throws ServletException, IOException {
				resp.setContentType("text/html;charset=utf-8");
				resp.setStatus(HttpServletResponse.SC_OK);
				PrintWriter out = resp.getWriter();
				out.write(om.writeValueAsString(mr));
				out.flush();
				out.close();
			}

			@Override
			public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
				doGet(req, resp);
			}
		};
		return createServletHandler("/json", httpServlet);

	}

	public static ServletContextHandler createServletHandler(String path, HttpServlet servlet) {
		ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
		contextHandler.setContextPath(path);
		ServletHolder holder = new ServletHolder(servlet);
		contextHandler.setResourceBase(".");
		contextHandler.addServlet(holder, "/");
		contextHandler.setClassLoader(Thread.currentThread().getContextClassLoader());
		return contextHandler;
	}

	public static ServletContextHandler createStaticHandler(String resourceBase, String path) {
		ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
		contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false");
		contextHandler.setContextPath(path);
		String res = Thread.currentThread().getContextClassLoader().getResource(resourceBase).getPath();
		contextHandler.setResourceBase(res);
		contextHandler.addServlet(new ServletHolder(new DefaultServlet()), "/");
		contextHandler.setClassLoader(Thread.currentThread().getContextClassLoader());
		return contextHandler;
	}

	public static Server getJettyServer(List<ServletContextHandler> handlers) throws Exception {
		QueuedThreadPool pool = new QueuedThreadPool();
		pool.setName("iot-metrics");
		pool.setDaemon(true);

		Server server = new Server(pool);
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
