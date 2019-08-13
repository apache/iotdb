package org.apache.iotdb.db.service;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metrics.server.MetricsSystem;
import org.apache.iotdb.db.metrics.server.ServerArgument;
import org.apache.iotdb.db.metrics.ui.MetricsWebUI;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to show sql execute state on web page.
 */
public class MetricsService implements MetricsServiceMBean,IService{
	
	private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
	private final String mbeanName = String.format("%s:%s=%s",IoTDBConstant.IOTDB_PACKAGE,IoTDBConstant.JMX_TYPE,getID().getJmxName());
	
	private MetricsWebUI metricsWebUI;
	private Thread metricsServiceThread;
	private MetricsSystem metricsSystem = new MetricsSystem(new ServerArgument(getMetricsPort()));

	public static final MetricsService getInstance() {
	    return MetricsServiceHolder.INSTANCE;
	}
	
	@Override
	public ServiceType getID() {
		return ServiceType.METRICS_SERVICE;
	}
	
	@Override
	public int getMetricsPort() {
		IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
		return config.getMetricsPort();
	}

	@Override
	public void start() throws StartupException {
		try {
		    JMXService.registerMBean(getInstance(), mbeanName);
		    metricsSystem.start();
		    startService();
		  } catch (Exception e) {
		    logger.error("Failed to start {} because: ", this.getID().getName(), e);
		    throw new StartupException(e);
		  }
	}

	@Override
	public void stop() {
		metricsSystem.stop();
		stopService();
		JMXService.deregisterMBean(mbeanName);
	}

	@Override
	public synchronized void startService() throws StartupException {
		logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
		metricsServiceThread = new MetricsServiceThread();
		metricsServiceThread.setName(ThreadName.METRICS_SERVICE.getName());
		metricsServiceThread.start();
	    logger.info("{}: start {} successfully, listening on ip {} port {}", IoTDBConstant.GLOBAL_DB_NAME,
	            this.getID().getName(), IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
	            IoTDBDescriptor.getInstance().getConfig().getMetricsPort());
	}

	@Override
	public void restartService() throws StartupException {
		stopService();
		startService();
	}

	@Override
	public void stopService() {
	    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
		((MetricsServiceThread) metricsServiceThread).close();
		logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
	}

    private static class MetricsServiceHolder {

	    private static final MetricsService INSTANCE = new MetricsService();

	    private MetricsServiceHolder() {}
	}
    
    private class MetricsServiceThread extends Thread {
    	
    	private Server server;
    	
        @Override
        public void run() {
        	try {
	            int port = getMetricsPort();
	            metricsWebUI = new MetricsWebUI(metricsSystem.getMetricRegistry());
	            metricsWebUI.getHandlers().add(metricsSystem.getServletHandlers());
	            metricsWebUI.initialize();
	            server = metricsWebUI.getServer();
	            server.start();
	            
	        	ServerConnector connector = new ServerConnector(server);
	        	connector.setPort(port);
	        	connector.setAcceptQueueSize(Math.min(connector.getAcceptors(), 8));
	        	connector.setIdleTimeout(60000);
	        	connector.start();
	        	server.setConnectors(new Connector[] { connector });
	        	
	        	server.join();
			} catch (Exception e) {
		        logger.error("{}: failed to start {}, because ",IoTDBConstant.GLOBAL_DB_NAME,getID().getName(), e);
			}
        }
        
        public synchronized void close(){
        	try {
				server.stop();
				ThreadPool threadPool = server.getThreadPool();
				if (threadPool != null && LifeCycle.class.isInstance(threadPool)) {
					LifeCycle.stop(threadPool);
				}
			} catch (Exception e) {
			    logger.error("{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME,getID().getName(), e);
			    Thread.currentThread().interrupt();
			}
        }
   } 
}
