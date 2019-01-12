package org.apache.iotdb.db.service;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A service to handle jdbc request from client.
 */
public class JDBCService implements JDBCServiceMBean, IService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCService.class);
    private Thread jdbcServiceThread;
    private boolean isStart;

    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;
    private TSServiceImpl impl;
    
    private final String STATUS_UP = "UP";
    private final String STATUS_DOWN = "DOWN";
    
    private final String MBEAN_NAME = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());
    
	private static class JDBCServiceHolder{
		private static final JDBCService INSTANCE = new JDBCService();
	}
	
	public static final JDBCService getInstance() {
		return JDBCServiceHolder.INSTANCE;
	}    
	
    private JDBCService() {
        isStart = false;
    }
    
	@Override
	public String getJDBCServiceStatus() {
		return isStart ? STATUS_UP : STATUS_DOWN;
	}
	
	@Override
	public int getRPCPort() {
		IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
		return config.rpcPort;
	}
	
	@Override
	public void start() throws StartupException {
		try {
			JMXService.registerMBean(getInstance(), MBEAN_NAME);
			startService();
		} catch (Exception e) {
			String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
			throw new StartupException(errorMessage);
		}
		
	}

	@Override
	public void stop() {
		stopService();
		JMXService.deregisterMBean(MBEAN_NAME);
	}

	@Override
	public ServiceType getID() {
		return ServiceType.JDBC_SERVICE;
	}
    
    @Override
    public synchronized void startService() throws StartupException {
        if (isStart) {
            LOGGER.info("{}: {} has been already running now", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
            return;
        }
        LOGGER.info("{}: start {}...",IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());

        try {
        	jdbcServiceThread = new Thread(new JDBCServiceThread());
        	jdbcServiceThread.setName(ThreadName.JDBC_SERVICE.getName());
        } catch (IOException e) {
        	String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
			throw new StartupException(errorMessage);
        }
        jdbcServiceThread.start();

        LOGGER.info("{}: start {} successfully, listening on port {}",
        		IoTDBConstant.GLOBAL_DB_NAME, 
        		this.getID().getName(), 
        		IoTDBDescriptor.getInstance().getConfig().rpcPort);
        isStart = true;
    }

    @Override
    public synchronized void restartService() throws StartupException {
        stopService();
        startService();
    }

    @Override
    public synchronized void stopService() {
        if (!isStart) {
            LOGGER.info("{}: {} isn't running now",IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
            return;
        }
        LOGGER.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
        close();
        LOGGER.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    }

    private synchronized void close() {
    	if (poolServer != null) {
            poolServer.stop();
            poolServer = null;
        }

        if (serverTransport != null) {
            serverTransport.close();
            serverTransport = null;
        }
        isStart = false;
    }

    private class JDBCServiceThread implements Runnable {

        public JDBCServiceThread() throws IOException {
            protocolFactory = new TBinaryProtocol.Factory();
            impl = new TSServiceImpl();
            processor = new TSIService.Processor<TSIService.Iface>(impl);
        }

        @Override
        public void run() {
            try {
                serverTransport = new TServerSocket(IoTDBDescriptor.getInstance().getConfig().rpcPort);
                poolArgs = new TThreadPoolServer.Args(serverTransport);
                poolArgs.executorService = IoTDBThreadPoolFactory.createJDBCClientThreadPool(poolArgs, ThreadName.JDBC_CLIENT.getName());
                poolArgs.processor(processor);
                poolArgs.protocolFactory(protocolFactory);
                poolServer = new TThreadPoolServer(poolArgs);
                poolServer.setServerEventHandler(new JDBCServiceEventHandler(impl));
                poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("{}: failed to start {}, because ",IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
            } catch (Exception e) {
                LOGGER.error("{}: {} exit, because ",IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
            } finally {
                close();
                LOGGER.info("{}: close TThreadPoolServer and TServerSocket for {}",IoTDBConstant.GLOBAL_DB_NAME, getID().getName());
            }
        }
    }
}
