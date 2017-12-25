package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService.Processor;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;

/**
 * A server to handle jdbc request from client.
 */
public class JDBCServer implements JDBCServerMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);
    private Thread jdbcServerThread;
    private boolean isStart;

    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;
    private TSServiceImpl impl;

	@Override
	public String getJDBCServerStatus() {
		if(isStart) return "UP";
		else return "DOWN";
	}
	
	@Override
	public int getRPCPort() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		return config.rpcPort;
	}
	
    public JDBCServer() throws TTransportException {
        isStart = false;
    }

    @Override
    public synchronized void startServer() {
        if (isStart) {
            LOGGER.info("{}: jdbc server has been already running now", TsFileDBConstant.GLOBAL_DB_NAME);
            return;
        }
        LOGGER.info("{}: start jdbc server...",TsFileDBConstant.GLOBAL_DB_NAME);

        try {
            jdbcServerThread = new Thread(new JDBCServerThread());
            jdbcServerThread.setName("JDBC-Server");
        } catch (IOException e) {
            LOGGER.error("{}: failed to start jdbc server. {}",TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
            return;
        }
        jdbcServerThread.start();

        LOGGER.info("{}: start jdbc server successfully, listening on port {}",
        		TsFileDBConstant.GLOBAL_DB_NAME, TsfileDBDescriptor.getInstance().getConfig().rpcPort);
        isStart = true;
    }

    @Override
    public synchronized void restartServer() {
        stopServer();
        startServer();
    }

    @Override
    public synchronized void stopServer() {
        if (!isStart) {
            LOGGER.info("{}: jdbc server isn't running now",TsFileDBConstant.GLOBAL_DB_NAME);
            return;

        }
        LOGGER.info("{}: closing jdbc server...", TsFileDBConstant.GLOBAL_DB_NAME);
        close();
        LOGGER.info("{}: close jdbc server successfully", TsFileDBConstant.GLOBAL_DB_NAME);
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

    private class JDBCServerThread implements Runnable {

        public JDBCServerThread() throws IOException {
            protocolFactory = new TBinaryProtocol.Factory();
            impl = new TSServiceImpl();
            processor = new TSIService.Processor<TSIService.Iface>(impl);
        }

        @Override
        public void run() {
            try {
        		serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
                poolArgs = new TThreadPoolServer.Args(serverTransport);
                poolArgs.executorService = IoTDBThreadPoolFactory.createJDBCClientThreadPool(poolArgs, "JDBC-Client");
                poolArgs.processor(processor);
                poolArgs.protocolFactory(protocolFactory);
                poolServer = new TThreadPoolServer(poolArgs);
                poolServer.setServerEventHandler(new JDBCServerEventHandler(impl));
                poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("{}: failed to start jdbc server, because ",TsFileDBConstant.GLOBAL_DB_NAME, e);
            } catch (Exception e) {
                LOGGER.error("{}: jdbc server exit, because ",TsFileDBConstant.GLOBAL_DB_NAME, e);
            } finally {
                close();
                LOGGER.info("{}: close TThreadPoolServer and TServerSocket for jdbc server",TsFileDBConstant.GLOBAL_DB_NAME);
            }
        }
    }

    public static void main(String[] args) throws TTransportException, InterruptedException {
        JDBCServer server = new JDBCServer();
        server.startServer();
    }




}
