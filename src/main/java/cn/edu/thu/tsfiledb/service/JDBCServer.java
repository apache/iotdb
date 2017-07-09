package cn.edu.thu.tsfiledb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth.dao.DBDao;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.StartupException;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;

public class JDBCServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);
    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;

    public void startServer(){
	LOGGER.info("TsFileDB Server: start server...");
	DBDao dBdao = new DBDao();
	dBdao.open();
	FileNodeManager.getInstance().managerRecovery();
	Thread jdbcServerThread;
	try {
	    jdbcServerThread = new Thread(new JDBCServerThread());
	} catch (IOException e) {
	    LOGGER.error("TsFileDB Server: failed to start server. {}", e.getMessage());
	    if (dBdao != null) {
		dBdao.close();
	    }
	    try {
		FileNodeManager.getInstance().close();
	    } catch (LRUManagerException e1) {
		e1.printStackTrace();
	    }
	    return;
	}
	jdbcServerThread.start();

	LOGGER.info("TsFileDB Server: start server successfully");
	LOGGER.info("Listening on port: {}", TsfileDBDescriptor.getInstance().getConfig().rpcPort);
    }
    
    public static void main(String[] args) throws StartupException {
	StartupChecks checks = new StartupChecks().withDefaultTest();
	checks.verify();
	JDBCServer server = new JDBCServer();
	server.startServer();
    }

    private class JDBCServerThread implements Runnable {

	public JDBCServerThread() throws IOException {
	    protocolFactory = new TBinaryProtocol.Factory();
	    processor = new TSIService.Processor<TSIService.Iface>(new TSServiceImpl());
	}

	@Override
	public void run() {
	    try {
		serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
		poolArgs = new TThreadPoolServer.Args(serverTransport);
		poolArgs.processor(processor);
		poolArgs.protocolFactory(protocolFactory);
		poolServer = new TThreadPoolServer(poolArgs);
		poolServer.serve();
	    } catch (TTransportException e) {
		LOGGER.error("TsFileDB Server: failed to start server, because ", e);
	    } catch (Exception e) {
		LOGGER.error("TsFileDB Server: server exit, because ", e);
	    } finally {
		if (poolServer != null) {
		    poolServer.stop();
		    poolServer = null;
		}

		if (serverTransport != null) {
		    serverTransport.close();
		    serverTransport = null;
		}
		LOGGER.info("TsFileDB Server: close TThreadPoolServer and TServerSocket");
	    }
	}
    }

}
