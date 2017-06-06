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

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;

public class JDBCServer implements JDBCServerMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);
    private DBdao dBdao;
    private Thread jdbcServerThread;
    private boolean isStart;

    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;

    // private boolean isMerging;

    public JDBCServer() throws TTransportException {
	isStart = false;
	// isMerging = false;
    }

    @Override
    public synchronized void startServer() {
	if (isStart) {
	    LOGGER.info("TsFileDB Server: server has been already running now");
	    return;
	}
	LOGGER.info("TsFileDB Server: start server...");
	dBdao = new DBdao();
	dBdao.open();
	FileNodeManager.getInstance().ManagerRecovery();
	try {
	    jdbcServerThread = new Thread(new JDBCServerThread());
	} catch (IOException e) {
	    LOGGER.error("TsFileDB Server: failed to start server. {}", e.getMessage());
	    if(dBdao != null){
		dBdao.close();
	    }
	    return;
	}
	jdbcServerThread.start();

	LOGGER.info("TsFileDB Server: start server successfully");
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
	    LOGGER.info("TsFileDB Server: server isn't running now");
	    return;
	}

	LOGGER.info("TsFileDB Server: closing jdbc server...");

	if (dBdao != null) {
	    dBdao.close();
	    dBdao = null;
	}

	try {
	    FileNodeManager.getInstance().close();
	} catch (LRUManagerException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	close();
	LOGGER.info("TsFileDB Server: close server successfully");
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

    class JDBCServerThread implements Runnable {

	public JDBCServerThread() throws IOException {
	    protocolFactory = new TBinaryProtocol.Factory();
	    processor = new TSIService.Processor<TSIService.Iface>(new TSServiceImpl());
	}

	@Override
	public void run() {
	    try {
		serverTransport = new TServerSocket(JDBCServerConfig.PORT);
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
		close();
		LOGGER.info("TsFileDB Server: close TThreadPoolServer and TServerSocket");
	    }
	}
    }

    public static void main(String[] args) throws TTransportException, InterruptedException {
	JDBCServer server = new JDBCServer();
	server.startServer();
    }

    @Override
    public synchronized void mergeAll() {
	LOGGER.info("TsFileDB Server: start merging...");
	try {
	    FileNodeManager.getInstance().mergeAll();
	} catch (FileNodeManagerException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	LOGGER.info("TsFileDB Server: finish merge operation.");
    }
}
