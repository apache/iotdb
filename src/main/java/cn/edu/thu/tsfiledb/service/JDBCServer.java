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
	
	private boolean isMerging; 
	
	public JDBCServer() throws TTransportException {
		isStart = false;
		isMerging = false;
	}

	@Override
	public synchronized void startServer() {
		if(isStart){
			LOGGER.info("tsfile-service JDBCServer: jdbc server has been already running now");
			return;
		}
		LOGGER.info("tsfile-service JDBCServer: starting jdbc server...");
		dBdao = new DBdao();
		dBdao.open();
		FileNodeManager.getInstance().managerRecovery();
		try {
			jdbcServerThread = new Thread(new JDBCServerThread());
		} catch (IOException e) {
			LOGGER.error("Server start Error. {}", e.getMessage());
			e.printStackTrace();
//			return;
		}
		jdbcServerThread.start();
		
		LOGGER.info("tsfile-service JDBCServer: start jdbc server successfully");
		isStart = true;
		
	}

	@Override
	public synchronized void restartServer() {
		stopServer();
		startServer();
	}

	@Override
	public synchronized void stopServer() {
		if(!isStart){
			LOGGER.info("tsfile-service JDBCServer: jdbc server isn't running now");
			return;
		}
		
		LOGGER.info("tsfile-service JDBCServer: closing jdbc server...");

		if(dBdao != null){
			dBdao.close();
			dBdao = null;
		}
		
		LOGGER.info("tsfile-service JDBCServer: flush data in memory to disk");
		try {
			FileNodeManager.getInstance().close();
		} catch (LRUManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		close();
		LOGGER.info("tsfile-service JDBCServer: close jdbc server successfully");
	}
	
	private void close(){
		if(poolServer != null){
			poolServer.stop();
			poolServer = null;
		}
		
		if(serverTransport != null){
			serverTransport.close();
			serverTransport = null;
		}
		isStart = false;
	}
	
	class JDBCServerThread implements Runnable{
		
		public JDBCServerThread() throws IOException{
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
		        // poolServer.serve(); will block next statement to execute
		        poolServer.serve();
			} catch (TTransportException e) {
				LOGGER.error("tsfile-service JDBCServer: failed to start jdbc server, because ", e);
			} catch (Exception e) {
				LOGGER.error("tsfile-service JDBCServer: jdbc server exit, because ", e);
			}  finally {
				close();
				LOGGER.info("tsfile-service JDBCServer: close TThreadPoolServer and TServerSocket");
			}
		}
	}

	public static void main(String[] args) throws TTransportException, InterruptedException{
		JDBCServer server = new JDBCServer();
		server.startServer();
	}

	@Override
	public synchronized void mergeAll() {
		LOGGER.info("tsfile-service JDBCServer : start merging..");
		try {
			FileNodeManager.getInstance().mergeAll();
		} catch (FileNodeManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOGGER.info("tsfile-service JDBCServer : Done.");
	}
}
