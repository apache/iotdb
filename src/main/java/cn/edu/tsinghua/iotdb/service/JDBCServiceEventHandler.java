package cn.edu.tsinghua.iotdb.service;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCServiceEventHandler implements TServerEventHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServiceEventHandler.class);
	private TSServiceImpl serviceImpl;

	public JDBCServiceEventHandler(TSServiceImpl serviceImpl) {
		this.serviceImpl = serviceImpl;
	}

	@Override
	public ServerContext createContext(TProtocol arg0, TProtocol arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
		try {
			serviceImpl.handleClientExit();
		} catch (TException e) {
			LOGGER.error("failed to clear client status", e);
		}
	}

	@Override
	public void preServe() {
		// TODO Auto-generated method stub

	}

	@Override
	public void processContext(ServerContext arg0, TTransport arg1, TTransport arg2) {
		// TODO Auto-generated method stub

	}

}
