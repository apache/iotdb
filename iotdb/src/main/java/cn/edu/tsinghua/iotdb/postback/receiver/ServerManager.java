package cn.edu.tsinghua.iotdb.postback.receiver;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;

/**
 * @author lta
 */
public class ServerManager {
	private TServerSocket serverTransport;
	private Factory protocolFactory;
	private TProcessor processor;
	private TThreadPoolServer.Args poolArgs;
	private TServer poolServer;
	private IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

	private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);

	private static class ServerManagerHolder {
		private static final ServerManager INSTANCE = new ServerManager();
	}

	private ServerManager() {
	}

	public static final ServerManager getInstance() {
		return ServerManagerHolder.INSTANCE;
	}

	public void startServer() {
		if (!conf.isPostbackEnable) {
			return;
		}
		try {
			if (conf.ipWhiteList == null) {
				LOGGER.error(
						"IoTDB post back receiver: Postback server failed to start because IP white list is null, please set IP white list!");
				return;
			}
			conf.ipWhiteList = conf.ipWhiteList.replaceAll(" ", "");
			serverTransport = new TServerSocket(conf.postbackServerPort);
			protocolFactory = new TBinaryProtocol.Factory();
			processor = new ServerService.Processor<ServerServiceImpl>(new ServerServiceImpl());
			poolArgs = new TThreadPoolServer.Args(serverTransport);
			poolArgs.processor(processor);
			poolArgs.protocolFactory(protocolFactory);
			poolServer = new TThreadPoolServer(poolArgs);
			LOGGER.info("Postback server has started.");
			Runnable runnable = new Runnable() {
				public void run() {
					poolServer.serve();
				}
			};
			Thread thread = new Thread(runnable);
			thread.start();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back receiver: cannot start postback server because {}", e.getMessage());
		}
	}

	public void closeServer() {
		if (conf.isPostbackEnable && poolServer != null) {
			poolServer.stop();
			serverTransport.close();
			LOGGER.info("Stop postback server.");
		}
	}
}