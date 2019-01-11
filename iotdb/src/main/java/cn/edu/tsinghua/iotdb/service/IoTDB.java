package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBDefaultThreadExceptionHandler;
import cn.edu.tsinghua.iotdb.conf.IoTDBConstant;
import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.exception.*;
import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.postback.receiver.ServerManager;
import cn.edu.tsinghua.iotdb.query.control.FileReaderManager;
import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.writelog.manager.WriteLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class IoTDB implements IoTDBMBean{
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private RegisterManager registerManager = new RegisterManager();
    private final String MBEAN_NAME = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, "IoTDB");

	private ServerManager serverManager = ServerManager.getInstance();
	
    private static class IoTDBHolder {
		private static final IoTDB INSTANCE = new IoTDB();
	}

	public static final IoTDB getInstance() {
		return IoTDBHolder.INSTANCE;
	}

	public void active() {
		StartupChecks checks = new StartupChecks().withDefaultTest();
		try {
			checks.verify();
		} catch (StartupException e) {
			LOGGER.error("{}: failed to start because some checks failed. {}", IoTDBConstant.GLOBAL_DB_NAME, e.getMessage());
			return;
		}
		try {
			setUp();
		} catch (StartupException e) {
			LOGGER.error(e.getMessage());
			deactivate();
			LOGGER.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
			return;
		}
		LOGGER.info("{} has started.", IoTDBConstant.GLOBAL_DB_NAME);
	}
	
	private void setUp() throws StartupException {
		setUncaughtExceptionHandler();
		
		FileNodeManager.getInstance().recovery();
		try {
			systemDataRecovery();
		} catch (RecoverException e) {
			String errorMessage = String.format("Failed to recovery system data because of %s", e.getMessage());
			throw new StartupException(errorMessage);
		}
		// When registering statMonitor, we should start recovering some statistics with latest values stored
		// Warn: registMonitor() method should be called after systemDataRecovery()
		if (IoTDBDescriptor.getInstance().getConfig().enableStatMonitor){
			StatMonitor.getInstance().recovery();
		}

		registerManager.register(FileNodeManager.getInstance());
		registerManager.register(MultiFileLogNodeManager.getInstance());
		registerManager.register(JMXService.getInstance());
		registerManager.register(JDBCService.getInstance());
		registerManager.register(Monitor.INSTANCE);
		registerManager.register(CloseMergeService.getInstance());
		registerManager.register(StatMonitor.getInstance());
		registerManager.register(BasicMemController.getInstance());
		registerManager.register(FileReaderManager.getInstance());
		
		JMXService.registerMBean(getInstance(), MBEAN_NAME);

		initErrorInformation();

		serverManager.startServer();
	}

	public void deactivate() {
		serverManager.closeServer();
		registerManager.deregisterAll();
		JMXService.deregisterMBean(MBEAN_NAME);
	}

	@Override
	public void stop() {
		deactivate();
	}

	private void setUncaughtExceptionHandler(){
		Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
	}

	private void initErrorInformation(){
    	ExceptionBuilder.getInstance().loadInfo();
    }

	/**
	 * Recover data using system log.
	 * @throws RecoverException 
	 *
	 * @throws IOException
	 */
	private void systemDataRecovery() throws RecoverException {
		LOGGER.info("{}: start checking write log...", IoTDBConstant.GLOBAL_DB_NAME);
		// QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
		WriteLogNodeManager writeLogManager = MultiFileLogNodeManager.getInstance();
		List<String> filenodeNames = null;
		try {
			filenodeNames = MManager.getInstance().getAllFileNames();
		} catch (PathErrorException e) {
			throw new RecoverException(e);
		}
		for (String filenodeName : filenodeNames) {
			if(writeLogManager.hasWAL(filenodeName)){
				try {
					FileNodeManager.getInstance().recoverFileNode(filenodeName);
				} catch (FileNodeProcessorException | FileNodeManagerException e) {
					throw new RecoverException(e);
				}
			}
		}
		IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
		boolean enableWal = config.enableWal;
		config.enableWal = false;
		writeLogManager.recover();
		config.enableWal = enableWal;
	}

	public static void main(String[] args) {
		IoTDB daemon = IoTDB.getInstance();
		daemon.active();
	}

}

