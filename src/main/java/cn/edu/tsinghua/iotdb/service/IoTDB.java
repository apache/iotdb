package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.concurrent.IoTDBDefaultThreadExceptionHandler;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;

import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.writelog.manager.WriteLogNodeManager;

public class IoTDB implements IoTDBMBean{
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private RegisterManager registerManager = new RegisterManager();
    private final String MBEAN_NAME = String.format("%s:%s=%s", TsFileDBConstant.IOTDB_PACKAGE, TsFileDBConstant.JMX_TYPE, "IoTDB");

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
			LOGGER.error("{}: failed to start because some checks failed. {}", TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
			return;
		}
		try {
			setUp();
		} catch (StartupException e) {
			LOGGER.error(e.getMessage());
			deactivate();
			LOGGER.error("{} exit", TsFileDBConstant.GLOBAL_DB_NAME);
			return;
		}
		LOGGER.info("{} has started.", TsFileDBConstant.GLOBAL_DB_NAME);
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
		if (TsfileDBDescriptor.getInstance().getConfig().enableStatMonitor){
			StatMonitor.getInstance().recovery();
		}

		registerManager.register(FileNodeManager.getInstance());
		registerManager.register(MultiFileLogNodeManager.getInstance());
		IService DBDaoService = new DBDao();
		registerManager.register(DBDaoService);
		registerManager.register(JMXService.getInstance());
		registerManager.register(JDBCService.getInstance());
		registerManager.register(Monitor.INSTANCE);
		registerManager.register(CloseMergeService.getInstance());
		registerManager.register(StatMonitor.getInstance());
		registerManager.register(BasicMemController.getInstance());
		
		JMXService.registerMBean(getInstance(), MBEAN_NAME);
	}

	public void deactivate(){
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

	/**
	 * Recover data using system log.
	 * @throws RecoverException 
	 *
	 * @throws IOException
	 */
	private void systemDataRecovery() throws RecoverException {
		LOGGER.info("{}: start checking write log...", TsFileDBConstant.GLOBAL_DB_NAME);
		// QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
		WriteLogNodeManager writeLogManager = MultiFileLogNodeManager.getInstance();
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
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

