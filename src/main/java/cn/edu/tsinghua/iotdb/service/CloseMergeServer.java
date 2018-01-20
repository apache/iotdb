package cn.edu.tsinghua.iotdb.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;

/**
 * A service that triggers close and merge operation regularly
 * 
 * @author liukun
 *
 */
public class CloseMergeServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(CloseMergeServer.class);

	private MergeServerThread mergeService = new MergeServerThread();
	private CloseServerThread closeService = new CloseServerThread();
	private ScheduledExecutorService service;
	private CloseAndMergeDaemon closeAndMergeDaemon = new CloseAndMergeDaemon();
	private static TsfileDBConfig dbConfig = TsfileDBDescriptor.getInstance().getConfig(); 
	private static final long mergeDelay = dbConfig.periodTimeForMerge;
	private static final long closeDelay = dbConfig.periodTimeForFlush;
	private static final long mergePeriod = dbConfig.periodTimeForMerge;
	private static final long closePeriod = dbConfig.periodTimeForFlush;

	private volatile boolean isStart = false;
	private long closeAllLastTime;
	private long mergeAllLastTime;

	private static CloseMergeServer SERVER = new CloseMergeServer();

	public synchronized static CloseMergeServer getInstance() {
		if (SERVER == null) {
			SERVER = new CloseMergeServer();
		}
		return SERVER;
	}

	private CloseMergeServer() {
		service = IoTDBThreadPoolFactory.newScheduledThreadPool(2, "CloseAndMerge");
	}

	public void startServer() {
		if (dbConfig.enableTimingCloseAndMerge) {
			if (!isStart) {
				LOGGER.info("Start the close and merge server");
				closeAndMergeDaemon.start();
				isStart = true;
				closeAllLastTime = System.currentTimeMillis();
				mergeAllLastTime = System.currentTimeMillis();
			} else {
				LOGGER.warn("The close and merge server has been already running");
			}
		} else {
			LOGGER.info("The close and merge server can't be started, it is disabled by configuration.");
		}
	}

	public void closeServer() {
		if (dbConfig.enableTimingCloseAndMerge) {
			if (isStart) {
				LOGGER.info("Prepare to shutdown the close and merge server");
				isStart = false;
				synchronized (service) {
					service.shutdown();
					service.notify();
				}
				SERVER = null;
				LOGGER.info("Shutdown close and merge server successfully");
			} else {
				LOGGER.warn("The close and merge server is not running now");
			}
		}
	}

	private class CloseAndMergeDaemon extends Thread {

		public CloseAndMergeDaemon() {
			super("MergeAndCloseServer");
		}

		@Override
		public void run() {
			service.scheduleWithFixedDelay(mergeService, mergeDelay, mergePeriod, TimeUnit.SECONDS);
			service.scheduleWithFixedDelay(closeService, closeDelay, closePeriod, TimeUnit.SECONDS);
			while (!service.isShutdown()) {
				synchronized (service) {
					try {
						service.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private class MergeServerThread extends Thread {

		public MergeServerThread() {
			super("merge_server_thread");
		}

		@Override
		public void run() {
			long thisMergeTime = System.currentTimeMillis();
			DateTime startDateTime = new DateTime(mergeAllLastTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime endDateTime = new DateTime(thisMergeTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			long timeInterval = thisMergeTime - mergeAllLastTime;
			LOGGER.info("Start the merge action regularly, last time is {}, this time is {}, time interval is {}s.",
					startDateTime, endDateTime, timeInterval / 1000);
			mergeAllLastTime = System.currentTimeMillis();
			try {
				FileNodeManager.getInstance().mergeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				LOGGER.error("Merge all error.", e);
			}
		}
	}

	private class CloseServerThread extends Thread {

		public CloseServerThread() {
			super("close_server_thread");
		}

		@Override
		public void run() {
			long thisCloseTime = System.currentTimeMillis();
			DateTime startDateTime = new DateTime(closeAllLastTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime endDateTime = new DateTime(thisCloseTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			long timeInterval = thisCloseTime - closeAllLastTime;
			LOGGER.info("Start the close action regularly, last time is {}, this time is {}, time interval is {}s.",
					startDateTime, endDateTime, timeInterval / 1000);
			closeAllLastTime = System.currentTimeMillis();
			try {
				FileNodeManager.getInstance().closeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				LOGGER.error("close all error.", e);
			}
		}
	}
}
