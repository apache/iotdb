package cn.edu.tsinghua.iotdb.service;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.StartupException;

/**
 * A service that triggers close and merge operation regularly
 * 
 * @author liukun
 *
 */
public class CloseMergeService implements IService{

	private static final Logger LOGGER = LoggerFactory.getLogger(CloseMergeService.class);

	private MergeServiceThread mergeService = new MergeServiceThread();
	private CloseServiceThread closeService = new CloseServiceThread();
	private ScheduledExecutorService service;
	private CloseAndMergeDaemon closeAndMergeDaemon = new CloseAndMergeDaemon();
	private static IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig(); 
	private static final long mergeDelay = dbConfig.periodTimeForMerge;
	private static final long closeDelay = dbConfig.periodTimeForFlush;
	private static final long mergePeriod = dbConfig.periodTimeForMerge;
	private static final long closePeriod = dbConfig.periodTimeForFlush;

	private volatile boolean isStart = false;
	private long closeAllLastTime;
	private long mergeAllLastTime;

	private static CloseMergeService CLOSE_MERGE_SERVICE = new CloseMergeService();

	public synchronized static CloseMergeService getInstance() {
		if (CLOSE_MERGE_SERVICE == null) {
			CLOSE_MERGE_SERVICE = new CloseMergeService();
		}
		return CLOSE_MERGE_SERVICE;
	}

	private CloseMergeService() {
		service = IoTDBThreadPoolFactory.newScheduledThreadPool(2, ThreadName.CLOSE_MERGE_SERVICE.getName());
	}

	public void startService() {
		if (dbConfig.enableTimingCloseAndMerge) {
			if (!isStart) {
				LOGGER.info("Start the close and merge service");
				closeAndMergeDaemon.start();
				isStart = true;
				closeAllLastTime = System.currentTimeMillis();
				mergeAllLastTime = System.currentTimeMillis();
			} else {
				LOGGER.warn("The close and merge service has been already running.");
			}
		} else {
			LOGGER.info("Cannot start close and merge service, it is disabled by configuration.");
		}
	}

	public void closeService() {
		if (dbConfig.enableTimingCloseAndMerge) {
			if (isStart) {
				LOGGER.info("Prepare to shutdown the close and merge service.");
				isStart = false;
				synchronized (service) {
					service.shutdown();
					service.notify();
				}
				CLOSE_MERGE_SERVICE = null;
				LOGGER.info("Shutdown close and merge service successfully.");
			} else {
				LOGGER.warn("The close and merge service is not running now.");
			}
		}
	}

	private class CloseAndMergeDaemon extends Thread {

		public CloseAndMergeDaemon() {
			super(ThreadName.CLOSE_MERGE_DAEMON.getName());
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

	private class MergeServiceThread extends Thread {

		public MergeServiceThread() {
			super(ThreadName.MERGE_DAEMON.getName());
		}

		@Override
		public void run() {
			long thisMergeTime = System.currentTimeMillis();
            ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(mergeAllLastTime), IoTDBDescriptor.getInstance().getConfig().getZoneID());
            ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisMergeTime), IoTDBDescriptor.getInstance().getConfig().getZoneID());
			long timeInterval = thisMergeTime - mergeAllLastTime;
			LOGGER.info("Start the merge action regularly, last time is {}, this time is {}, time interval is {}s.",
					startDateTime, endDateTime, timeInterval / 1000);
			mergeAllLastTime = System.currentTimeMillis();
			try {
				FileNodeManager.getInstance().mergeAll();
			} catch (Exception e) {
				LOGGER.error("Merge all error.", e);
			}
		}
	}

	private class CloseServiceThread extends Thread {

		public CloseServiceThread() {
			super(ThreadName.CLOSE_DAEMON.getName());
		}

		@Override
		public void run() {
			long thisCloseTime = System.currentTimeMillis();
            ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(closeAllLastTime), IoTDBDescriptor.getInstance().getConfig().getZoneID());
            ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisCloseTime), IoTDBDescriptor.getInstance().getConfig().getZoneID());
			long timeInterval = thisCloseTime - closeAllLastTime;
			LOGGER.info("Start the close action regularly, last time is {}, this time is {}, time interval is {}s.",
					startDateTime, endDateTime, timeInterval / 1000);
			closeAllLastTime = System.currentTimeMillis();
			try {
				FileNodeManager.getInstance().closeAll();
			} catch (Exception e) {
				LOGGER.error("close all error.", e);
			}
		}
	}

	@Override
	public void start() throws StartupException {
		try {
			startService();
		} catch (Exception e) {
			String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
			throw new StartupException(errorMessage);
		}
	}

	@Override
	public void stop() {
		closeService();
	}

	@Override
	public ServiceType getID() {
		return ServiceType.CLOSE_MERGE_SERVICE;
	}
}
