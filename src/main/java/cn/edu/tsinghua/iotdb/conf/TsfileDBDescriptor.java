package cn.edu.tsinghua.iotdb.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsfileDBDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TsfileDBDescriptor.class);

	private static class TsfileDBDescriptorHolder{
		private static final TsfileDBDescriptor INSTANCE = new TsfileDBDescriptor();
	}
	
	private TsfileDBDescriptor() {
		loadProps();
	}

	public static final TsfileDBDescriptor getInstance() {
		return TsfileDBDescriptorHolder.INSTANCE;
	}

	public TsfileDBConfig getConfig() {
		return conf;
	}

	private TsfileDBConfig conf = new TsfileDBConfig();

	/**
	 * load an properties file and set TsfileDBConfig variables
	 *
	 */
	private void loadProps() {
		InputStream inputStream = null;
		String url = System.getProperty(TsFileDBConstant.IOTDB_CONF, null);
		if (url == null) {
			url = System.getProperty(TsFileDBConstant.IOTDB_HOME, null);
			if (url != null) {
				url = url + File.separatorChar + "conf" + File.separatorChar + TsfileDBConfig.CONFIG_NAME;
			} else {
				LOGGER.warn("Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration", TsfileDBConfig.CONFIG_NAME);
				// update all data path
				conf.updateDataPath();
				return;
			}
		} else{
			url += (File.separatorChar + TsfileDBConfig.CONFIG_NAME);
		}
		
		try {
			inputStream = new FileInputStream(new File(url));
		} catch (FileNotFoundException e) {
			LOGGER.warn("Fail to find config file {}", url);
			// update all data path
			conf.updateDataPath();
			return;
		}

		LOGGER.info("Start to read config file {}", url);
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
			conf.enableStatMonitor = Boolean.parseBoolean(properties.getProperty("enable_stat_monitor", conf.enableStatMonitor + ""));
			conf.backLoopPeriodSec = Integer.parseInt(properties.getProperty("back_loop_period_sec", conf.backLoopPeriodSec + ""));
			int statMonitorDetectFreqSec = Integer.parseInt(properties.getProperty("stat_monitor_detect_freq_sec", conf.statMonitorDetectFreqSec + ""));
			int statMonitorRetainIntervalSec = Integer.parseInt(properties.getProperty("stat_monitor_retain_interval_sec", conf.statMonitorRetainIntervalSec + ""));
			// the conf value must > default value, or may cause system unstable
			if (conf.statMonitorDetectFreqSec < statMonitorDetectFreqSec) {
				conf.statMonitorDetectFreqSec = statMonitorDetectFreqSec;
			} else {
				LOGGER.info("The stat_monitor_detect_freq_sec value is smaller than default, use default value");
			}

			if (conf.statMonitorRetainIntervalSec < statMonitorRetainIntervalSec) {
				conf.statMonitorRetainIntervalSec = statMonitorRetainIntervalSec;
			}else {
				LOGGER.info("The stat_monitor_retain_interval_sec value is smaller than default, use default value");
			}

			conf.rpcPort = Integer.parseInt(properties.getProperty("rpc_port",conf.rpcPort+""));
			
			conf.enableWal = Boolean.parseBoolean(properties.getProperty("enable_wal", conf.enableWal+""));

			conf.walCleanupThreshold = Integer.parseInt(properties.getProperty("wal_cleanup_threshold", conf.walCleanupThreshold+""));
			conf.flushWalThreshold = Integer.parseInt(properties.getProperty("flush_wal_threshold", conf.flushWalThreshold+""));
			conf.flushWalPeriodInMs = Integer.parseInt(properties.getProperty("flush_wal_period_in_ms", conf.flushWalPeriodInMs+""));
			
			conf.dataDir = properties.getProperty("data_dir", conf.dataDir);
			
			
			conf.mergeConcurrentThreads = Integer.parseInt(properties.getProperty("merge_concurrent_threads", conf.mergeConcurrentThreads + ""));
			conf.maxOpenFolder = Integer.parseInt(properties.getProperty("max_opened_folder", conf.maxOpenFolder + ""));
			
			conf.fetchSize = Integer.parseInt(properties.getProperty("fetch_size", conf.fetchSize + ""));
			
			conf.periodTimeForFlush = Long.parseLong(properties.getProperty("period_time_for_flush_in_second", conf.periodTimeForFlush+"").trim());
			conf.periodTimeForMerge = Long.parseLong(properties.getProperty("period_time_for_merge_in_second", conf.periodTimeForMerge+"").trim());
			conf.enableTimingCloseAndMerge = Boolean.parseBoolean(properties.getProperty("enable_timing_close_and_Merge", conf.enableTimingCloseAndMerge+"").trim());
			
			conf.memThresholdWarning = (long) (Runtime.getRuntime().maxMemory() * Double.parseDouble(properties.getProperty("mem_threshold_warning", conf.memThresholdWarning+"").trim()) );
			conf.memThresholdDangerous = (long) (Runtime.getRuntime().maxMemory() * Double.parseDouble(properties.getProperty("mem_threshold_dangerous", conf.memThresholdDangerous+"").trim()));

			conf.memMonitorInterval = Long.parseLong(properties.getProperty("mem_monitor_interval", conf.memMonitorInterval+"").trim());

			conf.memControllerType = Integer.parseInt(properties.getProperty("mem_controller_type", conf.memControllerType+"").trim());
			conf.memControllerType = conf.memControllerType >= BasicMemController.CONTROLLER_TYPE.values().length ? 0 : conf.memControllerType;

			conf.bufferwriteMetaSizeThreshold = Long.parseLong(properties.getProperty("bufferwrite_meta_size_threshold", conf.bufferwriteMetaSizeThreshold + "").trim());
			conf.bufferwriteFileSizeThreshold = Long.parseLong(properties.getProperty("bufferwrite_file_size_threshold", conf.bufferwriteFileSizeThreshold + "").trim());

			conf.overflowMetaSizeThreshold = Long.parseLong(properties.getProperty("overflow_meta_size_threshold", conf.overflowMetaSizeThreshold + "").trim());
			conf.overflowFileSizeThreshold = Long.parseLong(properties.getProperty("overflow_file_size_threshold", conf.overflowFileSizeThreshold + "").trim());

			if(conf.memThresholdWarning <= 0)
				conf.memThresholdWarning = TsFileDBConstant.MEM_THRESHOLD_WARNING_DEFAULT;
			if(conf.memThresholdDangerous < conf.memThresholdWarning)
				conf.memThresholdDangerous = Math.max(conf.memThresholdWarning, TsFileDBConstant.MEM_THRESHOLD_DANGEROUS_DEFAULT);

			conf.concurrentFlushThread  = Integer.parseInt(properties.getProperty("concurrent_flush_thread", conf.concurrentFlushThread + ""));
			if(conf.concurrentFlushThread <= 0)
				conf.concurrentFlushThread = Runtime.getRuntime().availableProcessors();

			conf.enableMemMonitor = Boolean.parseBoolean(properties.getProperty("enable_mem_monitor", conf.enableMemMonitor + "").trim());
			conf.enableSmallFlush = Boolean.parseBoolean(properties.getProperty("enable_small_flush", conf.enableSmallFlush + "").trim());
			conf.smallFlushInterval = Long.parseLong(properties.getProperty("small_flush_interval", conf.smallFlushInterval + "").trim());

			String tmpTimeZone = properties.getProperty("time_zone", conf.timeZone.getID());
			try {
				conf.timeZone = DateTimeZone.forID(tmpTimeZone.trim());
				LOGGER.info("Time zone has been set to {}", conf.timeZone);
			} catch (Exception e) {
				LOGGER.error("Time zone foramt error {}, use default configuration {}", tmpTimeZone, conf.timeZone);
			}

		} catch (IOException e) {
			LOGGER.warn("Cannot load config file because {}, use default configuration", e.getMessage());
		} catch (Exception e) {
			LOGGER.warn("Error format in config file because {}, use default configuration", e.getMessage());
		} finally {
			// update all data path
			conf.updateDataPath();
		}
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				LOGGER.error("Fail to close config file input stream because {}", e.getMessage());
			}
		}
	}
}
