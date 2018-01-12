package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Comparator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.flushthread.FlushManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.ErrorDebugException;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.common.DataFileInfo;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.IStatistic;
import cn.edu.tsinghua.iotdb.monitor.MonitorConstants;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class FileNodeManager implements IStatistic {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeManager.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final String restoreFileName = "fileNodeManager.restore";
	private final String baseDir;
	/**
	 * This map is used to manage all filenode processor,<br>
	 * the key is filenode name which is storage group path.
	 */
	private ConcurrentHashMap<String, FileNodeProcessor> processorMap;
	/**
	 * This set is used to store overflowed filenode name.<br>
	 * The overflowed filenode will be merge.
	 */
	private volatile Set<String> overflowedFileNodeName;
	private volatile Set<String> backUpOverflowedFileNodeName;
	private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;

	/**
	 * Stat information
	 */
	private final String statStorageDeltaName = MonitorConstants.statStorageGroupPrefix
			+ MonitorConstants.MONITOR_PATH_SEPERATOR + MonitorConstants.fileNodeManagerPath;

	// There is no need to add concurrently
	private HashMap<String, AtomicLong> statParamsHashMap = new HashMap<String, AtomicLong>() {
		{
			for (MonitorConstants.FileNodeManagerStatConstants fileNodeManagerStatConstant : MonitorConstants.FileNodeManagerStatConstants
					.values()) {
				put(fileNodeManagerStatConstant.name(), new AtomicLong(0));
			}
		}
	};

	private void updateStatHashMapWhenFail(TSRecord tsRecord) {
		statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_FAIL.name())
				.incrementAndGet();
		statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_FAIL.name())
				.addAndGet(tsRecord.dataPointList.size());
	}
	/**
	 * @return the key represent the params' name, values is AtomicLong type
	 */
	public HashMap<String, AtomicLong> getStatParamsHashMap() {
		return statParamsHashMap;
	}

	@Override
	public List<String> getAllPathForStatistic() {
		List<String> list = new ArrayList<>();
		for (MonitorConstants.FileNodeManagerStatConstants statConstant : MonitorConstants.FileNodeManagerStatConstants
				.values()) {
			list.add(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name());
		}
		return list;
	}

	@Override
	public HashMap<String, TSRecord> getAllStatisticsValue() {
		long curTime = System.currentTimeMillis();
		TSRecord tsRecord = StatMonitor.convertToTSRecord(getStatParamsHashMap(), statStorageDeltaName, curTime);
		return new HashMap<String, TSRecord>() {
			{
				put(statStorageDeltaName, tsRecord);
			}
		};
	}

	/**
	 * Init Stat MetaDta TODO: Modify the throws operation
	 */
	@Override
	public void registStatMetadata() {
		HashMap<String, String> hashMap = new HashMap<String, String>() {
			{
				for (MonitorConstants.FileNodeManagerStatConstants statConstant : MonitorConstants.FileNodeManagerStatConstants
						.values()) {
					put(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name(),
							MonitorConstants.DataType);
				}
			}
		};
		StatMonitor.getInstance().registStatStorageGroup(hashMap);
	}

	private Action overflowBackUpAction = new Action() {
		@Override
		public void act() throws Exception {
			synchronized (overflowedFileNodeName) {
				backUpOverflowedFileNodeName = new HashSet<>();
				backUpOverflowedFileNodeName.addAll(overflowedFileNodeName);
			}
		}
	};

	private Action overflowFlushAction = new Action() {
		@Override
		public void act() throws Exception {
			synchronized (backUpOverflowedFileNodeName) {
				writeOverflowSetToDisk();
			}
		}
	};

	private static class FileNodeManagerHolder {
		private static FileNodeManager INSTANCE = new FileNodeManager(TsFileDBConf.fileNodeDir);
	}

	public static FileNodeManager getInstance() {
		return FileNodeManagerHolder.INSTANCE;
	}

	/**
	 * This function is just for unit test
	 */
	public synchronized void resetFileNodeManager() {
		this.backUpOverflowedFileNodeName = new HashSet<>();
		this.overflowedFileNodeName = new HashSet<>();

		for (String key : statParamsHashMap.keySet()) {
			statParamsHashMap.put(key, new AtomicLong());
		}
	}

	private FileNodeManager(String baseDir) {
		processorMap = new ConcurrentHashMap<String, FileNodeProcessor>();

		if (baseDir.charAt(baseDir.length() - 1) != File.separatorChar)
			baseDir += File.separatorChar;
		this.baseDir = baseDir;
		File dir = new File(baseDir);
		if (dir.mkdirs()) {
			LOGGER.info("{} dir home doesn't exist, create it", dir.getPath());
		}

		TsFileConf.duplicateIncompletedPage = true;
		this.overflowedFileNodeName = readOverflowSetFromDisk();
		if (overflowedFileNodeName == null) {
			LOGGER.error("Read the {} file error.", restoreFileName);
			overflowedFileNodeName = new HashSet<>();
		}
		if (TsFileDBConf.enableStatMonitor) {
			StatMonitor statMonitor = StatMonitor.getInstance();
			registStatMetadata();
			statMonitor.registStatistics(statStorageDeltaName, this);
		}
	}

	private FileNodeProcessor constructNewProcessor(String filenodeName) throws FileNodeManagerException {
		try {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			return new FileNodeProcessor(baseDir, filenodeName, parameters);
		} catch (FileNodeProcessorException e) {
			LOGGER.error(String.format("Can't construct the FileNodeProcessor, the filenode is %s", filenodeName), e);
			throw new FileNodeManagerException(e);
		}
	}

	private FileNodeProcessor getProcessor(String path, boolean isWriteLock) throws FileNodeManagerException {
		String filenodeName;
		try {
			filenodeName = MManager.getInstance().getFileNameByPath(path);
		} catch (PathErrorException e) {
			LOGGER.error("MManager get filenode name error, path is {}", path);
			throw new FileNodeManagerException(e);
		}
		FileNodeProcessor processor = null;
		processor = processorMap.get(filenodeName);
		if (processor != null) {
			processor.lock(isWriteLock);
		} else {
			filenodeName = filenodeName.intern();
			// calculate the value with same key synchronously
			synchronized (filenodeName) {
				processor = processorMap.get(filenodeName);
				if (processor != null) {
					processor.lock(isWriteLock);
				} else {
					// calculate the value with the key monitor
					LOGGER.debug("Calcuate the processor, the filenode is {}, Thread is {}", filenodeName,
							Thread.currentThread().getId());
					processor = constructNewProcessor(filenodeName);
					processor.lock(isWriteLock);
					processorMap.put(filenodeName, processor);
				}
			}
		}
		// processorMap.putIfAbsent(path, processor);
		return processor;
	}

	public void recovery() {

		try {
			List<String> filenodeNames = MManager.getInstance().getAllFileNames();
			for (String filenodeName : filenodeNames) {
				FileNodeProcessor fileNodeProcessor = getProcessor(filenodeName, true);
				if (fileNodeProcessor.shouldRecovery()) {
					LOGGER.info("Recovery the filenode processor, the filenode is {}, the status is {}", filenodeName,
							fileNodeProcessor.getFileNodeProcessorStatus());
					fileNodeProcessor.fileNodeRecovery();
				} else {
					fileNodeProcessor.writeUnlock();
				}
				// add index check sum
				fileNodeProcessor.rebuildIndex();
			}
		} catch (PathErrorException | FileNodeManagerException | FileNodeProcessorException e) {
			LOGGER.error("Restore all FileNode failed, the reason is {}", e.getMessage());
		}
	}

	/**
	 * insert TsRecord into storage group
	 * @param tsRecord: input Data
	 * @param isMonitor: if true the insertion is done by StatMonitor then the Stat Info will not be recorded.
	 *                 else the statParamsHashMap will be updated
	 * @return an int value represents the insert type
	 * @throws FileNodeManagerException
	 */
	public int insert(TSRecord tsRecord, boolean isMonitor) throws FileNodeManagerException {
		long timestamp = tsRecord.time;
		String deltaObjectId = tsRecord.deltaObjectId;

		if (!isMonitor) {
			statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS.name())
					.addAndGet(tsRecord.dataPointList.size());
		}

		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, true);
		int insertType = 0;

		try {
			long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
			String filenodeName = fileNodeProcessor.getProcessorName();
			if (timestamp <= lastUpdateTime) {
				Map<String, Object> parameters = new HashMap<>();
				parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
				parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
				// get overflow processor
				OverflowProcessor overflowProcessor;
				try {
					overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName, parameters);
				} catch (FileNodeProcessorException e) {
					LOGGER.error(
							String.format("Get the overflow processor failed, the filenode is {}, insert time is {}",
									filenodeName, timestamp),
							e);
					if (!isMonitor) {
						updateStatHashMapWhenFail(tsRecord);
					}
					throw new FileNodeManagerException(e);
				}
				// write wal
				try {
					if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
						if (!WriteLogManager.isRecovering) {
							WriteLogManager.getInstance().write(filenodeName, tsRecord, WriteLogManager.OVERFLOW);
						}
					}
				} catch (IOException | PathErrorException e) {
					LOGGER.error("Error in write WAL.", e);
					if (!isMonitor) {
						updateStatHashMapWhenFail(tsRecord);
					}
					throw new FileNodeManagerException(e);
				}
				// write overflow data
				for (DataPoint dataPoint : tsRecord.dataPointList) {
					try {
						overflowProcessor.insert(deltaObjectId, dataPoint.getMeasurementId(), timestamp,
								dataPoint.getType(), dataPoint.getValue().toString());
					} catch (ProcessorException e) {
						LOGGER.error("Insert into overflow error, the reason is {}", e.getMessage());
						if (!isMonitor) {
							updateStatHashMapWhenFail(tsRecord);
						}
						throw new FileNodeManagerException(e);
					}
				}
				// change the type of tsfile to overflowed
				fileNodeProcessor.changeTypeToChanged(deltaObjectId, timestamp);
				addFileNodeNameToOverflowSet(filenodeName);
				insertType = 1;
			} else {
				// get bufferwrite processor
				BufferWriteProcessor bufferWriteProcessor;
				try {
					bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(filenodeName, timestamp);
				} catch (FileNodeProcessorException e) {
					LOGGER.error("Get the bufferwrite processor failed, the filenode is {}, insert time is {}",
							filenodeName, timestamp);
					if (!isMonitor) {
						updateStatHashMapWhenFail(tsRecord);
					}
					throw new FileNodeManagerException(e);
				}
				// Add a new interval file to newfilelist
				if (bufferWriteProcessor.isNewProcessor()) {
					bufferWriteProcessor.setNewProcessor(false);
					String bufferwriteRelativePath = bufferWriteProcessor.getFileRelativePath();
					try {
						fileNodeProcessor.addIntervalFileNode(timestamp, bufferwriteRelativePath);
					} catch (Exception e) {
						if (!isMonitor) {
							updateStatHashMapWhenFail(tsRecord);
						}
						throw new FileNodeManagerException(e);
					}
				}
				// write wal
				try {
					if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
						if (!WriteLogManager.isRecovering) {
							WriteLogManager.getInstance().write(filenodeName, tsRecord, WriteLogManager.BUFFERWRITER);
						}
					}
				} catch (IOException | PathErrorException e) {
					LOGGER.error("Error in write WAL.", e);
					if (!isMonitor) {
						updateStatHashMapWhenFail(tsRecord);
					}
					throw new FileNodeManagerException(e);
				}
				// Write data
				boolean shouldClose = false;
				try {
					shouldClose = bufferWriteProcessor.write(tsRecord);
				} catch (BufferWriteProcessorException e) {
					if (!isMonitor) {
						updateStatHashMapWhenFail(tsRecord);
					}
					throw new FileNodeManagerException(e);
				}
				fileNodeProcessor.setIntervalFileNodeStartTime(deltaObjectId, timestamp);
				fileNodeProcessor.setLastUpdateTime(deltaObjectId, timestamp);
				insertType = 2;
				if (shouldClose) {
					fileNodeProcessor.closeBufferWrite();
				}
			}
		} catch (FileNodeProcessorException e) {
			LOGGER.error(
					String.format("close the buffer write processor %s error.", fileNodeProcessor.getProcessorName()),
					e);
			e.printStackTrace();
		} finally {
			fileNodeProcessor.writeUnlock();
		}
		//Modify the insert
		if (!isMonitor) {
			fileNodeProcessor.getStatParamsHashMap()
					.get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_POINTS_SUCCESS.name())
					.addAndGet(tsRecord.dataPointList.size());
			fileNodeProcessor.getStatParamsHashMap()
					.get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_REQ_SUCCESS.name()).incrementAndGet();
			statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_SUCCESS.name()).incrementAndGet();
			statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_SUCCESS.name())
					.addAndGet(tsRecord.dataPointList.size());
		}
		return insertType;
	}

	private void addFileNodeNameToOverflowSet(String filenodeName) throws FileNodeManagerException {
		synchronized (overflowedFileNodeName) {
			if (!overflowedFileNodeName.contains(filenodeName)) {
				overflowedFileNodeName.add(filenodeName);
			}
		}
	}

	private Set<String> getOverflowedFileNodeNameAndClear() throws FileNodeManagerException {
		synchronized (overflowedFileNodeName) {
			Set<String> result = overflowedFileNodeName;
			overflowedFileNodeName = new HashSet<String>();
			return result;
		}
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			String v) throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, true);
		try {
			// write wal
			try {
				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					if (!WriteLogManager.isRecovering) {
						WriteLogManager.getInstance().write(fileNodeProcessor.getProcessorName(),
								new UpdatePlan(startTime, endTime, v, new Path(deltaObjectId + "." + measurementId)));
					}
				}
			} catch (IOException | PathErrorException e) {
				LOGGER.error("Error in write WAL.", e);
				throw new FileNodeManagerException(e);
			}

			long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
			if (startTime > lastUpdateTime) {
				LOGGER.warn("The update range is error, startTime {} is great than lastUpdateTime {}", startTime,
						lastUpdateTime);
				return;
			}
			if (endTime > lastUpdateTime) {
				endTime = lastUpdateTime;
			}
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			String filenodeName = fileNodeProcessor.getProcessorName();
			// get overflow processor
			OverflowProcessor overflowProcessor;
			try {
				overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName, parameters);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(
						String.format("Get the overflow processor failed, the filenode is {}, update time is {} to {}",
								filenodeName, startTime, endTime),
						e);
				throw new FileNodeManagerException(e);
			}
			try {
				overflowProcessor.update(deltaObjectId, measurementId, startTime, endTime, type, v);
			} catch (OverflowProcessorException e) {
				LOGGER.error("Update error: deltaObjectId {}, measurementId {}, startTime {}, endTime {}, value {}",
						deltaObjectId, measurementId, startTime, endTime, v);
				throw new FileNodeManagerException(e);
			}
			// change the type of tsfile to overflowed
			fileNodeProcessor.changeTypeToChanged(deltaObjectId, startTime, endTime);
			addFileNodeNameToOverflowSet(filenodeName);
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type)
			throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, true);
		try {
			// write wal
			try {
				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					if (!WriteLogManager.isRecovering) {
						WriteLogManager.getInstance().write(fileNodeProcessor.getProcessorName(),
								new DeletePlan(timestamp, new Path(deltaObjectId + "." + measurementId)));
					}
				}
			} catch (IOException | PathErrorException e) {
				LOGGER.error("Error in write WAL,", e);
				throw new FileNodeManagerException(e);
			}

			long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
			// no tsfile data, the delete operation is invalid
			if (lastUpdateTime == -1) {
				LOGGER.warn("The last update time is -1, delete overflow is invalid");
			} else {
				if (timestamp > lastUpdateTime) {
					timestamp = lastUpdateTime;
				}
				Map<String, Object> parameters = new HashMap<>();
				parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
				parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
				String filenodeName = fileNodeProcessor.getProcessorName();
				// get overflow processor
				OverflowProcessor overflowProcessor;
				try {
					overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName, parameters);
				} catch (FileNodeProcessorException e) {
					LOGGER.error(
							String.format("Get the overflow processor failed, the filenode is {}, delete time is {}",
									filenodeName, timestamp),
							e);
					throw new FileNodeManagerException(e);
				}
				try {
					overflowProcessor.delete(deltaObjectId, measurementId, timestamp, type);
				} catch (OverflowProcessorException e) {
					LOGGER.error("Delete error: the deltaObjectId {}, the measurementId {}, the timestamp {}",
							deltaObjectId, measurementId, timestamp);
					throw new FileNodeManagerException(e);
				}
				fileNodeProcessor.changeTypeToChangedForDelete(deltaObjectId, timestamp);
				addFileNodeNameToOverflowSet(filenodeName);
			}
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	public int beginQuery(String deltaObjectId) throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, true);
		try {
			LOGGER.debug("Get the FileNodeProcessor: filenode is {}, begin query.",
					fileNodeProcessor.getProcessorName());
			int token = fileNodeProcessor.addMultiPassLock();
			return token;
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	public QueryStructure query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
			throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, false);
		LOGGER.debug("Get the FileNodeProcessor: filenode is {}, query.", fileNodeProcessor.getProcessorName());
		try {
			QueryStructure queryStructure = null;
			// query operation must have overflow processor
			if (!fileNodeProcessor.hasOverflowProcessor()) {
				Map<String, Object> parameters = new HashMap<>();
				parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
				parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
				try {
					fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getProcessorName(), parameters);
				} catch (FileNodeProcessorException e) {
					LOGGER.error(String.format("Get the overflow processor failed, the filenode is {}",
							fileNodeProcessor.getProcessorName()), e);
					throw new FileNodeManagerException(e);
				}
			}
			try {
				queryStructure = fileNodeProcessor.query(deltaObjectId, measurementId, timeFilter, freqFilter,
						valueFilter);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(String.format("Query error: the deltaObjectId {}, the measurementId {}", deltaObjectId,
						measurementId), e);
				throw new FileNodeManagerException(e);
			}
			// return query structure
			return queryStructure;
		} finally {
			fileNodeProcessor.readUnlock();
		}
	}

	/**
	 * 
	 * @param path
	 *            : the column path
	 * @param startTime
	 *            : the startTime of index
	 * @param endTime
	 *            : the endTime of index
	 *
	 * @throws FileNodeManagerException
	 */
	public List<DataFileInfo> indexBuildQuery(Path path, long startTime, long endTime) throws FileNodeManagerException {
		String deltaObjectId = path.getDeltaObjectToString();
		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, false);
		try {
			LOGGER.debug("Get the FileNodeProcessor: the filenode is {}, query.", fileNodeProcessor.getProcessorName());
			return fileNodeProcessor.indexQuery(deltaObjectId, startTime, endTime);
		} finally {
			fileNodeProcessor.readUnlock();
		}
	}

	public void endQuery(String deltaObjectId, int token) throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = getProcessor(deltaObjectId, true);
		try {
			LOGGER.debug("Get the FileNodeProcessor: {}, filenode is {}, end query.",
					fileNodeProcessor.getProcessorName());
			fileNodeProcessor.removeMultiPassLock(token);
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	public synchronized boolean mergeAll() throws FileNodeManagerException {

		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.MERGE;
			// flush information first
			Set<String> allChangedFileNodes = getOverflowedFileNodeNameAndClear();
			// set a flag to notify is merging status
			LOGGER.info("Begin to merge all overflowed filenode: {}", allChangedFileNodes);
			ExecutorService singleMergingService = Executors.newSingleThreadExecutor();
			MergeAllProcessors mergeAllProcessors = new MergeAllProcessors(allChangedFileNodes);
			singleMergingService.execute(mergeAllProcessors);
			return true;
		} else {
			return false;
		}
	}

	public void clearOneFileNode(String filenodeName) throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = getProcessor(filenodeName, true);
		try {
			fileNodeProcessor.clearFileNode();
			overflowedFileNodeName.remove(filenodeName);
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	/**
	 * close one processor
	 * 
	 * @param namespacePath
	 * @return
	 * @throws LRUManagerException
	 */
	private boolean closeOneProcessor(String namespacePath) throws FileNodeManagerException {
		if (processorMap.containsKey(namespacePath)) {
			Processor processor = processorMap.get(namespacePath);
			try {
				processor.writeLock();
				// wait until the processor can be closed
				while (!processor.canBeClosed()) {
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (InterruptedException e) {
						LOGGER.warn("Interrupted when waitting to close one processor.");
					}
				}
				processor.close();
				processorMap.remove(namespacePath);
			} catch (ProcessorException e) {
				LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}.", namespacePath);
				throw new FileNodeManagerException(e);
			} finally {
				processor.writeUnlock();
			}
		}
		return true;
	}

	public synchronized boolean deleteOneFileNode(String namespacePath) throws FileNodeManagerException {

		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			try {
				FileNodeProcessor fileNodeProcessor = getProcessor(namespacePath, true);
				try {
					closeOneProcessor(namespacePath);
					// delete filenode/bufferwrite/overflow dir
					String fileNodePath = TsFileDBConf.fileNodeDir;
					fileNodePath = standardizeDir(fileNodePath) + namespacePath;
					FileUtils.deleteDirectory(new File(fileNodePath));

					String bufferwritePath = TsFileDBConf.bufferWriteDir;
					bufferwritePath = standardizeDir(bufferwritePath) + namespacePath;
					FileUtils.deleteDirectory(new File(bufferwritePath));

					String overflowPath = TsFileDBConf.overflowDataDir;
					overflowPath = standardizeDir(overflowPath) + namespacePath;
					FileUtils.deleteDirectory(new File(overflowPath));
					return true;
				} catch (IOException e) {
					throw new FileNodeManagerException(e);
				} finally {
					fileNodeProcessor.writeUnlock();
				}
			} finally {
				fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			}
		} else {
			return false;
		}
	}

	private String standardizeDir(String originalPath) {
		String res = originalPath;
		if ((originalPath.length() > 0 && originalPath.charAt(originalPath.length() - 1) != File.separatorChar)
				|| originalPath.length() == 0) {
			res = originalPath + File.separatorChar;
		}
		return res;
	}

	public synchronized void addTimeSeries(Path path, String dataType, String encoding, String[] encodingArgs)
			throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = getProcessor(path.getFullPath(), true);
		try {
			if (fileNodeProcessor.hasBufferwriteProcessor()) {
				BufferWriteProcessor bufferWriteProcessor = null;
				try {
					bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor();
					bufferWriteProcessor.addTimeSeries(path.getMeasurementToString(), dataType, encoding, encodingArgs);
				} catch (FileNodeProcessorException e) {
					LOGGER.error("Get the bufferwrite processor failed, the filenode is {}",
							fileNodeProcessor.getProcessorName());
					throw new FileNodeManagerException(e);
				} catch (IOException e) {
					LOGGER.error("Add timeseries error ", e);
					throw new FileNodeManagerException(e);
				}
			} else {
				return;
			}
		} finally {
			fileNodeProcessor.writeUnlock();
		}
	}

	public synchronized boolean closeOneFileNode(String namespacePath) throws FileNodeManagerException {

		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			try {
				FileNodeProcessor fileNodeProcessor = getProcessor(namespacePath, true);
				try {
					closeOneProcessor(namespacePath);
					return true;
				} finally {
					fileNodeProcessor.writeUnlock();
				}
			} finally {
				fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			}
		} else {
			return false;
		}
	}

	private void close(String nsPath, Iterator<Entry<String, FileNodeProcessor>> processorIterator)
			throws FileNodeManagerException {
		if (processorMap.containsKey(nsPath)) {
			Processor processor = processorMap.get(nsPath);
			if (processor.tryWriteLock()) {
				try {
					if (processor.canBeClosed()) {
						try {
							LOGGER.info("Close the processor, the nameSpacePath is {}", nsPath);
							processor.close();
							processorMap.remove(nsPath);
						} catch (ProcessorException e) {
							LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}",
									nsPath);
							throw new FileNodeManagerException(e);
						}
					} else {
						LOGGER.warn("The processor can't be closed, the nameSpacePath is {}", nsPath);
					}
				} finally {
					processor.writeUnlock();
				}
			} else {
				LOGGER.warn("Can't get the write lock the processor and close the processor, the nameSpacePath is {}",
						nsPath);
			}
		} else {
			LOGGER.warn("The processorMap does't contains the nameSpacePath {}", nsPath);
		}
	}

	/**
	 * Try to close All
	 * 
	 * @return true - close successfully false - can't close because of merge
	 * @throws FileNodeManagerException
	 */
	public synchronized boolean closeAll() throws FileNodeManagerException {
		LOGGER.info("start closing file node manager");
		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			try {
				Iterator<Entry<String, FileNodeProcessor>> processorIterator = processorMap.entrySet().iterator();
				while (processorIterator.hasNext()) {
					Entry<String, FileNodeProcessor> processorEntry = processorIterator.next();
					try {
						close(processorEntry.getKey(), processorIterator);
					} catch (FileNodeManagerException e) {
						LOGGER.error("Close processor error when close all processors, the nameSpacePath is {}",
								processorEntry.getKey());
						throw e;
					}
				}
				return processorMap.isEmpty();
			} catch (FileNodeManagerException e) {
				throw new FileNodeManagerException(e);
			} finally {
				LOGGER.info("shutdown file node manager successfully");
				fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			}
		} else {
			LOGGER.info("failed to shutdown file node manager because of merge operation");
			return false;
		}
	}

	/**
	 * Backup information about the nameSpacePath of overflow set
	 * 
	 * @throws FileNodeManagerException
	 */
	private void writeOverflowSetToDisk() throws FileNodeManagerException {

		SerializeUtil<Set<String>> serializeUtil = new SerializeUtil<>();
		File fileNodeManagerStoreFile = new File(baseDir, restoreFileName);
		try {
			serializeUtil.serialize(backUpOverflowedFileNodeName, fileNodeManagerStoreFile.getPath());
		} catch (IOException e) {
			LOGGER.error(
					"Serialize the overflow nameSpacePath Set error, and delete the overflow restore file, the file path is {}",
					fileNodeManagerStoreFile);
			File restoreFile = new File(fileNodeManagerStoreFile.getPath());
			restoreFile.delete();
			throw new FileNodeManagerException(
					"Serialize the overflow nameSpacePath Set error, and delete the overflow restore file");
		}
	}

	/**
	 * Read information about the nameSpacePath of overflow set from recovery
	 * file
	 * 
	 * @return
	 */
	private Set<String> readOverflowSetFromDisk() {
		SerializeUtil<Set<String>> serializeUtil = new SerializeUtil<>();
		Set<String> overflowSet = null;
		File fileNodeManagerStoreFile = new File(baseDir, restoreFileName);
		try {
			overflowSet = serializeUtil.deserialize(fileNodeManagerStoreFile.getPath()).orElse(new HashSet<>());
		} catch (IOException e) {
			LOGGER.error(
					"Deserizlize the overflow nameSpaceSet error, and delete the filenode manager restore file, the restore file path is {}",
					fileNodeManagerStoreFile);
			// delete restore file
			if (fileNodeManagerStoreFile.exists()) {
				fileNodeManagerStoreFile.delete();
			}
		}
		return overflowSet;
	}

	private class MergeAllProcessors implements Runnable {

		private Set<String> allChangedFileNodes;

		public MergeAllProcessors(Set<String> allChangedFileNodes) {
			this.allChangedFileNodes = allChangedFileNodes;
		}

		@Override
		public void run() {
			ExecutorService mergeExecutorPool = IoTDBThreadPoolFactory
					.newFixedThreadPool(TsFileDBConf.mergeConcurrentThreads, "Merge");
			for (String fileNodeNamespacePath : allChangedFileNodes) {
				MergeOneProcessor mergeOneProcessorThread = new MergeOneProcessor(fileNodeNamespacePath);
				mergeExecutorPool.execute(mergeOneProcessorThread);
			}
			mergeExecutorPool.shutdown();
			while (!mergeExecutorPool.isTerminated()) {
				LOGGER.info("Not merge finished, wait 2000ms");
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					LOGGER.error("Interruption error when merge, the reason is {}", e.getMessage());
				}
			}
			fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			LOGGER.info("Merge finished, the merged filenode is {}", allChangedFileNodes);
		}
	}

	private class MergeOneProcessor implements Runnable {

		private String fileNodeNamespacePath;

		public MergeOneProcessor(String fileNodeNamespacePath) {
			this.fileNodeNamespacePath = fileNodeNamespacePath;
		}

		@Override
		public void run() {
			FileNodeProcessor fileNodeProcessor = null;
			try {
				do {
					fileNodeProcessor = getProcessor(fileNodeNamespacePath, true);
				} while (fileNodeProcessor == null);
				LOGGER.info("Get the FileNodeProcessor: the filenode is {}, merge.",
						fileNodeProcessor.getProcessorName());
				// close buffer write
				if (fileNodeProcessor.hasBufferwriteProcessor()) {
					while (!fileNodeProcessor.getBufferWriteProcessor().canBeClosed()) {

					}
					fileNodeProcessor.getBufferWriteProcessor().close();
					fileNodeProcessor.setBufferwriteProcessroToClosed();
				}
				// get overflow processor
				Map<String, Object> parameters = new HashMap<>();
				parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
				parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
				// try to get overflow processor
				fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getProcessorName(), parameters);
				// must close the overflow processor
				while (!fileNodeProcessor.getOverflowProcessor().canBeClosed()) {

				}
				fileNodeProcessor.getOverflowProcessor().close();
			} catch (FileNodeManagerException | FileNodeProcessorException | BufferWriteProcessorException
					| OverflowProcessorException e) {
				LOGGER.error("Merge the filenode processor {} error, the reason is {}",
						fileNodeProcessor.getProcessorName(), e.getMessage());
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new ErrorDebugException(e);
			}
			try {
				fileNodeProcessor.merge();
			} catch (FileNodeProcessorException e) {
				LOGGER.error("Merge the filenode processor {} error, the reason is {}",
						fileNodeProcessor.getProcessorName(), e.getMessage());
				throw new ErrorDebugException(e);
			}
		}
	}

	private enum FileNodeManagerStatus {
		NONE, MERGE, CLOSE;
	}

	public void forceFlush(BasicMemController.UsageLevel level) {
		// TODO : for each FileNodeProcessor, call its forceFlush()
		// you may add some delicate process like below
		// or you could provide multiple methods for different urgency
		switch (level) {
		case WARNING:
			// only select the most urgent (most active or biggest in size)
			// processors to flush
			// only select top 10% active memory user to flush
			try {
				flushTop(0.1f);
			} catch (IOException e) {
				LOGGER.error("force flush memory data error", e.getMessage());
				e.printStackTrace();
			}
			break;
		case DANGEROUS:
			// force all processors to flush
			try {
				flushAll();
			} catch (IOException e) {
				LOGGER.error("force flush memory data error:{}", e.getMessage());
				e.printStackTrace();
			}
			break;
		case SAFE:
			// if the flush thread pool is not full ( or half full), start a new
			// flush task
			if (FlushManager.getInstance().getActiveCnt() < 0.5 * FlushManager.getInstance().getThreadCnt()) {
				try {
					flushTop(0.01f);
				} catch (IOException e) {
					LOGGER.error("force flush memory data error:{}", e.getMessage());
					e.printStackTrace();
				}
			}
			break;
		}
	}

	private void flushAll() throws IOException {
		for (FileNodeProcessor processor : processorMap.values()) {
			if (processor.tryLock(true)) {
				try {
					processor.flush();
				} finally {
					processor.unlock(true);
				}
			}
		}
	}

	private void flushTop(float percentage) throws IOException {
		List<FileNodeProcessor> tempProcessors = new ArrayList<>(processorMap.values());
		// sort the tempProcessors as descending order
		Collections.sort(tempProcessors, new Comparator<FileNodeProcessor>() {
			@Override
			public int compare(FileNodeProcessor o1, FileNodeProcessor o2) {
				return (int) (o2.memoryUsage() - o1.memoryUsage());
			}
		});
		int flushNum = (int) (tempProcessors.size() * percentage) > 1 ? (int) (tempProcessors.size() * percentage) : 1;
		for (int i = 0; i < flushNum && i < tempProcessors.size(); i++) {
			FileNodeProcessor processor = tempProcessors.get(i);
			// 64M
			if (processor.memoryUsage() > TsFileConf.groupSizeInByte/2) {
				processor.writeLock();
				try {
					processor.flush();
				} finally {
					processor.writeUnlock();
				}
			}
		}
	}
}
