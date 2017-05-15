package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.LRUManager;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;
import cn.edu.thu.tsfiledb.exception.ErrorDebugException;
import cn.edu.thu.tsfiledb.metadata.MManager;

public class FileNodeManager extends LRUManager<FileNodeProcessor> {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeManager.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TSFileDBConfig TsFileDBConf = TSFileDBDescriptor.getInstance().getConfig();
	private static final String restoreFileName = "fileNodeManager.restore";
	private final String fileNodeManagerStoreFile;

	private Set<String> overflowNameSpaceSet;
	private Set<String> backUpOverflowNameSpaceSet;

	private static final Lock instanceLock = new ReentrantLock(false);
	private static FileNodeManager instance;

	private FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;

	private Action overflowBackUpAction = new Action() {
		@Override
		public void act() throws Exception {
			synchronized (overflowNameSpaceSet) {
				backUpOverflowNameSpaceSet = new HashSet<>();
				backUpOverflowNameSpaceSet.addAll(overflowNameSpaceSet);
			}
		}
	};

	private Action overflowFlushAction = new Action() {
		@Override
		public void act() throws Exception {
			synchronized (backUpOverflowNameSpaceSet) {
				writeOverflowSetToDisk();
			}
		}
	};

	public static FileNodeManager getInstance() {
		instanceLock.lock();
		try {
			if (instance == null) {
				instance = new FileNodeManager(TsFileDBConf.maxFileNodeNum, MManager.getInstance(),
						TsFileDBConf.FileNodeDir);
			}
			return instance;
		} finally {
			instanceLock.unlock();
		}
	}

	public static void init(int maxNodeNum, MManager mManager, String fileNodeDirPath) {
		instanceLock.lock();
		try {
			instance = new FileNodeManager(maxNodeNum, mManager, fileNodeDirPath);
		} finally {
			instanceLock.unlock();
		}
	}

	private FileNodeManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
		super(maxLRUNumber, mManager, normalDataDir);
		this.fileNodeManagerStoreFile = this.normalDataDir + restoreFileName;
		this.overflowNameSpaceSet = readOverflowSetFromDisk();
		if (overflowNameSpaceSet == null) {
			LOGGER.error("Read the overflow nameSpacePath set from filenode manager restore file error.");
			overflowNameSpaceSet = new HashSet<>();
		}
	}

	@Override
	protected FileNodeProcessor constructNewProcessor(String namespacePath) throws FileNodeManagerException {
		try {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			return new FileNodeProcessor(normalDataDir, namespacePath, parameters);
		} catch (FileNodeProcessorException e) {
			LOGGER.error("Can't construct the FileNodeProcessor, the nameSpacePath is {}", namespacePath);
			e.printStackTrace();
			throw new FileNodeManagerException(
					"Can't construct the FileNodeProcessor, the nameSpacePath is " + namespacePath);
		}
	}

	@Override
	protected void initProcessor(FileNodeProcessor processor, String namespacePath, Map<String, Object> args)
			throws LRUManagerException {
	}

	public void ManagerRecovery() {

		// Get all nameSpacePath from MManager

		// Check each nameSpacePath status

		// get all nsp from mmanager and restore all nsp
	}

	public int insert(TSRecord tsRecord) throws FileNodeManagerException {
		long timestamp = tsRecord.time;
		String deltaObjectId = tsRecord.deltaObjectId;

		FileNodeProcessor fileNodeProcessor = null;
		try {
			// try to get this filenodeProcessor until it not null
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true);
			} while (fileNodeProcessor == null);
		} catch (LRUManagerException e) {
			if (fileNodeProcessor != null) {
				// if get processor successfully, the processor must be not null
				fileNodeProcessor.writeUnlock();
			}
			throw new FileNodeManagerException(e);
		}
		long lastUpdataTime = fileNodeProcessor.getLastUpdateTime();
		LOGGER.debug("Get the FileNodeProcessor: {}, the last update time is: {}, the record time is: {}",
				fileNodeProcessor.getNameSpacePath(), lastUpdataTime, timestamp);
		LOGGER.debug("Insert record is {}", tsRecord);
		int insertType = 0;
		String nameSpacePath = fileNodeProcessor.getNameSpacePath();
		if (timestamp <= lastUpdataTime) {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			OverflowProcessor overflowProcessor;
			try {
				overflowProcessor = fileNodeProcessor.getOverflowProcessor(nameSpacePath, parameters);
			} catch (FileNodeProcessorException e) {
				LOGGER.error("Get the overflow processor failed, the nameSpacePath is {}, insert time is {}",
						nameSpacePath, timestamp);
				e.printStackTrace();
				throw new FileNodeManagerException(e);
			}
			// overflowProcessor.writeLock();
			for (DataPoint dataPoint : tsRecord.dataPointList) {
				try {
					overflowProcessor.insert(deltaObjectId, dataPoint.getMeasurementId(), timestamp,
							dataPoint.getType(), dataPoint.getValue().toString());
				} catch (ProcessorException e) {
					if (fileNodeProcessor != null) {
						fileNodeProcessor.writeUnlock();
					}
					throw new FileNodeManagerException(e);
				}
			}
			fileNodeProcessor.changeTypeToChanged(timestamp);
			addNameSpaceToOverflowList(fileNodeProcessor.getNameSpacePath());
			// overflowProcessor.writeUnlock();
			LOGGER.debug("Unlock the OverflowProcessor: {}", fileNodeProcessor.getNameSpacePath());
			insertType = 1;
		} else {
			BufferWriteProcessor bufferWriteProcessor;
			try {
				bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(nameSpacePath, timestamp);
			} catch (FileNodeProcessorException e) {
				LOGGER.error("Get the bufferwrite processor failed, the nameSpacePath is {}, insert time is {}",
						nameSpacePath, timestamp);
				e.printStackTrace();
				throw new FileNodeManagerException(e);
			}
			// add the new interval file to newfilelist
			if (bufferWriteProcessor.isNewProcessor()) {
				bufferWriteProcessor.setNewProcessor(false);
				String fileAbsolutePath = bufferWriteProcessor.getFileAbsolutePath();
				fileNodeProcessor.addIntervalFileNode(timestamp, fileAbsolutePath);
			}
			// bufferWriteProcessor.writeLock();
			try {
				bufferWriteProcessor.write(tsRecord);
			} catch (BufferWriteProcessorException e) {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new FileNodeManagerException(e);
			}
			fileNodeProcessor.setLastUpdateTime(timestamp);
			// bufferWriteProcessor.writeUnlock();
			LOGGER.debug("Unlock the BufferWriteProcessor: {}", fileNodeProcessor.getNameSpacePath());
			insertType = 2;
		}
		fileNodeProcessor.writeUnlock();
		LOGGER.debug("Unlock the FileNodeProcessor: {}", fileNodeProcessor.getNameSpacePath());
		return insertType;
	}

	private void addNameSpaceToOverflowList(String namespacePath) throws FileNodeManagerException {

		synchronized (overflowNameSpaceSet) {
			if (!overflowNameSpaceSet.contains(namespacePath)) {
				overflowNameSpaceSet.add(namespacePath);
				// should not write to disk
				// write to disk, only overflow rowgroup flush
				// writeOverflowSetToDisk();
			}
		}
	}

	private Set<String> getOverflowNameSpaceListAndClear() throws FileNodeManagerException {

		synchronized (overflowNameSpaceSet) {
			// should not flush overflowset to disk
			// write to disk, only overflow rowgroup flush
			// writeOverflowSetToDisk();
			Set<String> result = overflowNameSpaceSet;
			overflowNameSpaceSet = new HashSet<String>();
			return result;
		}
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			String v) throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true);
			} while (fileNodeProcessor == null);
		} catch (LRUManagerException e) {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		}

		long lastUpdateTime = fileNodeProcessor.getLastUpdateTime();
		LOGGER.debug("Get the FileNodeProcessor: {}, the last update time is: {}, the update time is from {} to {}",
				fileNodeProcessor.getNameSpacePath(), lastUpdateTime, startTime, endTime);
		if (startTime > lastUpdateTime) {
			LOGGER.warn("The update range is error, startTime {} is gt lastUpdateTime {}", startTime, lastUpdateTime);
			fileNodeProcessor.writeUnlock();
			return;
		}
		if (endTime > lastUpdateTime) {
			endTime = lastUpdateTime;
		}
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
		String namespacePath = fileNodeProcessor.getNameSpacePath();
		OverflowProcessor overflowProcessor;
		try {
			overflowProcessor = fileNodeProcessor.getOverflowProcessor(namespacePath, parameters);
		} catch (FileNodeProcessorException e) {
			LOGGER.error("Get the overflow processor failed, the nameSpacePath is {}, update time is {} to {}",
					namespacePath, startTime, endTime);
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		}
		// overflowProcessor.writeLock();
		try {
			overflowProcessor.update(deltaObjectId, measurementId, startTime, endTime, type, v);
		} catch (OverflowProcessorException e) {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		}
		fileNodeProcessor.changeTypeToChanged(startTime, endTime);
		addNameSpaceToOverflowList(fileNodeProcessor.getNameSpacePath());
		// overflowProcessor.writeUnlock();
		fileNodeProcessor.writeUnlock();
		LOGGER.debug("Unlock the FileNodeProcessor: {}", fileNodeProcessor.getNameSpacePath());
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type)
			throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true);
			} while (fileNodeProcessor == null);
		} catch (LRUManagerException e) {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		}
		long lastUpdateTime = fileNodeProcessor.getLastUpdateTime();
		LOGGER.debug("Get the FileNodeProcessor: {}, the last update time is: {}, the delete time is from 0 to {}",
				fileNodeProcessor.getNameSpacePath(), lastUpdateTime, timestamp);
		// no bufferwrite data, the delete operation is invalid
		if (lastUpdateTime == -1) {
			LOGGER.warn("The last update time is -1, delete overflow is invalid");
			fileNodeProcessor.writeUnlock();
			LOGGER.debug("Unlock the FileNodeProcessor: {}", fileNodeProcessor.getNameSpacePath());
		} else {
			if (timestamp > lastUpdateTime) {
				timestamp = lastUpdateTime + 1;
			}
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			String namespacePath = fileNodeProcessor.getNameSpacePath();
			OverflowProcessor overflowProcessor;
			try {
				overflowProcessor = fileNodeProcessor.getOverflowProcessor(namespacePath, parameters);
			} catch (FileNodeProcessorException e) {
				LOGGER.error("Get the overflow processor failed, the nameSpacePath is {}, delete time is {}",
						namespacePath, timestamp);
				e.printStackTrace();
				throw new FileNodeManagerException(e);

			}
			// overflowProcessor.writeLock();
			try {
				overflowProcessor.delete(deltaObjectId, measurementId, timestamp, type);
			} catch (OverflowProcessorException e) {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				e.printStackTrace();
				throw new FileNodeManagerException(e);
			}
			// overflowProcessor.writeUnlock();
			fileNodeProcessor.changeTypeToChangedForDelete(timestamp);
			addNameSpaceToOverflowList(fileNodeProcessor.getNameSpacePath());
			fileNodeProcessor.writeUnlock();
			LOGGER.debug("Unlock the FileNodeProcessor: {}", fileNodeProcessor.getNameSpacePath());
		}
	}

	public int beginQuery(String deltaObjectId) throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true);
			} while (fileNodeProcessor == null);
			LOGGER.debug("Get the FileNodeProcessor: {}, begin query.", fileNodeProcessor.getNameSpacePath());
			int token = fileNodeProcessor.addMultiPassLock();
			return token;
		} catch (LRUManagerException e) {
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		} finally {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
		}
	}

	public QueryStructure query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
			throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, false);
			} while (fileNodeProcessor == null);
			LOGGER.debug("Get the FileNodeProcessor: {}, query.", fileNodeProcessor.getNameSpacePath());

			QueryStructure queryStructure = null;
			// query operation must have overflow processor
			if (!fileNodeProcessor.hasOverflowProcessor()) {
				Map<String, Object> parameters = new HashMap<>();
				parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
				parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
				fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getNameSpacePath(), parameters);
			}
			queryStructure = fileNodeProcessor.query(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter);
			// return query structure
			return queryStructure;
		} catch (LRUManagerException e) {
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		} finally {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.readUnlock();
			}
		}
	}

	public void endQuery(String deltaObjectId, int token) throws FileNodeManagerException {
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true);
			} while (fileNodeProcessor == null);
			LOGGER.debug("Get the FileNodeProcessor: {}, end query.", fileNodeProcessor.getNameSpacePath());
			fileNodeProcessor.removeMultiPassLock(token);
		} catch (LRUManagerException e) {
			e.printStackTrace();
			throw new FileNodeManagerException(e);
		} finally {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
		}
	}

	public synchronized boolean mergeAll() throws FileNodeManagerException {
		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.MERGE;
			// flush information first
			Set<String> allChangedFileNodes = getOverflowNameSpaceListAndClear();
			// set a flag to notify is merging status
			ExecutorService singleMergingService = Executors.newSingleThreadExecutor();
			MergeAllProcessors mergeAllProcessors = new MergeAllProcessors(allChangedFileNodes);
			singleMergingService.execute(mergeAllProcessors);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Try to close All
	 * 
	 * @return true - close successfully false - can't close because of merge
	 * @throws FileNodeManagerException
	 */
	public synchronized boolean closeAll() throws FileNodeManagerException {
		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			try {
				return super.close();
			} catch (LRUManagerException e) {
				e.printStackTrace();
				throw new FileNodeManagerException(e);
			} finally {
				fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			}
		} else {
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
		try {
			serializeUtil.serialize(backUpOverflowNameSpaceSet, fileNodeManagerStoreFile);
		} catch (IOException e) {
			LOGGER.error(
					"Serialize the overflow nameSpacePath Set error, and delete the overflow restore file, the file path is {}",
					fileNodeManagerStoreFile);
			File restoreFile = new File(fileNodeManagerStoreFile);
			restoreFile.delete();
			e.printStackTrace();
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
		try {
			overflowSet = serializeUtil.deserialize(fileNodeManagerStoreFile).orElse(new HashSet<>());
		} catch (IOException e) {
			LOGGER.error(
					"Deserizlize the overflow nameSpaceSet error, and delete the filenode manager restore file, the restore file path is {}",
					fileNodeManagerStoreFile);
			// delete restore file
			File restoreFile = new File(fileNodeManagerStoreFile);
			if (restoreFile.exists()) {
				restoreFile.delete();
			}
			e.printStackTrace();
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
			ExecutorService mergeExecutorPool = Executors.newFixedThreadPool(TsFileDBConf.mergeConcurrentThreadNum);
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
					fileNodeProcessor = getProcessorByLRU(fileNodeNamespacePath, true);
				} while (fileNodeProcessor == null);
				LOGGER.info("Get the FileNodeProcessor: {}, merge.", fileNodeProcessor.getNameSpacePath());

				// if bufferwrite and overflow exist
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
				fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getNameSpacePath(), parameters);
				// must close the overflow processor
				while (!fileNodeProcessor.getOverflowProcessor().canBeClosed()) {

				}
				fileNodeProcessor.getOverflowProcessor().close();
			} catch (LRUManagerException | FileNodeProcessorException | BufferWriteProcessorException
					| OverflowProcessorException e) {
				LOGGER.error("Merge the filenode processor error, the nameSpacePath is {}", e.getMessage());
				e.printStackTrace();
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new ErrorDebugException(e);
			}
			try {
				fileNodeProcessor.merge();
			} catch (FileNodeProcessorException e) {
				e.printStackTrace();
				throw new ErrorDebugException(e);
			}
		}
	}

	private enum FileNodeManagerStatus {
		NONE, MERGE, CLOSE;
	}
}
