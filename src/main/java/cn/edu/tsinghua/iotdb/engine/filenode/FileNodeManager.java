package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.lru.LRUManager;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.ErrorDebugException;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.LRUManagerException;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class FileNodeManager extends LRUManager<FileNodeProcessor> {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeManager.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final String restoreFileName = "fileNodeManager.restore";
	private final String fileNodeManagerStoreFile;

	private Set<String> overflowNameSpaceSet;
	private Set<String> backUpOverflowNameSpaceSet;

	private static class FileNodeManagerHolder {
		private static final FileNodeManager INSTANCE = new FileNodeManager(TsFileDBConf.maxOpenFolder,
				MManager.getInstance(), TsFileDBConf.fileNodeDir);
	}

	private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;

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
		return FileNodeManagerHolder.INSTANCE;
	}

	private FileNodeManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
		super(maxLRUNumber, mManager, normalDataDir);
		TsFileConf.duplicateIncompletedPage = true;
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
			LOGGER.error(String.format("Can't construct the FileNodeProcessor, the nameSpacePath is {}", namespacePath),
					e);
			throw new FileNodeManagerException(e);
		}
	}

	@Override
	protected void initProcessor(FileNodeProcessor processor, String namespacePath, Map<String, Object> args)
			throws LRUManagerException {
	}

	public void managerRecovery() {

		try {
			List<String> nsPaths = mManager.getAllFileNames();
			for (String nsPath : nsPaths) {
				FileNodeProcessor fileNodeProcessor = null;
				fileNodeProcessor = getProcessorByLRU(nsPath, true);
				if (fileNodeProcessor.shouldRecovery()) {
					LOGGER.info("Recovery the filenode processor, the nameSpacePath is {}, the status is {}", nsPath,
							fileNodeProcessor.getFileNodeProcessorStatus());
					fileNodeProcessor.fileNodeRecovery();
				} else {
					fileNodeProcessor.writeUnlock();
				}
			}
		} catch (PathErrorException | LRUManagerException | FileNodeProcessorException e) {
			LOGGER.error("Restore all FileNode failed, the reason is {}", e.getMessage());
		}
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
		long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
		LOGGER.debug("Get the FileNodeProcessor: {}, the last update time is: {}, the record time is: {}",
				fileNodeProcessor.getNameSpacePath(), lastUpdateTime, timestamp);
		LOGGER.debug("Insert record is {}", tsRecord);
		int insertType = 0;
		String nameSpacePath = fileNodeProcessor.getNameSpacePath();
		if (timestamp <= lastUpdateTime) {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			OverflowProcessor overflowProcessor;
			try {
				overflowProcessor = fileNodeProcessor.getOverflowProcessor(nameSpacePath, parameters);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(
						String.format("Get the overflow processor failed, the nameSpacePath is {}, insert time is {}",
								nameSpacePath, timestamp),
						e);
				throw new FileNodeManagerException(e);
			}

			// for WAL
			try {
				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					if (!WriteLogManager.isRecovering) {
						WriteLogManager.getInstance().write(nameSpacePath, tsRecord, WriteLogManager.OVERFLOW);
					}
				}
			} catch (IOException | PathErrorException e) {
				LOGGER.error("Error in write WAL", e);
				throw new FileNodeManagerException(e);
			}

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
			fileNodeProcessor.changeTypeToChanged(deltaObjectId, timestamp);
			addNameSpaceToOverflowList(nameSpacePath);
			// overflowProcessor.writeUnlock();
			insertType = 1;
		} else {
			BufferWriteProcessor bufferWriteProcessor;
			try {
				bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(nameSpacePath, timestamp);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(String.format(
						"Get the bufferwrite processor failed, the nameSpacePath is {}, insert time is {}",
						nameSpacePath, timestamp), e);
				throw new FileNodeManagerException(e);
			}
			// Add the new interval file to newfilelist
			if (bufferWriteProcessor.isNewProcessor()) {
				bufferWriteProcessor.setNewProcessor(false);
				String fileAbsolutePath = bufferWriteProcessor.getFileAbsolutePath();
				try {
					fileNodeProcessor.addIntervalFileNode(timestamp, fileAbsolutePath);
				} catch (Exception e) {
					fileNodeProcessor.writeUnlock();
					throw new FileNodeManagerException(e);
				}
			}

			// For WAL
			try {
				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					if (!WriteLogManager.isRecovering) {
						WriteLogManager.getInstance().write(nameSpacePath, tsRecord, WriteLogManager.BUFFERWRITER);
					}
				}
			} catch (IOException | PathErrorException e) {
				LOGGER.error("Error in write WAL.", e);
				throw new FileNodeManagerException(e);
			}

			// Write data
			try {
				bufferWriteProcessor.write(tsRecord);
			} catch (BufferWriteProcessorException e) {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new FileNodeManagerException(e);
			}
			fileNodeProcessor.setIntervalFileNodeStartTime(deltaObjectId, timestamp);
			fileNodeProcessor.setLastUpdateTime(deltaObjectId, timestamp);
			// bufferWriteProcessor.writeUnlock();
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
			throw new FileNodeManagerException(e);
		}

		// for WAL
		try {
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				if (!WriteLogManager.isRecovering) {
					WriteLogManager.getInstance().write(fileNodeProcessor.getNameSpacePath(),
							new UpdatePlan(startTime, endTime, v, new Path(deltaObjectId + "." + measurementId)));
				}
			}
		} catch (IOException | PathErrorException e) {
			LOGGER.error("Error in write WAL: {}", e.getMessage());
			throw new FileNodeManagerException(e);
		}

		long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
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
			LOGGER.error(
					String.format("Get the overflow processor failed, the nameSpacePath is {}, update time is {} to {}",
							namespacePath, startTime, endTime),
					e);
			throw new FileNodeManagerException(e);
		}
		// overflowProcessor.writeLock();
		try {
			overflowProcessor.update(deltaObjectId, measurementId, startTime, endTime, type, v);
		} catch (OverflowProcessorException e) {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
			throw new FileNodeManagerException(e);
		}
		fileNodeProcessor.changeTypeToChanged(deltaObjectId, startTime, endTime);
		addNameSpaceToOverflowList(namespacePath);
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
			throw new FileNodeManagerException(e);
		}

		// for WAL
		try {
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				if (!WriteLogManager.isRecovering) {
					WriteLogManager.getInstance().write(fileNodeProcessor.getNameSpacePath(),
							new DeletePlan(timestamp, new Path(deltaObjectId + "." + measurementId)));
				}
			}
		} catch (IOException | PathErrorException e) {
			LOGGER.error("Error in write WAL: {}", e.getMessage());
			throw new FileNodeManagerException(e);
		}

		long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deltaObjectId);
		LOGGER.debug("Get the FileNodeProcessor: {}, the last update time is: {}, the delete time is from 0 to {}",
				fileNodeProcessor.getNameSpacePath(), lastUpdateTime, timestamp);
		// no bufferwrite data, the delete operation is invalid
		if (lastUpdateTime == -1) {
			LOGGER.warn("The last update time is -1, delete overflow is invalid");
			fileNodeProcessor.writeUnlock();
			LOGGER.debug("Unlock the FileNodeProcessor: {}", fileNodeProcessor.getNameSpacePath());
		} else {
			if (timestamp > lastUpdateTime) {
				timestamp = lastUpdateTime;
			}
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
			String namespacePath = fileNodeProcessor.getNameSpacePath();
			OverflowProcessor overflowProcessor;
			try {
				overflowProcessor = fileNodeProcessor.getOverflowProcessor(namespacePath, parameters);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(
						String.format("Get the overflow processor failed, the nameSpacePath is {}, delete time is {}",
								namespacePath, timestamp),
						e);
				throw new FileNodeManagerException(e);
			}
			// overflowProcessor.writeLock();
			try {
				overflowProcessor.delete(deltaObjectId, measurementId, timestamp, type);
			} catch (OverflowProcessorException e) {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new FileNodeManagerException(e);
			}
			// overflowProcessor.writeUnlock();
			fileNodeProcessor.changeTypeToChangedForDelete(deltaObjectId, timestamp);
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
			throw new FileNodeManagerException(e);
		} catch (FileNodeProcessorException e) {
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

	public void clearOneFileNode(String namespacePath) throws FileNodeManagerException {

		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorByLRU(namespacePath, true);
			} while (fileNodeProcessor == null);
			fileNodeProcessor.clearFileNode();
			overflowNameSpaceSet.remove(namespacePath);
			LOGGER.debug("Get the FileNodeProcessor: {}, begin query.", fileNodeProcessor.getNameSpacePath());
		} catch (LRUManagerException e) {
			throw new FileNodeManagerException(e);
		} finally {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
		}
	}

	public synchronized boolean deleteOneFileNode(String namespacePath) throws FileNodeManagerException {

		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			// check the filenode has bufferwrite or not
			FileNodeProcessor fileNodeProcessor = null;
			try {
				try {
					do {
						fileNodeProcessor = getProcessorByLRU(namespacePath, true);
					} while (fileNodeProcessor == null);
				} catch (LRUManagerException e) {
					throw new FileNodeManagerException(e);
				}
				try {
					super.closeOneProcessor(namespacePath);
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
				} catch (LRUManagerException | IOException e) {
					throw new FileNodeManagerException(e);
				}
			} finally {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
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

	// TODO: should synchronized
	public synchronized void addTimeSeries(Path path, String dataType, String encoding, String[] encodingArgs)
			throws FileNodeManagerException {
		// TODO: optimize and do't get the filenode processor instance
		FileNodeProcessor fileNodeProcessor = null;
		try {
			do {
				fileNodeProcessor = getProcessorWithDeltaObjectIdByLRU(path.getFullPath(), true);
			} while (fileNodeProcessor == null);
			if (fileNodeProcessor.hasBufferwriteProcessor()) {
				BufferWriteProcessor bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor();
				bufferWriteProcessor.addTimeSeries(path.getMeasurementToString(), dataType, encoding, encodingArgs);
			} else {
				return;
			}
		} catch (LRUManagerException | IOException | FileNodeProcessorException e) {
			throw new FileNodeManagerException(e);
		} finally {
			if (fileNodeProcessor != null) {
				fileNodeProcessor.writeUnlock();
			}
		}
	}

	public synchronized boolean closeOneFileNode(String namespacePath) throws FileNodeManagerException {
		if (fileNodeManagerStatus == FileNodeManagerStatus.NONE) {
			fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
			// check the filenode has bufferwrite or not
			FileNodeProcessor fileNodeProcessor = null;
			try {
				try {
					do {
						fileNodeProcessor = getProcessorByLRU(namespacePath, true);
					} while (fileNodeProcessor == null);
					if (!fileNodeProcessor.hasBufferwriteProcessor()) {
						return true;
					}
				} catch (LRUManagerException e) {
					throw new FileNodeManagerException(e);
				}
				// close bufferwrite data
				try {
					return super.closeOneProcessor(namespacePath);
				} catch (LRUManagerException e) {
					throw new FileNodeManagerException(e);
				}
			} finally {
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				fileNodeManagerStatus = FileNodeManagerStatus.NONE;
			}
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
				return super.closeAll();
			} catch (LRUManagerException e) {
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
			ExecutorService mergeExecutorPool = Executors.newFixedThreadPool(TsFileDBConf.mergeConcurrentThreads);
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
				LOGGER.error("Merge the filenode processor error", e);
				if (fileNodeProcessor != null) {
					fileNodeProcessor.writeUnlock();
				}
				throw new ErrorDebugException(e);
			}
			try {
				fileNodeProcessor.merge();
			} catch (FileNodeProcessorException e) {
				throw new ErrorDebugException(e);
			}
		}
	}

	private enum FileNodeManagerStatus {
		NONE, MERGE, CLOSE;
	}
}
