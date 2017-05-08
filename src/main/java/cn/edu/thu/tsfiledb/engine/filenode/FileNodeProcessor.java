package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.ProcessorRuntimException;
import cn.edu.thu.tsfiledb.engine.lru.LRUProcessor;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.exception.ProcessorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;

public class FileNodeProcessor extends LRUProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();
	private static final String LOCK_SIGNAL = "lock___signal";

	private long lastUpdateTime = -1;
	private IntervalFileNode emptyIntervalFileNode;
	private List<IntervalFileNode> newFileNodes;
	private FileNodeProcessorState isMerging;
	// this is used when work ->merge operation
	private int numOfMergeFile = 0;
	private FileNodeProcessorStore fileNodeProcessorStore = null;

	private static final String restoreFile = ".restore";
	private String fileNodeRestoreFilePath = null;

	private BufferWriteProcessor bufferWriteProcessor = null;
	private OverflowProcessor overflowProcessor = null;

	private Set<Integer> oldMultiPassTokenSet = new HashSet<>();
	private Set<Integer> newMultiPassTokenSet = new HashSet<>();
	private ReadWriteLock oldMultiPassLock = new ReentrantReadWriteLock(false);
	private ReadWriteLock newMultiPassLock = new ReentrantReadWriteLock(false);

	private Action flushFileNodeProcessorAction = new Action() {

		@Override
		public void act() throws Exception {
			synchronized (fileNodeProcessorStore) {
				WriteStoreToDisk(fileNodeProcessorStore);
			}
		}
	};

	private Action bufferwriteFlushAction = new Action() {

		@Override
		public void act() throws Exception {
			// update the lastUpdateTime Notice: Thread safe
			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setLastUpdateTime(lastUpdateTime);
			}
		}
	};

	private Action bufferwriteCloseAction = new Action() {

		@Override
		public void act() throws Exception {

			// update the lastUpdatetime, newIntervalList and Notice: thread
			// safe
			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setLastUpdateTime(lastUpdateTime);
				addLastTimeToIntervalFile();
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			}
		}
	};

	private void addLastTimeToIntervalFile() {
		if (lastUpdateTime == -1) {
			LOGGER.error("The lastUpdateTime is -1 when close the bufferwrite file");
			throw new ProcessorRuntimException("The lastUpdateTime is -1 when close the bufferwrite file");
		}
		if (!newFileNodes.isEmpty()) {
			newFileNodes.get(newFileNodes.size() - 1).setEndTime(lastUpdateTime);
		} else {
			throw new ProcessorRuntimException("The intervalFile list is empty when close bufferwrite file");
		}
	}

	public void addIntervalFileNode(long startTime, String fileName) {
		IntervalFileNode intervalFileNode = new IntervalFileNode(startTime, OverflowChangeType.NO_CHANGE, fileName,
				null);
		newFileNodes.add(intervalFileNode);
	}

	private Action overflowFlushAction = new Action() {

		@Override
		public void act() throws Exception {

			// update the new IntervalFileNode List and emptyIntervalFile.
			// Notice: thread safe
			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			}
		}
	};

	public FileNodeProcessor(String fileNodeDirPath, String nameSpacePath, Map<String, Object> parameters)
			throws FileNodeProcessorException {
		super(nameSpacePath);
		String dataDirPath = fileNodeDirPath + nameSpacePath;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.warn("The filenode processor data dir doesn't exists, and mkdir the dir {}", dataDirPath);
		}
		fileNodeRestoreFilePath = new File(dataDir, nameSpacePath + restoreFile).getAbsolutePath();
		try {
			fileNodeProcessorStore = ReadStoreToDisk();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			LOGGER.error("Restore the FileNodeProcessor information error, the nameSpacePath is {}", nameSpacePath);
			throw new FileNodeProcessorException(
					"Restore the FileNodeProcessor information error, the nameSpacePath is " + nameSpacePath);
		}
		lastUpdateTime = fileNodeProcessorStore.getLastUpdateTime();
		emptyIntervalFileNode = fileNodeProcessorStore.getEmptyIntervalFileNode();
		newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		isMerging = fileNodeProcessorStore.getFileNodeProcessorState();
		numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
		// status is not NONE, or the last intervalFile is not closed
		if (isMerging != FileNodeProcessorState.NONE
				|| (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed())) {
			FileNodeRecovery(parameters);
		}
	}

	private void FileNodeRecovery(Map<String, Object> parameters) throws FileNodeProcessorException {

		// restore bufferwrite
		if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
			// this bufferwrite file is not close by normal operation
			String damagedFileName = newFileNodes.get(newFileNodes.size() - 1).filePath;
			// all information to recovery the damaged file.
			// contains file path, action parameters and nameSpacePath
			parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
			parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			try {
				bufferWriteProcessor = new BufferWriteProcessor(nameSpacePath, damagedFileName, parameters);
			} catch (BufferWriteProcessorException e) {
				LOGGER.error("Restore the bufferwrite failed, the reason is {}", e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException("Restore the bufferwrte failed, the reason is " + e.getMessage());
			}
		}
		// restore the overflow processor
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
		try {
			overflowProcessor = new OverflowProcessor(nameSpacePath, parameters);
		} catch (OverflowProcessorException e) {
			LOGGER.error("Restore the overflow failed, the reason is {}", e.getMessage());
			e.printStackTrace();
			throw new FileNodeProcessorException("Restore the overflow failed, the reason is " + e.getMessage());
		}

		if (isMerging == FileNodeProcessorState.MERGING_WRITE) {
			// lock the filenode processor
			writeLock();
			// re-merge all file
			// if bufferwrite processor is not null, and close
			if (bufferWriteProcessor != null) {
				try {
					bufferWriteProcessor.close();
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					throw new FileNodeProcessorException(
							"Close the bufferwrite processor failed, the reason is " + e.getMessage());
				}
				bufferWriteProcessor = null;
			}
			// Get the overflow processor, and close
			// overflowProcessor = getOverflowProcessor(nameSpacePath,
			// parameters);
			try {
				overflowProcessor.close();
			} catch (OverflowProcessorException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(
						"Close the overflow processor failed, the reason is " + e.getMessage());
			}
			merge();
		}
		if (isMerging == FileNodeProcessorState.WAITING) {
			switchWaitingToWorkingv2(newFileNodes);
		}
	}

	public BufferWriteProcessor getBufferWriteProcessor(String namespacePath, long insertTime)
			throws FileNodeProcessorException {
		if (bufferWriteProcessor == null) {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
			parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			// construct processor or restore
			try {
				bufferWriteProcessor = new BufferWriteProcessor(namespacePath,
						insertTime + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis(),
						parameters);
			} catch (BufferWriteProcessorException e) {
				e.printStackTrace();
				LOGGER.error("Get the bufferwrite processor instance failed, the nameSpacePath is {}, reason is {}",
						namespacePath, e.getMessage());
				throw new FileNodeProcessorException(String.format(
						"Get the bufferwrite processor instance failed, the nameSpacePath is %s, reason is %s",
						namespacePath, e.getMessage()));
			}
			;
		}
		return bufferWriteProcessor;
	}

	public BufferWriteProcessor getBufferWriteProcessor() throws FileNodeProcessorException {
		if (bufferWriteProcessor == null) {
			LOGGER.error("The bufferwrite processor is null when get the bufferwriteProcessor");
			throw new FileNodeProcessorException("The bufferwrite processor is null");
		}
		return bufferWriteProcessor;
	}

	public OverflowProcessor getOverflowProcessor(String namespacePath, Map<String, Object> parameters)
			throws FileNodeProcessorException {
		if (overflowProcessor == null) {
			// construct processor or restore
			// add the parameters
			// parameters contains the OVERFLOW_FLUSH_MANAGER_ACTION
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			try {
				overflowProcessor = new OverflowProcessor(namespacePath, parameters);
			} catch (OverflowProcessorException e) {
				LOGGER.error("Get the overflow processor instance failed, the nameSpacePath is {}, reason is {}",
						namespacePath, e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException(String.format(
						"Get the overflow processor instance failed, the nameSpacePath is %s, reason is %s",
						namespacePath, e.getMessage()));
			}
		}
		return overflowProcessor;
	}

	public OverflowProcessor getOverflowProcessor() throws FileNodeProcessorException {
		if (overflowProcessor == null) {
			LOGGER.error("The overflow processor is null when get the overflowProcessor");
			throw new FileNodeProcessorException("The overflow processor is null");
		}
		return overflowProcessor;
	}

	public boolean hasOverflowProcessor() {

		return overflowProcessor != null;
	}

	public void setBufferwriteProcessroToClosed() {

		bufferWriteProcessor = null;
	}

	public boolean hasBufferwriteProcessor() {

		return bufferWriteProcessor != null;
	}

	public void setLastUpdateTime(long timestamp) {

		this.lastUpdateTime = timestamp;
	}

	public long getLastUpdateTime() {

		return lastUpdateTime;
	}

	/**
	 * For insert overflow
	 * 
	 * @param timestamp
	 */
	public void changeTypeToChanged(long timestamp) {
		if (newFileNodes.isEmpty()) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.endTime = lastUpdateTime;
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
			return;
		}

		int index = searchIndexNodeByTimestamp(timestamp);
		newFileNodes.get(index).changeTypeToChanged(isMerging);
	}

	/**
	 * For update overflow
	 * 
	 * @param startTime
	 * @param endTime
	 */
	public void changeTypeToChanged(long startTime, long endTime) {
		if (newFileNodes.isEmpty()) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.endTime = lastUpdateTime;
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
			return;
		}

		int left = searchIndexNodeByTimestamp(startTime);
		int right = searchIndexNodeByTimestamp(endTime);
		for (int i = left; i <= right; i++) {
			newFileNodes.get(i).changeTypeToChanged(isMerging);
		}
	}

	/**
	 * For delete overflow
	 * 
	 * @param timestamp
	 */
	public void changeTypeToChangedForDelete(long timestamp) {
		if (newFileNodes.isEmpty()) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.endTime = lastUpdateTime;
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
			return;
		}

		int index = searchIndexNodeByTimestamp(timestamp);
		for (int i = 0; i <= index; i++) {
			newFileNodes.get(i).changeTypeToChanged(isMerging);
		}
	}

	/**
	 * Search the index of the interval by the timestamp
	 * 
	 * @param timestamp
	 * @return index of interval
	 */
	private int searchIndexNodeByTimestamp(long timestamp) {

		int index = 1;
		while (index < newFileNodes.size()) {
			if (timestamp < newFileNodes.get(index).startTime) {
				break;
			} else {
				index++;
			}
		}
		return index - 1;
	}

	// Token for query which used to
	private int multiPassLockToken = 0;

	public int addMultiPassLock() {
		LOGGER.debug("{} addMultiPassLock: read lock newMultiPassLock", LOCK_SIGNAL);
		newMultiPassLock.readLock().lock();
		while (newMultiPassTokenSet.contains(multiPassLockToken)) {
			multiPassLockToken++;
		}
		newMultiPassTokenSet.add(multiPassLockToken);
		LOGGER.debug("{} add multi token:{}, nsPath:{}, new set:{}, lock:{}", LOCK_SIGNAL, multiPassLockToken,
				nameSpacePath, newMultiPassTokenSet, newMultiPassLock);
		return multiPassLockToken;
	}

	public boolean removeMultiPassLock(int token) {
		if (newMultiPassTokenSet.contains(token)) {
			newMultiPassLock.readLock().unlock();
			newMultiPassTokenSet.remove(token);
			LOGGER.debug("{} remove multi token:{}, nspath:{}, new set:{}, lock:{}", LOCK_SIGNAL, token, nameSpacePath,
					newMultiPassTokenSet, newMultiPassLock);
			return true;
		} else if (oldMultiPassTokenSet != null && oldMultiPassTokenSet.contains(token)) {
			// remove token first， then unlock
			oldMultiPassLock.readLock().unlock();
			oldMultiPassTokenSet.remove(token);
			LOGGER.debug("{} remove multi token:{}, old set:{}, lock:{}", LOCK_SIGNAL, token, oldMultiPassTokenSet,
					oldMultiPassLock);
			return true;
		} else {
			LOGGER.error("remove token error:{},new set:{}, old set:{}", token, newMultiPassTokenSet,
					oldMultiPassTokenSet);
			// should add throw exception
			return false;
		}
	}

	public QueryStructure query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
			throws FileNodeProcessorException {
		// query overflow data
		QueryStructure queryStructure = null;
		List<Object> overflowData = null;

		if (overflowProcessor == null) {
			LOGGER.error("The overflow processor is null, when query the deltaObjectId is {}, measurementId is {}",
					deltaObjectId, measurementId);
			throw new FileNodeProcessorException(String.format(
					"The overflow processor is null, when query the deltaObjectId is %s, measurementId is %s",
					deltaObjectId, measurementId));
		}
		// query overflow data from overflow processor
		overflowData = overflowProcessor.query(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter);
		// query bufferwrite data in memory and disk
		Pair<DynamicOneColumnData, List<RowGroupMetaData>> bufferwriteDataInMemory = new Pair<DynamicOneColumnData, List<RowGroupMetaData>>(
				null, null);
		// if no bufferwrite processor, there are not bufferwrite data in memory
		if (bufferWriteProcessor != null) {
			// get data from bufferwrite memory
			bufferwriteDataInMemory = bufferWriteProcessor.getIndexAndRowGroupList(deltaObjectId, measurementId);
		}
		// query bufferwrite data in files
		List<IntervalFileNode> bufferwriteDataInFiles = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			// add the same intervalFileNode, but not the same reference
			bufferwriteDataInFiles.add(intervalFileNode.backUp());
		}
		queryStructure = new QueryStructure(bufferwriteDataInMemory.left, bufferwriteDataInMemory.right,
				bufferwriteDataInFiles, overflowData);
		return queryStructure;
	}

	public void merge() throws FileNodeProcessorException {

		LOGGER.debug("Merge: the nameSpacePath {} is begining to merge. {}", nameSpacePath, LOCK_SIGNAL);
		//
		// change status from work to merge
		//
		isMerging = FileNodeProcessorState.MERGING_WRITE;
		if (emptyIntervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
			if (!newFileNodes.isEmpty()) {
				newFileNodes.get(0).overflowChangeType = OverflowChangeType.CHANGED;
				emptyIntervalFileNode.overflowChangeType = OverflowChangeType.NO_CHANGE;
			} else {
				emptyIntervalFileNode.overflowChangeType = OverflowChangeType.CHANGED;
			}
		}
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			if (intervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
				intervalFileNode.overflowChangeType = OverflowChangeType.CHANGED;
			}
		}
		// add numOfMergeFile to control the number of the merge file
		List<IntervalFileNode> backupIntervalFiles = switchFileNodeToMergev2();
		try {
			//
			// change the overflow work to merge
			//
			overflowProcessor.switchWorkingToMerge();
		} catch (ProcessorException e) {
			LOGGER.error("Merge: Can't change overflow processor status from work to merge");
			e.printStackTrace();
			throw new FileNodeProcessorException(e);
		}
		synchronized (fileNodeProcessorStore) {
			fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
			fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
			// flush this filenode information
			try {
				WriteStoreToDisk(fileNodeProcessorStore);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(
						"Merge: write filenode information to revocery file failed, the nameSpacePath is {}, the reason is {}",
						nameSpacePath, e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException(
						"Merge: write filenode information to revocery file failed, the nameSpacePath is "
								+ nameSpacePath);
			}
		}

		// unlock this filenode
		LOGGER.debug("Merge: the nameSpacePath {}, status from work to merge. {}", nameSpacePath, LOCK_SIGNAL);
		writeUnlock();
		LOGGER.debug("Merge: the nameSpacePath {}, unlock the filenode write lock. {}", nameSpacePath, LOCK_SIGNAL);

		// query buffer data and overflow data, and merge them
		List<Path> pathList = new ArrayList<>();
		try {
			ArrayList<String> pathStrings = mManager.getPaths(nameSpacePath + FileNodeConstants.PATH_SEPARATOR + "*");
			for (String string : pathStrings) {
				pathList.add(new Path(string));
			}
		} catch (PathErrorException e) {
			LOGGER.error("Can't get all the paths from MManager, the nameSpacePath is {}", nameSpacePath);
			e.printStackTrace();
			throw new FileNodeProcessorException(e);
		}
		for (IntervalFileNode backupIntervalFile : backupIntervalFiles) {
			if (backupIntervalFile.overflowChangeType == OverflowChangeType.CHANGED) {
				// query data and merge
				try {
					queryAndWriteDataForMerge(pathList, backupIntervalFile);
				} catch (IOException | WriteProcessException e) {
					LOGGER.error("Merge: query and write data error, the reason is {}", e.getMessage());
					e.printStackTrace();
					throw new FileNodeProcessorException(
							"Merge: query and write data error, the reason is " + e.getMessage());
				}
			} else if (backupIntervalFile.overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
				LOGGER.error("The overflowChangeType of backupIntervalFile must not be {}",
						OverflowChangeType.MERGING_CHANGE);
				// handle this error, throw one runtime exception
				throw new FileNodeProcessorException("The overflowChangeType of backupIntervalFile must not be "
						+ OverflowChangeType.MERGING_CHANGE);
			} else {
				LOGGER.info("The overflowChangedType of backup IntervalFile is {}, start time is {}, end time is {}.",
						backupIntervalFile.overflowChangeType, backupIntervalFile.startTime,
						backupIntervalFile.endTime);
			}
		}
		//
		// change status from merge to wait
		//
		switchMergeToWaitingv2(backupIntervalFiles);
		//
		// change status from wait to work
		//
		switchWaitingToWorkingv2(backupIntervalFiles);
	}

	private List<IntervalFileNode> switchFileNodeToMergev2() {
		List<IntervalFileNode> result = new ArrayList<>();
		if (newFileNodes.isEmpty()) {
			if (emptyIntervalFileNode.overflowChangeType == OverflowChangeType.NO_CHANGE) {
				throw new ProcessorRuntimException(String.format(
						"The newFileNodes is empty, but the emptyIntervalFileNode OverflowChangeType is %s",
						emptyIntervalFileNode.overflowChangeType));
			}

			IntervalFileNode intervalFileNode = new IntervalFileNode(0, emptyIntervalFileNode.endTime,
					OverflowChangeType.CHANGED, null, null);
			result.add(intervalFileNode);
		} else if (newFileNodes.size() == 1) {
			IntervalFileNode temp = newFileNodes.get(0);
			IntervalFileNode intervalFileNode = new IntervalFileNode(0, temp.endTime, temp.overflowChangeType,
					temp.filePath, null);
			result.add(intervalFileNode);
		} else {
			// add first
			IntervalFileNode temp = newFileNodes.get(0);
			IntervalFileNode intervalFileNode = new IntervalFileNode(0, newFileNodes.get(1).startTime - 1,
					temp.overflowChangeType, temp.filePath, null);
			result.add(intervalFileNode);
			// second to the last -1
			for (int i = 1; i < newFileNodes.size() - 1; i++) {
				temp = newFileNodes.get(i);
				intervalFileNode = new IntervalFileNode(temp.startTime, newFileNodes.get(i + 1).startTime - 1,
						temp.overflowChangeType, temp.filePath, null);
				result.add(intervalFileNode);
			}
			// last interval
			temp = newFileNodes.get(newFileNodes.size() - 1);
			intervalFileNode = new IntervalFileNode(temp.startTime, temp.endTime, temp.overflowChangeType,
					temp.filePath, null);
			result.add(intervalFileNode);
		}
		return result;
	}

	private void switchMergeToWaitingv2(List<IntervalFileNode> backupIntervalFiles) throws FileNodeProcessorException {
		LOGGER.debug("Merge: switch merge to wait, the backupIntervalFiles is {}", backupIntervalFiles);
		writeLock();
		try {
			oldMultiPassTokenSet = newMultiPassTokenSet;
			oldMultiPassLock = newMultiPassLock;
			newMultiPassTokenSet = new HashSet<>();
			newMultiPassLock = new ReentrantReadWriteLock(false);
			LOGGER.debug(
					"Merge: swith merge to wait, switch oldMultiPassTokenSet to newMultiPassTokenSet, switch oldMultiPassLock to newMultiPassLock");

			LOGGER.info("Merge switch merge to wait, the overflowChangeType of emptyIntervalFileNode is {}",
					emptyIntervalFileNode.overflowChangeType);
			if (emptyIntervalFileNode.overflowChangeType == OverflowChangeType.NO_CHANGE) {
				// backup from newFilenodes
				// no action
			} else {
				// backup just from emptyIntervalFileNode
				assert (backupIntervalFiles.size() == 1);
				newFileNodes.add(0, emptyIntervalFileNode);
			}

			List<IntervalFileNode> result = new ArrayList<>();
			int lenOfBackUpList = backupIntervalFiles.size();
			boolean putoff = false;
			for (int i = 0; i < lenOfBackUpList; i++) {
				IntervalFileNode backupIntervalFile = backupIntervalFiles.get(i);
				if (backupIntervalFile.startTime == -1) {
					if (putoff || newFileNodes.get(i).overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
						putoff = true;
					}
				} else {
					if (putoff || newFileNodes.get(i).overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
						backupIntervalFile.overflowChangeType = OverflowChangeType.CHANGED;
						putoff = false;
					}
					result.add(backupIntervalFile);
				}
			}

			for (int i = lenOfBackUpList; i < newFileNodes.size(); i++) {
				IntervalFileNode newFileNode = newFileNodes.get(i);
				if (putoff || newFileNode.overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
					newFileNode.overflowChangeType = OverflowChangeType.CHANGED;
					putoff = false;
				}
				result.add(newFileNode);
			}

			if (putoff) {
				emptyIntervalFileNode.endTime = lastUpdateTime;
				emptyIntervalFileNode.overflowChangeType = OverflowChangeType.CHANGED;
			}
			isMerging = FileNodeProcessorState.WAITING;
			newFileNodes = result;

			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
				fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
				try {
					WriteStoreToDisk(fileNodeProcessorStore);
				} catch (FileNodeProcessorException e) {
					LOGGER.error(
							"Merge: write filenode information to revocery file failed, the nameSpacePath is {}, the reason is {}",
							nameSpacePath, e.getMessage());
					e.printStackTrace();
					throw new FileNodeProcessorException(
							"Merge: write filenode information to revocery file failed, the nameSpacePath is "
									+ nameSpacePath);
				}
			}
		} finally {
			writeUnlock();
		}
	}

	private void switchWaitingToWorkingv2(List<IntervalFileNode> backupIntervalFiles)
			throws FileNodeProcessorException {

		LOGGER.debug("Merge: switch wait to work, newIntervalFileNodes is {}", newFileNodes);

		writeLock();
		try {
			oldMultiPassLock.writeLock().lock();
			try {
				// delete the all files which are in the newFileNodes
				// notice: the last restore file of the interval file

				String bufferwriteDirPath = TsFileConf.BufferWriteDir;
				if (bufferwriteDirPath.length() > 0
						&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
					bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
				}
				bufferwriteDirPath = bufferwriteDirPath + nameSpacePath;
				File bufferwriteDir = new File(bufferwriteDirPath);
				if (!bufferwriteDir.exists()) {
					bufferwriteDir.mkdirs();
				}

				Set<String> bufferFiles = new HashSet<>();
				for (IntervalFileNode bufferFileNode : newFileNodes) {
					String bufferFilePath = bufferFileNode.filePath;
					if (bufferFilePath != null) {
						File bufferFile = new File(bufferwriteDir, bufferFilePath);
						bufferFiles.add(bufferFile.getAbsolutePath());
					}
				}
				// add the restore file, if the last file is not closed
				if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
					String bufferFileRestorePath = newFileNodes.get(newFileNodes.size() - 1).filePath + ".restore";
					File bufferRestoreFile = new File(bufferwriteDir, bufferFileRestorePath);
					bufferFiles.add(bufferRestoreFile.getAbsolutePath());
				}

				for (File file : bufferwriteDir.listFiles()) {
					if (!bufferFiles.contains(file.getAbsolutePath())) {
						file.delete();
					}
				}
				// overflow switch
				overflowProcessor.switchMergeToWorking();
				// write status to file
				isMerging = FileNodeProcessorState.NONE;
				synchronized (fileNodeProcessorStore) {
					fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
					fileNodeProcessorStore.setNewFileNodes(newFileNodes);
					fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
					WriteStoreToDisk(fileNodeProcessorStore);
				}
			} catch (ProcessorException e) {
				LOGGER.error("Merge: switch wait to work failed. the reason is {}", e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			} finally {
				oldMultiPassTokenSet = null;
				oldMultiPassLock.writeLock().unlock();
				oldMultiPassLock = null;
			}
		} finally {
			writeUnlock();
		}
	}

	private void queryAndWriteDataForMerge(List<Path> pathList, IntervalFileNode backupIntervalFile)
			throws IOException, WriteProcessException {

		
		FilterExpression timeFilter = FilterUtilsForOverflow.construct(null, null, "0",
				"(>=" + backupIntervalFile.startTime + ")&" + "(<=" + backupIntervalFile.endTime + ")");

		LOGGER.info("Merge query and merge: namespace {}, time filter {}", nameSpacePath, timeFilter);

		QueryDataSet data = null;
		long startTime = -1;
		long endTime = -1;

		OverflowQueryEngine queryEngine = new OverflowQueryEngine();
		data = queryEngine.query(pathList, timeFilter, null, null, null, TsFileConf.defaultFetchSize);
		if (!data.hasNextRecord()) {
			// No record in this query
			LOGGER.warn("Merge query: namespace {}, time filter {}, no query data", nameSpacePath, timeFilter);
			// Set the IntervalFile
			backupIntervalFile.startTime = -1;
			backupIntervalFile.endTime = -1;
		} else {
			TSRecordWriter recordWriter;
			RowRecord firstRecord = data.getNextRecord();
			// get the outputPate and FileSchema
			String outputPath = constructOutputFilePath(nameSpacePath,
					firstRecord.timestamp + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis());
			FileSchema fileSchema;
			try {
				fileSchema = constructFileSchema(nameSpacePath);
			} catch (PathErrorException e) {
				LOGGER.error("Get the FileSchema error, the nameSpacePath is {}", nameSpacePath);
				throw new WriteProcessException("Get the FileSchema error, the nameSpacePath is " + nameSpacePath);
			}
			// construct TSRecordWriter
			TSRandomAccessFileWriter raf = new RandomAccessOutputStream(new File(outputPath));
			TSFileIOWriter tsfileIOWriter = new TSFileIOWriter(fileSchema, raf);
			WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
			recordWriter = new TSRecordWriter(TsFileConf, tsfileIOWriter, writeSupport, fileSchema);

			TSRecord filledRecord = removeNullTSRecord(firstRecord);
			recordWriter.write(filledRecord);
			startTime = endTime = firstRecord.getTime();

			while (data.hasNextRecord()) {
				RowRecord row = data.getNextRecord();
				filledRecord = removeNullTSRecord(row);
				endTime = filledRecord.time;
				try {
					recordWriter.write(filledRecord);
				} catch (WriteProcessException e) {
					LOGGER.error("Merge query: write one record error, the tsrecord is {}", filledRecord);
					e.printStackTrace();
				}
			}
			recordWriter.close();

			LOGGER.debug("Merge query: namespace {}, time filter {}, filepath {} successfully", nameSpacePath,
					timeFilter, outputPath);
			backupIntervalFile.startTime = startTime;
			backupIntervalFile.endTime = endTime;
			backupIntervalFile.filePath = outputPath;
			backupIntervalFile.errFilePath = null;
		}

	}

	private String constructOutputFilePath(String nameSpacePath, String fileName) {

		String dataDirPath = TsFileConf.BufferWriteDir;
		if (dataDirPath.charAt(dataDirPath.length() - 1) != File.separatorChar) {
			dataDirPath = dataDirPath + File.separatorChar + nameSpacePath;
		}
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			LOGGER.warn("The bufferwrite processor data dir doesn't exists, and mkdir the dir {}", dataDirPath);
			dataDir.mkdirs();
		}
		File outputFile = new File(dataDir, fileName);
		return outputFile.getAbsolutePath();
	}

	private FileSchema constructFileSchema(String nameSpacePath) throws PathErrorException, WriteProcessException {

		List<ColumnSchema> columnSchemaList;
		String deltaObjectType = null;
		try {
			deltaObjectType = mManager.getDeltaObjectTypeByPath(nameSpacePath);
		} catch (PathErrorException e) {
			LOGGER.error("Get the deltaObjectType from MManager error using nameSpacePath is {}", nameSpacePath);
			throw e;
		}

		try {
			columnSchemaList = mManager.getSchemaForOneType(deltaObjectType);
		} catch (PathErrorException e) {
			LOGGER.error("The list of ColumnSchema error from MManager error using deltaObjectType is {}",
					deltaObjectType);
			throw e;
		}

		FileSchema fileSchema = null;

		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, deltaObjectType);
		} catch (WriteProcessException e) {
			LOGGER.error("Get the FileSchema error, the list of ColumnSchema is {}", columnSchemaList);
			throw e;
		}
		return fileSchema;

	}

	private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String deltaObjectType)
			throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();

		for (ColumnSchema col : schemaList) {
			JSONObject measurement = new JSONObject();
			measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
			measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
			measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
			for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
				if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
					String[] valueArray = entry.getValue().split(",");
					measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
				} else
					measurement.put(entry.getKey(), entry.getValue().toString());
			}
			rowGroup.put(measurement);
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deltaObjectType);
		return new FileSchema(jsonSchema);
	}

	private TSRecord removeNullTSRecord(RowRecord src) {
		TSRecord record = src.toTSRecord();
		TSRecord filledRecord = new TSRecord(record.time, record.deltaObjectId);

		for (DataPoint dataPoint : record.dataPointList) {
			if (!"null".equals(dataPoint.getValue())) {
				filledRecord.addTuple(dataPoint);
			}
		}
		return filledRecord;
	}

	@Override
	public boolean canBeClosed() {
		if (isMerging == FileNodeProcessorState.NONE && newMultiPassLock.writeLock().tryLock()) {
			try {
				if (oldMultiPassLock.writeLock().tryLock()) {
					try {
						return true;
					} finally {
						oldMultiPassLock.writeLock().unlock();
					}
				}
			} finally {
				newMultiPassLock.writeLock().unlock();
			}
		}
		return false;
	}

	@Override
	public void close() throws FileNodeProcessorException {
		// close bufferwrite
		if (bufferWriteProcessor != null) {
			try {
				bufferWriteProcessor.close();
			} catch (BufferWriteProcessorException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
		// close overflow
		if (overflowProcessor != null) {
			try {
				overflowProcessor.close();
			} catch (OverflowProcessorException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
	}

	private void WriteStoreToDisk(FileNodeProcessorStore fileNodeProcessorStore) throws FileNodeProcessorException {

		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				serializeUtil.serialize(fileNodeProcessorStore, fileNodeRestoreFilePath);
				LOGGER.info("Write restore information to the restore file");
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
	}

	private FileNodeProcessorStore ReadStoreToDisk() throws FileNodeProcessorException {

		FileNodeProcessorStore fileNodeProcessorStore = null;
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		try {
			fileNodeProcessorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
					.orElse(new FileNodeProcessorStore(-1,
							new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null),
							new ArrayList<IntervalFileNode>(), FileNodeProcessorState.NONE, 0));
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileNodeProcessorException(e);
		}
		return fileNodeProcessorStore;
	}
}
