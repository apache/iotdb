package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.ByteArrayInputStream;
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
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
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
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.engine.QueryForMerge;

public class FileNodeProcessor extends LRUProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();
	private static final String LOCK_SIGNAL = "lock___signal";

	// private long lastUpdateTime = -1;
	private Map<String, Long> lastUpdateTimeMap;

	private Map<String, List<IntervalFileNode>> indexOfFiles;
	private IntervalFileNode emptyIntervalFileNode;
	private IntervalFileNode currentIntervalFileNode;
	private List<IntervalFileNode> newFileNodes;
	private FileNodeProcessorStatus isMerging;
	// this is used when work ->merge operation
	private int numOfMergeFile = 0;
	private FileNodeProcessorStore fileNodeProcessorStore = null;

	private static final String restoreFile = ".restore";
	private String fileNodeRestoreFilePath = null;

	private BufferWriteProcessor bufferWriteProcessor = null;
	private OverflowProcessor overflowProcessor = null;

	private Set<Integer> oldMultiPassTokenSet = null;
	private Set<Integer> newMultiPassTokenSet = new HashSet<>();
	private ReadWriteLock oldMultiPassLock = null;
	private ReadWriteLock newMultiPassLock = new ReentrantReadWriteLock(false);

	private boolean shouldRecovery = false;

	private Map<String, Object> parameters = null;

	private Action flushFileNodeProcessorAction = new Action() {

		@Override
		public void act() throws Exception {
			synchronized (fileNodeProcessorStore) {
				writeStoreToDisk(fileNodeProcessorStore);
			}
		}
	};

	private Action bufferwriteFlushAction = new Action() {

		@Override
		public void act() throws Exception {
			// update the lastUpdateTime Notice: Thread safe
			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
			}
		}
	};

	private Action bufferwriteCloseAction = new Action() {

		@Override
		public void act() throws Exception {

			// update the lastUpdatetime, newIntervalList and Notice: thread
			// safe
			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
				addLastTimeToIntervalFile();
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			}
		}
	};

	private void addLastTimeToIntervalFile() {

		if (lastUpdateTimeMap.isEmpty()) {
			LOGGER.error("The lastUpdateTimeMap is empty when close the bufferwrite file");
			throw new ProcessorRuntimException("The lastUpdateTimeMap is empty when close the bufferwrite file");
		}
		if (!newFileNodes.isEmpty()) {
			// end time with one start time
			Map<String, Long> endTimeMap = new HashMap<>();
			for (Entry<String, Long> startTime : currentIntervalFileNode.getStartTimeMap().entrySet()) {
				String deltaObjectId = startTime.getKey();
				endTimeMap.put(deltaObjectId, lastUpdateTimeMap.get(deltaObjectId));
			}
			currentIntervalFileNode.setEndTimeMap(endTimeMap);
		} else {
			throw new ProcessorRuntimException("The intervalFile list is empty when close bufferwrite file");
		}
	}

	public void addIntervalFileNode(long startTime, String fileName) throws Exception {

		IntervalFileNode intervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, fileName);
		this.currentIntervalFileNode = intervalFileNode;
		newFileNodes.add(intervalFileNode);
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		flushFileNodeProcessorAction.act();
	}

	public void setIntervalFileNodeStartTime(String deltaObjectId, long startTime) {

		if (currentIntervalFileNode.getStartTime(deltaObjectId) == -1) {
			currentIntervalFileNode.setStartTime(deltaObjectId, startTime);
			if (!indexOfFiles.containsKey(deltaObjectId)) {
				indexOfFiles.put(deltaObjectId, new ArrayList<>());
			}
			indexOfFiles.get(deltaObjectId).add(currentIntervalFileNode);
		}
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
		this.parameters = parameters;
		String dataDirPath = fileNodeDirPath + nameSpacePath;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.warn("The filenode processor data dir doesn't exists, and mkdir the dir {}", dataDirPath);
		}
		fileNodeRestoreFilePath = new File(dataDir, nameSpacePath + restoreFile).getAbsolutePath();
		try {
			fileNodeProcessorStore = readStoreToDisk();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			LOGGER.error("Restore the FileNodeProcessor information error, the nameSpacePath is {}", nameSpacePath);
			throw new FileNodeProcessorException(
					"Restore the FileNodeProcessor information error, the nameSpacePath is " + nameSpacePath);
		}
		lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		emptyIntervalFileNode = fileNodeProcessorStore.getEmptyIntervalFileNode();
		newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		isMerging = fileNodeProcessorStore.getFileNodeProcessorState();
		numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
		indexOfFiles = new HashMap<>();
		// status is not NONE, or the last intervalFile is not closed
		if (isMerging != FileNodeProcessorStatus.NONE
				|| (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed())) {
			shouldRecovery = true;
			// FileNodeRecovery();
		} else {
			// add file into the index of file
			addALLFileIntoIndex(newFileNodes);
		}
	}

	private void addALLFileIntoIndex(List<IntervalFileNode> fileList) {
		// clear map
		indexOfFiles.clear();
		// add all file to index
		for (IntervalFileNode fileNode : fileList) {
			if (!fileNode.getStartTimeMap().isEmpty()) {
				for (String deltaObjectId : fileNode.getStartTimeMap().keySet()) {
					if (!indexOfFiles.containsKey(deltaObjectId)) {
						indexOfFiles.put(deltaObjectId, new ArrayList<>());
					}
					indexOfFiles.get(deltaObjectId).add(fileNode);
				}
			}
		}
	}

	public boolean shouldRecovery() {
		return shouldRecovery;
	}

	public FileNodeProcessorStatus getFileNodeProcessorStatus() {
		return isMerging;
	}

	public void fileNodeRecovery() throws FileNodeProcessorException {
		// restore bufferwrite
		if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
			//
			// add the current file
			//
			currentIntervalFileNode = newFileNodes.get(newFileNodes.size() - 1);

			// this bufferwrite file is not close by normal operation
			String damagedFilePath = newFileNodes.get(newFileNodes.size() - 1).filePath;
			String[] fileNames = damagedFilePath.split("\\" + File.separator);
			// all information to recovery the damaged file.
			// contains file path, action parameters and nameSpacePath
			parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
			parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			try {
				bufferWriteProcessor = new BufferWriteProcessor(nameSpacePath, fileNames[fileNames.length - 1],
						parameters);
			} catch (BufferWriteProcessorException e) {
				// unlock
				writeUnlock();
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
			// unlock
			writeUnlock();
			LOGGER.error("Restore the overflow failed, the reason is {}", e.getMessage());
			e.printStackTrace();
			throw new FileNodeProcessorException("Restore the overflow failed, the reason is " + e.getMessage());
		}

		shouldRecovery = false;

		if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
			// re-merge all file
			// if bufferwrite processor is not null, and close
			if (bufferWriteProcessor != null) {
				try {
					bufferWriteProcessor.close();
					overflowProcessor.close();
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					writeUnlock();
					throw new FileNodeProcessorException(
							"Close the bufferwrite processor failed, the reason is " + e.getMessage());
				} catch (OverflowProcessorException e) {
					e.printStackTrace();
					writeUnlock();
					throw new FileNodeProcessorException(
							"Close the overflow processor failed, the reason is " + e.getMessage());
				}
				bufferWriteProcessor = null;
			}
			merge();
		} else if (isMerging == FileNodeProcessorStatus.WAITING) {
			// unlock
			writeUnlock();
			switchWaitingToWorkingv2(newFileNodes);
		} else {
			writeUnlock();
		}

		// add file into index of file
		addALLFileIntoIndex(newFileNodes);
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

	public OverflowProcessor getOverflowProcessor(String nameSpacePath, Map<String, Object> parameters)
			throws FileNodeProcessorException {
		if (overflowProcessor == null) {
			// construct processor or restore
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			try {
				overflowProcessor = new OverflowProcessor(nameSpacePath, parameters);
			} catch (OverflowProcessorException e) {
				LOGGER.error("Get the overflow processor instance failed, the nameSpacePath is {}, reason is {}",
						nameSpacePath, e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException(String.format(
						"Get the overflow processor instance failed, the nameSpacePath is %s, reason is %s",
						nameSpacePath, e.getMessage()));
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

	public void setLastUpdateTime(String deltaObjectId, long timestamp) {

		lastUpdateTimeMap.put(deltaObjectId, timestamp);
	}

	public long getLastUpdateTime(String deltaObjectId) {

		if (lastUpdateTimeMap.containsKey(deltaObjectId)) {
			return lastUpdateTimeMap.get(deltaObjectId);
		} else {
			return -1;
		}
	}

	public Map<String, Long> getLastUpdateTimeMap() {

		return lastUpdateTimeMap;
	}

	/**
	 * For insert overflow
	 * 
	 * @param timestamp
	 */
	public void changeTypeToChanged(String deltaObjectId, long timestamp) {
		if (!indexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = indexOfFiles.get(deltaObjectId);
			int index = searchIndexNodeByTimestamp(deltaObjectId, timestamp, temp);
			temp.get(index).changeTypeToChanged(isMerging);
			if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
				temp.get(index).addMergeChanged(deltaObjectId);
			}
		}
	}

	/**
	 * For update overflow
	 * 
	 * @param startTime
	 * @param endTime
	 */
	public void changeTypeToChanged(String deltaObjectId, long startTime, long endTime) {
		if (!indexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = indexOfFiles.get(deltaObjectId);
			int left = searchIndexNodeByTimestamp(deltaObjectId, startTime, temp);
			int right = searchIndexNodeByTimestamp(deltaObjectId, endTime, temp);
			for (int i = left; i <= right; i++) {
				temp.get(i).changeTypeToChanged(isMerging);
				if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
					temp.get(i).addMergeChanged(deltaObjectId);
				}
			}
		}
	}

	/**
	 * For delete overflow
	 * 
	 * @param timestamp
	 */
	public void changeTypeToChangedForDelete(String deltaObjectId, long timestamp) {
		if (!indexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = indexOfFiles.get(deltaObjectId);
			int index = searchIndexNodeByTimestamp(deltaObjectId, timestamp, temp);
			for (int i = 0; i <= index; i++) {
				temp.get(i).changeTypeToChanged(isMerging);
				if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
					temp.get(i).addMergeChanged(deltaObjectId);
				}
			}
		}
	}

	/**
	 * Search the index of the interval by the timestamp
	 * 
	 * @param deltaObjectId
	 * @param timestamp
	 * @param fileList
	 * @return index of interval
	 */
	private int searchIndexNodeByTimestamp(String deltaObjectId, long timestamp, List<IntervalFileNode> fileList) {
		int index = 1;
		while (index < fileList.size()) {
			if (timestamp < fileList.get(index).getStartTime(deltaObjectId)) {
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
		LOGGER.debug("AddMultiPassLock: read lock newMultiPassLock. {}", LOCK_SIGNAL);
		newMultiPassLock.readLock().lock();
		while (newMultiPassTokenSet.contains(multiPassLockToken)) {
			multiPassLockToken++;
		}
		newMultiPassTokenSet.add(multiPassLockToken);
		LOGGER.debug("Add multi token:{}, nsPath:{}. {}", multiPassLockToken, nameSpacePath, LOCK_SIGNAL);
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
		Pair<List<Object>, List<RowGroupMetaData>> bufferwriteDataInMemory = new Pair<List<Object>, List<RowGroupMetaData>>(
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
		DynamicOneColumnData currentPage = null;
		Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = null;
		if (bufferWriteProcessor != null) {
			currentPage = (DynamicOneColumnData) bufferwriteDataInMemory.left.get(0);
			pageList = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) bufferwriteDataInMemory.left.get(1);
		}
		queryStructure = new QueryStructure(currentPage, pageList, bufferwriteDataInMemory.right,
				bufferwriteDataInFiles, overflowData);
		return queryStructure;
	}

	public void merge() throws FileNodeProcessorException {

		LOGGER.debug("Merge: the nameSpacePath {} is begining to merge. {}", nameSpacePath, LOCK_SIGNAL);
		//
		// change status from work to merge
		//
		isMerging = FileNodeProcessorStatus.MERGING_WRITE;
		//
		// check the empty file
		//
		if (emptyIntervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
			for (Entry<String, Long> entry : emptyIntervalFileNode.getEndTimeMap().entrySet()) {
				String deltaObjectId = entry.getKey();
				if (indexOfFiles.containsKey(deltaObjectId)) {
					indexOfFiles.get(deltaObjectId).get(0).overflowChangeType = OverflowChangeType.CHANGED;
					emptyIntervalFileNode.removeTime(deltaObjectId);
				}
			}
			if (emptyIntervalFileNode.checkEmpty()) {
				emptyIntervalFileNode.clear();
			} else {
				if (!newFileNodes.isEmpty()) {
					IntervalFileNode first = newFileNodes.get(0);
					for (String deltaObjectId : emptyIntervalFileNode.getStartTimeMap().keySet()) {
						first.setStartTime(deltaObjectId, emptyIntervalFileNode.getStartTime(deltaObjectId));
						first.setEndTime(deltaObjectId, emptyIntervalFileNode.getEndTime(deltaObjectId));
						first.overflowChangeType = OverflowChangeType.CHANGED;
					}
					emptyIntervalFileNode.clear();
				} else {
					emptyIntervalFileNode.overflowChangeType = OverflowChangeType.CHANGED;
				}
			}
		}
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			if (intervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
				intervalFileNode.overflowChangeType = OverflowChangeType.CHANGED;
			}
		}

		addALLFileIntoIndex(newFileNodes);
		synchronized (fileNodeProcessorStore) {
			fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
			fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
			// flush this filenode information
			try {
				writeStoreToDisk(fileNodeProcessorStore);
			} catch (FileNodeProcessorException e) {
				LOGGER.error(
						"Merge: write filenode information to revocery file failed, the nameSpacePath is {}, the reason is {}",
						nameSpacePath, e.getMessage());
				e.printStackTrace();
				writeUnlock();
				throw new FileNodeProcessorException(
						"Merge: write filenode information to revocery file failed, the nameSpacePath is "
								+ nameSpacePath);
			}
		}
		// add numOfMergeFile to control the number of the merge file
		List<IntervalFileNode> backupIntervalFiles = new ArrayList<>();

		backupIntervalFiles = switchFileNodeToMergev2();
		//
		// clear empty file
		//
		boolean needEmtpy = false;
		if (emptyIntervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
			needEmtpy = true;
		}
		emptyIntervalFileNode.clear();
		try {
			//
			// change the overflow work to merge
			//
			overflowProcessor.switchWorkingToMerge();
		} catch (ProcessorException e) {
			LOGGER.error("Merge: Can't change overflow processor status from work to merge");
			e.printStackTrace();
			writeUnlock();
			throw new FileNodeProcessorException(e);
		}
		// unlock this filenode
		LOGGER.debug("Merge: the nameSpacePath {}, status from work to merge. {}", nameSpacePath, LOCK_SIGNAL);
		writeUnlock();
		LOGGER.debug("Merge: the nameSpacePath {}, unlock the filenode write lock. {}", nameSpacePath, LOCK_SIGNAL);

		// query buffer data and overflow data, and merge them
		for (IntervalFileNode backupIntervalFile : backupIntervalFiles) {
			if (backupIntervalFile.overflowChangeType == OverflowChangeType.CHANGED) {
				// query data and merge
				try {
					if (backupIntervalFile.getStartTimeMap().size() != backupIntervalFile.getEndTimeMap().size()) {
						throw new FileNodeProcessorException(
								"Merge: the size of startTimeMap is not equal to the size of startTimeMap");
					}
					queryAndWriteDataForMerge(backupIntervalFile);
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
				LOGGER.info(
						"The overflowChangedType of backup IntervalFile is {}, start time map is {}, end time map is {}.",
						backupIntervalFile.overflowChangeType, backupIntervalFile.getStartTimeMap(),
						backupIntervalFile.getEndTimeMap());
			}
		}
		//
		// change status from merge to wait
		//
		switchMergeToWaitingv2(backupIntervalFiles, needEmtpy);
		//
		// change status from wait to work
		//
		switchWaitingToWorkingv2(backupIntervalFiles);
	}

	private List<IntervalFileNode> switchFileNodeToMergev2() throws FileNodeProcessorException {
		List<IntervalFileNode> result = new ArrayList<>();
		if (emptyIntervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
			// add empty
			result.add(emptyIntervalFileNode.backUp());
			if (!newFileNodes.isEmpty()) {
				throw new FileNodeProcessorException(
						String.format("The status of empty file is %s, but the new file list is not empty",
								emptyIntervalFileNode.overflowChangeType));
			}
			return result;
		}
		if (!newFileNodes.isEmpty()) {
			for (IntervalFileNode intervalFileNode : newFileNodes) {
				if (intervalFileNode.overflowChangeType == OverflowChangeType.NO_CHANGE) {
					result.add(intervalFileNode.backUp());
				} else {
					Map<String, Long> startTimeMap = new HashMap<>();
					Map<String, Long> endTimeMap = new HashMap<>();
					for (String deltaObjectId : intervalFileNode.getEndTimeMap().keySet()) {
						List<IntervalFileNode> temp = indexOfFiles.get(deltaObjectId);
						int index = temp.indexOf(intervalFileNode);
						int size = temp.size();
						// start time
						if (index == 0) {
							startTimeMap.put(deltaObjectId, 0L);
						} else {
							startTimeMap.put(deltaObjectId, intervalFileNode.getStartTime(deltaObjectId));
						}
						// end time
						if (index < size - 1) {
							endTimeMap.put(deltaObjectId, temp.get(index + 1).getStartTime(deltaObjectId) - 1);
						} else {
							endTimeMap.put(deltaObjectId, intervalFileNode.getEndTime(deltaObjectId));
						}
					}
					IntervalFileNode node = new IntervalFileNode(startTimeMap, endTimeMap,
							intervalFileNode.overflowChangeType, intervalFileNode.filePath);
					result.add(node);
				}
			}
		} else {
			throw new FileNodeProcessorException("No file was changed when merging");
		}
		return result;
	}

	private void switchMergeToWaitingv2(List<IntervalFileNode> backupIntervalFiles, boolean needEmpty)
			throws FileNodeProcessorException {
		LOGGER.debug("Merge: switch merge to wait, the backupIntervalFiles is {}", backupIntervalFiles);
		writeLock();
		try {
			oldMultiPassTokenSet = newMultiPassTokenSet;
			oldMultiPassLock = newMultiPassLock;
			newMultiPassTokenSet = new HashSet<>();
			newMultiPassLock = new ReentrantReadWriteLock(false);

			LOGGER.info(
					"Merge: switch merge to wait, the overflowChangeType of emptyIntervalFileNode is {}, the newFileNodes is {}",
					emptyIntervalFileNode.overflowChangeType, newFileNodes);
			List<IntervalFileNode> result = new ArrayList<>();
			int beginIndex = 0;
			if (needEmpty) {
				IntervalFileNode empty = backupIntervalFiles.get(0);
				if (!empty.checkEmpty()) {
					for (String deltaObjectId : empty.getStartTimeMap().keySet()) {
						if (indexOfFiles.containsKey(deltaObjectId)) {
							IntervalFileNode temp = indexOfFiles.get(deltaObjectId).get(0);
							if (temp.getMergeChanged().contains(deltaObjectId)) {
								empty.overflowChangeType = OverflowChangeType.CHANGED;
								break;
							}
						}
					}
					empty.clearMergeChanged();
					result.add(empty.backUp());
					beginIndex++;
				}
			}
			// reconstruct the file index
			addALLFileIntoIndex(backupIntervalFiles);
			// check the merge changed file
			for (int i = beginIndex; i < backupIntervalFiles.size(); i++) {
				IntervalFileNode newFile = newFileNodes.get(i - beginIndex);
				IntervalFileNode temp = backupIntervalFiles.get(i);
				if (newFile.overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
					for (String deltaObjectId : newFile.getMergeChanged()) {
						if (temp.getStartTimeMap().containsKey(deltaObjectId)) {
							temp.overflowChangeType = OverflowChangeType.CHANGED;
						} else {
							changeTypeToChanged(deltaObjectId, newFile.getStartTime(deltaObjectId),
									newFile.getEndTime(deltaObjectId));
						}
					}
				}
				if (!temp.checkEmpty()) {
					result.add(temp);
				}
			}
			// add new file when merge
			for (int i = backupIntervalFiles.size() - beginIndex; i < newFileNodes.size(); i++) {
				result.add(newFileNodes.get(i).backUp());
			}

			isMerging = FileNodeProcessorStatus.WAITING;
			newFileNodes = result;
			// reconstruct the index
			addALLFileIntoIndex(newFileNodes);
			// clear merge changed
			for (IntervalFileNode fileNode : newFileNodes) {
				fileNode.clearMergeChanged();
			}

			synchronized (fileNodeProcessorStore) {
				fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
				fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
				try {
					writeStoreToDisk(fileNodeProcessorStore);
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
			if (oldMultiPassLock != null) {
				LOGGER.info("The old Multiple Pass Token set is {}, the old Multiple Pass Lock is {}",
						oldMultiPassTokenSet, oldMultiPassLock);
				oldMultiPassLock.writeLock().lock();
			}
			try {
				// delete the all files which are in the newFileNodes
				// notice: the last restore file of the interval file

				String bufferwriteDirPath = TsFileDBConf.bufferWriteDir;
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
						bufferFiles.add(bufferFilePath);
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
				for (IntervalFileNode fileNode : newFileNodes) {
					if (fileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
						fileNode.overflowChangeType = OverflowChangeType.CHANGED;
					}
				}
				// overflow switch
				overflowProcessor.switchMergeToWorking();
				// write status to file
				isMerging = FileNodeProcessorStatus.NONE;
				synchronized (fileNodeProcessorStore) {
					fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
					fileNodeProcessorStore.setNewFileNodes(newFileNodes);
					fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
					writeStoreToDisk(fileNodeProcessorStore);
				}
			} catch (ProcessorException e) {
				LOGGER.error("Merge: switch wait to work failed. the reason is {}", e.getMessage());
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			} finally {
				oldMultiPassTokenSet = null;
				if (oldMultiPassLock != null) {
					oldMultiPassLock.writeLock().unlock();
				}
				oldMultiPassLock = null;
			}
		} finally {
			writeUnlock();
		}
	}

	private void queryAndWriteDataForMerge(IntervalFileNode backupIntervalFile)
			throws IOException, WriteProcessException, FileNodeProcessorException {

		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();

		TSRandomAccessFileWriter raf = null;
		TSFileIOWriter tsfileIOWriter = null;
		WriteSupport<TSRecord> writeSupport = null;
		TSRecordWriter recordWriter = null;
		String outputPath = null;
		for (String deltaObjectId : backupIntervalFile.getStartTimeMap().keySet()) {
			// query one deltaObjectId
			long startTime = backupIntervalFile.getStartTime(deltaObjectId);
			long endTime = backupIntervalFile.getEndTime(deltaObjectId);
			List<Path> pathList = new ArrayList<>();

			try {
				ArrayList<String> pathStrings = mManager
						.getPaths(deltaObjectId + FileNodeConstants.PATH_SEPARATOR + "*");
				for (String string : pathStrings) {
					pathList.add(new Path(string));
				}
			} catch (PathErrorException e) {
				LOGGER.error("Can't get all the paths from MManager, the deltaObjectId is {}", deltaObjectId);
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
			if (pathList.isEmpty()) {
				continue;
			}

			FilterExpression timeFilter = FilterUtilsForOverflow.construct(null, null, "0",
					"(>=" + startTime + ")&" + "(<=" + endTime + ")");
			LOGGER.info("Merge query and merge: deltaObjectId {}, time filter {}", deltaObjectId, timeFilter);
			startTime = -1;
			endTime = -1;

			QueryForMerge queryer = new QueryForMerge(pathList, (SingleSeriesFilterExpression) timeFilter);
			int queryCount = 0;
			if (!queryer.hasNextRecord()) {
				LOGGER.warn("Merge query: deltaObjectId {}, time filter {}, no query data", deltaObjectId, timeFilter);
			} else {
				queryCount++;
				RowRecord firstRecord = queryer.getNextRecord();

				if (raf == null) {
					outputPath = constructOutputFilePath(nameSpacePath, firstRecord.timestamp
							+ FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis());

					FileSchema fileSchema;
					try {
						fileSchema = constructFileSchema(nameSpacePath);
					} catch (PathErrorException e) {
						LOGGER.error("Get the FileSchema error, the nameSpacePath is {}", nameSpacePath);
						throw new WriteProcessException(
								"Get the FileSchema error, the nameSpacePath is " + nameSpacePath);
					}
					raf = new RandomAccessOutputStream(new File(outputPath));
					tsfileIOWriter = new TSFileIOWriter(fileSchema, raf);
					writeSupport = new TSRecordWriteSupport();
					recordWriter = new TSRecordWriter(TsFileConf, tsfileIOWriter, writeSupport, fileSchema);
				}

				TSRecord filledRecord = removeNullTSRecord(firstRecord);
				recordWriter.write(filledRecord);
				startTime = endTime = firstRecord.getTime();

				while (queryer.hasNextRecord()) {
					queryCount++;
					RowRecord row = queryer.getNextRecord();
					filledRecord = removeNullTSRecord(row);
					endTime = filledRecord.time;
					try {
						recordWriter.write(filledRecord);
					} catch (WriteProcessException e) {
						LOGGER.error("Merge query: write one record error, the tsrecord is {}", filledRecord);
						e.printStackTrace();

						/**
						 * should throw exception
						 */
					}
				}
				startTimeMap.put(deltaObjectId, startTime);
				endTimeMap.put(deltaObjectId, endTime);
				System.out.println("   ============   Merge Record Count  : " + queryCount + " , " + deltaObjectId);
				LOGGER.debug("Merge query: deltaObjectId {}, time filter {}, filepath {} successfully", deltaObjectId,
						timeFilter, outputPath);
			}
		}
		if (recordWriter != null) {
			recordWriter.close();
		}
		backupIntervalFile.filePath = outputPath;
		backupIntervalFile.overflowChangeType = OverflowChangeType.NO_CHANGE;
		backupIntervalFile.setStartTimeMap(startTimeMap);
		backupIntervalFile.setEndTimeMap(endTimeMap);
	}

	private String constructOutputFilePath(String nameSpacePath, String fileName) {

		String dataDirPath = TsFileDBConf.bufferWriteDir;
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
		if (isMerging == FileNodeProcessorStatus.NONE) {
			if (newMultiPassLock.writeLock().tryLock()) {
				try {
					if (oldMultiPassLock != null) {
						if (oldMultiPassLock.writeLock().tryLock()) {
							try {
								return true;
							} finally {
								oldMultiPassLock.writeLock().unlock();
							}
						}
					} else {
						return true;
					}
				} finally {
					newMultiPassLock.writeLock().unlock();
				}
			}
		}
		return false;
	}

	@Override
	public void close() throws FileNodeProcessorException {
		// close bufferwrite
		synchronized (fileNodeProcessorStore) {
			fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
			writeStoreToDisk(fileNodeProcessorStore);
		}

		if (bufferWriteProcessor != null) {
			try {
				while (!bufferWriteProcessor.canBeClosed()) {

				}
				bufferWriteProcessor.close();
				bufferWriteProcessor = null;
			} catch (BufferWriteProcessorException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
		// close overflow
		if (overflowProcessor != null) {
			try {
				while (!overflowProcessor.canBeClosed()) {

				}
				overflowProcessor.close();
				overflowProcessor = null;
			} catch (OverflowProcessorException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
	}

	private void writeStoreToDisk(FileNodeProcessorStore fileNodeProcessorStore) throws FileNodeProcessorException {

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

	private FileNodeProcessorStore readStoreToDisk() throws FileNodeProcessorException {

		synchronized (fileNodeRestoreFilePath) {
			FileNodeProcessorStore fileNodeProcessorStore = null;
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				fileNodeProcessorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
						.orElse(new FileNodeProcessorStore(new HashMap<>(),
								new IntervalFileNode(OverflowChangeType.NO_CHANGE, null),
								new ArrayList<IntervalFileNode>(), FileNodeProcessorStatus.NONE, 0));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
			return fileNodeProcessorStore;
		}
	}
}
