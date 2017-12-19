package cn.edu.tsinghua.iotdb.engine.filenode;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.lru.LRUProcessor;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.index.common.DataFileInfo;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.engine.QueryForMerge;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileNodeProcessor extends LRUProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();

	/**
	 * Used to keep the oldest timestamp for each deltaObjectId. The key is
	 * deltaObjectId.
	 */
	private Map<String, Long> lastUpdateTimeMap;
	private Map<String, List<IntervalFileNode>> InvertedindexOfFiles;
	private IntervalFileNode emptyIntervalFileNode;
	private IntervalFileNode currentIntervalFileNode;
	private List<IntervalFileNode> newFileNodes;
	private FileNodeProcessorStatus isMerging;
	// this is used when work->merge operation
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
				// deep copy
				Map<String, Long> tempLastUpdateMap = new HashMap<>(lastUpdateTimeMap);
				fileNodeProcessorStore.setLastUpdateTimeMap(tempLastUpdateMap);
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

		if (!newFileNodes.isEmpty()) {
			// end time with one start time
			Map<String, Long> endTimeMap = new HashMap<>();
			for (Entry<String, Long> startTime : currentIntervalFileNode.getStartTimeMap().entrySet()) {
				String deltaObjectId = startTime.getKey();
				endTimeMap.put(deltaObjectId, lastUpdateTimeMap.get(deltaObjectId));
			}
			currentIntervalFileNode.setEndTimeMap(endTimeMap);
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
			if (!InvertedindexOfFiles.containsKey(deltaObjectId)) {
				InvertedindexOfFiles.put(deltaObjectId, new ArrayList<>());
			}
			InvertedindexOfFiles.get(deltaObjectId).add(currentIntervalFileNode);
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

	public void clearFileNode() {
		emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		newFileNodes = new ArrayList<>();
		isMerging = FileNodeProcessorStatus.NONE;
		numOfMergeFile = 0;
		fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
		fileNodeProcessorStore.setFileNodeProcessorState(isMerging);
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		fileNodeProcessorStore.setNumOfMergeFile(numOfMergeFile);
		fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
	}

	public FileNodeProcessor(String fileNodeDirPath, String nameSpacePath, Map<String, Object> parameters)
			throws FileNodeProcessorException {
		super(nameSpacePath);
		this.parameters = parameters;
		if (fileNodeDirPath.length() > 0
				&& fileNodeDirPath.charAt(fileNodeDirPath.length() - 1) != File.separatorChar) {
			fileNodeDirPath = fileNodeDirPath + File.separatorChar;
		}
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
			LOGGER.error("Restore the FileNodeProcessor information error, the nameSpacePath is {}", nameSpacePath);
			throw new FileNodeProcessorException(e);
		}
		// TODO deep clone
		lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		emptyIntervalFileNode = fileNodeProcessorStore.getEmptyIntervalFileNode();
		newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		isMerging = fileNodeProcessorStore.getFileNodeProcessorState();
		numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
		InvertedindexOfFiles = new HashMap<>();
		// status is not NONE, or the last intervalFile is not closed
		if (isMerging != FileNodeProcessorStatus.NONE
				|| (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed())) {
			shouldRecovery = true;
		} else {
			// add file into the index of file
			addALLFileIntoIndex(newFileNodes);
		}
	}

	private void addALLFileIntoIndex(List<IntervalFileNode> fileList) {
		// clear map
		InvertedindexOfFiles.clear();
		// add all file to index
		for (IntervalFileNode fileNode : fileList) {
			if (!fileNode.getStartTimeMap().isEmpty()) {
				for (String deltaObjectId : fileNode.getStartTimeMap().keySet()) {
					if (!InvertedindexOfFiles.containsKey(deltaObjectId)) {
						InvertedindexOfFiles.put(deltaObjectId, new ArrayList<>());
					}
					InvertedindexOfFiles.get(deltaObjectId).add(fileNode);
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
				LOGGER.error("Restore the bufferwrite failed");
				throw new FileNodeProcessorException(e);
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
			LOGGER.error("Restore the overflow failed.");
			throw new FileNodeProcessorException(e);
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
				LOGGER.error("Get the bufferwrite processor instance failed, the nameSpacePath is {}.", namespacePath);
				throw new FileNodeProcessorException(e);
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
						nameSpacePath, e);
				throw new FileNodeProcessorException(e);
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
		if (!InvertedindexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = InvertedindexOfFiles.get(deltaObjectId);
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
		if (!InvertedindexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = InvertedindexOfFiles.get(deltaObjectId);
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
		if (!InvertedindexOfFiles.containsKey(deltaObjectId)) {
			LOGGER.warn("No any interval node to be changed overflow type");
			emptyIntervalFileNode.setStartTime(deltaObjectId, 0L);
			emptyIntervalFileNode.setEndTime(deltaObjectId, getLastUpdateTime(deltaObjectId));
			emptyIntervalFileNode.changeTypeToChanged(isMerging);
		} else {
			List<IntervalFileNode> temp = InvertedindexOfFiles.get(deltaObjectId);
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
		LOGGER.debug("Add MultiPassLock: read lock newMultiPassLock.");
		newMultiPassLock.readLock().lock();
		while (newMultiPassTokenSet.contains(multiPassLockToken)) {
			multiPassLockToken++;
		}
		newMultiPassTokenSet.add(multiPassLockToken);
		LOGGER.debug("Add multi token:{}, nsPath:{}.", multiPassLockToken, nameSpacePath);
		return multiPassLockToken;
	}

	public boolean removeMultiPassLock(int token) {
		if (newMultiPassTokenSet.contains(token)) {
			newMultiPassLock.readLock().unlock();
			newMultiPassTokenSet.remove(token);
			LOGGER.debug("Remove multi token:{}, nspath:{}, new set:{}, lock:{}", token, nameSpacePath,
					newMultiPassTokenSet, newMultiPassLock);
			return true;
		} else if (oldMultiPassTokenSet != null && oldMultiPassTokenSet.contains(token)) {
			// remove token first， then unlock
			oldMultiPassLock.readLock().unlock();
			oldMultiPassTokenSet.remove(token);
			LOGGER.debug("Remove multi token:{}, old set:{}, lock:{}", token, oldMultiPassTokenSet, oldMultiPassLock);
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
		TSDataType dataType = null;
		try {
			dataType = mManager.getSeriesType(deltaObjectId + "." + measurementId);
		} catch (PathErrorException e) {
			throw new FileNodeProcessorException(e);
		}
		overflowData = overflowProcessor.query(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter,
				dataType);
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

	public List<DataFileInfo> indexQuery(String deltaObjectId, long startTime, long endTime) {
		List<DataFileInfo> dataFileInfos = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			if (intervalFileNode.isClosed()) {
				long s1 = intervalFileNode.getStartTime(deltaObjectId);
				long e1 = intervalFileNode.getEndTime(deltaObjectId);
				if (e1 >= startTime && (s1 <= endTime || endTime == -1)) {
					DataFileInfo dataFileInfo = new DataFileInfo(s1, e1, intervalFileNode.filePath);
					dataFileInfos.add(dataFileInfo);
				}
			}
		}
		return dataFileInfos;
	}

	public void merge() throws FileNodeProcessorException {

		LOGGER.debug("Merge: the nameSpacePath {} is begining to merge.", nameSpacePath);
		//
		// change status from work to merge
		//
		isMerging = FileNodeProcessorStatus.MERGING_WRITE;
		//
		// check the empty file
		//
		Map<String, Long> startTimeMap = emptyIntervalFileNode.getStartTimeMap();
		if (emptyIntervalFileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
			Iterator<Entry<String, Long>> iterator = emptyIntervalFileNode.getEndTimeMap().entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, Long> entry = iterator.next();
				String deltaObjectId = entry.getKey();
				if (InvertedindexOfFiles.containsKey(deltaObjectId)) {
					InvertedindexOfFiles.get(deltaObjectId).get(0).overflowChangeType = OverflowChangeType.CHANGED;
					startTimeMap.remove(deltaObjectId);
					iterator.remove();
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
				LOGGER.error("Merge: write filenode information to revocery file failed, the nameSpacePath is {}.",
						nameSpacePath);
				writeUnlock();
				throw new FileNodeProcessorException(e);
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
			writeUnlock();
			throw new FileNodeProcessorException(e);
		}
		// unlock this filenode
		LOGGER.debug("Merge: the nameSpacePath {}, status from work to merge.", nameSpacePath);
		writeUnlock();
		LOGGER.debug("Merge: the nameSpacePath {}, unlock the filenode write lock.", nameSpacePath);

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
					LOGGER.error("Merge: query and write data error.");
					throw new FileNodeProcessorException(e);
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
		// merge index begin
		//
		mergeIndex();
		//
		// merge index end
		//

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
						List<IntervalFileNode> temp = InvertedindexOfFiles.get(deltaObjectId);
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
			LOGGER.error("No file was changed when mergin, the filenode is {}",nameSpacePath);
			throw new FileNodeProcessorException("No file was changed when merging, the filenode is "+nameSpacePath);
		}
		return result;
	}

    private List<DataFileInfo> getDataFileInfoForIndex(Path path, List<IntervalFileNode> sourceFileNodes) {
        String deltaObjectId = path.getDeltaObjectToString();
        List<DataFileInfo> dataFileInfos = new ArrayList<>();
        for (IntervalFileNode intervalFileNode : sourceFileNodes) {
            if (intervalFileNode.isClosed()) {
                if (intervalFileNode.getStartTime(deltaObjectId) != -1) {
                    DataFileInfo dataFileInfo = new DataFileInfo(intervalFileNode.getStartTime
                            (deltaObjectId),
                            intervalFileNode.getEndTime(deltaObjectId), intervalFileNode.filePath);
                    dataFileInfos.add(dataFileInfo);
                }
            }
        }
        return dataFileInfos;
    }

	private void mergeIndex() throws FileNodeProcessorException {
		try {
			Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(nameSpacePath);
			if (!allIndexSeries.isEmpty()) {
				LOGGER.info("merge all file and modify index file, the nameSpacePath is {}, the index path is {}",
						nameSpacePath, allIndexSeries);
				for (Entry<String, Set<IndexType>> entry : allIndexSeries.entrySet()) {
					String series = entry.getKey();
					Path path = new Path(series);
					List<DataFileInfo> dataFileInfos = getDataFileInfoForIndex(path, newFileNodes);
					if (!dataFileInfos.isEmpty()) {
						try {
							for (IndexType indexType : entry.getValue())
								IndexManager.getIndexInstance(indexType).build(path, dataFileInfos, null);
						} catch (IndexManagerException e) {
							e.printStackTrace();
							throw new FileNodeProcessorException(e.getMessage());
						}
					}
				}
			}
		} catch (PathErrorException e) {
			LOGGER.error("failed to find all fileList to be merged." + e.getMessage());
			throw new FileNodeProcessorException(e.getMessage());
		}
	}

	private void switchMergeIndex() throws FileNodeProcessorException {
		try {
			Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(nameSpacePath);
			if (!allIndexSeries.isEmpty()) {
				LOGGER.info("mergeswith all file and modify index file, the nameSpacePath is {}, the index path is {}",
						nameSpacePath, allIndexSeries);
				for (Entry<String, Set<IndexType>> entry : allIndexSeries.entrySet()) {
					String series = entry.getKey();
					Path path = new Path(series);
					List<DataFileInfo> dataFileInfos = getDataFileInfoForIndex(path, newFileNodes);
					if (!dataFileInfos.isEmpty()) {
						try {
							for (IndexType indexType : entry.getValue())
								IndexManager.getIndexInstance(indexType).mergeSwitch(path, dataFileInfos);
						} catch (IndexManagerException e) {
							e.printStackTrace();
							throw new FileNodeProcessorException(e.getMessage());
						}
					}
				}
			}
		} catch (PathErrorException e) {
			LOGGER.error("failed to find all fileList to be mergeSwitch" + e.getMessage());
			throw new FileNodeProcessorException(e.getMessage());
		}
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
						if (InvertedindexOfFiles.containsKey(deltaObjectId)) {
							IntervalFileNode temp = InvertedindexOfFiles.get(deltaObjectId).get(0);
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
					LOGGER.error("Merge: write filenode information to revocery file failed, the nameSpacePath is {}.",
							nameSpacePath);
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

		if (oldMultiPassLock != null) {
			LOGGER.info("The old Multiple Pass Token set is {}, the old Multiple Pass Lock is {}", oldMultiPassTokenSet,
					oldMultiPassLock);
			oldMultiPassLock.writeLock().lock();
		}
		try {
			writeLock();
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

				// merge switch
				switchMergeIndex();

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
				LOGGER.error("Merge: switch wait to work failed.");
				throw new FileNodeProcessorException(e);
			} finally {
				writeUnlock();
			}
		} finally {
			oldMultiPassTokenSet = null;
			if (oldMultiPassLock != null) {
				oldMultiPassLock.writeLock().unlock();
			}
			oldMultiPassLock = null;
		}

	}

	private void queryAndWriteDataForMerge(IntervalFileNode backupIntervalFile)
			throws IOException, WriteProcessException, FileNodeProcessorException {

		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();

		TsFileWriter recordWriter = null;
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

				if (recordWriter == null) {
					outputPath = constructOutputFilePath(nameSpacePath, firstRecord.timestamp
							+ FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis());
					FileSchema fileSchema = constructFileSchema(nameSpacePath);
					recordWriter = new TsFileWriter(new File(outputPath), fileSchema, TsFileConf);
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
						LOGGER.error(
								String.format("Merge query: write one record error, the tsrecord is {}", filledRecord),
								e);
					}
				}
				startTimeMap.put(deltaObjectId, startTime);
				endTimeMap.put(deltaObjectId, endTime);
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

	private FileSchema constructFileSchema(String nameSpacePath) throws WriteProcessException {

		List<ColumnSchema> columnSchemaList;
		columnSchemaList = mManager.getSchemaForFileName(nameSpacePath);

		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, nameSpacePath);
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
					// try {
					// TimeUnit.MICROSECONDS.sleep(1000);
					// } catch (InterruptedException e) {
					// e.printStackTrace();
					// }
				}
				bufferWriteProcessor.close();
				bufferWriteProcessor = null;
				/*
				 * add index for close
				 */
                Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(nameSpacePath);

                if (!allIndexSeries.isEmpty()) {
                    LOGGER.info("Close buffer write file and append index file, the nameSpacePath is {}, the index " +
                                    "type is {}, the index path is {}",
                            nameSpacePath, "kvindex", allIndexSeries);
					for (Entry<String, Set<IndexType>> entry : allIndexSeries.entrySet()) {
                        Path path = new Path(entry.getKey());
                        String deltaObjectId = path.getDeltaObjectToString();
						if (currentIntervalFileNode.getStartTime(deltaObjectId) != -1) {
							DataFileInfo dataFileInfo = new DataFileInfo(currentIntervalFileNode.getStartTime(deltaObjectId),
									currentIntervalFileNode.getEndTime(deltaObjectId), currentIntervalFileNode.filePath);
							for (IndexType indexType : entry.getValue())
								IndexManager.getIndexInstance(indexType).build(path, dataFileInfo, null);
						}
                    }
				}

			} catch (BufferWriteProcessorException | PathErrorException | IndexManagerException  e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
		// close overflow
		if (overflowProcessor != null) {
			try {
				while (!overflowProcessor.canBeClosed()) {
					// try {
					// TimeUnit.MICROSECONDS.sleep(1000);
					// } catch (InterruptedException e) {
					// e.printStackTrace();
					// }
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

	public void rebuildIndex() throws FileNodeProcessorException {
		mergeIndex();
		switchMergeIndex();
	}
}
