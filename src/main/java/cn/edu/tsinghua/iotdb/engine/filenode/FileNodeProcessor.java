package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowProcessor;
import cn.edu.tsinghua.iotdb.engine.pool.MergeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.ErrorDebugException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.index.common.DataFileInfo;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.IStatistic;
import cn.edu.tsinghua.iotdb.monitor.MonitorConstants;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.JsonConverter;

public class FileNodeProcessor extends Processor implements IStatistic {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();
	private static final Directories directories = Directories.getInstance();

	/**
	 * Used to keep the oldest timestamp for each deltaObjectId. The key is
	 * deltaObjectId.
	 */
	private volatile boolean isOverflowed;
	private Map<String, Long> lastUpdateTimeMap;
	private Map<String, Long> flushLastUpdateTimeMap;
	private Map<String, List<IntervalFileNode>> InvertedindexOfFiles;
	private IntervalFileNode emptyIntervalFileNode;
	private IntervalFileNode currentIntervalFileNode;
	private List<IntervalFileNode> newFileNodes;
	private FileNodeProcessorStatus isMerging;
	// this is used when work->merge operation
	private int numOfMergeFile = 0;
	private FileNodeProcessorStore fileNodeProcessorStore = null;

	public static final String RESTORE_FILE_SUFFIX = ".restore";
	private String fileNodeRestoreFilePath = null;
	private String baseDirPath;
	// last merge time
	private long lastMergeTime = -1;

	private BufferWriteProcessor bufferWriteProcessor = null;
	private OverflowProcessor overflowProcessor = null;

	private Set<Integer> oldMultiPassTokenSet = null;
	private Set<Integer> newMultiPassTokenSet = new HashSet<>();
	private ReadWriteLock oldMultiPassLock = null;
	private ReadWriteLock newMultiPassLock = new ReentrantReadWriteLock(false);
	// system recovery
	private boolean shouldRecovery = false;
	// statistic monitor parameters
	private Map<String, Object> parameters = null;
	private final String statStorageDeltaName;

	private FileSchema fileSchema;

	private final HashMap<String, AtomicLong> statParamsHashMap = new HashMap<String, AtomicLong>() {
		{
			for (MonitorConstants.FileNodeProcessorStatConstants statConstant : MonitorConstants.FileNodeProcessorStatConstants
					.values()) {
				put(statConstant.name(), new AtomicLong(0));
			}
		}
	};

	public HashMap<String, AtomicLong> getStatParamsHashMap() {
		return statParamsHashMap;
	}

	@Override
	public void registStatMetadata() {
		HashMap<String, String> hashMap = new HashMap<String, String>() {
			{
				for (MonitorConstants.FileNodeProcessorStatConstants statConstant : MonitorConstants.FileNodeProcessorStatConstants
						.values()) {
					put(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name(),
							MonitorConstants.DataType);
				}
			}
		};
		StatMonitor.getInstance().registStatStorageGroup(hashMap);
	}

	@Override
	public List<String> getAllPathForStatistic() {
		List<String> list = new ArrayList<>();
		for (MonitorConstants.FileNodeProcessorStatConstants statConstant : MonitorConstants.FileNodeProcessorStatConstants
				.values()) {
			list.add(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name());
		}
		return list;
	}

	@Override
	public HashMap<String, TSRecord> getAllStatisticsValue() {
		Long curTime = System.currentTimeMillis();
		HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
		TSRecord tsRecord = new TSRecord(curTime, statStorageDeltaName);
		HashMap<String, AtomicLong> hashMap = getStatParamsHashMap();
		tsRecord.dataPointList = new ArrayList<DataPoint>() {
			{
				for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
					add(new LongDataPoint(entry.getKey(), entry.getValue().get()));
				}
			}
		};
		tsRecordHashMap.put(statStorageDeltaName, tsRecord);
		return tsRecordHashMap;
	}

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
				// update flushLastUpdateTimeMap
				for (Entry<String, Long> entry : lastUpdateTimeMap.entrySet()) {
					flushLastUpdateTimeMap.put(entry.getKey(), entry.getValue() + 1);
				}
				fileNodeProcessorStore.setLastUpdateTimeMap(tempLastUpdateMap);
			}
		}
	};

	private Action bufferwriteCloseAction = new Action() {

		@Override
		public void act() throws Exception {

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

	public void addIntervalFileNode(long startTime, String baseDir, String fileName) throws Exception {

		IntervalFileNode intervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, baseDir, fileName);
		this.currentIntervalFileNode = intervalFileNode;
		newFileNodes.add(intervalFileNode);
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		flushFileNodeProcessorAction.act();
	}

	public void setIntervalFileNodeStartTime(String deltaObjectId) {
		if (currentIntervalFileNode.getStartTime(deltaObjectId) == -1) {
			currentIntervalFileNode.setStartTime(deltaObjectId, flushLastUpdateTimeMap.get(deltaObjectId));
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
				fileNodeProcessorStore.setOverflowed(isOverflowed);
				fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			}
		}
	};

	public void clearFileNode() {
		isOverflowed = false;
		emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		newFileNodes = new ArrayList<>();
		isMerging = FileNodeProcessorStatus.NONE;
		numOfMergeFile = 0;
		fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
		fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		fileNodeProcessorStore.setNumOfMergeFile(numOfMergeFile);
		fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
	}

	public FileNodeProcessor(String fileNodeDirPath, String processorName) throws FileNodeProcessorException {
		super(processorName);
		statStorageDeltaName = MonitorConstants.statStorageGroupPrefix + MonitorConstants.MONITOR_PATH_SEPERATOR
				+ MonitorConstants.fileNodePath + MonitorConstants.MONITOR_PATH_SEPERATOR
				+ processorName.replaceAll("\\.", "_");

		this.parameters = new HashMap<>();
		if (fileNodeDirPath.length() > 0
				&& fileNodeDirPath.charAt(fileNodeDirPath.length() - 1) != File.separatorChar) {
			fileNodeDirPath = fileNodeDirPath + File.separatorChar;
		}
		this.baseDirPath = fileNodeDirPath + processorName;
		File dataDir = new File(this.baseDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.info("The data directory of the filenode processor {} doesn't exist. Create new directory {}",
					getProcessorName(), baseDirPath);
		}
		fileNodeRestoreFilePath = new File(dataDir, processorName + RESTORE_FILE_SUFFIX).getPath();
		try {
			fileNodeProcessorStore = readStoreFromDisk();
		} catch (FileNodeProcessorException e) {
			LOGGER.error("The fileNode processor {} encountered an error when recoverying restore information.",
					processorName, e);
			throw new FileNodeProcessorException(e);
		}
		// TODO deep clone the lastupdate time
		lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		emptyIntervalFileNode = fileNodeProcessorStore.getEmptyIntervalFileNode();
		newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		isMerging = fileNodeProcessorStore.getFileNodeProcessorStatus();
		numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
		InvertedindexOfFiles = new HashMap<>();
		// deep clone
		flushLastUpdateTimeMap = new HashMap<>();
		for (Entry<String, Long> entry : lastUpdateTimeMap.entrySet()) {
			flushLastUpdateTimeMap.put(entry.getKey(), entry.getValue() + 1);
		}
		// construct the fileschema
		try {
			this.fileSchema = constructFileSchema(processorName);
		} catch (WriteProcessException e) {
			throw new FileNodeProcessorException(e);
		}
		// status is not NONE, or the last intervalFile is not closed
		if (isMerging != FileNodeProcessorStatus.NONE
				|| (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed())) {
			shouldRecovery = true;
		} else {
			// add file into the index of file
			addALLFileIntoIndex(newFileNodes);
		}
		// RegistStatService
		if (TsFileDBConf.enableStatMonitor) {
			StatMonitor statMonitor = StatMonitor.getInstance();
			registStatMetadata();
			statMonitor.registStatistics(statStorageDeltaName, this);
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

	/**
	 * if overflow insert, update and delete write into this filenode processor,
	 * set <code>isOverflowed</code> to true,
	 */
	public void setOverflowed(boolean isOverflowed) {
		if (this.isOverflowed != isOverflowed) {
			this.isOverflowed = isOverflowed;
		}
	}

	public boolean isOverflowed() {
		return isOverflowed;
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
			String damagedFilePath = newFileNodes.get(newFileNodes.size() - 1).getFilePath();
			String[] fileNames = damagedFilePath.split("\\" + File.separator);
			// all information to recovery the damaged file.
			// contains file path, action parameters and processorName
			parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
			parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			String baseDir = directories.getTsFileFolder(newFileNodes.get(newFileNodes.size() - 1).getBaseDirIndex());
			LOGGER.info("The filenode processor {} will recovery the bufferwrite processor, the bufferwrite file is {}",
					getProcessorName(), fileNames[fileNames.length - 1]);
			try {
				bufferWriteProcessor = new BufferWriteProcessor(baseDir, getProcessorName(), fileNames[fileNames.length - 1],
						parameters, fileSchema);
			} catch (BufferWriteProcessorException e) {
				// unlock
				writeUnlock();
				LOGGER.error(
						"The filenode processor {} failed to recovery the bufferwrite processor, the last bufferwrite file is {}.",
						getProcessorName(), fileNames[fileNames.length - 1]);
				throw new FileNodeProcessorException(e);
			}
		}
		// restore the overflow processor
		LOGGER.info("The filenode processor {} will recovery the overflow processor.", getProcessorName());
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
		try {
			overflowProcessor = new OverflowProcessor(getProcessorName(), parameters, fileSchema);
		} catch (IOException e) {
			writeUnlock();
			LOGGER.error("The filenode processor {} failed to recovery the overflow processor.", getProcessorName());
			throw new FileNodeProcessorException(e);
		}

		shouldRecovery = false;

		if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
			// re-merge all file
			// if bufferwrite processor is not null, and close
			LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.", getProcessorName(),
					isMerging);
			merge();
		} else if (isMerging == FileNodeProcessorStatus.WAITING) {
			// unlock
			LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.", getProcessorName(),
					isMerging);
			writeUnlock();
			switchWaitingToWorkingv2(newFileNodes);
		} else {
			writeUnlock();
		}
		// add file into index of file
		addALLFileIntoIndex(newFileNodes);
	}

	public BufferWriteProcessor getBufferWriteProcessor(String processorName, long insertTime)
			throws FileNodeProcessorException {
		if (bufferWriteProcessor == null) {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
			parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			String baseDir = directories.getNextFolderForTsfile();
			LOGGER.info("Allocate folder {} for the new bufferwrite processor.", baseDir);
			// construct processor or restore
			try {
				bufferWriteProcessor = new BufferWriteProcessor(baseDir, processorName,
						insertTime + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis(),
						parameters, fileSchema);
			} catch (BufferWriteProcessorException e) {
				LOGGER.error("The filenode processor {} failed to get the bufferwrite processor.", processorName, e);
				throw new FileNodeProcessorException(e);
			}
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

	public OverflowProcessor getOverflowProcessor(String processorName) throws IOException {
		if (overflowProcessor == null) {
			Map<String, Object> parameters = new HashMap<>();
			// construct processor or restore
			parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
			parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
			overflowProcessor = new OverflowProcessor(processorName, parameters, fileSchema);
		}
		return overflowProcessor;
	}

	public OverflowProcessor getOverflowProcessor() {
		if (overflowProcessor == null) {
			LOGGER.error("The overflow processor is null when getting the overflowProcessor");
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
		if (!lastUpdateTimeMap.containsKey(deltaObjectId) || lastUpdateTimeMap.get(deltaObjectId) < timestamp) {
			lastUpdateTimeMap.put(deltaObjectId, timestamp);
		}
	}

	public long getLastUpdateTime(String deltaObjectId) {

		if (lastUpdateTimeMap.containsKey(deltaObjectId)) {
			return lastUpdateTimeMap.get(deltaObjectId);
		} else {
			return -1;
		}
	}

	public long getFlushLastUpdateTime(String deltaObjectId) {
		if (!flushLastUpdateTimeMap.containsKey(deltaObjectId)) {
			flushLastUpdateTimeMap.put(deltaObjectId, 0L);
		}
		return flushLastUpdateTimeMap.get(deltaObjectId);
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
			LOGGER.warn(
					"Can not find any tsfile which will be overflowed in the filenode processor {}, the data is [deltaObject:{},time:{}]",
					getProcessorName(), deltaObjectId, timestamp);
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
			LOGGER.warn(
					"Can not find any tsfile which will be overflowed in the filenode processor {}, the data is [deltaObject:{}, start time:{}, end time:{}]",
					getProcessorName(), deltaObjectId, startTime, endTime);
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
			LOGGER.warn(
					"Can not find any tsfile which will be overflowed in the filenode processor {}, the data is [deltaObject:{}, delete time:{}]",
					getProcessorName(), deltaObjectId, timestamp);
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
		LOGGER.debug("Add multi token:{}, nsPath:{}.", multiPassLockToken, getProcessorName());
		return multiPassLockToken;
	}

	public boolean removeMultiPassLock(int token) {
		if (newMultiPassTokenSet.contains(token)) {
			newMultiPassLock.readLock().unlock();
			newMultiPassTokenSet.remove(token);
			LOGGER.debug("Remove multi token:{}, nspath:{}, new set:{}, lock:{}", token, getProcessorName(),
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

	public QueryDataSource query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
			throws FileNodeProcessorException {
		// query overflow data
		TSDataType dataType = null;
		try {
			dataType = mManager.getSeriesType(deltaObjectId + "." + measurementId);
		} catch (PathErrorException e) {
			throw new FileNodeProcessorException(e);
		}
		OverflowSeriesDataSource overflowSeriesDataSource;
		try {
			overflowSeriesDataSource = overflowProcessor.query(deltaObjectId, measurementId, timeFilter, freqFilter,
					valueFilter, dataType);
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileNodeProcessorException(e);
		}
		// tsfile data
		List<IntervalFileNode> bufferwriteDataInFiles = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			// add the same intervalFileNode, but not the same reference
			if (intervalFileNode.isClosed()) {
				bufferwriteDataInFiles.add(intervalFileNode.backUp());
			}
		}
		Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> bufferwritedata = new Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>>(
				null, null);
		// bufferwrite data
		UnsealedTsFile unsealedTsFile = null;

		if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()
				&& !newFileNodes.get(newFileNodes.size() - 1).getStartTimeMap().isEmpty()) {
			unsealedTsFile = new UnsealedTsFile();
			unsealedTsFile.setFilePath(newFileNodes.get(newFileNodes.size() - 1).getFilePath());
			if (bufferWriteProcessor == null) {
				LOGGER.error(
						"The last of tsfile {} in filenode processor {} is not closed, but the bufferwrite processor is null.",
						newFileNodes.get(newFileNodes.size() - 1).getRelativePath(), getProcessorName());
				throw new FileNodeProcessorException(String.format(
						"The last of tsfile %s in filenode processor %s is not closed, but the bufferwrite processor is null.",
						newFileNodes.get(newFileNodes.size() - 1).getRelativePath(), getProcessorName()));
			}
			bufferwritedata = bufferWriteProcessor.queryBufferwriteData(deltaObjectId, measurementId, dataType);
			unsealedTsFile.setTimeSeriesChunkMetaDatas(bufferwritedata.right);
		}
		GlobalSortedSeriesDataSource globalSortedSeriesDataSource = new GlobalSortedSeriesDataSource(
				new Path(deltaObjectId + "." + measurementId), bufferwriteDataInFiles, unsealedTsFile,
				bufferwritedata.left);
		return new QueryDataSource(globalSortedSeriesDataSource, overflowSeriesDataSource);

	}

	public List<DataFileInfo> indexQuery(String deltaObjectId, long startTime, long endTime) {
		List<DataFileInfo> dataFileInfos = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : newFileNodes) {
			if (intervalFileNode.isClosed()) {
				long s1 = intervalFileNode.getStartTime(deltaObjectId);
				long e1 = intervalFileNode.getEndTime(deltaObjectId);
				if (e1 >= startTime && (s1 <= endTime || endTime == -1)) {
					DataFileInfo dataFileInfo = new DataFileInfo(s1, e1, intervalFileNode.getFilePath());
					dataFileInfos.add(dataFileInfo);
				}
			}
		}
		return dataFileInfos;
	}

	/**
	 * append one specified tsfile to this filenode processor
	 * 
	 * @param appendFile
	 *            the appended tsfile information
	 * @param appendFilePath
	 *            the path of appended file
	 * @throws FileNodeProcessorException
	 */
	public void appendFile(IntervalFileNode appendFile, String appendFilePath) throws FileNodeProcessorException {
		try {
			// move file
			File originFile = new File(appendFilePath);
			File targetFile = new File(appendFile.getFilePath());
			if (!originFile.exists()) {
				throw new FileNodeProcessorException(
						String.format("The appended file %s does not exist.", appendFilePath));
			}
			if (targetFile.exists()) {
				throw new FileNodeProcessorException(
						String.format("The appended target file %s already exists.", appendFile.getFilePath()));
			}
			originFile.renameTo(targetFile);
			// append the new tsfile
			this.newFileNodes.add(appendFile);
			// update the lastUpdateTime
			for (Entry<String, Long> entry : appendFile.getEndTimeMap().entrySet()) {
				lastUpdateTimeMap.put(entry.getKey(), entry.getValue());
			}
			bufferwriteFlushAction.act();
			fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			// reconstruct the inverted index of the newFileNodes
			flushFileNodeProcessorAction.act();
			addALLFileIntoIndex(newFileNodes);
		} catch (Exception e) {
			LOGGER.error("Failed to append the tsfile {} to filenode processor {}.", appendFile, getProcessorName(), e);
			throw new FileNodeProcessorException(e);
		}
	}

	public void addTimeSeries(String measurementToString, String dataType, String encoding, String[] encodingArgs) {
		ColumnSchema col = new ColumnSchema(measurementToString, TSDataType.valueOf(dataType),
				TSEncoding.valueOf(encoding));
		JSONObject measurement = constrcutMeasurement(col);
		fileSchema.registerMeasurement(JsonConverter.convertJsonToMeasureMentDescriptor(measurement));
	}

	private JSONObject constrcutMeasurement(ColumnSchema col) {
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
		return measurement;
	}

	/**
	 * submit the merge task to the <code>MergePool</code>
	 * 
	 * @return null -can't submit the merge task, because this filenode is not
	 *         overflowed or it is merging now. Future<?> - submit the merge
	 *         task successfully.
	 */
	public Future<?> submitToMerge() {
		if (lastMergeTime > 0) {
			long thisMergeTime = System.currentTimeMillis();
			long mergeTimeInterval = thisMergeTime - lastMergeTime;
			DateTime lastDateTime = new DateTime(lastMergeTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime thisDateTime = new DateTime(thisMergeTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info("The filenode {} last merge time is {}, this merge time is {}, merge time interval is {}s",
					getProcessorName(), lastDateTime, thisDateTime, mergeTimeInterval / 1000);
		}
		lastMergeTime = System.currentTimeMillis();

		if (overflowProcessor != null) {
			if (overflowProcessor
					.getFileSize() < TsfileDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold) {
				LOGGER.info(
						"Skip this merge taks submission, because the size{} of overflow processor {} does not reaches the threshold {}.",
						MemUtils.bytesCntToStr(overflowProcessor.getFileSize()), getProcessorName(),
						MemUtils.bytesCntToStr(TsfileDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold));
				return null;
			}
		} else {
			LOGGER.info("Skip this merge taks submission, because the filenode processor {} has no overflow processor.",
					getProcessorName());
			return null;
		}
		if (isOverflowed && isMerging == FileNodeProcessorStatus.NONE) {
			Runnable MergeThread;
			MergeThread = () -> {
				try {
					long mergeStartTime = System.currentTimeMillis();
					writeLock();
					merge();
					long mergeEndTime = System.currentTimeMillis();
					DateTime startDateTime = new DateTime(mergeStartTime,
							TsfileDBDescriptor.getInstance().getConfig().timeZone);
					DateTime endDateTime = new DateTime(mergeEndTime,
							TsfileDBDescriptor.getInstance().getConfig().timeZone);
					long intervalTime = mergeEndTime - mergeStartTime;
					LOGGER.info(
							"The filenode processor {} merge start time is {}, merge end time is {}, merge consumes {}ms.",
							getProcessorName(), startDateTime, endDateTime, intervalTime);
				} catch (FileNodeProcessorException e) {
					LOGGER.error("The filenode processor {} encountered an error when merging.", getProcessorName(), e);
					throw new ErrorDebugException(e);
				}
			};
			LOGGER.info("Submit the merge task, the merge filenode is {}", getProcessorName());
			return MergeManager.getInstance().submit(MergeThread);
		} else {
			if (!isOverflowed) {
				LOGGER.info("Skip this merge taks submission, because the filenode processor {} is not overflowed.",
						getProcessorName());
			} else {
				LOGGER.warn(
						"Skip this merge task submission, because last merge task is not over yet, the merge filenode processor is {}",
						getProcessorName());
			}
		}
		return null;
	}

	/**
	 * Prepare for merge, close the bufferwrite and overflow
	 */
	private void prepareForMerge() {
		try {
			LOGGER.info("The filenode processor {} prepares for merge, closes the bufferwrite processor",
					getProcessorName());
			closeBufferWrite();
			// try to get overflow processor
			getOverflowProcessor(getProcessorName());
			// must close the overflow processor
			while (!getOverflowProcessor().canBeClosed()) {
				try {
					LOGGER.info(
							"The filenode processor {} prepares for merge, the overflow {} can't be closed, wait 100ms,",
							getProcessorName(), getProcessorName());
					TimeUnit.MICROSECONDS.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			LOGGER.info("The filenode processor {} prepares for merge, closes the overflow processor",
					getProcessorName());
			getOverflowProcessor().close();
		} catch (FileNodeProcessorException | OverflowProcessorException | IOException e) {
			e.printStackTrace();
			LOGGER.error("The filenode processor {} prepares for merge error.", getProcessorName(), e);
			writeUnlock();
			throw new ErrorDebugException(e);
		}
	}

	/**
	 * Merge this storage group, merge the tsfile data with overflow data.
	 * 
	 * @throws FileNodeProcessorException
	 */
	public void merge() throws FileNodeProcessorException {
		//
		// close bufferwrite and overflow, prepare for merge
		//
		LOGGER.info("The filenode processor {} begins to merge.", getProcessorName());
		prepareForMerge();
		//
		// change status from overflowed to no overflowed
		//
		isOverflowed = false;
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
			fileNodeProcessorStore.setOverflowed(isOverflowed);
			fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
			fileNodeProcessorStore.setNewFileNodes(newFileNodes);
			fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
			// flush this filenode information
			try {
				writeStoreToDisk(fileNodeProcessorStore);
			} catch (FileNodeProcessorException e) {
				LOGGER.error("The filenode processor {} writes restore information error when merging.",
						getProcessorName(), e);
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
		// attention
		try {
			overflowProcessor.switchWorkToMerge();
		} catch (IOException e) {
			LOGGER.error("The filenode processor {} can't switch overflow processor from work to merge.",
					getProcessorName(), e);
			writeUnlock();
			throw new FileNodeProcessorException(e);
		}
		LOGGER.info("The filenode processor {} switches from {} to {}.", getProcessorName(),
				FileNodeProcessorStatus.NONE, FileNodeProcessorStatus.MERGING_WRITE);
		writeUnlock();

		// query tsfile data and overflow data, and merge them
		int numOfMergeFiles = 0;
		int allNeedMergeFiles = backupIntervalFiles.size();
		for (IntervalFileNode backupIntervalFile : backupIntervalFiles) {
			numOfMergeFiles++;
			if (backupIntervalFile.overflowChangeType == OverflowChangeType.CHANGED) {
				// query data and merge
				String filePathBeforeMerge = backupIntervalFile.getRelativePath();
				try {
					LOGGER.info(
							"The filenode processor {} begins merging the {}/{} tsfile[{}] with overflow file, the process is {}%",
							getProcessorName(), numOfMergeFiles, allNeedMergeFiles, filePathBeforeMerge,
							(int) (((numOfMergeFiles - 1) / (float) allNeedMergeFiles) * 100));
					long startTime = System.currentTimeMillis();
					String newFile = queryAndWriteDataForMerge(backupIntervalFile);
					long endTime = System.currentTimeMillis();
					long timeConsume = endTime - startTime;
					DateTime startDateTime = new DateTime(startTime,
							TsfileDBDescriptor.getInstance().getConfig().timeZone);
					DateTime endDateTime = new DateTime(endTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
					LOGGER.info(
							"The fileNode processor {} has merged the {}/{} tsfile[{}->{}] over, start time of merge is {}, end time of merge is {}, time consumption is {}ms, the process is {}%",
							getProcessorName(), numOfMergeFiles, allNeedMergeFiles, filePathBeforeMerge, newFile,
							startDateTime, endDateTime, timeConsume,
							(int) (numOfMergeFiles) / (float) allNeedMergeFiles * 100);
				} catch (IOException | WriteProcessException | PathErrorException e) {
					LOGGER.error("Merge: query and write data error.", e);
					throw new FileNodeProcessorException(e);
				}
			} else if (backupIntervalFile.overflowChangeType == OverflowChangeType.MERGING_CHANGE) {
				LOGGER.error("The overflowChangeType of backupIntervalFile must not be {}",
						OverflowChangeType.MERGING_CHANGE);
				// handle this error, throw one runtime exception
				throw new FileNodeProcessorException("The overflowChangeType of backupIntervalFile must not be "
						+ OverflowChangeType.MERGING_CHANGE);
			} else {
				LOGGER.debug("The filenode processor {} is merging, the interval file {} doesn't need to be merged.",
						getProcessorName(), backupIntervalFile.getRelativePath());
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
							intervalFileNode.overflowChangeType, intervalFileNode.getBaseDirIndex(), intervalFileNode.getRelativePath());
					result.add(node);
				}
			}
		} else {
			LOGGER.error("No file was changed when merging, the filenode is {}", getProcessorName());
			throw new FileNodeProcessorException(
					"No file was changed when merging, the filenode is " + getProcessorName());
		}
		return result;
	}

	private List<DataFileInfo> getDataFileInfoForIndex(Path path, List<IntervalFileNode> sourceFileNodes) {
		String deltaObjectId = path.getDeltaObjectToString();
		List<DataFileInfo> dataFileInfos = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : sourceFileNodes) {
			if (intervalFileNode.isClosed()) {
				if (intervalFileNode.getStartTime(deltaObjectId) != -1) {
					DataFileInfo dataFileInfo = new DataFileInfo(intervalFileNode.getStartTime(deltaObjectId),
							intervalFileNode.getEndTime(deltaObjectId), intervalFileNode.getFilePath());
					dataFileInfos.add(dataFileInfo);
				}
			}
		}
		return dataFileInfos;
	}

	private void mergeIndex() throws FileNodeProcessorException {
		try {
			Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(getProcessorName());
			if (!allIndexSeries.isEmpty()) {
				LOGGER.info("merge all file and modify index file, the nameSpacePath is {}, the index path is {}",
						getProcessorName(), allIndexSeries);
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
			LOGGER.error("Failed to find all fileList to be merged. Because" + e.getMessage());
			throw new FileNodeProcessorException(e.getMessage());
		}
	}

	private void switchMergeIndex() throws FileNodeProcessorException {
		try {
			Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(getProcessorName());
			if (!allIndexSeries.isEmpty()) {
				LOGGER.info("mergeswith all file and modify index file, the nameSpacePath is {}, the index path is {}",
						getProcessorName(), allIndexSeries);
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
			LOGGER.error("Failed to find all fileList to be mergeSwitch because of" + e.getMessage());
			throw new FileNodeProcessorException(e.getMessage());
		}
	}

	private void switchMergeToWaitingv2(List<IntervalFileNode> backupIntervalFiles, boolean needEmpty)
			throws FileNodeProcessorException {
		LOGGER.info("The status of filenode processor {} switches from {} to {}.", getProcessorName(),
				FileNodeProcessorStatus.MERGING_WRITE, FileNodeProcessorStatus.WAITING);
		writeLock();
		try {
			oldMultiPassTokenSet = newMultiPassTokenSet;
			oldMultiPassLock = newMultiPassLock;
			newMultiPassTokenSet = new HashSet<>();
			newMultiPassLock = new ReentrantReadWriteLock(false);
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
				IntervalFileNode fileNode = newFileNodes.get(i);
				if (fileNode.isClosed()) {
					result.add(fileNode.backUp());
				} else {
					result.add(fileNode);
				}
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
				fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
				fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
				fileNodeProcessorStore.setNewFileNodes(newFileNodes);
				try {
					writeStoreToDisk(fileNodeProcessorStore);
				} catch (FileNodeProcessorException e) {
					LOGGER.error("Merge: failed to write filenode information to revocery file, the filenode is {}.",
							getProcessorName(), e);
					throw new FileNodeProcessorException(
							"Merge: write filenode information to revocery file failed, the filenode is "
									+ getProcessorName());
				}
			}
		} finally {
			writeUnlock();
		}
	}

	private void switchWaitingToWorkingv2(List<IntervalFileNode> backupIntervalFiles)
			throws FileNodeProcessorException {

		LOGGER.info("The status of filenode processor {} switches from {} to {}.", getProcessorName(),
				FileNodeProcessorStatus.WAITING, FileNodeProcessorStatus.NONE);

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

				List<String> bufferwriteDirPathList = directories.getAllTsFileFolders();
				List<File> bufferwriteDirList = new ArrayList<>();
				for(String bufferwriteDirPath : bufferwriteDirPathList) {
					if (bufferwriteDirPath.length() > 0
							&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
						bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
					}
					bufferwriteDirPath = bufferwriteDirPath + getProcessorName();
					File bufferwriteDir = new File(bufferwriteDirPath);
					bufferwriteDirList.add(bufferwriteDir);
					if (!bufferwriteDir.exists()) {
						bufferwriteDir.mkdirs();
					}
				}

				Set<String> bufferFiles = new HashSet<>();
				for (IntervalFileNode bufferFileNode : newFileNodes) {
					String bufferFilePath = bufferFileNode.getFilePath();
					if (bufferFilePath != null) {
						bufferFiles.add(bufferFilePath);
					}
				}
				// add the restore file, if the last file is not closed
				if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
					String bufferFileRestorePath = newFileNodes.get(newFileNodes.size() - 1).getFilePath() + ".restore";
					bufferFiles.add(bufferFileRestorePath);
				}

				for(File bufferwriteDir : bufferwriteDirList) {
					for (File file : bufferwriteDir.listFiles()) {
						if (!bufferFiles.contains(file.getPath())) {
							file.delete();
						}
					}
				}

				// merge switch
				switchMergeIndex();

				for (IntervalFileNode fileNode : newFileNodes) {
					if (fileNode.overflowChangeType != OverflowChangeType.NO_CHANGE) {
						fileNode.overflowChangeType = OverflowChangeType.CHANGED;
					}
				}
				// overflow switch from merge to work
				overflowProcessor.switchMergeToWork();
				// write status to file
				isMerging = FileNodeProcessorStatus.NONE;
				synchronized (fileNodeProcessorStore) {
					fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
					fileNodeProcessorStore.setNewFileNodes(newFileNodes);
					fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
					writeStoreToDisk(fileNodeProcessorStore);
				}
			} catch (IOException e) {
				LOGGER.info("The filenode processor {} encountered an error when its status switched from {} to {}.",
						getProcessorName(), FileNodeProcessorStatus.NONE, FileNodeProcessorStatus.MERGING_WRITE, e);
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

	private TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
		TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
		record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
				timeValuePair.getValue().getValue().toString()));
		return record;
	}

	private String queryAndWriteDataForMerge(IntervalFileNode backupIntervalFile)
			throws IOException, WriteProcessException, FileNodeProcessorException, PathErrorException {
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();

		TsFileWriter recordWriter = null;
		String outputPath = null;
		String baseDir = null;
		String fileName = null;
		for (String deltaObjectId : backupIntervalFile.getStartTimeMap().keySet()) {
			// query one deltaObjectId
			List<Path> pathList = new ArrayList<>();
			try {
				List<String> pathStrings = mManager.getLeafNodePathInNextLevel(deltaObjectId);
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
			long startTime = -1;
			long endTime = -1;
			for (Path path : pathList) {
				// query one measurenment in the special deltaObjectId
				TSDataType dataType = mManager.getSeriesType(path.getFullPath());
				OverflowSeriesDataSource overflowSeriesDataSource = overflowProcessor.queryMerge(deltaObjectId,
						path.getMeasurementToString(), dataType, true);
				Filter<Long> timeFilter = FilterFactory.and(
						TimeFilter.gtEq(backupIntervalFile.getStartTime(deltaObjectId)),
						TimeFilter.ltEq(backupIntervalFile.getEndTime(deltaObjectId)));
				SeriesFilter<Long> seriesFilter = new SeriesFilter<>(path, timeFilter);
				SeriesReader seriesReader = SeriesReaderFactory.getInstance()
						.createSeriesReaderForMerge(backupIntervalFile, overflowSeriesDataSource, seriesFilter);
				try {
					if (!seriesReader.hasNext()) {
						LOGGER.debug("The time-series {} has no data with the filter {} in the filenode processor {}",
								path, seriesFilter, getProcessorName());
					} else {
						TimeValuePair timeValuePair = seriesReader.next();
						if (recordWriter == null) {
							baseDir = directories.getNextFolderForTsfile();
							fileName = String.valueOf(timeValuePair.getTimestamp()
									+ FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis());
							outputPath = constructOutputFilePath(baseDir, getProcessorName(), fileName);
							fileName = getProcessorName() + File.separatorChar + fileName;
							recordWriter = new TsFileWriter(new File(outputPath), fileSchema, TsFileConf);
						}
						TSRecord record = constructTsRecord(timeValuePair, deltaObjectId,
								path.getMeasurementToString());
						recordWriter.write(record);
						startTime = endTime = timeValuePair.getTimestamp();
						if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
							startTimeMap.put(deltaObjectId, startTime);
						}
						if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
							endTimeMap.put(deltaObjectId, endTime);
						}
						while (seriesReader.hasNext()) {
							record = constructTsRecord(seriesReader.next(), deltaObjectId,
									path.getMeasurementToString());
							endTime = record.time;
							recordWriter.write(record);
						}
						if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
							endTimeMap.put(deltaObjectId, endTime);
						}
					}
				} finally {
					seriesReader.close();
				}
			}
		}
		if (recordWriter != null) {
			recordWriter.close();
		}
		backupIntervalFile.setBaseDirIndex(directories.getTsFileFolderIndex(baseDir));
		backupIntervalFile.setRelativePath(fileName);
		backupIntervalFile.overflowChangeType = OverflowChangeType.NO_CHANGE;
		backupIntervalFile.setStartTimeMap(startTimeMap);
		backupIntervalFile.setEndTimeMap(endTimeMap);
		return fileName;
	}

	private String constructOutputFilePath(String baseDir, String processorName, String fileName) {

		if (baseDir.charAt(baseDir.length() - 1) != File.separatorChar) {
			baseDir = baseDir + File.separatorChar + processorName;
		}
		File dataDir = new File(baseDir);
		if (!dataDir.exists()) {
			LOGGER.warn("The bufferwrite processor data dir doesn't exists, create new directory {}", baseDir);
			dataDir.mkdirs();
		}
		File outputFile = new File(dataDir, fileName);
		return outputFile.getPath();
	}

	private FileSchema constructFileSchema(String processorName) throws WriteProcessException {

		List<ColumnSchema> columnSchemaList;
		columnSchemaList = mManager.getSchemaForFileName(processorName);

		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
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
						} else {
							LOGGER.info("The filenode {} can't be closed, because it can't get oldMultiPassLock {}",
									getProcessorName(), oldMultiPassLock);
							return false;
						}
					} else {
						return true;
					}
				} finally {
					newMultiPassLock.writeLock().unlock();
				}
			} else {
				LOGGER.info("The filenode {} can't be closed, because it can't get newMultiPassLock {}",
						getProcessorName(), newMultiPassLock);
				return false;
			}
		} else {
			LOGGER.info("The filenode {} can't be closed, because the filenode status is {}", getProcessorName(),
					isMerging);
			return false;
		}
	}

	@Override
	public boolean flush() throws IOException {
		if (bufferWriteProcessor != null) {
			bufferWriteProcessor.flush();
		}
		if (overflowProcessor != null) {
			return overflowProcessor.flush();
		}
		return false;
	}

	/**
	 * Close the bufferwrite processor
	 * 
	 * @throws FileNodeProcessorException
	 */
	public void closeBufferWrite() throws FileNodeProcessorException {
		if (bufferWriteProcessor != null) {
			try {
				while (!bufferWriteProcessor.canBeClosed()) {
					try {
						LOGGER.info("The bufferwrite {} can't be closed, wait 100ms",
								bufferWriteProcessor.getProcessorName());
						TimeUnit.MICROSECONDS.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				bufferWriteProcessor.close();
				bufferWriteProcessor = null;
				/**
				 * add index for close
				 */
				Map<String, Set<IndexType>> allIndexSeries = mManager.getAllIndexPaths(getProcessorName());

				if (!allIndexSeries.isEmpty()) {
					LOGGER.info(
							"Close buffer write file and append index file, the nameSpacePath is {}, the index "
									+ "type is {}, the index path is {}",
							getProcessorName(), "kvindex", allIndexSeries);
					for (Entry<String, Set<IndexType>> entry : allIndexSeries.entrySet()) {
						Path path = new Path(entry.getKey());
						String deltaObjectId = path.getDeltaObjectToString();
						if (currentIntervalFileNode.getStartTime(deltaObjectId) != -1) {
							DataFileInfo dataFileInfo = new DataFileInfo(
									currentIntervalFileNode.getStartTime(deltaObjectId),
									currentIntervalFileNode.getEndTime(deltaObjectId),
									currentIntervalFileNode.getFilePath());
							for (IndexType indexType : entry.getValue())
								IndexManager.getIndexInstance(indexType).build(path, dataFileInfo, null);
						}
					}
				}
			} catch (BufferWriteProcessorException | PathErrorException | IndexManagerException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
	}

	/**
	 * Close the overflow processor
	 * 
	 * @throws FileNodeProcessorException
	 */
	public void closeOverflow() throws FileNodeProcessorException {
		// close overflow
		if (overflowProcessor != null) {
			try {
				while (!overflowProcessor.canBeClosed()) {
					try {
						LOGGER.info("The overflow {} can't be closed, wait 100ms",
								overflowProcessor.getProcessorName());
						TimeUnit.MICROSECONDS.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				overflowProcessor.close();
				overflowProcessor.clear();
				overflowProcessor = null;
			} catch (OverflowProcessorException | IOException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
	}

	@Override
	public void close() throws FileNodeProcessorException {
		closeBufferWrite();
		closeOverflow();
	}

	public void delete() throws ProcessorException {
		if (TsFileDBConf.enableStatMonitor) {
			// remove the monitor
			LOGGER.info("Deregister the filenode processor: {} from monitor.", getProcessorName());
			StatMonitor.getInstance().deregistStatistics(statStorageDeltaName);
		}
		closeBufferWrite();
		closeOverflow();
	}

	@Override
	public long memoryUsage() {
		long memSize = 0;
		if (bufferWriteProcessor != null) {
			memSize += bufferWriteProcessor.memoryUsage();
		}
		if (overflowProcessor != null) {
			memSize += overflowProcessor.memoryUsage();
		}
		return memSize;
	}

	private void writeStoreToDisk(FileNodeProcessorStore fileNodeProcessorStore) throws FileNodeProcessorException {

		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				serializeUtil.serialize(fileNodeProcessorStore, fileNodeRestoreFilePath);
				LOGGER.debug("The filenode processor {} writes restore information to the restore file",
						getProcessorName());
			} catch (IOException e) {
				throw new FileNodeProcessorException(e);
			}
		}
	}

	private FileNodeProcessorStore readStoreFromDisk() throws FileNodeProcessorException {

		synchronized (fileNodeRestoreFilePath) {
			FileNodeProcessorStore fileNodeProcessorStore = null;
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				fileNodeProcessorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
						.orElse(new FileNodeProcessorStore(false, new HashMap<>(),
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

	public String getFileNodeRestoreFilePath() {
		return fileNodeRestoreFilePath;
	}
}
