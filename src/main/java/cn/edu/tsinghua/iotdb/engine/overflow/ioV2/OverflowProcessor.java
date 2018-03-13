package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.memtable.MemSeriesLazyMerger;
import cn.edu.tsinghua.iotdb.engine.querycontext.*;
import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.writelog.node.WriteLogNode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.engine.pool.FlushManager;
import cn.edu.tsinghua.iotdb.engine.utils.FlushStatus;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class OverflowProcessor extends Processor {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private OverflowResource workResource;
	private OverflowResource mergeResource;

	private OverflowSupport workSupport;
	private OverflowSupport flushSupport;

	private volatile FlushStatus flushStatus = new FlushStatus();
	private volatile boolean isMerge;
	private int valueCount;
	private String parentPath;
	private long lastFlushTime = -1;
	private AtomicLong dataPahtCount = new AtomicLong();
	private ReentrantLock queryFlushLock = new ReentrantLock();

	private Action overflowFlushAction = null;
	private Action filenodeFlushAction = null;
	private FileSchema fileSchema;

	private long memThreshold = TsFileConf.groupSizeInByte;
	private AtomicLong memSize = new AtomicLong();

	private WriteLogNode logNode;

	public OverflowProcessor(String processorName, Map<String, Object> parameters, FileSchema fileSchema)
			throws IOException {
		super(processorName);
		this.fileSchema = fileSchema;
		String overflowDirPath = TsFileDBConf.overflowDataDir;
		if (overflowDirPath.length() > 0
				&& overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
			overflowDirPath = overflowDirPath + File.separatorChar;
		}
		this.parentPath = overflowDirPath + processorName;
		File processorDataDir = new File(parentPath);
		if (!processorDataDir.exists()) {
			processorDataDir.mkdirs();
		}
		// recover file
		recovery(processorDataDir);
		// memory
		workSupport = new OverflowSupport();
		overflowFlushAction = (Action) parameters.get(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);

		if (TsfileDBDescriptor.getInstance().getConfig().enableWal)
			logNode = MultiFileLogNodeManager.getInstance().getNode(
					processorName + TsFileDBConstant.OVERFLOW_LOG_NODE_SUFFIX, getOverflowRestoreFile(),
					FileNodeManager.getInstance().getRestoreFilePath(processorName));
	}

	private void recovery(File parentFile) throws IOException {
		String[] subFilePaths = clearFile(parentFile.list());
		if (subFilePaths.length == 0) {
			workResource = new OverflowResource(parentPath, String.valueOf(dataPahtCount.getAndIncrement()));
			return;
		} else if (subFilePaths.length == 1) {
			long count = Long.valueOf(subFilePaths[0]);
			dataPahtCount.addAndGet(count + 1);
			workResource = new OverflowResource(parentPath, String.valueOf(count));
			LOGGER.info("The overflow processor {} recover from work status.", getProcessorName());
		} else {
			long count1 = Long.valueOf(subFilePaths[0]);
			long count2 = Long.valueOf(subFilePaths[1]);
			if (count1 > count2) {
				long temp = count1;
				count1 = count2;
				count2 = temp;
			}
			dataPahtCount.addAndGet(count2 + 1);
			// work dir > merge dir
			workResource = new OverflowResource(parentPath, String.valueOf(count2));
			mergeResource = new OverflowResource(parentPath, String.valueOf(count1));
			LOGGER.info("The overflow processor {} recover from merge status.", getProcessorName());
		}
	}

	private String[] clearFile(String[] subFilePaths) {
		List<String> files = new ArrayList<>();
		for (String file : subFilePaths) {
			try {
				Long.valueOf(file);
				files.add(file);
			} catch (NumberFormatException e) {
			}
		}
		return files.toArray(new String[files.size()]);
	}

	/**
	 * insert one time-series record
	 *
	 * @param tsRecord
	 * @throws IOException
	 */
	public void insert(TSRecord tsRecord) throws IOException {
		// memory control
		long memUage = MemUtils.getRecordSize(tsRecord);
		BasicMemController.getInstance().reportUse(this, memUage);
		// write data
		workSupport.insert(tsRecord);
		valueCount++;
		// check flush
		memUage = memSize.addAndGet(memUage);
		if (memUage > memThreshold) {
			LOGGER.warn("The usage of memory {} in overflow processor {} reaches the threshold {}",
					MemUtils.bytesCntToStr(memUage), getProcessorName(), MemUtils.bytesCntToStr(memThreshold));
			flush();
		}
	}

	/**
	 * update one time-series data which time range is from startTime from
	 * endTime.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param startTime
	 * @param endTime
	 * @param type
	 * @param value
	 */
	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			byte[] value) {
		workSupport.update(deltaObjectId, measurementId, startTime, endTime, type, value);
		valueCount++;
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			String value) {
		workSupport.update(deltaObjectId, measurementId, startTime, endTime, type, convertStringToBytes(type, value));
		valueCount++;
	}

	private byte[] convertStringToBytes(TSDataType type, String o) {
		switch (type) {
		case INT32:
			return BytesUtils.intToBytes(Integer.valueOf(o));
		case INT64:
			return BytesUtils.longToBytes(Long.valueOf(o));
		case BOOLEAN:
			return BytesUtils.boolToBytes(Boolean.valueOf(o));
		case FLOAT:
			return BytesUtils.floatToBytes(Float.valueOf(o));
		case DOUBLE:
			return BytesUtils.doubleToBytes(Double.valueOf(o));
		case TEXT:
			return BytesUtils.StringToBytes(o);
		default:
			LOGGER.error("Unsupport data type: {}", type);
			throw new UnsupportedOperationException("Unsupport data type:" + type);
		}
	}

	/**
	 * delete one time-series data which time range is from 0 to time-stamp.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param type
	 */
	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type) {
		workSupport.delete(deltaObjectId, measurementId, timestamp, type);
		valueCount++;
	}

	/**
	 * query all overflow data which contain insert data in memory, insert data
	 * in file, update/delete data in memory, update/delete data in file.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param dataType
	 * @return OverflowSeriesDataSource
	 * @throws IOException
	 */
	public OverflowSeriesDataSource query(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) throws IOException {
		queryFlushLock.lock();
		try {
			// query insert data in memory and unseqTsFiles
			// memory
			RawSeriesChunk insertInMem = queryOverflowInsertInMemory(deltaObjectId, measurementId, timeFilter,
					freqFilter, valueFilter, dataType);

			List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
			// work file
			Pair<String, List<TimeSeriesChunkMetaData>> insertInDiskWork = queryWorkDataInOverflowInsert(deltaObjectId,
					measurementId, dataType);
			if (insertInDiskWork.left != null) {
				overflowInsertFileList.add(0, new OverflowInsertFile(insertInDiskWork.left, insertInDiskWork.right));
			}
			// merge file
			Pair<String, List<TimeSeriesChunkMetaData>> insertInDiskMerge = queryMergeDataInOverflowInsert(
					deltaObjectId, measurementId, dataType);
			if (insertInDiskMerge.left != null) {
				overflowInsertFileList.add(0, new OverflowInsertFile(insertInDiskMerge.left, insertInDiskMerge.right));
			}
			// query update/delete data in memory and overflowFiles
			UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries = new UpdateDeleteInfoOfOneSeries();
			// memory
			DynamicOneColumnData updateDataInMem = queryOverflowUpdateInMemory(deltaObjectId, measurementId, timeFilter,
					freqFilter, valueFilter, dataType);
			updateDeleteInfoOfOneSeries.setOverflowUpdateInMem(updateDataInMem);
			List<OverflowUpdateDeleteFile> overflowUpdateFileList = new ArrayList<>();
			// work file
			Pair<String, List<TimeSeriesChunkMetaData>> updateInDiskWork = queryWorkDataInOverflowUpdate(deltaObjectId,
					measurementId, dataType);
			if (updateInDiskWork.left != null) {
				overflowUpdateFileList.add(0,
						new OverflowUpdateDeleteFile(updateInDiskWork.left, updateInDiskWork.right));
			}
			// merge file
			Pair<String, List<TimeSeriesChunkMetaData>> updateInDiskMerge = queryMergeDataInOverflowUpdate(
					deltaObjectId, measurementId, dataType);
			if (updateInDiskMerge.left != null) {
				overflowUpdateFileList.add(0,
						new OverflowUpdateDeleteFile(updateInDiskMerge.left, updateInDiskMerge.right));
			}
			updateDeleteInfoOfOneSeries.setOverflowUpdateFileList(overflowUpdateFileList);
			updateDeleteInfoOfOneSeries.setDataType(dataType);
			return new OverflowSeriesDataSource(new Path(deltaObjectId + "." + measurementId), dataType,
					overflowInsertFileList, insertInMem, updateDeleteInfoOfOneSeries);
		} finally {
			queryFlushLock.unlock();
		}
	}

	/**
	 * query insert data in memory table. while flushing, merge the work memory
	 * table with flush memory table.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param dataType
	 * @return insert data in SeriesChunkInMemTable
	 */
	private RawSeriesChunk queryOverflowInsertInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) {

		MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
		if (flushStatus.isFlushing()) {
			memSeriesLazyMerger
					.addMemSeries(flushSupport.queryOverflowInsertInMemory(deltaObjectId, measurementId, dataType));
		}
		memSeriesLazyMerger
				.addMemSeries(workSupport.queryOverflowInsertInMemory(deltaObjectId, measurementId, dataType));
		return new RawSeriesChunkLazyLoadImpl(dataType, memSeriesLazyMerger);
	}

	/**
	 * query update/delete data in memory. while flushing, merge the work
	 * {@code IntervalTreeOperation} with flush {@code IntervalTreeOperation}}.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param dataType
	 * @return update/delete result in DynamicOneColumnData
	 */
	private DynamicOneColumnData queryOverflowUpdateInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) {
		DynamicOneColumnData columnData = workSupport.queryOverflowUpdateInMemory(deltaObjectId, measurementId,
				timeFilter, freqFilter, valueFilter, dataType, null);
		if (flushStatus.isFlushing()) {
			columnData = flushSupport.queryOverflowUpdateInMemory(deltaObjectId, measurementId, timeFilter, freqFilter,
					valueFilter, dataType, columnData);
		}
		return columnData;
	}

	/**
	 * Get the update/delete data which is WORK in overflowFile.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of overflowFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryWorkDataInOverflowUpdate(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				workResource.getUpdateDeleteFilePath(),
				workResource.getUpdateDeleteMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the insert data which is WORK in unseqTsFile.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of unseqTsFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryWorkDataInOverflowInsert(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				workResource.getInsertFilePath(),
				workResource.getInsertMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the all merge data in unseqTsFile and overflowFile.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return MergeSeriesDataSource
	 */
	public MergeSeriesDataSource queryMerge(String deltaObjectId, String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> mergeInsert = queryMergeDataInOverflowInsert(deltaObjectId,
				measurementId, dataType);
		Pair<String, List<TimeSeriesChunkMetaData>> mergeUpdate = queryMergeDataInOverflowUpdate(deltaObjectId,
				measurementId, dataType);
		return new MergeSeriesDataSource(new OverflowInsertFile(mergeInsert.left, mergeInsert.right),
				new OverflowUpdateDeleteFile(mergeUpdate.left, mergeUpdate.right));
	}

	public OverflowSeriesDataSource queryMerge(String deltaObjectId, String measurementId, TSDataType dataType,
			boolean isMerge) {
		Pair<String, List<TimeSeriesChunkMetaData>> mergeInsert = queryMergeDataInOverflowInsert(deltaObjectId,
				measurementId, dataType);
		Pair<String, List<TimeSeriesChunkMetaData>> mergeUpdate = queryMergeDataInOverflowUpdate(deltaObjectId,
				measurementId, dataType);
		OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(
				new Path(deltaObjectId + "." + measurementId));
		overflowSeriesDataSource.setRawSeriesChunk(null);
		overflowSeriesDataSource
				.setOverflowInsertFileList(Arrays.asList(new OverflowInsertFile(mergeInsert.left, mergeInsert.right)));
		UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries = new UpdateDeleteInfoOfOneSeries();
		updateDeleteInfoOfOneSeries.setDataType(dataType);
		updateDeleteInfoOfOneSeries.setOverflowUpdateInMem(null);
		updateDeleteInfoOfOneSeries.setOverflowUpdateFileList(
				Arrays.asList(new OverflowUpdateDeleteFile(mergeUpdate.left, mergeUpdate.right)));
		overflowSeriesDataSource.setUpdateDeleteInfoOfOneSeries(updateDeleteInfoOfOneSeries);
		return overflowSeriesDataSource;
	}

	/**
	 * Get the update/delete data which is MERGE in overflowFile.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of overflowFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryMergeDataInOverflowUpdate(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		if (!isMerge) {
			return new Pair<String, List<TimeSeriesChunkMetaData>>(null, null);
		}
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				mergeResource.getUpdateDeleteFilePath(),
				mergeResource.getUpdateDeleteMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the insert data which is MERGE in unseqTsFile
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of unseqTsFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryMergeDataInOverflowInsert(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		if (!isMerge) {
			return new Pair<String, List<TimeSeriesChunkMetaData>>(null, null);
		}
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				mergeResource.getInsertFilePath(),
				mergeResource.getInsertMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	private void switchWorkToFlush() {
		queryFlushLock.lock();
		try {
			flushSupport = workSupport;
			workSupport = new OverflowSupport();
		} finally {
			queryFlushLock.unlock();
		}
	}

	private void switchFlushToWork() {
		queryFlushLock.lock();
		try {
			flushSupport.clear();
			workResource.appendMetadatas();
			flushSupport = null;
		} finally {
			queryFlushLock.unlock();
		}
	}

	public void switchWorkToMerge() throws IOException {
		if (mergeResource == null) {
			mergeResource = workResource;
			// TODO: NEW ONE workResource
			workResource = new OverflowResource(parentPath, String.valueOf(dataPahtCount.getAndIncrement()));
		}
		isMerge = true;
		LOGGER.info("The overflow processor {} switch from WORK to MERGE", getProcessorName());
	}

	public void switchMergeToWork() throws IOException {
		if (mergeResource != null) {
			mergeResource.close();
			mergeResource.deleteResource();
			mergeResource = null;
		}
		isMerge = false;
		LOGGER.info("The overflow processor {} switch from MERGE to WORK", getProcessorName());
	}

	public boolean isMerge() {
		return isMerge;
	}

	public boolean isFlush() {
		synchronized (flushStatus) {
			return flushStatus.isFlushing();
		}
	}

	private void flushOperation(String flushFunction) {
		long flushStartTime = System.currentTimeMillis();
		try {
			LOGGER.info("The overflow processor {} starts flushing {}.", getProcessorName(), flushFunction);
			// flush data
			workResource.flush(fileSchema, flushSupport.getMemTabale(), flushSupport.getOverflowSeriesMap(),
					getProcessorName());
			filenodeFlushAction.act();
			// write-ahead log
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				logNode.notifyEndFlush(null);
			}
		} catch (IOException e) {
			LOGGER.error("Flush overflow processor {} rowgroup to file error in {}. Thread {} exits.",
					getProcessorName(), flushFunction, Thread.currentThread().getName(), e);
		} catch (Exception e) {
			LOGGER.error("FilenodeFlushAction action failed. Thread {} exits.", Thread.currentThread().getName(), e);
		} finally {
			synchronized (flushStatus) {
				flushStatus.setUnFlushing();
				// switch from flush to work.
				switchFlushToWork();
				flushStatus.notify();
			}
			// BasicMemController.getInstance().reportFree(this,
			// oldMemUsage);
		}
		// log flush time
		LOGGER.info("The overflow processor {} ends flushing {}.", getProcessorName(), flushFunction);
		long flushEndTime = System.currentTimeMillis();
		long timeInterval = flushEndTime - flushStartTime;
		DateTime startDateTime = new DateTime(flushStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		DateTime endDateTime = new DateTime(flushEndTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		LOGGER.info(
				"The overflow processor {} flush {}, start time is {}, flush end time is {}, time consumption is {}ms",
				getProcessorName(), flushFunction, startDateTime, endDateTime, timeInterval);
	}

	private Future<?> flush(boolean synchronization) throws OverflowProcessorException {
		// statistic information for flush
		if (lastFlushTime > 0) {
			long thisFLushTime = System.currentTimeMillis();
			DateTime lastDateTime = new DateTime(lastFlushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime thisDateTime = new DateTime(thisFLushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info(
					"The overflow processor {} last flush time is {}, this flush time is {}, flush time interval is {}s",
					getProcessorName(), lastDateTime, thisDateTime, (thisFLushTime - lastFlushTime) / 1000);
		}
		lastFlushTime = System.currentTimeMillis();
		// value count
		if (valueCount > 0) {
			synchronized (flushStatus) {
				while (flushStatus.isFlushing()) {
					try {
						flushStatus.wait();
					} catch (InterruptedException e) {
						LOGGER.error("Waiting the flushstate error in flush row group to store.", e);
					}
				}
			}
			try {
				// backup newIntervalFile list and emptyIntervalFileNode
				overflowFlushAction.act();
			} catch (Exception e) {
				LOGGER.error("Flush the overflow rowGroup to file faied, when overflowFlushAction act");
				throw new OverflowProcessorException(e);
			}

			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				try {
					logNode.notifyStartFlush();
				} catch (IOException e) {
					LOGGER.error("Overflow processor {} encountered an error when notifying log node, {}",
							getProcessorName(), e.getMessage());
				}
			}
			BasicMemController.getInstance().reportFree(this, memSize.get());
			memSize.set(0);
			valueCount = 0;
			// switch from work to flush
			flushStatus.setFlushing();
			switchWorkToFlush();
			if (synchronization) {
				flushOperation("synchronously");
			} else {
				FlushManager.getInstance().submit(new Runnable() {
					public void run() {
						flushOperation("asynchronously");
					}
				});
			}
		}
		return null;
	}

	@Override
	public boolean flush() throws IOException {
		try {
			flush(false);
		} catch (OverflowProcessorException e) {
			throw new IOException(e);
		}
		return false;
	}

	@Override
	public void close() throws OverflowProcessorException {
		LOGGER.info("The overflow processor {} starts close operation.", getProcessorName());
		long closeStartTime = System.currentTimeMillis();
		// flush data
		flush(true);
		LOGGER.info("The overflow processor {} ends close operation.", getProcessorName());
		// log close time
		long closeEndTime = System.currentTimeMillis();
		long timeInterval = closeEndTime - closeStartTime;
		DateTime startDateTime = new DateTime(closeStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		DateTime endDateTime = new DateTime(closeStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		LOGGER.info("The close operation of overflow processor {} starts at {} and ends at {}. It comsumes {}ms.",
				getProcessorName(), startDateTime, endDateTime, timeInterval);
	}

	public void clear() throws IOException {
		if (workResource != null) {
			workResource.close();
		}
		if (mergeResource != null) {
			mergeResource.close();
		}
	}

	@Override
	public boolean canBeClosed() {
		// TODO: consider merge
		return !isMerge;
	}

	@Override
	public long memoryUsage() {
		return memSize.get();
	}

	public String getOverflowRestoreFile() {
		return workResource.getPositionFilePath();
	}

	/**
	 * @return The sum of all timeseries's metadata size within this file.
	 */
	public long getMetaSize() {
		// TODO : [MemControl] implement this
		return 0;
	}

	/**
	 * @return The size of overflow file corresponding to this processor.
	 */
	public long getFileSize() {
		return workResource.getInsertFile().length() + workResource.getUpdateDeleteFile().length() + memoryUsage();
	}

	/**
	 * Close current OverflowFile and open a new one for future writes. Block
	 * new writes and wait until current writes finish.
	 */
	private void rollToNewFile() {
		// TODO : [MemControl] implement this
	}

	/**
	 * Check whether current overflow file contains too many metadata or size of
	 * current overflow file is too large If true, close current file and open a
	 * new one.
	 */
	private boolean checkSize() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		long metaSize = getMetaSize();
		long fileSize = getFileSize();
		LOGGER.info("The overflow processor {}, the size of metadata reaches {}, the size of file reaches {}.",
				getProcessorName(), MemUtils.bytesCntToStr(metaSize), MemUtils.bytesCntToStr(fileSize));
		if (metaSize >= config.overflowMetaSizeThreshold || fileSize >= config.overflowFileSizeThreshold) {
			LOGGER.info(
					"The overflow processor {}, size({}) of the file {} reaches threshold {}, size({}) of metadata reaches threshold {}.",
					getProcessorName(), MemUtils.bytesCntToStr(fileSize), workResource.getInsertFilePath(),
					MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold), MemUtils.bytesCntToStr(metaSize),
					MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold));
			rollToNewFile();
			return true;
		} else {
			return false;
		}
	}

	public WriteLogNode getLogNode() {
		return logNode;
	}
}
