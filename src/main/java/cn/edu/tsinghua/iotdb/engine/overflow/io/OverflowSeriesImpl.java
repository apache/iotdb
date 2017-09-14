package cn.edu.tsinghua.iotdb.engine.overflow.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.tsinghua.iotdb.engine.overflow.IIntervalTreeOperator;
import cn.edu.tsinghua.iotdb.engine.overflow.IntervalTreeOperation;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.LongStatistics;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * This class is used to flush overflow intervalTree data to
 * {@code OverflowFileIO}. It's also used to stored the series metadata for
 * overflow.
 * 
 * @author kangrong
 * @author liukun
 *
 */
public class OverflowSeriesImpl {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowSeriesImpl.class);

	private IIntervalTreeOperator workingOverflowIndex;
	private IIntervalTreeOperator flushingOverflowIndex;
	private IIntervalTreeOperator queryOverflowIndex;
	private String measurementId;
	private TSDataType type;
	private OverflowFileIO overflowFileIO;
	/**
	 * metaForReader contains all metadata flushed to output stream before
	 * opening this fileBlock. it is null or not empty.
	 */
	private List<TimeSeriesChunkMetaData> metaForReader;
	/**
	 * metaForWriter contains all metadata flushed to output stream after
	 * opening this fileBlock. It means metaForWriter need to be added to file's
	 * tail. it is not null.
	 */
	private List<TimeSeriesChunkMetaData> metaForWriter;
	// time statistics
	private Statistics<Long> statistics = new LongStatistics();
	private Statistics<Long> flushingStatistics = new LongStatistics();

	private boolean isFlushing = false;
	private int valueCount = 0;
	private int flushingValueCount = -1;

	private boolean isMerging;
	private OverflowSeriesImpl mergingSeriesImpl;

	private ReadWriteLock overflowConvertLock = new ReentrantReadWriteLock(false);

	public OverflowSeriesImpl(String measurementId, TSDataType type, OverflowFileIO overflowFileIO,
			Compressor compressor, List<TimeSeriesChunkMetaData> metaForReader) {
		this.measurementId = measurementId;
		this.overflowFileIO = overflowFileIO;
		this.type = type;
		this.metaForReader = metaForReader;
		this.metaForWriter = new ArrayList<TimeSeriesChunkMetaData>();
		workingOverflowIndex = new IntervalTreeOperation(type);
		flushingOverflowIndex = new IntervalTreeOperation(type);
		queryOverflowIndex = new IntervalTreeOperation(type);
	}


	/**
	 * Insert one data point into the overflow index with the special timestamp
	 * 
	 * @param timestamp
	 * @param value
	 */
	public void insert(long timestamp, byte[] value) {
		valueCount++;
		statistics.updateStats(timestamp, timestamp);
		workingOverflowIndex.insert(timestamp, value);
	}

	/**
	 * Update the value from parameter starttime to parameter endtime
	 * 
	 * @param startTime
	 * @param endTime
	 * @param value
	 */
	public void update(long startTime, long endTime, byte[] value) {
		valueCount++;
		statistics.updateStats(startTime, endTime);
		workingOverflowIndex.update(startTime, endTime, value);
	}

	/**
	 * Delete all value which timestamp is earlier than the parameter timestamp
	 * 
	 * @param timestamp
	 */
	public void delete(long timestamp) {
		valueCount++;
		statistics.updateStats(timestamp, timestamp);
		workingOverflowIndex.delete(timestamp);
	}

	/**
	 * Query the overflow result from metadataReader, metadataWriter,
	 * workingindex, flushingindex
	 * 
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @return
	 */
	public List<Object> query(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter) {
		List<Object> ret = null;
		overflowConvertLock.readLock().lock();
		try {
			// no overflow data
			if (metaForReader == null && metaForWriter.isEmpty() && workingOverflowIndex.isEmpty() && !isFlushing
					&& !isMerging)
				return ret;
			DynamicOneColumnData newerData = queryToDynamicOneColumnData(timeFilter, freqFilter, valueFilter, null);
			if (isMerging) {
				newerData = mergingSeriesImpl.queryToDynamicOneColumnData(timeFilter, freqFilter, valueFilter,
						newerData);
			}
			ret = queryOverflowIndex.getDynamicList(timeFilter, valueFilter, freqFilter, newerData);
		} finally {
			overflowConvertLock.readLock().unlock();
		}
		return ret;
	}

	private DynamicOneColumnData queryToDynamicOneColumnData(SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
			DynamicOneColumnData newerData) {
		if (workingOverflowIndex != null)
			newerData = workingOverflowIndex.queryMemory(timeFilter, valueFilter, freqFilter, newerData);
		if (isFlushing)
			newerData = flushingOverflowIndex.queryMemory(timeFilter, valueFilter, freqFilter, newerData);
		if (!metaForWriter.isEmpty())
			newerData = readFileFromFileBlockForWriter(newerData, metaForWriter, timeFilter, freqFilter, valueFilter);
		if (metaForReader != null)
			newerData = readFileFromFileBlockForReader(newerData, metaForReader, timeFilter, freqFilter, valueFilter);
		return newerData;
	}

	private DynamicOneColumnData readFileFromFileBlockForReader(DynamicOneColumnData newerData,
			List<TimeSeriesChunkMetaData> TimeSeriesChunkMetaDataList, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) {
		for (TimeSeriesChunkMetaData seriesMetaData : TimeSeriesChunkMetaDataList) {
			int chunkSize = (int) seriesMetaData.getTotalByteSize();
			long offset = seriesMetaData.getProperties().getFileOffset();
			InputStream in = overflowFileIO.getSeriesChunkBytes(chunkSize, offset);
			try {
				newerData = workingOverflowIndex.queryFileBlock(timeFilter, valueFilter, freqFilter, in, newerData);
			} catch (IOException e) {
				LOGGER.error("Read overflow file block failed, reason is {}", e.getMessage());
				// should throw the reason of the exception and handled by high
				// level function
				return null;
			}
		}
		return newerData;
	}

	private DynamicOneColumnData readFileFromFileBlockForWriter(DynamicOneColumnData newerData,
			List<TimeSeriesChunkMetaData> TimeSeriesChunkMetaDataList, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) {
		for (int i = TimeSeriesChunkMetaDataList.size() - 1; i >= 0; i--) {
			TimeSeriesChunkMetaData seriesMetaData = TimeSeriesChunkMetaDataList.get(i);
			int chunkSize = (int) seriesMetaData.getTotalByteSize();
			long offset = seriesMetaData.getProperties().getFileOffset();
			InputStream in = overflowFileIO.getSeriesChunkBytes(chunkSize, offset);
			try {
				newerData = workingOverflowIndex.queryFileBlock(timeFilter, valueFilter, freqFilter, in, newerData);
			} catch (IOException e) {
				LOGGER.error("Read overflow file block failed, reason {}", e.getMessage());
				// should throw the reason of the exception and handled by high
				// level function
				return null;
			}
		}
		return newerData;
	}

	public OFSeriesListMetadata getOFSeriesListMetadata() {
		return new OFSeriesListMetadata(measurementId, metaForWriter);
	}

	public TSDataType getTSDataType() {
		return type;
	}

	public long getMemorySize() {
		return workingOverflowIndex.calcMemSize();
	}

	public void setMetaForWriter(List<TimeSeriesChunkMetaData> metaForWriter) {
		this.metaForWriter = metaForWriter;
	}

	public boolean isEmptyForWrite() {
		return metaForWriter.isEmpty();
	}

	/**
	 * Calling this method immediately before actual flushing operation for
	 * control the total size of flushing block, as flushing operation may be
	 * time-consuming.
	 * 
	 * Switch the work index to flush and reset work index. Switch the word
	 * statistics to flush and reset work statistics. Set flushcount to
	 * valuecount and reset valuecount
	 * 
	 */
	public void switchWorkingToFlushing() {
		if (valueCount != 0) {
			isFlushing = true;
			flushingOverflowIndex = workingOverflowIndex;
			workingOverflowIndex = new IntervalTreeOperation(type);
			flushingStatistics = statistics;
			statistics = new LongStatistics();
			flushingValueCount = valueCount;
			valueCount = 0;
		}
	}

	/**
	 * Switch flushing to working when flushingValueCount is not zero
	 * 
	 * @param fileWriter
	 * @return
	 * @throws IOException
	 */
	public void flushToFileWriter(OverflowFileIO fileWriter) throws IOException {
		if (flushingValueCount > 0) {
			fileWriter.toTail();
			long seriesBeginOffset = fileWriter.getPos();
			fileWriter.startSeries(flushingValueCount, measurementId, CompressionTypeName.UNCOMPRESSED, type,
					flushingStatistics);
			fileWriter.writeOverflow(flushingOverflowIndex);
			long seriesSize = fileWriter.getPos() - seriesBeginOffset;
			TimeSeriesChunkMetaData timeSeriesChunkMetaData = fileWriter.endSeries(seriesSize);
			switchFlushingToWorking(timeSeriesChunkMetaData);
		}
	}

	private void switchFlushingToWorking(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
		overflowConvertLock.writeLock().lock();
		flushingOverflowIndex = null;
		flushingStatistics.reset();
		metaForWriter.add(timeSeriesChunkMetaData);
		isFlushing = false;
		flushingValueCount = -1;
		overflowConvertLock.writeLock().unlock();
	}

	public void setMergingSeriesImpl(OverflowSeriesImpl mergingSeriesImpl) {
		this.mergingSeriesImpl = mergingSeriesImpl;
	}

	public boolean hasMergingSeriesImpl() {
		return mergingSeriesImpl != null;
	}

	// bug: should be deprecated
	// add one input parameters: mergingSeriesImpl
	public void switchWorkingToMerging() {

		isMerging = true;
	}

	// bug: should be deprecated
	public void switchMergeToWorking() throws IOException {
		overflowConvertLock.writeLock().lock();
		if(mergingSeriesImpl!=null){
			mergingSeriesImpl.close();
		}
		mergingSeriesImpl = null;
		isMerging = false;
		overflowConvertLock.writeLock().unlock();
	}
	
	public void close() throws IOException{
		overflowFileIO.close();
	}
}
