package org.apache.iotdb.db.engine.overflow.ioV2;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * This class is only used to store and query overflow overflowIndex
 * {@code IIntervalTreeOperator} data in memory.
 * 
 * @author liukun
 */
public class OverflowSeriesImpl {

	/**
	 * The data of update and delete in memory for this time series.
	 */
	//public IIntervalTreeOperator overflowIndex;
	private String measurementId;
	private TSDataType dataType;
	private Statistics<Long> statistics;
	private int valueCount;

	public OverflowSeriesImpl(String measurementId, TSDataType dataType) {
		this.measurementId = measurementId;
		this.dataType = dataType;
		statistics = new LongStatistics();
		//overflowIndex = new IntervalTreeOperation(dataType);
	}

	public void insert(long time, byte[] value) {

	}

	public void update(long startTime, long endTime, byte[] value) {
		//overflowIndex.update(startTime, endTime, value);
		statistics.updateStats(startTime, endTime);
		valueCount++;
	}

	public void delete(long timestamp) {
		//overflowIndex.delete(timestamp);
		statistics.updateStats(timestamp, timestamp);
		valueCount++;
	}

	public BatchData query(BatchData data) {
		//return overflowIndex.queryMemory(data);
		return null;
	}

	public long getSize() {
		//return overflowIndex.calcMemSize();
		return 0;
	}

//	public IIntervalTreeOperator getOverflowIndex() {
//		return overflowIndex;
//	}

	public String getMeasurementId() {
		return measurementId;
	}

	public TSDataType getDataType() {
		return dataType;
	}

	public Statistics<Long> getStatistics() {
		return statistics;
	}

	public int getValueCount() {
		return valueCount;
	}
}
