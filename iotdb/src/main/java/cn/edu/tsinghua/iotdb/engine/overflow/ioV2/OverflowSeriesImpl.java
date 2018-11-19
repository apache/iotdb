package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import cn.edu.tsinghua.iotdb.engine.overflow.IIntervalTreeOperator;
import cn.edu.tsinghua.iotdb.engine.overflow.IntervalTreeOperation;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.LongStatistics;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

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
	public IIntervalTreeOperator overflowIndex;
	private String measurementId;
	private TSDataType dataType;
	private Statistics<Long> statistics;
	private int valueCount;

	public OverflowSeriesImpl(String measurementId, TSDataType dataType) {
		this.measurementId = measurementId;
		this.dataType = dataType;
		statistics = new LongStatistics();
		overflowIndex = new IntervalTreeOperation(dataType);
	}

	public void insert(long time, byte[] value) {

	}

	public void update(long startTime, long endTime, byte[] value) {
		overflowIndex.update(startTime, endTime, value);
		statistics.updateStats(startTime, endTime);
		valueCount++;
	}

	public void delete(long timestamp) {
		overflowIndex.delete(timestamp);
		statistics.updateStats(timestamp, timestamp);
		valueCount++;
	}

	public DynamicOneColumnData query(DynamicOneColumnData data) {
		return overflowIndex.queryMemory(data);
	}

	public long getSize() {
		return overflowIndex.calcMemSize();
	}

	public IIntervalTreeOperator getOverflowIndex() {
		return overflowIndex;
	}

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
