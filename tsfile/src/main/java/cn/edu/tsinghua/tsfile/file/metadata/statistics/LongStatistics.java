package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for long type
 *
 * @author kangrong
 */
public class LongStatistics extends Statistics<Long> {
	private long max;
	private long min;
	private long first;
	private double sum;
	private long last;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToLong(maxBytes);
		min = BytesUtils.bytesToLong(minBytes);
	}

	@Override
	public Long getMin() {
		return min;
	}

	@Override
	public Long getMax() {
		return max;
	}

	@Override
	public Long getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	public Long getLast() {
		return last;
	}

	@Override
	public void updateStats(long value) {
		if (isEmpty) {
			initializeStats(value, value, value, value, value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, value, value);
		}
	}

	private void updateStats(long minValue, long maxValue, long firstValue, double sumValue, long lastValue) {
		if (minValue < min) {
			min = minValue;
		}
		if (maxValue > max) {
			max = maxValue;
		}
		sum += sumValue;
		this.last = lastValue;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		LongStatistics longStats = (LongStatistics) stats;
		if (isEmpty) {
			initializeStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(), longStats.getSum(),
					longStats.getLast());
			isEmpty = false;
		} else {
			updateStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(), longStats.getSum(),
					longStats.getLast());
		}

	}

	private void initializeStats(long min, long max, long firstValue, double sum, long last) {
		this.min = min;
		this.max = max;
		this.first = firstValue;
		this.sum += sum;
		this.last = last;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.longToBytes(max);
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.longToBytes(min);
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.longToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public byte[] getLastBytes() {
		return BytesUtils.longToBytes(last);
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
	}

	@Override
	public void updateStats(long minValue, long maxValue) {
		if (minValue < min) {
			min = minValue;
		}
		if (maxValue > max) {
			max = maxValue;
		}
	}

}
