package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for int type
 *
 * @author kangrong
 */
public class IntegerStatistics extends Statistics<Integer> {
	private int max;
	private int min;
	private int first;
	private double sum;
	private int last;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToInt(maxBytes);
		min = BytesUtils.bytesToInt(minBytes);
	}

	@Override
	public void updateStats(int value) {
		if (isEmpty) {
			initializeStats(value, value, value, value, value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, value, value);
			isEmpty = false;
		}
	}

	private void updateStats(int minValue, int maxValue, int firstValue, double sumValue, int lastValue) {
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
	public Integer getMax() {
		return max;
	}

	@Override
	public Integer getMin() {
		return min;
	}

	@Override
	public Integer getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	public Integer getLast() {
		return last;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		IntegerStatistics intStats = (IntegerStatistics) stats;
		if (isEmpty) {
			initializeStats(intStats.getMin(), intStats.getMax(), intStats.getFirst(), intStats.getSum(),
					intStats.getLast());
			isEmpty = false;
		} else {
			updateStats(intStats.getMin(), intStats.getMax(), intStats.getFirst(), intStats.getSum(),
					intStats.getLast());
		}

	}

	private void initializeStats(int min, int max, int first, double sum, int last) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.sum = sum;
		this.last = last;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.intToBytes(max);
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.intToBytes(min);
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.intToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public byte[] getLastBytes() {
		return BytesUtils.intToBytes(last);
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
	}
}
