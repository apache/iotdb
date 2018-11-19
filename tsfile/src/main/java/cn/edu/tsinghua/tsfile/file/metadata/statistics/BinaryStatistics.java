package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for string type
 *
 * @author CGF
 */
public class BinaryStatistics extends Statistics<Binary> {
	private Binary max = new Binary("");
	private Binary min = new Binary("");
	private Binary first = new Binary("");
	private double sum;
	private Binary last = new Binary("");

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = new Binary(maxBytes);
		min = new Binary(minBytes);
	}

	@Override
	public Binary getMin() {
		return min;
	}

	@Override
	public Binary getMax() {
		return max;
	}

	@Override
	public Binary getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}
	
	@Override
	public Binary getLast(){
		return last;
	}

	public void initializeStats(Binary min, Binary max, Binary first, double sum,Binary last) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.sum = sum;
		this.last = last;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		BinaryStatistics stringStats = (BinaryStatistics) stats;
		if (isEmpty) {
			initializeStats(stringStats.getMin(), stringStats.getMax(), stringStats.getFirst(), stringStats.getSum(),stringStats.getLast());
			isEmpty = false;
		} else {
			updateStats(stringStats.getMin(), stringStats.getMax(), stringStats.getFirst(), stringStats.getSum(),stringStats.getLast());
		}
	}

	@Override
	public void updateStats(Binary value) {
		if (isEmpty) {
			initializeStats(value, value, value, 0,value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, 0,value);
			isEmpty = false;
		}
	}

	private void updateStats(Binary minValue, Binary maxValue, Binary firstValue, double sum,Binary lastValue) {
		if (minValue.compareTo(min) < 0) {
			min = minValue;
		}
		if (maxValue.compareTo(max) > 0) {
			max = maxValue;
		}
		this.last = lastValue;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.StringToBytes(max.getStringValue());
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.StringToBytes(min.getStringValue());
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.StringToBytes(first.getStringValue());
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}
	
	@Override
	public byte[] getLastBytes(){
		return BytesUtils.StringToBytes(last.getStringValue());
	}
	
	@Override
	public String toString(){
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
	}
}
