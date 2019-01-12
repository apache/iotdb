package org.apache.iotdb.tsfile.file.metadata.statistics;


import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Statistics for double type
 *
 * @author kangrong
 */
public class DoubleStatistics extends Statistics<Double> {
	private double max;
	private double min;
	private double first;
	private double sum;
	private double last;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToDouble(maxBytes);
		min = BytesUtils.bytesToDouble(minBytes);
	}

	@Override
	public void updateStats(double value) {
		if (this.isEmpty) {
			initializeStats(value, value, value, value,value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, value,value);
		}
	}

	private void updateStats(double minValue, double maxValue, double firstValue, double sumValue,double lastValue) {
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
	public Double getMax() {
		return max;
	}

	@Override
	public Double getMin() {
		return min;
	}

	@Override
	public Double getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}
	
	@Override
	public Double getLast(){
		return last;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		DoubleStatistics doubleStats = (DoubleStatistics) stats;
		if (this.isEmpty) {
			initializeStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum(),doubleStats.getLast());
			isEmpty = false;
		} else {
			updateStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum(),doubleStats.getLast());
		}

	}

	public void initializeStats(double min, double max, double first, double sum,double last) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.sum = sum;
		this.last = last;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.doubleToBytes(max);
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.doubleToBytes(min);
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.doubleToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}
	
	@Override
	public byte[] getLastBytes(){
		return BytesUtils.doubleToBytes(last);
	}

	@Override
	public ByteBuffer getMaxBytebuffer() {
		return ReadWriteIOUtils.getByteBuffer(max);
	}

	@Override
	public ByteBuffer getMinBytebuffer() { return ReadWriteIOUtils.getByteBuffer(min); }

	@Override
	public ByteBuffer getFirstBytebuffer() {
		return ReadWriteIOUtils.getByteBuffer(first);
	}

	@Override
	public ByteBuffer getSumBytebuffer() {
		return ReadWriteIOUtils.getByteBuffer(sum);
	}

	@Override
	public ByteBuffer getLastBytebuffer() {
		return ReadWriteIOUtils.getByteBuffer(last);
	}

	@Override
	public int sizeOfDatum() {
		return 8;
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
	}

	@Override
	void fill(InputStream inputStream) throws IOException {
		this.min = ReadWriteIOUtils.readDouble(inputStream);
		this.max = ReadWriteIOUtils.readDouble(inputStream);
		this.first = ReadWriteIOUtils.readDouble(inputStream);
		this.last = ReadWriteIOUtils.readDouble(inputStream);
		this.sum = ReadWriteIOUtils.readDouble(inputStream);
	}

	@Override
	void fill(ByteBuffer byteBuffer) throws IOException {
		this.min = ReadWriteIOUtils.readDouble(byteBuffer);
		this.max = ReadWriteIOUtils.readDouble(byteBuffer);
		this.first = ReadWriteIOUtils.readDouble(byteBuffer);
		this.last = ReadWriteIOUtils.readDouble(byteBuffer);
		this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
	}
}
