package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author CGF
 */
public class BooleanStatistics extends Statistics<Boolean> {
	private boolean max;
	private boolean min;
	private boolean first;
	private double sum;
	private boolean last;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToBool(maxBytes);
		min = BytesUtils.bytesToBool(minBytes);
	}

	@Override
	public void updateStats(boolean value) {
		if (isEmpty) {
			initializeStats(value, value, value, 0, value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, 0, value);
			isEmpty = false;
		}
	}

	private void updateStats(boolean minValue, boolean maxValue, boolean firstValue, double sumValue,
			boolean lastValue) {
		if (!minValue && min) {
			min = minValue;
		}
		if (maxValue && !max) {
			max = maxValue;
		}
		this.last = lastValue;
	}

	@Override
	public Boolean getMax() {
		return max;
	}

	@Override
	public Boolean getMin() {
		return min;
	}

	@Override
	public Boolean getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}
	
	@Override
	public Boolean getLast(){
		return last;
	}

	@Override
	public ByteBuffer getMaxBytebuffer() { return ReadWriteIOUtils.getByteBuffer(max); }

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
	protected void mergeStatisticsValue(Statistics<?> stats) {
		BooleanStatistics boolStats = (BooleanStatistics) stats;
		if (isEmpty) {
			initializeStats(boolStats.getMin(), boolStats.getMax(), boolStats.getFirst(), boolStats.getSum(),
					boolStats.getLast());
			isEmpty = false;
		} else {
			updateStats(boolStats.getMin(), boolStats.getMax(), boolStats.getFirst(), boolStats.getSum(),
					boolStats.getLast());
		}
	}

	public void initializeStats(boolean min, boolean max, boolean firstValue, double sumValue, boolean lastValue) {
		this.min = min;
		this.max = max;
		this.first = firstValue;
		this.last = lastValue;
	}

	@Override
	public byte[] getMaxBytes() {
		return BytesUtils.boolToBytes(max);
	}

	@Override
	public byte[] getMinBytes() {
		return BytesUtils.boolToBytes(min);
	}

	@Override
	public byte[] getFirstBytes() {
		return BytesUtils.boolToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public byte[] getLastBytes() {
		return BytesUtils.boolToBytes(last);
	}


	@Override
	public int sizeOfDatum() {
		return 1;
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + ",last:" + last + "]";
	}

	@Override
	void fill(InputStream inputStream) throws IOException {
		this.min = ReadWriteIOUtils.readBool(inputStream);
		this.max = ReadWriteIOUtils.readBool(inputStream);
		this.first = ReadWriteIOUtils.readBool(inputStream);
		this.last = ReadWriteIOUtils.readBool(inputStream);
		this.sum = ReadWriteIOUtils.readDouble(inputStream);
	}

	@Override
	void fill(ByteBuffer byteBuffer) throws IOException {
		this.min = ReadWriteIOUtils.readBool(byteBuffer);
		this.max = ReadWriteIOUtils.readBool(byteBuffer);
		this.first = ReadWriteIOUtils.readBool(byteBuffer);
		this.last = ReadWriteIOUtils.readBool(byteBuffer);
		this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
	}


}
