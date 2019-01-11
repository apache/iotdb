package cn.edu.tsinghua.tsfile.file.metadata.statistics;


import cn.edu.tsinghua.tsfile.utils.Binary;
import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This statistic is used as Unsupported data type. It just return a 0-byte array while asked max or
 * min.
 *
 * @author kangrong
 */
public class NoStatistics extends Statistics<Long> {
    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    }

    @Override
    public Long getMin() {
        return null;
    }

    @Override
    public Long getMax() {
        return null;
    }

    @Override
    public void updateStats(boolean value) {
    }

    @Override
    public void updateStats(int value) {
    }

    @Override
    public void updateStats(long value) {
    }

    @Override
    public void updateStats(Binary value) {
    }

    @Override
    protected void mergeStatisticsValue(Statistics<?> stats) {
    }

    @Override
    public byte[] getMaxBytes() {
        return new byte[0];
    }

    @Override
    public byte[] getMinBytes() {
        return new byte[0];
    }

    @Override
    public String toString() {
        return "no stats";
    }

	@Override
	public Long getFirst() {
		return null;
	}

	@Override
	public double getSum() {
		return 0;
	}
	
	@Override
	public Long getLast(){
		return null;
	}

	@Override
	public byte[] getFirstBytes() {
		return new byte[0];
	}

	@Override
	public byte[] getSumBytes() {
		return new byte[0];
	}
	
	@Override
	public byte[] getLastBytes(){
		return new byte[0];
	}

    @Override
    public int sizeOfDatum() {
        return 0;
    }

    @Override
    public ByteBuffer getMaxBytebuffer() {
        return ReadWriteIOUtils.getByteBuffer(0);
    }

    @Override
    public ByteBuffer getMinBytebuffer() { return ReadWriteIOUtils.getByteBuffer(0); }

    @Override
    public ByteBuffer getFirstBytebuffer() {
        return ReadWriteIOUtils.getByteBuffer(0);
    }

    @Override
    public ByteBuffer getSumBytebuffer() {
        return ReadWriteIOUtils.getByteBuffer(0);
    }

    @Override
    public ByteBuffer getLastBytebuffer() {
        return ReadWriteIOUtils.getByteBuffer(0);
    }


    @Override
    void fill(InputStream inputStream) throws IOException {
        //nothing
    }

    @Override
    void fill(ByteBuffer byteBuffer) throws IOException {
    }

    @Override
	public void updateStats(long min, long max) {
		throw new UnsupportedOperationException();
	}
}
