package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.file.metadata.statistics.NoStatistics;
import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class PageHeader {

    private int uncompressedSize;
    private int compressedSize;
    private int numOfValues;
    private Statistics<?> statistics;
    private long max_timestamp;
    private long min_timestamp;

    //this field does not need to be serialized.
    private int serializedSize;

    public static int calculatePageHeaderSize(TSDataType type) {
        return 3 * Integer.BYTES + 2 * Long.BYTES + Statistics.getStatsByType(type).getSerializedSize();
    }

    public int calculatePageHeaderSize() {
            return 3 * Integer.BYTES + 2 * Long.BYTES + statistics.getSerializedSize();
    }

    public int getSerializedSize() {
        return serializedSize;
    }


    public PageHeader(int uncompressedSize, int compressedSize, int numOfValues, Statistics<?> statistics, long max_timestamp, long min_timestamp) {
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
        this.numOfValues = numOfValues;
        if (statistics == null) statistics = new NoStatistics();
        this.statistics = statistics;
        this.max_timestamp = max_timestamp;
        this.min_timestamp = min_timestamp;
        serializedSize = calculatePageHeaderSize();
    }


    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public int getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
    }

    public int getNumOfValues() {
        return numOfValues;
    }

    public void setNumOfValues(int numOfValues) {
        this.numOfValues = numOfValues;
    }

    public Statistics<?> getStatistics() {
        return statistics;
    }

    /*public void setStatistics(Statistics<?> statistics) {
        this.statistics = statistics;
        if(statistics != null)
            serializedSize = calculatePageHeaderSize();
    }*/

    public long getMax_timestamp() {
        return max_timestamp;
    }

    public void setMax_timestamp(long max_timestamp) {
        this.max_timestamp = max_timestamp;
    }

    public long getMin_timestamp() {
        return min_timestamp;
    }

    public void setMin_timestamp(long min_timestamp) {
        this.min_timestamp = min_timestamp;
    }


    public int serializeTo(OutputStream outputStream) throws IOException {
        int length = 0;
        length += ReadWriteIOUtils.write(uncompressedSize, outputStream);
        length += ReadWriteIOUtils.write(compressedSize, outputStream);
        length += ReadWriteIOUtils.write(numOfValues, outputStream);
        length += ReadWriteIOUtils.write(max_timestamp, outputStream);
        length += ReadWriteIOUtils.write(min_timestamp, outputStream);
        length += statistics.serialize(outputStream);
        assert length == getSerializedSize();
        return length;
    }

    public static PageHeader deserializeFrom(InputStream inputStream, TSDataType dataType) throws IOException {
        int uncompressedSize = ReadWriteIOUtils.readInt(inputStream);
        int compressedSize = ReadWriteIOUtils.readInt(inputStream);
        int numOfValues = ReadWriteIOUtils.readInt(inputStream);
        long max_timestamp = ReadWriteIOUtils.readLong(inputStream);
        long min_timestamp = ReadWriteIOUtils.readLong(inputStream);
        Statistics statistics = Statistics.deserialize(inputStream, dataType);
        return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, max_timestamp, min_timestamp);
    }

    public static PageHeader deserializeFrom(ByteBuffer buffer, TSDataType dataType) throws IOException {
        int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
        int compressedSize = ReadWriteIOUtils.readInt(buffer);
        int numOfValues = ReadWriteIOUtils.readInt(buffer);
        long max_timestamp = ReadWriteIOUtils.readLong(buffer);
        long min_timestamp = ReadWriteIOUtils.readLong(buffer);
        Statistics statistics = Statistics.deserialize(buffer, dataType);
        return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, max_timestamp, min_timestamp);
    }


    @Override
    public String toString() {
        return "PageHeader{" +
                "uncompressedSize=" + uncompressedSize +
                ", compressedSize=" + compressedSize +
                ", numOfValues=" + numOfValues +
                ", statistics=" + statistics +
                ", max_timestamp=" + max_timestamp +
                ", min_timestamp=" + min_timestamp +
                ", serializedSize=" + serializedSize +
                '}';
    }
}

