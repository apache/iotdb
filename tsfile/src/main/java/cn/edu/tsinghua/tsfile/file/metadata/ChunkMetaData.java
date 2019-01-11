package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * MetaData of one chunk
 */
public class ChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkMetaData.class);

    private String measurementUID;

    /**
     * Byte offset of the corresponding data in the file
     * Notice:  include the chunk header and marker
     */
    private long offsetOfChunkHeader;

    private long numOfPoints;

    private long startTime;

    private long endTime;

    private TSDataType tsDataType;

    /**
     * The maximum time of the tombstones that take effect on this chunk. Only data with larger timestamps than this
     * should be exposed to user.
     */
    private long maxTombstoneTime;

    private TsDigest valuesStatistics;


    public int getSerializedSize(){
        return (Integer.BYTES + measurementUID.length()) +  // measurementUID
                4 * Long.BYTES  + //4 long: offsetOfChunkHeader, numOfPoints, startTime, endTime
                TSDataType.getSerializedSize() +  // TSDataType
                (valuesStatistics==null? TsDigest.getNullDigestSize():valuesStatistics.getSerializedSize());

    }


    private ChunkMetaData(){}

    public ChunkMetaData(String measurementUID, TSDataType tsDataType, long fileOffset, long startTime, long endTime) {
        this.measurementUID = measurementUID;
        this.tsDataType = tsDataType;
        this.offsetOfChunkHeader = fileOffset;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return String.format("numPoints %d", numOfPoints);
    }

    public long getNumOfPoints() {
        return numOfPoints;
    }

    public void setNumOfPoints(long numRows) {
        this.numOfPoints = numRows;
    }

    /**
     * @return Byte offset of header of this chunk (includes the marker)
     */
    public long getOffsetOfChunkHeader() {
        return offsetOfChunkHeader;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public TsDigest getDigest() {
        return valuesStatistics;
    }

    public void setDigest(TsDigest digest) {
        this.valuesStatistics = digest;

    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public TSDataType getTsDataType() {
        return tsDataType;
    }

    public void setTsDataType(TSDataType tsDataType) {
        this.tsDataType = tsDataType;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(measurementUID, outputStream);
        byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
        byteLen += ReadWriteIOUtils.write(numOfPoints, outputStream);
        byteLen += ReadWriteIOUtils.write(startTime, outputStream);
        byteLen += ReadWriteIOUtils.write(endTime, outputStream);
        byteLen += ReadWriteIOUtils.write(tsDataType, outputStream);

        if(valuesStatistics==null) byteLen += TsDigest.serializeNullTo(outputStream);
        else byteLen += valuesStatistics.serializeTo(outputStream);

        assert  byteLen == getSerializedSize();
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(measurementUID, buffer);
        byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, buffer);
        byteLen += ReadWriteIOUtils.write(numOfPoints, buffer);
        byteLen += ReadWriteIOUtils.write(startTime, buffer);
        byteLen += ReadWriteIOUtils.write(endTime, buffer);
        byteLen += ReadWriteIOUtils.write(tsDataType, buffer);

        if(valuesStatistics==null) byteLen += TsDigest.serializeNullTo(buffer);
        else byteLen += valuesStatistics.serializeTo(buffer);

        assert  byteLen == getSerializedSize();
        return byteLen;
    }

    public static ChunkMetaData deserializeFrom(InputStream inputStream) throws IOException {
        ChunkMetaData chunkMetaData = new ChunkMetaData();

        chunkMetaData.measurementUID = ReadWriteIOUtils.readString(inputStream);

        chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(inputStream);


        chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(inputStream);
        chunkMetaData.startTime = ReadWriteIOUtils.readLong(inputStream);
        chunkMetaData.endTime = ReadWriteIOUtils.readLong(inputStream);

        chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(inputStream);

        chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(inputStream);


        return chunkMetaData;
    }

    public static ChunkMetaData deserializeFrom(ByteBuffer buffer) {
        ChunkMetaData chunkMetaData = new ChunkMetaData();

        chunkMetaData.measurementUID = ReadWriteIOUtils.readString(buffer);
        chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
        chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(buffer);
        chunkMetaData.startTime = ReadWriteIOUtils.readLong(buffer);
        chunkMetaData.endTime = ReadWriteIOUtils.readLong(buffer);
        chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

        chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(buffer);


        return chunkMetaData;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }

}
